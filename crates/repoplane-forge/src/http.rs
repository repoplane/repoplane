//! One HTTP layer under every forge client. A `Transport` moves bytes; the `HttpClient` decides
//! what an answer means — read through the forge's own `Classifier` — waits out a rate limit
//! while the budget lasts, retries a transient failure, and scrubs the token from anything it
//! reports. No adapter has a retry loop of its own.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response};
use serde::de::DeserializeOwned;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::token::Token;

#[async_trait]
pub trait Transport: Send + Sync {
    async fn send(&self, req: Request<Bytes>, timeout: Duration)
    -> Result<Response<Bytes>, String>;
}

/// The real network. Honours the system proxy settings, which bank networks require.
pub struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub fn new() -> Result<Arc<Self>, ForgeError> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("repoplane-worker/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| ForgeError::Config(format!("build HTTP client: {e}")))?;
        Ok(Arc::new(ReqwestTransport { client }))
    }
}

#[async_trait]
impl Transport for ReqwestTransport {
    async fn send(
        &self,
        req: Request<Bytes>,
        timeout: Duration,
    ) -> Result<Response<Bytes>, String> {
        let (parts, body) = req.into_parts();
        let mut b = self
            .client
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .timeout(timeout);
        if !body.is_empty() {
            b = b.body(body);
        }
        let resp = b.send().await.map_err(describe)?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp.bytes().await.map_err(describe)?;
        let mut out = Response::new(bytes);
        *out.status_mut() = status;
        *out.headers_mut() = headers;
        Ok(out)
    }
}

/// The error's cause chain, without the URL reqwest would otherwise print.
fn describe(e: reqwest::Error) -> String {
    let mut msg = if e.is_timeout() {
        "timeout".to_string()
    } else if e.is_connect() {
        "connect".to_string()
    } else {
        "request failed".to_string()
    };
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        src = s.source();
    }
    msg
}

/// A scripted answer to one request.
pub type Handler = dyn Fn(&Request<Bytes>) -> Response<Bytes> + Send + Sync;

/// Answers from a closure and records what it was asked: `METHOD /path?query`.
pub struct ScriptedTransport {
    handler: Box<Handler>,
    seen: Mutex<Vec<String>>,
}

impl ScriptedTransport {
    pub fn new(
        handler: impl Fn(&Request<Bytes>) -> Response<Bytes> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(ScriptedTransport {
            handler: Box::new(handler),
            seen: Mutex::new(Vec::new()),
        })
    }

    pub fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }

    pub fn reply(status: u16, body: impl Into<String>) -> Response<Bytes> {
        let mut r = Response::new(Bytes::from(body.into()));
        *r.status_mut() = http::StatusCode::from_u16(status).unwrap();
        r
    }

    pub fn reply_with(
        status: u16,
        body: impl Into<String>,
        headers: &[(&str, &str)],
    ) -> Response<Bytes> {
        let mut r = Self::reply(status, body);
        for (k, v) in headers {
            r.headers_mut().insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        r
    }
}

#[async_trait]
impl Transport for ScriptedTransport {
    async fn send(
        &self,
        req: Request<Bytes>,
        _timeout: Duration,
    ) -> Result<Response<Bytes>, String> {
        let pq = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        self.seen
            .lock()
            .unwrap()
            .push(format!("{} {pq}", req.method()));
        Ok((self.handler)(&req))
    }
}

/// A non-2xx answer read the forge's way, with the pause it asked for if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    pub class: Class,
    pub retry_after: Option<Duration>,
}

/// Reads an answer. Returning `Class::Auth` for a 2xx vetoes it (Azure DevOps answers a bad
/// token with a 203 sign-in page).
pub type Classifier = fn(&Response<Bytes>) -> Option<Verdict>;

/// `Retry-After` in seconds, plus one so a retry never lands a moment early.
pub fn retry_after(resp: &Response<Bytes>) -> Option<Duration> {
    let s: u64 = resp
        .headers()
        .get("retry-after")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(s + 1))
}

/// The reading every forge shares: the status, and `Retry-After` on a 429 or 503.
pub fn classify_generic(resp: &Response<Bytes>) -> Option<Verdict> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return None;
    }
    let mut class = Class::of_status(status);
    let wait = retry_after(resp);
    if status == 503 && wait.is_some() {
        class = Class::RateLimited;
    }
    Some(Verdict {
        class,
        retry_after: if class == Class::RateLimited {
            wait
        } else {
            None
        },
    })
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Wall-clock budget for one logical request, pauses included. A rate limit that asks for
    /// longer is reported rather than slept through.
    pub budget: Duration,
    pub base: Duration,
    pub cap: Duration,
    /// The pause when a rate limit does not say how long.
    pub default_rate_limit_wait: Duration,
    pub timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            budget: Duration::from_secs(10 * 60),
            base: Duration::from_millis(500),
            cap: Duration::from_secs(30),
            default_rate_limit_wait: Duration::from_secs(60),
            timeout: Duration::from_secs(30),
        }
    }
}

pub struct HttpClient {
    forge: &'static str,
    transport: Arc<dyn Transport>,
    classify: Classifier,
    headers: HeaderMap,
    token: Token,
    policy: RetryPolicy,
    cancel: CancellationToken,
}

/// A successful answer.
pub struct Answer {
    pub status: u16,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Answer {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

impl HttpClient {
    /// `headers` carries the authentication; `token` is kept only to scrub it from errors.
    pub fn new(
        forge: &'static str,
        transport: Arc<dyn Transport>,
        classify: Classifier,
        headers: HeaderMap,
        token: Token,
        cancel: CancellationToken,
    ) -> Self {
        HttpClient {
            forge,
            transport,
            classify,
            headers,
            token,
            policy: RetryPolicy::default(),
            cancel,
        }
    }

    pub fn with_policy(mut self, policy: RetryPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn forge(&self) -> &'static str {
        self.forge
    }

    pub async fn get(&self, url: &str) -> Result<Answer, ForgeError> {
        self.send(Method::GET, url, None).await
    }

    /// A POST that reads rather than writes (GraphQL queries): safe to repeat.
    pub async fn post_query(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<Answer, ForgeError> {
        let raw = Bytes::from(serde_json::to_vec(body).expect("a JSON value encodes"));
        self.send(Method::POST, url, Some(raw)).await
    }

    /// Decodes an answer; an empty or `null` body is `None`.
    pub fn decode<T: DeserializeOwned>(
        &self,
        url: &str,
        a: &Answer,
    ) -> Result<Option<T>, ForgeError> {
        if a.body.is_empty() || a.body.trim_ascii() == b"null" {
            return Ok(None);
        }
        serde_json::from_slice(&a.body)
            .map(Some)
            .map_err(|e| ForgeError::Decode {
                forge: self.forge,
                method: "GET".into(),
                path: path_of(url),
                message: e.to_string(),
            })
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
    ) -> Result<(Answer, Option<T>), ForgeError> {
        let a = self.get(url).await?;
        let v = self.decode(url, &a)?;
        Ok((a, v))
    }

    async fn send(
        &self,
        method: Method,
        url: &str,
        body: Option<Bytes>,
    ) -> Result<Answer, ForgeError> {
        let path = path_of(url);
        let deadline = Instant::now() + self.policy.budget;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let mut req = Request::builder().method(method.clone()).uri(url);
            for (k, v) in &self.headers {
                req = req.header(k, v);
            }
            if body.is_some() {
                req = req.header(http::header::CONTENT_TYPE, "application/json");
            }
            let req = req
                .body(body.clone().unwrap_or_default())
                .map_err(|e| ForgeError::Config(format!("build request for {path}: {e}")))?;
            let sent = tokio::select! {
                r = self.transport.send(req, self.policy.timeout) => r,
                _ = self.cancel.cancelled() => return Err(ForgeError::Cancelled),
            };
            let resp = match sent {
                Ok(r) => r,
                Err(message) => {
                    let pause = self.backoff(attempt);
                    if Instant::now() + pause < deadline {
                        tracing::debug!(forge = self.forge, %method, path, attempt, "transport failure, retrying: {message}");
                        self.sleep(pause).await?;
                        continue;
                    }
                    return Err(ForgeError::Transport {
                        forge: self.forge,
                        method: method.to_string(),
                        path,
                        message: self.token.scrub(&message),
                    });
                }
            };
            let status = resp.status().as_u16();
            let Some(Verdict { class, retry_after }) = (self.classify)(&resp) else {
                let (parts, body) = resp.into_parts();
                return Ok(Answer {
                    status,
                    headers: parts.headers,
                    body,
                });
            };
            match class {
                Class::RateLimited => {
                    let wait = retry_after.unwrap_or(self.policy.default_rate_limit_wait);
                    if Instant::now() + wait > deadline {
                        return Err(ForgeError::RateLimitExceeded {
                            forge: self.forge,
                            method: method.to_string(),
                            path,
                            wait,
                        });
                    }
                    tracing::info!(forge = self.forge, %method, path, wait_secs = wait.as_secs(), "rate limited, waiting");
                    self.sleep(wait).await?;
                    continue;
                }
                Class::Transient => {
                    let pause = self.backoff(attempt);
                    if Instant::now() + pause < deadline {
                        tracing::debug!(forge = self.forge, %method, path, status, attempt, "transient failure, retrying");
                        self.sleep(pause).await?;
                        continue;
                    }
                }
                _ => {}
            }
            let text = String::from_utf8_lossy(resp.body());
            let text = self.token.scrub(text.trim());
            let text = if text.len() > 1024 {
                format!("{}…", &text[..text.floor_char_boundary(1024)])
            } else {
                text
            };
            return Err(ForgeError::Status {
                forge: self.forge,
                class,
                status,
                method: method.to_string(),
                path,
                body: text,
            });
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let exp = self
            .policy
            .base
            .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1)));
        exp.min(self.policy.cap)
            .mul_f64(rand::random::<f64>().max(0.1))
    }

    async fn sleep(&self, d: Duration) -> Result<(), ForgeError> {
        tokio::select! {
            _ = tokio::time::sleep(d) => Ok(()),
            _ = self.cancel.cancelled() => Err(ForgeError::Cancelled),
        }
    }
}

/// A URL's path, for messages: no host, no query, so nothing a query might carry leaks.
pub fn path_of(url: &str) -> String {
    url::Url::parse(url)
        .map(|u| u.path().to_string())
        .unwrap_or_else(|_| url.split('?').next().unwrap_or(url).to_string())
}

/// One header, for building a client's defaults.
pub fn header(name: &'static str, value: &str) -> Result<(HeaderName, HeaderValue), ForgeError> {
    let mut v = HeaderValue::from_str(value)
        .map_err(|_| ForgeError::Config(format!("{name}: the value is not a valid header")))?;
    if name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("private-token") {
        v.set_sensitive(true);
    }
    Ok((HeaderName::from_static(name), v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn client(t: Arc<dyn Transport>) -> HttpClient {
        HttpClient::new(
            "test",
            t,
            classify_generic,
            HeaderMap::new(),
            Token::new("tok-123").unwrap(),
            CancellationToken::new(),
        )
        .with_policy(RetryPolicy {
            budget: Duration::from_secs(5),
            base: Duration::from_millis(1),
            cap: Duration::from_millis(2),
            default_rate_limit_wait: Duration::from_millis(1),
            timeout: Duration::from_secs(1),
        })
    }

    #[tokio::test]
    async fn retries_transient_then_succeeds() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let t = ScriptedTransport::new(move |_| {
            if n2.fetch_add(1, Ordering::SeqCst) < 2 {
                ScriptedTransport::reply(502, "bad gateway")
            } else {
                ScriptedTransport::reply(200, "[1]")
            }
        });
        let c = client(t);
        let (_, v): (_, Option<Vec<i32>>) = c.get_json("http://x/a").await.unwrap();
        assert_eq!(v, Some(vec![1]));
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn rate_limit_beyond_budget_is_reported() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(429, "", &[("retry-after", "3600")])
        });
        let err = client(t).get("http://x/a").await.err().unwrap();
        assert!(matches!(err, ForgeError::RateLimitExceeded { .. }), "{err}");
        assert!(err.retryable());
    }

    #[tokio::test]
    async fn errors_never_carry_the_token() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply(403, "bad credential [sha: tok-123]")
        });
        let err = client(t).get("http://x/a?secret=1").await.err().unwrap();
        let s = err.to_string();
        assert!(!s.contains("tok-123"), "{s}");
        assert!(!s.contains("secret=1"), "{s}");
        assert_eq!(err.class(), Some(Class::Forbidden));
        assert!(!err.retryable());
    }
}
