//! GitHub and GitHub Enterprise Server: `GET /orgs/{org}/repos`, pages followed by their `Link`
//! header, topics included with each repository. HEADs are read over git (`ls-remote`), which
//! the REST and GraphQL budgets do not count: GraphQL's CPU-time limit would cap them near 600 a
//! minute whatever the concurrency.
//!
//! GitHub signals a rate limit in two ways, neither of them a plain 429: a 403 with
//! `X-RateLimit-Remaining: 0`, and a 403 or 429 whose body names a "secondary rate limit".
//! `classify` reads both, so the shared `HttpClient` waits them out like any other.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Response};
use repoplane_proto::ForgeKind;
use repoplane_proto::inventory::RepoSnapshot;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::git::{self, GitAuth};
use crate::http::{HttpClient, Transport, Verdict, classify_generic, header, retry_after};
use crate::{
    Listed, Lister, Page, Token, escape_segment, link, numeric_id, readable_branch, within,
};

const FORGE: &str = "github";
const PAGE_SIZE: u32 = 100;
/// The pause for a secondary rate limit or a bare 429, which say nothing of their own.
const SECONDARY_WAIT: Duration = Duration::from_secs(60);

pub struct Github {
    http: HttpClient,
    api: String,
    /// Where git reaches repositories: https://github.com, or the Enterprise Server's own host.
    git_base: String,
    org: String,
    git: GitAuth,
}

impl Github {
    pub fn new(
        base_url: &str,
        org: &str,
        token: Token,
        transport: Arc<dyn Transport>,
        cancel: CancellationToken,
    ) -> Result<Self, ForgeError> {
        let (api, git_base) = endpoints(base_url)?;
        let mut headers = HeaderMap::new();
        let (k, v) = header("authorization", &format!("Bearer {}", token.expose()))?;
        headers.insert(k, v);
        let (k, v) = header("accept", "application/vnd.github+json")?;
        headers.insert(k, v);
        let (k, v) = header("x-github-api-version", "2022-11-28")?;
        headers.insert(k, v);
        Ok(Github {
            http: HttpClient::new(FORGE, transport, classify, headers, token.clone(), cancel),
            api,
            git_base,
            org: org.to_string(),
            git: GitAuth {
                username: "x-access-token",
                token,
            },
        })
    }
}

/// The REST root and the git root for what the user configured. github.com serves its API from
/// a host of its own; GitHub Enterprise Server serves the API under the instance's `/api`.
fn endpoints(base_url: &str) -> Result<(String, String), ForgeError> {
    let base = base_url.trim().trim_end_matches('/');
    let u = url::Url::parse(base)
        .map_err(|e| ForgeError::Config(format!("GitHub base URL {base:?}: {e}")))?;
    let host = u.host_str().unwrap_or("").to_ascii_lowercase();
    if matches!(
        host.as_str(),
        "github.com" | "www.github.com" | "api.github.com"
    ) && u.path().trim_matches('/').is_empty()
    {
        return Ok(("https://api.github.com".into(), "https://github.com".into()));
    }
    Ok((format!("{base}/api/v3"), base.to_string()))
}

#[derive(Deserialize)]
struct Repo {
    id: i64,
    full_name: String,
    name: String,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    html_url: Option<String>,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    pushed_at: Option<String>,
}

impl Repo {
    /// GitHub has no `empty` flag in a listing, and `size` alone is no signal: it is computed
    /// lazily and reads 0 for many small repositories that have commits. A repository nobody
    /// ever pushed to has `pushed_at` equal to `created_at` (or none at all), so both together
    /// are the best guess the listing allows. It is only a guess: `heads` asks every repository
    /// whatever this says, and its answer (no `defaultBranchRef`) is the authoritative one.
    fn looks_empty(&self) -> bool {
        self.size == 0
            && match (&self.pushed_at, &self.created_at) {
                (None, _) => true,
                // Both are `YYYY-MM-DDTHH:MM:SSZ`, which order as strings.
                (Some(p), Some(c)) => p <= c,
                (Some(_), None) => false,
            }
    }
}

impl Repo {
    fn into_listed(self) -> Listed {
        let empty = self.looks_empty();
        let locator = self.full_name.clone();
        Listed {
            snapshot: RepoSnapshot {
                external_id: self.id.to_string(),
                full_name: self.full_name,
                name: self.name,
                default_branch: self.default_branch.filter(|b| !b.is_empty()),
                archived: self.archived,
                topics: Some(self.topics.unwrap_or_default()),
                locator,
                empty,
                web_url: self.html_url,
            },
        }
    }
}

#[async_trait]
impl Lister for Github {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Github
    }

    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
        let url = match cursor {
            None => format!(
                "{}/orgs/{}/repos?per_page={PAGE_SIZE}&sort=created&direction=asc&type=all",
                self.api,
                escape_segment(&self.org)
            ),
            // A cursor is a `Link` target this forge gave us; never follow one elsewhere. (GitHub
            // links to `/organizations/{id}/repos`, not the `/orgs/{org}` path first asked for.)
            Some(c) if c.starts_with(&format!("{}/", self.api)) => c.to_string(),
            Some(c) => {
                return Err(ForgeError::protocol(
                    FORGE,
                    format!("cursor {c:?} does not point at {}", self.api),
                ));
            }
        };
        let (answer, repos): (_, Option<Vec<Repo>>) = self.http.get_json(&url).await?;
        let next = link::next(answer.header("link"));
        if next.as_deref() == Some(url.as_str()) {
            return Err(ForgeError::protocol(
                FORGE,
                format!("page {url} links to itself"),
            ));
        }
        // On the first page, the `last` link says how many pages there are; their URLs are
        // fetched at once. Each must stay under the API this connection names.
        let rest = if cursor.is_none() {
            link::rest_of(answer.header("link")).filter(|urls| {
                urls.iter()
                    .all(|u| u.starts_with(&format!("{}/", self.api)))
            })
        } else {
            None
        };
        Ok(Page {
            repos: repos
                .unwrap_or_default()
                .into_iter()
                .map(Repo::into_listed)
                .collect(),
            next,
            rest,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Vec<Result<Option<String>, ForgeError>> {
        let remotes = repos.iter().map(|r| self.remote(&r.snapshot)).collect();
        git::heads(FORGE, remotes, &self.git).await
    }

    /// `GET /repositories/{id}`: by id, so a rename or a transfer is followed rather than
    /// mistaken for a deletion. A repository transferred to another owner is no longer ours.
    async fn get(&self, external_id: &str) -> Result<Option<Listed>, ForgeError> {
        let id = numeric_id(FORGE, external_id)?;
        let url = format!("{}/repositories/{id}", self.api);
        match self.http.get_json::<Repo>(&url).await {
            Ok((_, Some(r))) if within(&self.org, &r.full_name) => Ok(Some(r.into_listed())),
            Ok(_) => Ok(None),
            Err(e) if e.class() == Some(Class::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Github {
    /// `{git base}/{owner}/{repo}.git`, built from the configured base, never from a URL the
    /// forge sent: the credential only ever goes to the host the connection names.
    fn remote(&self, s: &RepoSnapshot) -> Result<Option<git::Remote>, ForgeError> {
        let Some(branch) = readable_branch(s) else {
            return Ok(None);
        };
        let (owner, name) = s
            .full_name
            .split_once('/')
            .ok_or_else(|| ForgeError::protocol(FORGE, format!("full name {:?}", s.full_name)))?;
        Ok(Some(git::Remote {
            url: git::remote_url(&self.git_base, &[owner, &format!("{name}.git")])?,
            branch: branch.to_string(),
        }))
    }
}

/// GitHub's reading of an answer. On a 403 or 429, in the order GitHub documents: `Retry-After`,
/// then an exhausted primary budget (`X-RateLimit-Remaining: 0`, wait until
/// `X-RateLimit-Reset`), then a secondary limit named in the body. Any other 403 is a missing
/// permission, not worth a retry.
pub fn classify(resp: &Response<Bytes>) -> Option<Verdict> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return None;
    }
    if status != 403 && status != 429 {
        return classify_generic(resp);
    }
    let limited = |wait| {
        Some(Verdict {
            class: Class::RateLimited,
            retry_after: wait,
        })
    };
    if let Some(wait) = retry_after(resp) {
        return limited(Some(wait));
    }
    let remaining = resp
        .headers()
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok());
    if remaining.map(str::trim) == Some("0") {
        return limited(until_reset(resp));
    }
    if String::from_utf8_lossy(resp.body())
        .to_ascii_lowercase()
        .contains("secondary rate limit")
        || status == 429
    {
        return limited(Some(SECONDARY_WAIT));
    }
    Some(Verdict {
        class: Class::Forbidden,
        retry_after: None,
    })
}

/// The time left until `X-RateLimit-Reset` (epoch seconds), plus one second.
fn until_reset(resp: &Response<Bytes>) -> Option<Duration> {
    let reset: u64 = resp
        .headers()
        .get("x-ratelimit-reset")?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(Duration::from_secs(reset.saturating_sub(now) + 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::ScriptedTransport;

    fn repo(id: i64, name: &str) -> serde_json::Value {
        serde_json::json!({
            "id": id, "full_name": format!("acme/{name}"), "name": name,
            "default_branch": "main", "archived": false, "size": 0,
            "created_at": "2026-09-27T16:36:39Z", "pushed_at": "2026-09-27T16:36:54Z",
            "topics": ["service"], "html_url": format!("https://github.com/acme/{name}")
        })
    }

    fn github(t: Arc<ScriptedTransport>) -> Github {
        Github::new(
            "https://github.com/",
            "acme",
            Token::new("t").unwrap(),
            t,
            CancellationToken::new(),
        )
        .unwrap()
    }

    #[test]
    fn endpoints_for_github_com_and_enterprise() {
        let api = |b| endpoints(b).unwrap();
        assert_eq!(
            api("https://github.com/"),
            ("https://api.github.com".into(), "https://github.com".into())
        );
        assert_eq!(
            api("https://ghe.corp.example/"),
            (
                "https://ghe.corp.example/api/v3".into(),
                "https://ghe.corp.example".into()
            )
        );
    }

    #[test]
    fn remotes_are_built_from_the_configured_base() {
        let g = github(ScriptedTransport::new(|_| {
            ScriptedTransport::reply(200, "[]")
        }));
        let s: Repo = serde_json::from_value(repo(1, "billing-api")).unwrap();
        let r = g.remote(&s.into_listed().snapshot).unwrap().unwrap();
        assert_eq!(r.url.as_str(), "https://github.com/acme/billing-api.git");
        assert_eq!(r.branch, "main");
    }

    #[tokio::test]
    async fn follows_link_across_pages() {
        let next = "https://api.github.com/organizations/42/repos?per_page=100&sort=created&direction=asc&type=all&page=2";
        let t = ScriptedTransport::new(move |req| {
            if req.uri().path().starts_with("/orgs/") {
                // A short page that is not the last: only the Link header says so.
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([repo(1, "a")]).to_string(),
                    &[(
                        "link",
                        &format!(r#"<{next}>; rel="next", <{next}>; rel="last""#),
                    )],
                )
            } else {
                ScriptedTransport::reply(200, serde_json::json!([repo(2, "b")]).to_string())
            }
        });
        let g = github(t.clone());
        let p1 = g.page(None).await.unwrap();
        let s = &p1.repos[0].snapshot;
        assert_eq!(
            (s.external_id.as_str(), s.full_name.as_str()),
            ("1", "acme/a")
        );
        assert_eq!(s.topics.as_deref(), Some(&["service".to_string()][..]));
        assert_eq!(s.web_url.as_deref(), Some("https://github.com/acme/a"));
        assert!(!s.empty);
        assert_eq!(p1.next.as_deref(), Some(next));
        let p2 = g.page(p1.next.as_deref()).await.unwrap();
        assert_eq!(p2.repos[0].snapshot.full_name, "acme/b");
        assert!(p2.next.is_none());
        assert_eq!(
            t.seen(),
            vec![
                "GET /orgs/acme/repos?per_page=100&sort=created&direction=asc&type=all",
                "GET /organizations/42/repos?per_page=100&sort=created&direction=asc&type=all&page=2"
            ]
        );
    }

    #[tokio::test]
    async fn refuses_a_foreign_cursor() {
        let g = github(ScriptedTransport::new(|_| {
            ScriptedTransport::reply(200, "[]")
        }));
        assert!(g.page(Some("https://elsewhere/x")).await.is_err());
        assert!(g.page(Some("https://api.github.com.evil/x")).await.is_err());
    }

    #[tokio::test]
    async fn a_page_linking_to_itself_is_refused() {
        let t = ScriptedTransport::new(|req| {
            let me = format!(
                "https://api.github.com{}",
                req.uri().path_and_query().unwrap()
            );
            ScriptedTransport::reply_with(200, "[]", &[("link", &format!(r#"<{me}>; rel="next""#))])
        });
        let err = github(t).page(None).await.err().unwrap();
        assert!(matches!(err, ForgeError::Protocol { .. }), "{err}");
    }

    #[test]
    fn empty_is_guessed_from_an_untouched_repository() {
        let r = |pushed: Option<&str>, size| Repo {
            id: 1,
            full_name: "acme/a".into(),
            name: "a".into(),
            default_branch: Some("main".into()),
            archived: false,
            topics: None,
            html_url: None,
            size,
            created_at: Some("2026-09-27T16:37:04Z".into()),
            pushed_at: pushed.map(String::from),
        };
        assert!(r(Some("2026-09-27T16:37:04Z"), 0).looks_empty());
        assert!(r(None, 0).looks_empty());
        assert!(!r(Some("2026-09-27T16:37:20Z"), 0).looks_empty());
        assert!(!r(Some("2026-09-27T16:37:04Z"), 12).looks_empty());
    }

    #[test]
    fn classifies_githubs_rate_limits() {
        let secondary = ScriptedTransport::reply(
            403,
            r#"{"message":"You have exceeded a secondary rate limit. Please wait a few minutes."}"#,
        );
        assert_eq!(
            classify(&secondary),
            Some(Verdict {
                class: Class::RateLimited,
                retry_after: Some(SECONDARY_WAIT)
            })
        );

        let reset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 120;
        let primary = ScriptedTransport::reply_with(
            403,
            r#"{"message":"API rate limit exceeded"}"#,
            &[
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-reset", &reset.to_string()),
            ],
        );
        let v = classify(&primary).unwrap();
        assert_eq!(v.class, Class::RateLimited);
        let wait = v.retry_after.unwrap().as_secs();
        assert!((119..=122).contains(&wait), "{wait}");

        let told = ScriptedTransport::reply_with(429, "", &[("retry-after", "7")]);
        assert_eq!(
            classify(&told).unwrap().retry_after,
            Some(Duration::from_secs(8))
        );
        let bare = ScriptedTransport::reply(429, "");
        assert_eq!(classify(&bare).unwrap().retry_after, Some(SECONDARY_WAIT));

        let plain = ScriptedTransport::reply_with(
            403,
            r#"{"message":"Resource not accessible by personal access token"}"#,
            &[("x-ratelimit-remaining", "4999")],
        );
        assert_eq!(
            classify(&plain),
            Some(Verdict {
                class: Class::Forbidden,
                retry_after: None
            })
        );
        assert_eq!(
            classify(&ScriptedTransport::reply(404, "")).unwrap().class,
            Class::NotFound
        );
        assert_eq!(classify(&ScriptedTransport::reply(200, "[]")), None);
    }

    #[tokio::test]
    async fn a_plain_403_is_not_retried() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply(403, r#"{"message":"Must have admin rights"}"#)
        });
        let err = github(t.clone()).page(None).await.err().unwrap();
        assert_eq!(err.class(), Some(Class::Forbidden));
        assert!(!err.retryable());
        assert_eq!(t.seen().len(), 1);
    }

    #[tokio::test]
    async fn get_follows_the_id_and_stays_in_the_org() {
        let t = ScriptedTransport::new(|req| match req.uri().path() {
            "/repositories/7" => ScriptedTransport::reply(200, repo(7, "renamed").to_string()),
            "/repositories/8" => {
                let mut r = repo(8, "x");
                r["full_name"] = "someone-else/x".into();
                ScriptedTransport::reply(200, r.to_string())
            }
            _ => ScriptedTransport::reply(404, r#"{"message":"Not Found"}"#),
        });
        let g = github(t);
        let found = g.get("7").await.unwrap().unwrap();
        assert_eq!(found.snapshot.full_name, "acme/renamed");
        assert_eq!(
            found.snapshot.topics.as_deref(),
            Some(&["service".to_string()][..])
        );
        assert!(
            g.get("8").await.unwrap().is_none(),
            "transferred out of the org"
        );
        assert!(g.get("9").await.unwrap().is_none(), "deleted");
        assert!(g.get("x").await.is_err());
    }

    /// The first page's `last` link names page 3: pages 2 and 3 can be fetched at once, but
    /// never from a host other than the API's.
    #[tokio::test]
    async fn the_first_page_announces_the_rest_within_the_api() {
        let page = |link: &'static str| {
            ScriptedTransport::new(move |_| {
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([repo(1, "a")]).to_string(),
                    &[("link", link)],
                )
            })
        };
        let g = github(page(
            r#"<https://api.github.com/organizations/42/repos?per_page=100&page=2>; rel="next", <https://api.github.com/organizations/42/repos?per_page=100&page=3>; rel="last""#,
        ));
        assert_eq!(
            g.page(None).await.unwrap().rest,
            Some(vec![
                "https://api.github.com/organizations/42/repos?per_page=100&page=2".into(),
                "https://api.github.com/organizations/42/repos?per_page=100&page=3".into(),
            ])
        );
        let g = github(page(
            r#"<https://api.github.com/x?page=2>; rel="next", <https://evil.example/x?page=3>; rel="last""#,
        ));
        assert_eq!(g.page(None).await.unwrap().rest, None);
    }
}
