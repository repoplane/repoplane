//! Azure DevOps: `GET {org}/_apis/git/repositories`, one call for every repository of every
//! project in the organisation, disabled ones included. That endpoint takes no `$top` and
//! answers no continuation token, so a listing is a single page. A HEAD costs one Refs request
//! per repository; a disabled repository answers 404 to it, so it is never asked.
//!
//! A bad token is not a 401 here: the forge redirects to a sign-in page, which a client that
//! follows redirects receives as a 203 of HTML. `X-TFS-FedAuthRedirect: Suppress` asks for a
//! plain 401 instead, and the classifier still treats the sign-in page as an auth failure.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use http::{HeaderMap, Response};
use repoplane_proto::ForgeKind;
use repoplane_proto::worker::RepoSnapshot;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::http::{HttpClient, Transport, Verdict, classify_generic, header};
use crate::{
    HEAD_CONCURRENCY, Listed, Lister, Page, Token, commit_id, escape_path, escape_segment,
};

const FORGE: &str = "azuredevops";
const API_VERSION: &str = "api-version=7.1";
const HEADS: &str = "refs/heads/";

pub struct AzureDevops {
    http: HttpClient,
    /// `{base}/{org}`: every API path hangs off the organisation.
    api: String,
    org: String,
}

impl AzureDevops {
    pub fn new(
        base_url: &str,
        org: &str,
        token: Token,
        transport: Arc<dyn Transport>,
        cancel: CancellationToken,
    ) -> Result<Self, ForgeError> {
        let base = match base_url.trim_end_matches('/') {
            "" => "https://dev.azure.com",
            b => b,
        };
        let mut headers = HeaderMap::new();
        let basic = BASE64.encode(format!(":{}", token.expose()));
        let (k, v) = header("authorization", &format!("Basic {basic}"))?;
        headers.insert(k, v);
        let (k, v) = header("accept", "application/json")?;
        headers.insert(k, v);
        // Without it a bad token is redirected to a sign-in page rather than refused.
        let (k, v) = header("x-tfs-fedauthredirect", "Suppress")?;
        headers.insert(k, v);
        Ok(AzureDevops {
            http: HttpClient::new(FORGE, transport, classify, headers, token, cancel),
            api: format!("{base}/{}", escape_segment(org)),
            org: org.to_string(),
        })
    }
}

/// The generic reading, plus the sign-in page in its three guises: a 203, a 2xx of HTML where
/// JSON was asked for, and the redirect to `_signin` itself when it is not followed. A rate
/// limit without `Retry-After` falls back to `X-RateLimit-Reset`.
fn classify(resp: &Response<Bytes>) -> Option<Verdict> {
    let status = resp.status().as_u16();
    let text = |name: &str| resp.headers().get(name).and_then(|v| v.to_str().ok());
    let signin = status == 203
        || ((200..300).contains(&status)
            && text("content-type").is_some_and(|c| c.contains("text/html")))
        || ((300..400).contains(&status)
            && text("location").is_some_and(|l| l.contains("_signin")));
    if signin {
        return Some(Verdict {
            class: Class::Auth,
            retry_after: None,
        });
    }
    let mut verdict = classify_generic(resp)?;
    if verdict.class == Class::RateLimited && verdict.retry_after.is_none() {
        verdict.retry_after = text("x-ratelimit-reset")
            .and_then(|s| s.trim().parse::<u64>().ok())
            .and_then(|reset| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                Some(Duration::from_secs(reset.saturating_sub(now) + 1))
            });
    }
    Some(verdict)
}

#[derive(Deserialize)]
struct Repos {
    value: Vec<Repo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Repo {
    id: String,
    name: String,
    project: Project,
    /// Absent on a repository with no commits.
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    is_disabled: bool,
    #[serde(default)]
    web_url: Option<String>,
}

#[derive(Deserialize)]
struct Project {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct Refs {
    value: Vec<Ref>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ref {
    name: String,
    object_id: String,
}

#[async_trait]
impl Lister for AzureDevops {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Azuredevops
    }

    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
        // The listing is one page, so there is no cursor this adapter could have handed out.
        if let Some(c) = cursor {
            return Err(ForgeError::protocol(
                FORGE,
                format!("cursor {c:?}: an Azure DevOps listing has a single page"),
            ));
        }
        let url = format!("{}/_apis/git/repositories?{API_VERSION}", self.api);
        let (answer, repos): (_, Option<Repos>) = self.http.get_json(&url).await?;
        // The endpoint is documented as unpaged; were it ever to page, say so rather than
        // report a partial organisation as the whole of it.
        if answer.header("x-ms-continuationtoken").is_some() {
            return Err(ForgeError::protocol(
                FORGE,
                format!("{} answered a continuation token", self.org),
            ));
        }
        let repos = repos.map(|r| r.value).unwrap_or_default();
        Ok(Page {
            repos: repos.into_iter().map(|r| self.listed(r)).collect(),
            next: None,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
        let lookups: Vec<_> = repos.iter().map(|r| self.head(r)).collect();
        stream::iter(lookups)
            .buffered(HEAD_CONCURRENCY)
            .try_collect()
            .await
    }
}

impl AzureDevops {
    fn listed(&self, r: Repo) -> Listed {
        let default_branch = r
            .default_branch
            .map(|b| b.strip_prefix(HEADS).map(str::to_string).unwrap_or(b))
            .filter(|b| !b.is_empty());
        Listed {
            snapshot: RepoSnapshot {
                external_id: r.id.clone(),
                full_name: format!("{}/{}/{}", self.org, r.project.name, r.name),
                name: r.name,
                // `size` is no emptiness signal: it reads 0 for most repositories with commits.
                // A repository with no commits is the one with no default branch.
                empty: default_branch.is_none(),
                default_branch,
                archived: r.is_disabled,
                // Azure DevOps has no topics at all, which is not the same as none set.
                topics: None,
                head: None,
                web_url: r.web_url,
            },
            // Ids rather than names: they need no escaping and survive a rename.
            locator: format!("{}/{}", r.project.id, r.id),
        }
    }

    async fn head(&self, r: &Listed) -> Result<Option<String>, ForgeError> {
        let s = &r.snapshot;
        // A disabled repository refuses every read, so it is not asked.
        let Some(branch) = s
            .default_branch
            .as_deref()
            .filter(|_| !s.empty && !s.archived)
        else {
            return Ok(None);
        };
        let (project, repo) = r
            .locator
            .split_once('/')
            .ok_or_else(|| ForgeError::protocol(FORGE, format!("locator {:?}", r.locator)))?;
        let url = format!(
            "{}/{}/_apis/git/repositories/{}/refs?filter=heads/{}&{API_VERSION}",
            self.api,
            escape_segment(project),
            escape_segment(repo),
            escape_path(branch)
        );
        match self.http.get_json::<Refs>(&url).await {
            Ok((_, refs)) => {
                // `filter` is a prefix: `heads/main` also matches `heads/maintenance`.
                let want = format!("{HEADS}{branch}");
                let found = refs
                    .into_iter()
                    .flat_map(|r| r.value)
                    .find(|r| r.name == want);
                found.map(|r| commit_id(FORGE, &r.object_id)).transpose()
            }
            // Deleted or disabled between the listing and now: no HEAD to record.
            Err(e) if e.class() == Some(Class::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::ScriptedTransport;

    const SHA: &str = "b8a1a303d868e0bc2ed18bc8a39aaeb6fe8c5147";
    const OTHER: &str = "841526d4bd7346b1eab08f010dd62ff154234504";

    fn repo(
        id: &str,
        project: &str,
        name: &str,
        branch: Option<&str>,
        disabled: bool,
    ) -> serde_json::Value {
        let mut r = serde_json::json!({
            "id": id, "name": name, "size": 0, "isDisabled": disabled,
            "project": {"id": format!("p-{project}"), "name": project, "state": "wellFormed"},
            "webUrl": format!("https://dev.azure.com/org/{project}/_git/{name}"),
        });
        if let Some(b) = branch {
            r["defaultBranch"] = serde_json::json!(b);
        }
        r
    }

    fn ado(t: Arc<ScriptedTransport>) -> AzureDevops {
        AzureDevops::new(
            "https://dev.azure.com/",
            "org",
            Token::new("pat").unwrap(),
            t,
            CancellationToken::new(),
        )
        .unwrap()
    }

    fn listed(branch: Option<&str>, archived: bool) -> Listed {
        Listed {
            snapshot: RepoSnapshot {
                external_id: "r1".into(),
                full_name: "org/fleet/a".into(),
                name: "a".into(),
                default_branch: branch.map(str::to_string),
                archived,
                topics: None,
                head: None,
                empty: branch.is_none(),
                web_url: None,
            },
            locator: "p1/r1".into(),
        }
    }

    #[tokio::test]
    async fn lists_every_project_in_one_page() {
        let t = ScriptedTransport::new(|req| {
            let auth = req
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(auth, format!("Basic {}", BASE64.encode(":pat")));
            ScriptedTransport::reply(
                200,
                serde_json::json!({"count": 3, "value": [
                    repo("r1", "fleet", "billing-api", Some("refs/heads/main"), false),
                    repo("r2", "platform", "core-api", Some("refs/heads/release/v2"), true),
                    repo("r3", "fleet", "no-commits", None, false),
                ]})
                .to_string(),
            )
        });
        let a = ado(t.clone());
        let p = a.page(None).await.unwrap();
        assert!(p.next.is_none());
        let s: Vec<_> = p.repos.iter().map(|r| &r.snapshot).collect();
        assert_eq!(s[0].full_name, "org/fleet/billing-api");
        assert_eq!(s[0].external_id, "r1");
        assert_eq!(s[0].default_branch.as_deref(), Some("main"));
        assert!(!s[0].empty, "size 0 does not make a repository empty");
        assert_eq!(s[1].default_branch.as_deref(), Some("release/v2"));
        assert!(s[1].archived);
        assert!(s[2].empty && s[2].default_branch.is_none());
        assert!(s.iter().all(|s| s.topics.is_none()));
        assert_eq!(p.repos[0].locator, "p-fleet/r1");
        assert_eq!(
            t.seen(),
            vec!["GET /org/_apis/git/repositories?api-version=7.1"]
        );
    }

    #[tokio::test]
    async fn refuses_any_cursor_and_a_continuation() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                200,
                r#"{"value":[]}"#,
                &[("x-ms-continuationtoken", "abc")],
            )
        });
        let a = ado(t.clone());
        assert!(a.page(Some("anything")).await.is_err());
        assert!(t.seen().is_empty());
        assert!(matches!(
            a.page(None).await,
            Err(ForgeError::Protocol { .. })
        ));
    }

    #[tokio::test]
    async fn a_sign_in_page_is_an_auth_failure_not_retried() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                203,
                "<html><head><title>Azure DevOps Services | Sign In</title></head></html>",
                &[("content-type", "text/html; charset=utf-8")],
            )
        });
        let err = ado(t.clone()).page(None).await.err().unwrap();
        assert_eq!(err.class(), Some(Class::Auth), "{err}");
        assert!(!err.retryable());
        assert!(!err.to_string().contains("pat"), "{err}");
        assert_eq!(t.seen().len(), 1);
    }

    #[test]
    fn html_on_200_and_a_sign_in_redirect_are_auth() {
        let html = ScriptedTransport::reply_with(200, "<html/>", &[("content-type", "text/html")]);
        assert_eq!(classify(&html).map(|v| v.class), Some(Class::Auth));
        let moved =
            ScriptedTransport::reply_with(302, "", &[("location", "https://x/_signin?realm=y")]);
        assert_eq!(classify(&moved).map(|v| v.class), Some(Class::Auth));
        let json =
            ScriptedTransport::reply_with(200, "{}", &[("content-type", "application/json")]);
        assert_eq!(classify(&json), None);
    }

    #[test]
    fn a_rate_limit_waits_for_retry_after_or_the_reset() {
        let r = ScriptedTransport::reply_with(429, "", &[("retry-after", "7")]);
        assert_eq!(
            classify(&r),
            Some(Verdict {
                class: Class::RateLimited,
                retry_after: Some(Duration::from_secs(8))
            })
        );
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let r = ScriptedTransport::reply_with(
            429,
            "",
            &[("x-ratelimit-reset", &(now + 30).to_string())],
        );
        let wait = classify(&r).unwrap().retry_after.unwrap();
        assert!(
            wait >= Duration::from_secs(29) && wait <= Duration::from_secs(31),
            "{wait:?}"
        );
    }

    #[tokio::test]
    async fn head_picks_the_exact_ref_not_a_prefix_match() {
        let t = ScriptedTransport::new(|req| {
            assert_eq!(
                req.uri().path_and_query().unwrap().as_str(),
                "/org/p1/_apis/git/repositories/r1/refs?filter=heads/main&api-version=7.1"
            );
            ScriptedTransport::reply(
                200,
                serde_json::json!({"count": 2, "value": [
                    {"name": "refs/heads/maintenance", "objectId": OTHER},
                    {"name": "refs/heads/main", "objectId": SHA.to_uppercase()},
                ]})
                .to_string(),
            )
        });
        let heads = ado(t).heads(&[listed(Some("main"), false)]).await.unwrap();
        assert_eq!(heads, vec![Some(SHA.to_string())]);
    }

    #[tokio::test]
    async fn only_a_prefix_match_means_no_head() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply(
                200,
                serde_json::json!({"value": [{"name": "refs/heads/maintenance", "objectId": OTHER}]}).to_string(),
            )
        });
        assert_eq!(
            ado(t).heads(&[listed(Some("main"), false)]).await.unwrap(),
            vec![None]
        );
    }

    #[tokio::test]
    async fn disabled_and_empty_repositories_are_not_asked() {
        let t = ScriptedTransport::new(|_| panic!("no request expected"));
        let heads = ado(t.clone())
            .heads(&[listed(Some("main"), true), listed(None, false)])
            .await
            .unwrap();
        assert_eq!(heads, vec![None, None]);
        assert!(t.seen().is_empty());
    }

    #[tokio::test]
    async fn a_repository_gone_since_the_listing_has_no_head() {
        let t =
            ScriptedTransport::new(|_| ScriptedTransport::reply(404, r#"{"message":"TF401019"}"#));
        assert_eq!(
            ado(t).heads(&[listed(Some("main"), false)]).await.unwrap(),
            vec![None]
        );
    }
}
