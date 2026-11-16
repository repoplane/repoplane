//! Azure DevOps: its repository endpoints take no `$top` and answer no continuation token, so
//! the adapter pages the listing itself. The first page asks the whole organisation in one call
//! (`GET {org}/_apis/git/repositories`, disabled repositories included), cuts each project's
//! repositories, by id, into slices of [`PAGE`], answers the first slice, and every other one as
//! a page to fetch at once. Such a page asks only its project
//! (`GET {org}/{project}/_apis/git/repositories`) and answers its slice. As a single page the
//! organisation was a single workflow payload: 888 KB for 2,144 repositories, past Temporal's
//! 2 MB limit near 4,800; a slice stays near 400 KB however large a project grows. HEADs are
//! read over git (`ls-remote`), outside the load Azure DevOps meters per user; a disabled
//! repository refuses every read, so it is never asked.
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
use http::{HeaderMap, Response};
use repoplane_proto::ForgeKind;
use repoplane_proto::inventory::RepoSnapshot;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::git::{self, GitAuth};
use crate::http::{HttpClient, Transport, Verdict, classify_generic, header};
use crate::{Listed, Lister, Page, Token, escape_segment, readable_branch};

const FORGE: &str = "azuredevops";
const API_VERSION: &str = "api-version=7.1";
const HEADS: &str = "refs/heads/";

pub struct AzureDevops {
    http: HttpClient,
    /// `{base}/{org}`: every API path hangs off the organisation.
    api: String,
    base: String,
    org: String,
    git: GitAuth,
    /// [`PAGE`]; smaller in tests.
    page_size: usize,
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
            http: HttpClient::new(FORGE, transport, classify, headers, token.clone(), cancel),
            api: format!("{base}/{}", escape_segment(org)),
            base: base.to_string(),
            org: org.to_string(),
            // Azure DevOps takes the PAT as the password, under any username.
            git: GitAuth {
                username: "repoplane",
                token,
            },
            page_size: PAGE,
        })
    }
}

/// Cuts an organisation's repositories into slices of `size` per project, in order of project
/// then id: the first slice, and a `{project}:{offset}` cursor for every other.
fn slices(repos: Vec<Repo>, size: usize) -> (Vec<Repo>, Vec<String>) {
    let mut by_project: std::collections::BTreeMap<(String, String), Vec<Repo>> =
        std::collections::BTreeMap::new();
    for r in repos {
        let key = (r.project.name.to_lowercase(), r.project.id.clone());
        by_project.entry(key).or_default().push(r);
    }
    let (mut first, mut rest) = (None, Vec::new());
    for ((_, project), mut repos) in by_project {
        repos.sort_by(|a, b| a.id.cmp(&b.id));
        let n = repos.len();
        let mut from = 0;
        if first.is_none() {
            repos.truncate(size);
            first = Some(repos);
            from = size;
        }
        rest.extend((from..n).step_by(size).map(|o| format!("{project}:{o}")));
    }
    (first.unwrap_or_default(), rest)
}

/// The repository endpoints are documented as unpaged; were one ever to page, say so rather
/// than report part of the organisation as the whole of it.
fn no_continuation(answer: &crate::http::Answer, org: &str) -> Result<(), ForgeError> {
    match answer.header("x-ms-continuationtoken") {
        Some(_) => Err(ForgeError::protocol(
            FORGE,
            format!("{org}: a repository listing answered a continuation token"),
        )),
        None => Ok(()),
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

/// Repositories per page: about 400 KB of workflow payload, under Temporal's 512 KB warning.
pub const PAGE: usize = 1_000;

#[async_trait]
impl Lister for AzureDevops {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Azuredevops
    }

    /// With no cursor, the first slice of the first project, and every other slice of every
    /// project as a cursor in `rest`, fetched at once; with a cursor (`{project}:{offset}`),
    /// that slice. A slice is [`PAGE`] repositories of one project, in order of id.
    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
        let Some(cursor) = cursor else {
            let (first, rest) = slices(self.all_repos().await?, self.page_size);
            return Ok(Page {
                repos: first.into_iter().map(|r| self.listed(r)).collect(),
                next: None,
                rest: Some(rest),
            });
        };
        // Only this adapter hands out cursors: a project id, and an offset into its listing.
        let (project, offset) = cursor
            .split_once(':')
            .filter(|(p, _)| !p.is_empty() && p.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'))
            .and_then(|(p, o)| Some((p, o.parse::<usize>().ok()?)))
            .ok_or_else(|| {
                ForgeError::protocol(FORGE, format!("cursor {cursor:?} is not a slice"))
            })?;
        let mut repos = self.project_repos(project).await?;
        repos.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Page {
            repos: repos
                .into_iter()
                .skip(offset)
                .take(self.page_size)
                .map(|r| self.listed(r))
                .collect(),
            next: None,
            rest: None,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Vec<Result<Option<String>, ForgeError>> {
        let remotes = repos.iter().map(|r| self.remote(&r.snapshot)).collect();
        git::heads(FORGE, remotes, &self.git).await
    }

    /// `GET {org}/_apis/git/repositories/{id}`: a repository id is unique across the
    /// organisation's projects. A disabled repository answers that with a 404 as if it were
    /// gone, so a 404 is checked against the organisation's listing, which does hold disabled
    /// repositories, before it is believed.
    async fn get(&self, external_id: &str) -> Result<Option<Listed>, ForgeError> {
        if external_id.is_empty()
            || !external_id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() || b == b'-')
        {
            return Err(ForgeError::protocol(
                FORGE,
                format!("{external_id:?} is not a repository id"),
            ));
        }
        let url = format!(
            "{}/_apis/git/repositories/{external_id}?{API_VERSION}",
            self.api
        );
        match self.http.get_json::<Repo>(&url).await {
            Ok((_, Some(r))) => Ok(Some(self.listed(r))),
            Ok((_, None)) => Ok(None),
            Err(e) if e.class() == Some(Class::NotFound) => Ok(self
                .all_repos()
                .await?
                .into_iter()
                .find(|r| r.id.eq_ignore_ascii_case(external_id))
                .map(|r| self.listed(r))),
            Err(e) => Err(e),
        }
    }
}

impl AzureDevops {
    /// A project's repositories, disabled ones included, in one call. A project deleted since
    /// the first page has none; repositories deleted since shift its slices and one can be
    /// skipped. Either way they are left unseen, and looked up by id before any is marked
    /// inaccessible (ADR 0005).
    async fn project_repos(&self, project: &str) -> Result<Vec<Repo>, ForgeError> {
        let url = format!(
            "{}/{project}/_apis/git/repositories?{API_VERSION}",
            self.api
        );
        match self.http.get_json::<Repos>(&url).await {
            Ok((answer, repos)) => {
                no_continuation(&answer, &self.org)?;
                Ok(repos.map(|r| r.value).unwrap_or_default())
            }
            Err(e) if e.class() == Some(Class::NotFound) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// Every repository of the organisation in one call, to cut into slices or to find one
    /// repository: never answered whole as a page, since a page goes into workflow history and
    /// the whole organisation can outgrow a payload.
    async fn all_repos(&self) -> Result<Vec<Repo>, ForgeError> {
        let url = format!("{}/_apis/git/repositories?{API_VERSION}", self.api);
        let (answer, repos): (_, Option<Repos>) = self.http.get_json(&url).await?;
        no_continuation(&answer, &self.org)?;
        Ok(repos.map(|r| r.value).unwrap_or_default())
    }

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
                // Ids rather than names: they need no escaping and survive a rename.
                locator: format!("{}/{}", r.project.id, r.id),
                web_url: r.web_url,
            },
        }
    }

    /// `{base}/{org}/{project}/_git/{repo}`, from the configured base and the repository's
    /// full name, never from a URL the forge sent. A disabled repository refuses every read, so
    /// it is not asked.
    fn remote(&self, s: &RepoSnapshot) -> Result<Option<git::Remote>, ForgeError> {
        let Some(branch) = readable_branch(s).filter(|_| !s.archived) else {
            return Ok(None);
        };
        let (project, name) = s
            .full_name
            .strip_prefix(&format!("{}/", self.org))
            .and_then(|rest| rest.split_once('/'))
            .ok_or_else(|| ForgeError::protocol(FORGE, format!("full name {:?}", s.full_name)))?;
        Ok(Some(git::Remote {
            url: git::remote_url(&self.base, &[&self.org, project, "_git", name])?,
            branch: branch.to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::ScriptedTransport;

    fn repo(
        id: &str,
        project: &str,
        name: &str,
        branch: Option<&str>,
        disabled: bool,
    ) -> serde_json::Value {
        let mut r = serde_json::json!({
            "id": id, "name": name, "size": 0, "isDisabled": disabled,
            "project": {"id": pid(project), "name": project, "state": "wellFormed"},
            "webUrl": format!("https://dev.azure.com/org/{project}/_git/{name}"),
        });
        if let Some(b) = branch {
            r["defaultBranch"] = serde_json::json!(b);
        }
        r
    }

    /// A project's id, hex like the GUIDs Azure DevOps gives.
    fn pid(project: &str) -> &'static str {
        match project {
            "fleet" => "f1ee7",
            "platform" => "b1a7",
            _ => "2e7a",
        }
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
                locator: "p1/r1".into(),
                empty: branch.is_none(),
                web_url: None,
            },
        }
    }

    /// The organisation in one call, cut per project into slices by id: the first answered,
    /// every other a cursor fetched at once, which asks only its project. A page never holds
    /// more than a slice, however large a project grows.
    #[tokio::test]
    async fn lists_the_organisation_in_slices_per_project() {
        let fleet = || {
            serde_json::json!({"count": 3, "value": [
                repo("a5", "fleet", "web", Some("refs/heads/main"), false),
                repo("a1", "fleet", "billing-api", Some("refs/heads/main"), false),
                repo("a3", "fleet", "no-commits", None, false),
            ]})
        };
        let t = ScriptedTransport::new(move |req| {
            let auth = req
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(auth, format!("Basic {}", BASE64.encode(":pat")));
            match req.uri().path() {
                "/org/_apis/git/repositories" => {
                    let mut all = fleet();
                    let v = all["value"].as_array_mut().unwrap();
                    v.push(repo(
                        "a2",
                        "platform",
                        "core-api",
                        Some("refs/heads/release/v2"),
                        true,
                    ));
                    v.push(repo("a4", "zeta", "tools", Some("refs/heads/main"), false));
                    ScriptedTransport::reply(200, all.to_string())
                }
                "/org/f1ee7/_apis/git/repositories" => {
                    ScriptedTransport::reply(200, fleet().to_string())
                }
                other => panic!("unexpected request for {other}"),
            }
        });
        let mut a = ado(t.clone());
        a.page_size = 2;
        let first = a.page(None).await.unwrap();
        assert!(first.next.is_none());
        assert_eq!(
            first.rest,
            Some(vec![
                "f1ee7:2".to_string(),
                "b1a7:0".to_string(),
                "2e7a:0".to_string()
            ])
        );
        let s: Vec<_> = first.repos.iter().map(|r| &r.snapshot).collect();
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].full_name, "org/fleet/billing-api");
        assert_eq!(s[0].external_id, "a1");
        assert_eq!(s[0].default_branch.as_deref(), Some("main"));
        assert!(!s[0].empty, "size 0 does not make a repository empty");
        assert!(s[1].empty && s[1].default_branch.is_none());
        assert!(s.iter().all(|s| s.topics.is_none()));
        assert_eq!(s[0].locator, "f1ee7/a1");

        let rest = a.page(Some("f1ee7:2")).await.unwrap();
        assert!(rest.next.is_none() && rest.rest.is_none());
        let names: Vec<_> = rest
            .repos
            .iter()
            .map(|r| r.snapshot.name.as_str())
            .collect();
        assert_eq!(names, ["web"]);
        assert_eq!(
            t.seen(),
            vec![
                "GET /org/_apis/git/repositories?api-version=7.1",
                "GET /org/f1ee7/_apis/git/repositories?api-version=7.1",
            ]
        );
    }

    /// A project deleted since the first page has no repositories: they go unseen, and are
    /// looked up by id before anything is marked. An organisation with none is one empty page.
    #[tokio::test]
    async fn a_project_gone_or_no_repository_at_all_is_an_empty_page() {
        let t = ScriptedTransport::new(|req| match req.uri().path() {
            "/org/_apis/git/repositories" => ScriptedTransport::reply(200, r#"{"value": []}"#),
            _ => ScriptedTransport::reply(404, r#"{"message":"TF200016: project not found"}"#),
        });
        let a = ado(t);
        let none = a.page(None).await.unwrap();
        assert!(none.repos.is_empty() && none.next.is_none());
        assert_eq!(none.rest, Some(vec![]));
        assert!(a.page(Some("dead:0")).await.unwrap().repos.is_empty());
    }

    #[tokio::test]
    async fn refuses_a_cursor_it_did_not_hand_out_and_a_paged_repository_list() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                200,
                r#"{"value":[]}"#,
                &[("x-ms-continuationtoken", "abc")],
            )
        });
        let a = ado(t.clone());
        for bad in ["../_apis/x:0", "f1ee7", "f1ee7:-1", ":0"] {
            assert!(a.page(Some(bad)).await.is_err(), "{bad}");
        }
        assert!(t.seen().is_empty());
        assert!(matches!(
            a.page(None).await,
            Err(ForgeError::Protocol { .. })
        ));
        assert!(matches!(
            a.page(Some("b1a7:0")).await,
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

    #[test]
    fn remotes_skip_disabled_and_empty_repositories() {
        let a = ado(ScriptedTransport::new(|_| panic!("no request expected")));
        let mut s = listed(Some("main"), false).snapshot;
        s.full_name = "org/my project/a b".into();
        let r = a.remote(&s).unwrap().unwrap();
        assert_eq!(
            r.url.as_str(),
            "https://dev.azure.com/org/my%20project/_git/a%20b"
        );
        assert_eq!(r.branch, "main");
        assert!(
            a.remote(&listed(Some("main"), true).snapshot)
                .unwrap()
                .is_none(),
            "disabled"
        );
        assert!(
            a.remote(&listed(None, false).snapshot).unwrap().is_none(),
            "empty"
        );
    }

    #[tokio::test]
    async fn get_looks_a_repository_up_by_its_guid() {
        let t = ScriptedTransport::new(|req| {
            if req.uri().path() == "/org/_apis/git/repositories/0a1b-2c" {
                ScriptedTransport::reply(
                    200,
                    repo("0a1b-2c", "fleet", "api", Some("refs/heads/main"), false).to_string(),
                )
            } else if req.uri().path() == "/org/_apis/git/repositories" {
                ScriptedTransport::reply(200, r#"{"value": [], "count": 0}"#)
            } else {
                ScriptedTransport::reply(404, r#"{"message":"TF401019"}"#)
            }
        });
        let a = ado(t);
        let found = a.get("0a1b-2c").await.unwrap().unwrap();
        assert_eq!(found.snapshot.full_name, "org/fleet/api");
        assert_eq!(found.snapshot.default_branch.as_deref(), Some("main"));
        assert!(a.get("ffff").await.unwrap().is_none());
        assert!(a.get("../etc").await.is_err());
    }

    /// A disabled repository answers its own lookup with a 404; the listing still holds it.
    #[tokio::test]
    async fn get_finds_a_disabled_repository_through_the_listing() {
        let t = ScriptedTransport::new(|req| {
            if req.uri().path() == "/org/_apis/git/repositories" {
                let v = serde_json::json!({"value": [repo("d1", "fleet", "archived", Some("refs/heads/main"), true)], "count": 1});
                ScriptedTransport::reply(200, v.to_string())
            } else {
                ScriptedTransport::reply(404, r#"{"message":"TF401019"}"#)
            }
        });
        let a = ado(t);
        let found = a.get("d1").await.unwrap().unwrap();
        assert!(found.snapshot.archived);
        assert!(a.get("e2").await.unwrap().is_none());
    }
}
