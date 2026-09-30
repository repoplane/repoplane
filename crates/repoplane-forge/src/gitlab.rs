//! GitLab: `GET /api/v4/groups/{group}/projects?include_subgroups=true`, every project of a group
//! and of its subgroups at any depth, in one listing. `org` is the group's full path and may be
//! nested itself (`acme/platform`).
//!
//! Offset pagination, followed through the `Link` header. GitLab offers keyset pagination on
//! `/projects` but silently ignores `pagination=keyset` on the group endpoint (checked on
//! gitlab.com: the answer carries `page=2` and `X-Total` rather than `id_after`), so asking for it
//! would only pretend. Ordering by id keeps pages stable while projects are created; totals are
//! never read, since gitlab.com drops `X-Total` beyond 10,000 projects.
//!
//! A HEAD costs one request per project: there is no batch REST endpoint, and the GraphQL
//! `tree { lastCommit }` batch took 19s for 100 projects on gitlab.com against about 6s for the
//! same HEADs over REST, uncomfortably close to GraphQL's 30s timeout.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use http::HeaderMap;
use repoplane_proto::ForgeKind;
use repoplane_proto::worker::RepoSnapshot;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::http::{HttpClient, Transport, classify_generic, header};
use crate::{
    HEAD_CONCURRENCY, Listed, Lister, Page, Token, commit_id, escape_segment, link, numeric_id,
    within,
};

const FORGE: &str = "gitlab";
const PAGE_SIZE: u32 = 100;

pub struct Gitlab {
    http: HttpClient,
    api: String,
    org: String,
}

impl Gitlab {
    pub fn new(
        base_url: &str,
        org: &str,
        token: Token,
        transport: Arc<dyn Transport>,
        cancel: CancellationToken,
    ) -> Result<Self, ForgeError> {
        let base = match base_url.trim_end_matches('/') {
            "" => "https://gitlab.com",
            b => b,
        };
        let mut headers = HeaderMap::new();
        let (k, v) = header("private-token", token.expose())?;
        headers.insert(k, v);
        let (k, v) = header("accept", "application/json")?;
        headers.insert(k, v);
        Ok(Gitlab {
            // GitLab's 429 carries `Retry-After` in seconds, which the generic reading honours.
            http: HttpClient::new(FORGE, transport, classify_generic, headers, token, cancel),
            api: format!("{base}/api/v4"),
            org: org.trim_matches('/').to_string(),
        })
    }

    fn first_page(&self) -> String {
        // No `archived` filter: archived projects are part of the fleet and come back flagged.
        format!(
            "{}/groups/{}/projects?include_subgroups=true&with_shared=false&order_by=id&sort=asc&per_page={PAGE_SIZE}&page=1",
            self.api,
            escape_segment(&self.org)
        )
    }
}

#[derive(Deserialize)]
struct Project {
    id: i64,
    path_with_namespace: String,
    /// The URL slug; `name` is the display name.
    path: String,
    #[serde(default)]
    default_branch: Option<String>,
    #[serde(default)]
    archived: bool,
    #[serde(default)]
    empty_repo: bool,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    web_url: Option<String>,
    #[serde(default)]
    marked_for_deletion_on: Option<String>,
    #[serde(default)]
    marked_for_deletion_at: Option<String>,
}

impl Project {
    fn into_listed(self) -> Listed {
        Listed {
            snapshot: RepoSnapshot {
                external_id: self.id.to_string(),
                full_name: self.path_with_namespace,
                name: self.path,
                default_branch: self.default_branch.filter(|b| !b.is_empty()),
                archived: self.archived,
                topics: Some(self.topics.unwrap_or_default()),
                head: None,
                empty: self.empty_repo,
                web_url: self.web_url,
            },
            // The numeric id survives renames and transfers, and needs no escaping.
            locator: self.id.to_string(),
        }
    }

    /// Scheduled for deletion: already renamed `…-deletion_scheduled-…` and no longer part of
    /// the fleet, even though GitLab keeps listing it until the delay runs out.
    fn pending_deletion(&self) -> bool {
        self.marked_for_deletion_on.is_some() || self.marked_for_deletion_at.is_some()
    }
}

#[derive(Deserialize)]
struct Branch {
    commit: BranchCommit,
}

#[derive(Deserialize)]
struct BranchCommit {
    id: String,
}

/// The `page` a listing URL asks for; 1 when it does not say.
fn page_number(url: &str) -> Option<u64> {
    let u = url::Url::parse(url).ok()?;
    match u.query_pairs().find(|(k, _)| k == "page") {
        Some((_, v)) => v.parse().ok(),
        None => Some(1),
    }
}

/// `url` with its `page` set to `page`, for when GitLab says `X-Next-Page` but sends no `Link`.
fn with_page(url: &str, page: &str) -> Option<String> {
    let mut u = url::Url::parse(url).ok()?;
    let pairs: Vec<(String, String)> = u
        .query_pairs()
        .filter(|(k, _)| k != "page")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    u.query_pairs_mut()
        .clear()
        .extend_pairs(pairs)
        .append_pair("page", page);
    Some(u.to_string())
}

#[async_trait]
impl Lister for Gitlab {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Gitlab
    }

    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
        let url = match cursor {
            None => self.first_page(),
            // A cursor is a `Link` target this forge gave us; never follow one elsewhere.
            Some(c) if c.starts_with(&format!("{}/", self.api)) => c.to_string(),
            Some(c) => {
                return Err(ForgeError::protocol(
                    FORGE,
                    format!("cursor {c:?} does not point at {}", self.api),
                ));
            }
        };
        let (answer, projects): (_, Option<Vec<Project>>) = self.http.get_json(&url).await?;
        let projects = projects.unwrap_or_default();
        let next = link::next(answer.header("link")).or_else(|| {
            answer
                .header("x-next-page")
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .and_then(|p| with_page(&url, p))
        });
        // An empty page ends the listing whatever the headers say. The check is on what GitLab
        // sent, before pending deletions are dropped, so a page of those does not end it early.
        let next = if projects.is_empty() { None } else { next };
        if let Some(n) = &next {
            match (page_number(&url), page_number(n)) {
                (Some(here), Some(there)) if there > here => {}
                _ => {
                    return Err(ForgeError::protocol(
                        FORGE,
                        format!("page {url} does not advance: next is {n}"),
                    ));
                }
            }
            if !n.starts_with(&format!("{}/", self.api)) {
                return Err(ForgeError::protocol(
                    FORGE,
                    format!("page {url} links outside the API: {n}"),
                ));
            }
        }
        Ok(Page {
            repos: projects
                .into_iter()
                .filter(|p| !p.pending_deletion())
                .map(Project::into_listed)
                .collect(),
            next,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
        let lookups: Vec<_> = repos.iter().map(|r| self.head(r)).collect();
        stream::iter(lookups)
            .buffered(HEAD_CONCURRENCY)
            .try_collect()
            .await
    }

    /// `GET /projects/{id}`: by id, so a rename or a move between subgroups is followed. A
    /// project moved out of the group, or scheduled for deletion, is no longer in the fleet.
    async fn get(&self, external_id: &str) -> Result<Option<Listed>, ForgeError> {
        let id = numeric_id(FORGE, external_id)?;
        let url = format!("{}/projects/{id}", self.api);
        match self.http.get_json::<Project>(&url).await {
            Ok((_, Some(p)))
                if within(&self.org, &p.path_with_namespace) && !p.pending_deletion() =>
            {
                Ok(Some(p.into_listed()))
            }
            Ok(_) => Ok(None),
            Err(e) if e.class() == Some(Class::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Gitlab {
    async fn head(&self, r: &Listed) -> Result<Option<String>, ForgeError> {
        let s = &r.snapshot;
        let Some(branch) = s.default_branch.as_deref().filter(|_| !s.empty) else {
            return Ok(None);
        };
        // GitLab wants the whole branch name as one segment: `release%2Fv2`. Kept as
        // `release/v2`, the route itself does not match, and the 404 would pass for "no HEAD".
        let url = format!(
            "{}/projects/{}/repository/branches/{}",
            self.api,
            escape_segment(&r.locator),
            escape_segment(branch)
        );
        match self.http.get_json::<Branch>(&url).await {
            Ok((_, Some(b))) => commit_id(FORGE, &b.commit.id).map(Some),
            Ok((_, None)) => Ok(None),
            // Listed as non-empty, yet the default branch (or the project) is gone.
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

    fn project(id: i64, path: &str) -> serde_json::Value {
        let name = path.rsplit('/').next().unwrap();
        serde_json::json!({
            "id": id, "path_with_namespace": path, "path": name, "name": name.to_uppercase(),
            "default_branch": "main", "archived": false, "empty_repo": false,
            "topics": ["service"], "web_url": format!("http://g/{path}"),
            "marked_for_deletion_on": null, "marked_for_deletion_at": null
        })
    }

    fn gitlab(t: Arc<ScriptedTransport>, org: &str) -> Gitlab {
        Gitlab::new(
            "http://g/",
            org,
            Token::new("t").unwrap(),
            t,
            CancellationToken::new(),
        )
        .unwrap()
    }

    fn listed(id: &str, branch: Option<&str>, empty: bool) -> Listed {
        Listed {
            snapshot: RepoSnapshot {
                external_id: id.into(),
                full_name: format!("acme/p{id}"),
                name: format!("p{id}"),
                default_branch: branch.map(String::from),
                archived: false,
                topics: Some(vec![]),
                head: None,
                empty,
                web_url: None,
            },
            locator: id.into(),
        }
    }

    #[tokio::test]
    async fn lists_a_nested_group_with_its_subgroups_across_pages() {
        let t = ScriptedTransport::new(|req| {
            assert!(req.headers().get("private-token").unwrap().is_sensitive());
            let q = req.uri().query().unwrap_or("");
            if !q.contains("&page=2") {
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([
                        project(1, "acme/platform/api"),
                        project(2, "acme/platform/core/deep/db")
                    ])
                    .to_string(),
                    &[
                        ("x-next-page", "2"),
                        (
                            "link",
                            r#"<http://g/api/v4/groups/acme%2Fplatform/projects?include_subgroups=true&page=2&per_page=100>; rel="next", <http://g/api/v4/groups/acme%2Fplatform/projects?include_subgroups=true&page=1&per_page=100>; rel="first""#,
                        ),
                    ],
                )
            } else {
                let mut archived = project(3, "acme/platform/old");
                archived["archived"] = true.into();
                archived["default_branch"] = serde_json::Value::Null;
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([archived]).to_string(),
                    &[("x-next-page", "")],
                )
            }
        });
        let g = gitlab(t.clone(), "acme/platform");
        let p1 = g.page(None).await.unwrap();
        let names: Vec<_> = p1
            .repos
            .iter()
            .map(|r| r.snapshot.full_name.as_str())
            .collect();
        assert_eq!(names, ["acme/platform/api", "acme/platform/core/deep/db"]);
        let db = &p1.repos[1];
        assert_eq!(
            (db.snapshot.name.as_str(), db.snapshot.external_id.as_str()),
            ("db", "2")
        );
        assert_eq!(db.locator, "2");
        let p2 = g.page(p1.next.as_deref()).await.unwrap();
        assert!(p2.next.is_none());
        let old = &p2.repos[0].snapshot;
        assert!(old.archived);
        assert_eq!(old.default_branch, None);
        assert_eq!(
            t.seen(),
            vec![
                "GET /api/v4/groups/acme%2Fplatform/projects?include_subgroups=true&with_shared=false&order_by=id&sort=asc&per_page=100&page=1",
                "GET /api/v4/groups/acme%2Fplatform/projects?include_subgroups=true&page=2&per_page=100",
            ]
        );
    }

    #[tokio::test]
    async fn follows_x_next_page_without_a_link() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                200,
                serde_json::json!([project(1, "acme/a")]).to_string(),
                &[("x-next-page", "2")],
            )
        });
        let p = gitlab(t, "acme").page(None).await.unwrap();
        let next = p.next.unwrap();
        assert!(
            next.starts_with("http://g/api/v4/groups/acme/projects?"),
            "{next}"
        );
        assert!(next.ends_with("&page=2"), "{next}");
    }

    #[tokio::test]
    async fn refuses_a_foreign_cursor() {
        let g = gitlab(
            ScriptedTransport::new(|_| ScriptedTransport::reply(200, "[]")),
            "acme",
        );
        assert!(
            g.page(Some("http://elsewhere/api/v4/groups/acme/projects?page=2"))
                .await
                .is_err()
        );
        assert!(g.page(Some("http://g/api/v4evil/x")).await.is_err());
    }

    #[tokio::test]
    async fn a_page_that_does_not_advance_is_a_protocol_error() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                200,
                serde_json::json!([project(1, "acme/a")]).to_string(),
                &[(
                    "link",
                    r#"<http://g/api/v4/groups/acme/projects?page=1>; rel="next""#,
                )],
            )
        });
        let err = gitlab(t, "acme").page(None).await.err().unwrap();
        assert!(matches!(err, ForgeError::Protocol { .. }), "{err}");
    }

    #[tokio::test]
    async fn projects_pending_deletion_are_not_listed() {
        let t = ScriptedTransport::new(|_| {
            let mut gone = project(2, "acme/b-deletion_scheduled-2");
            gone["marked_for_deletion_on"] = "2026-10-07".into();
            let mut going = project(3, "acme/c-deletion_scheduled-3");
            going["marked_for_deletion_at"] = "2026-10-07".into();
            ScriptedTransport::reply(
                200,
                serde_json::json!([project(1, "acme/a"), gone, going]).to_string(),
            )
        });
        let p = gitlab(t, "acme").page(None).await.unwrap();
        let names: Vec<_> = p
            .repos
            .iter()
            .map(|r| r.snapshot.full_name.as_str())
            .collect();
        assert_eq!(names, ["acme/a"]);
        assert!(p.next.is_none());
    }

    #[tokio::test]
    async fn heads_skip_empty_projects_and_escape_the_branch_whole() {
        let t = ScriptedTransport::new(|req| {
            if req.uri().path().ends_with("/branches/gone") {
                return ScriptedTransport::reply(404, r#"{"message":"404 Branch Not Found"}"#);
            }
            ScriptedTransport::reply(
                200,
                serde_json::json!({"commit": {"id": SHA.to_uppercase()}}).to_string(),
            )
        });
        let g = gitlab(t.clone(), "acme");
        let repos = [
            listed("1", Some("release/v2"), false),
            listed("2", Some("main"), true),
            listed("3", None, false),
            listed("4", Some("gone"), false),
        ];
        let heads = g.heads(&repos).await.unwrap();
        assert_eq!(heads, vec![Some(SHA.to_string()), None, None, None]);
        assert_eq!(
            t.seen(),
            vec![
                "GET /api/v4/projects/1/repository/branches/release%2Fv2",
                "GET /api/v4/projects/4/repository/branches/gone",
            ]
        );
    }

    #[tokio::test]
    async fn get_follows_the_id_and_stays_in_the_group() {
        let t = ScriptedTransport::new(|req| match req.uri().path() {
            "/api/v4/projects/7" => {
                ScriptedTransport::reply(200, project(7, "acme/platform/deep/moved").to_string())
            }
            "/api/v4/projects/8" => {
                ScriptedTransport::reply(200, project(8, "acme/other/x").to_string())
            }
            "/api/v4/projects/9" => {
                let mut p = project(9, "acme/platform/doomed");
                p["marked_for_deletion_on"] = "2026-10-07".into();
                ScriptedTransport::reply(200, p.to_string())
            }
            _ => ScriptedTransport::reply(404, r#"{"message":"404 Project Not Found"}"#),
        });
        let g = gitlab(t, "acme/platform");
        let found = g.get("7").await.unwrap().unwrap();
        assert_eq!(found.snapshot.full_name, "acme/platform/deep/moved");
        assert!(
            g.get("8").await.unwrap().is_none(),
            "moved out of the group"
        );
        assert!(g.get("9").await.unwrap().is_none(), "pending deletion");
        assert!(g.get("10").await.unwrap().is_none(), "deleted");
    }
}
