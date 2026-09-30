//! Forgejo (and Gitea): `GET /api/v1/orgs/{org}/repos`, pages followed by their `Link` header.
//! The server clamps `limit` to its own maximum (50 by default) whatever is asked, so the page
//! size is never assumed; topics come with each repository; a HEAD costs one request per
//! repository, since there is no batch endpoint.

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
    HEAD_CONCURRENCY, Listed, Lister, Page, Token, commit_id, escape_path, escape_segment, link,
};

const FORGE: &str = "forgejo";
const PAGE_SIZE: u32 = 50;

pub struct Forgejo {
    http: HttpClient,
    api: String,
    org: String,
}

impl Forgejo {
    pub fn new(
        base_url: &str,
        org: &str,
        token: Token,
        transport: Arc<dyn Transport>,
        cancel: CancellationToken,
    ) -> Result<Self, ForgeError> {
        let mut headers = HeaderMap::new();
        let (k, v) = header("authorization", &format!("token {}", token.expose()))?;
        headers.insert(k, v);
        let (k, v) = header("accept", "application/json")?;
        headers.insert(k, v);
        Ok(Forgejo {
            http: HttpClient::new(FORGE, transport, classify_generic, headers, token, cancel),
            api: format!("{}/api/v1", base_url.trim_end_matches('/')),
            org: org.to_string(),
        })
    }
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
    empty: bool,
    #[serde(default)]
    topics: Option<Vec<String>>,
    #[serde(default)]
    html_url: Option<String>,
}

#[derive(Deserialize)]
struct Branch {
    commit: BranchCommit,
}

#[derive(Deserialize)]
struct BranchCommit {
    id: String,
}

/// The `page` query parameter of a URL.
fn page_of(url: &str) -> Option<u32> {
    url::Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == "page")
        .and_then(|(_, v)| v.parse().ok())
}

#[async_trait]
impl Lister for Forgejo {
    fn kind(&self) -> ForgeKind {
        ForgeKind::Forgejo
    }

    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
        // The cursor is a page number. Forgejo writes its `Link` URLs from its own ROOT_URL,
        // which need not be the address this worker reaches it at (a container, a proxy), so
        // only the page number is taken from the header and the URL is always rebuilt here.
        let page: u32 = match cursor {
            None => 1,
            Some(c) => c.parse().ok().filter(|p| *p > 1).ok_or_else(|| {
                ForgeError::protocol(FORGE, format!("cursor {c:?} is not a page number"))
            })?,
        };
        let url = format!(
            "{}/orgs/{}/repos?limit={PAGE_SIZE}&page={page}",
            self.api,
            escape_segment(&self.org)
        );
        let (answer, repos): (_, Option<Vec<Repo>>) = self.http.get_json(&url).await?;
        let repos = repos.unwrap_or_default();
        let next = match link::next(answer.header("link")) {
            None => None,
            Some(target) => {
                let n = page_of(&target).ok_or_else(|| {
                    ForgeError::protocol(
                        FORGE,
                        format!("next page link {target:?} has no page number"),
                    )
                })?;
                if n <= page {
                    return Err(ForgeError::protocol(
                        FORGE,
                        format!("page {page} links back to page {n}"),
                    ));
                }
                Some(n.to_string())
            }
        };
        // An empty page ends the listing whatever the header says.
        let next = if repos.is_empty() { None } else { next };
        Ok(Page {
            repos: repos
                .into_iter()
                .map(|r| {
                    let owner_repo = r.full_name.clone();
                    Listed {
                        snapshot: RepoSnapshot {
                            external_id: r.id.to_string(),
                            full_name: r.full_name,
                            name: r.name,
                            default_branch: r.default_branch.filter(|b| !b.is_empty()),
                            archived: r.archived,
                            topics: Some(r.topics.unwrap_or_default()),
                            head: None,
                            empty: r.empty,
                            web_url: r.html_url,
                        },
                        locator: owner_repo,
                    }
                })
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
}

impl Forgejo {
    async fn head(&self, r: &Listed) -> Result<Option<String>, ForgeError> {
        let s = &r.snapshot;
        let Some(branch) = s.default_branch.as_deref().filter(|_| !s.empty) else {
            return Ok(None);
        };
        let (owner, name) = r
            .locator
            .split_once('/')
            .ok_or_else(|| ForgeError::protocol(FORGE, format!("full name {:?}", r.locator)))?;
        let url = format!(
            "{}/repos/{}/{}/branches/{}",
            self.api,
            escape_segment(owner),
            escape_segment(name),
            escape_path(branch)
        );
        match self.http.get_json::<Branch>(&url).await {
            Ok((_, Some(b))) => commit_id(FORGE, &b.commit.id).map(Some),
            Ok((_, None)) => Ok(None),
            // Listed as non-empty, yet the default branch is gone: no HEAD to record.
            Err(e) if e.class() == Some(Class::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::ScriptedTransport;

    fn repo(id: i64, name: &str, empty: bool) -> serde_json::Value {
        serde_json::json!({
            "id": id, "full_name": format!("acme/{name}"), "name": name,
            "default_branch": "main", "archived": false, "empty": empty,
            "topics": ["service"], "html_url": format!("http://f/acme/{name}")
        })
    }

    fn forgejo(t: Arc<ScriptedTransport>) -> Forgejo {
        Forgejo::new(
            "http://f/",
            "acme",
            Token::new("t").unwrap(),
            t,
            CancellationToken::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn follows_link_and_stops_on_the_last_page() {
        let t = ScriptedTransport::new(|req| {
            let q = req.uri().query().unwrap_or("");
            if q.contains("page=1") {
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([repo(1, "a", false), repo(2, "b", true)]).to_string(),
                    &[(
                        "link",
                        r#"<http://f/api/v1/orgs/acme/repos?limit=50&page=2>; rel="next""#,
                    )],
                )
            } else {
                ScriptedTransport::reply(200, serde_json::json!([repo(3, "c", false)]).to_string())
            }
        });
        let f = forgejo(t.clone());
        let p1 = f.page(None).await.unwrap();
        assert_eq!(p1.repos.len(), 2);
        assert_eq!(
            p1.repos[0].snapshot.topics.as_deref(),
            Some(&["service".to_string()][..])
        );
        let p2 = f.page(p1.next.as_deref()).await.unwrap();
        assert_eq!(p2.repos[0].snapshot.full_name, "acme/c");
        assert!(p2.next.is_none());
        assert_eq!(
            t.seen(),
            vec![
                "GET /api/v1/orgs/acme/repos?limit=50&page=1",
                "GET /api/v1/orgs/acme/repos?limit=50&page=2"
            ]
        );
    }

    #[tokio::test]
    async fn refuses_a_cursor_that_is_not_a_page_number() {
        let f = forgejo(ScriptedTransport::new(|_| {
            ScriptedTransport::reply(200, "[]")
        }));
        assert!(f.page(Some("http://elsewhere/x")).await.is_err());
        assert!(f.page(Some("1")).await.is_err());
    }

    /// Forgejo writes `Link` from its ROOT_URL; behind a proxy or in a container that host is
    /// not the one we reach. Only the page number may be taken from it.
    #[tokio::test]
    async fn ignores_the_host_in_link() {
        let t = ScriptedTransport::new(|req| {
            if req.uri().query().unwrap_or("").contains("page=1") {
                ScriptedTransport::reply_with(
                    200,
                    serde_json::json!([repo(1, "a", false)]).to_string(),
                    &[(
                        "link",
                        r#"<http://localhost:3000/api/v1/orgs/acme/repos?limit=50&page=2>; rel="next""#,
                    )],
                )
            } else {
                ScriptedTransport::reply(200, "[]")
            }
        });
        let f = forgejo(t.clone());
        let p1 = f.page(None).await.unwrap();
        assert_eq!(p1.next.as_deref(), Some("2"));
        let p2 = f.page(p1.next.as_deref()).await.unwrap();
        assert!(p2.next.is_none());
        assert_eq!(t.seen()[1], "GET /api/v1/orgs/acme/repos?limit=50&page=2");
    }

    #[tokio::test]
    async fn a_link_back_is_a_protocol_error() {
        let t = ScriptedTransport::new(|_| {
            ScriptedTransport::reply_with(
                200,
                serde_json::json!([repo(1, "a", false)]).to_string(),
                &[(
                    "link",
                    r#"<http://f/api/v1/orgs/acme/repos?limit=50&page=1>; rel="next""#,
                )],
            )
        });
        assert!(forgejo(t).page(None).await.is_err());
    }

    #[tokio::test]
    async fn heads_skip_empty_repositories() {
        let sha = "b8a1a303d868e0bc2ed18bc8a39aaeb6fe8c5147";
        let t = ScriptedTransport::new(move |req| {
            assert!(req.uri().path().ends_with("/repos/acme/a/branches/main"));
            ScriptedTransport::reply(200, serde_json::json!({"commit": {"id": sha}}).to_string())
        });
        let f = forgejo(t.clone());
        let listed: Vec<Listed> = ["a", "b"]
            .iter()
            .enumerate()
            .map(|(i, n)| Listed {
                snapshot: RepoSnapshot {
                    external_id: i.to_string(),
                    full_name: format!("acme/{n}"),
                    name: n.to_string(),
                    default_branch: Some("main".into()),
                    archived: false,
                    topics: Some(vec![]),
                    head: None,
                    empty: *n == "b",
                    web_url: None,
                },
                locator: format!("acme/{n}"),
            })
            .collect();
        let heads = f.heads(&listed).await.unwrap();
        assert_eq!(heads, vec![Some(sha.to_string()), None]);
        assert_eq!(t.seen().len(), 1);
    }
}
