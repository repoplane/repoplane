//! GitHub and GitHub Enterprise Server: `GET /orgs/{org}/repos`, pages followed by their `Link`
//! header, topics included with each repository. HEADs come from GraphQL, 25 aliased
//! `repository` lookups per query and a page's queries in flight together, rather than one REST
//! call per repository: four points of the GraphQL budget per page of 100, not 100 of the REST
//! one.
//!
//! GitHub signals a rate limit in three ways, none of them a plain 429: a 403 with
//! `X-RateLimit-Remaining: 0`, a 403 or 429 whose body names a "secondary rate limit", and a
//! GraphQL 200 whose `errors` say `RATE_LIMITED`. `classify` reads all three, so the shared
//! `HttpClient` waits them out like any other.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use http::{HeaderMap, Response};
use repoplane_proto::ForgeKind;
use repoplane_proto::inventory::RepoSnapshot;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::error::{Class, ForgeError};
use crate::http::{HttpClient, Transport, Verdict, classify_generic, header, retry_after};
use crate::{Listed, Lister, Page, Token, commit_id, escape_segment, link, numeric_id, within};

const FORGE: &str = "github";
const PAGE_SIZE: u32 = 100;
/// Repositories per GraphQL query. Resolving `defaultBranchRef` costs GitHub about 0.1s per
/// repository, done one after another within a query, and a query is cut off at 10s with a 502:
/// measured on github.com, 100 per query failed every time, 50 took 6s, 25 about 3s.
const HEAD_BATCH: usize = 25;
/// Queries in flight at once, so a page of 100 takes the time of one query of 25. GitHub asks
/// for restraint in concurrency, not for none.
const HEAD_QUERIES: usize = 4;
/// The pause for a secondary rate limit or a bare 429, which say nothing of their own.
const SECONDARY_WAIT: Duration = Duration::from_secs(60);

pub struct Github {
    http: HttpClient,
    api: String,
    graphql: String,
    org: String,
}

impl Github {
    pub fn new(
        base_url: &str,
        org: &str,
        token: Token,
        transport: Arc<dyn Transport>,
        cancel: CancellationToken,
    ) -> Result<Self, ForgeError> {
        let (api, graphql) = endpoints(base_url)?;
        let mut headers = HeaderMap::new();
        let (k, v) = header("authorization", &format!("Bearer {}", token.expose()))?;
        headers.insert(k, v);
        let (k, v) = header("accept", "application/vnd.github+json")?;
        headers.insert(k, v);
        let (k, v) = header("x-github-api-version", "2022-11-28")?;
        headers.insert(k, v);
        Ok(Github {
            http: HttpClient::new(FORGE, transport, classify, headers, token, cancel),
            api,
            graphql,
            org: org.to_string(),
        })
    }
}

/// The REST and GraphQL roots for what the user configured. github.com serves its API from a
/// host of its own; GitHub Enterprise Server serves both under the instance's `/api`.
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
        return Ok((
            "https://api.github.com".into(),
            "https://api.github.com/graphql".into(),
        ));
    }
    Ok((format!("{base}/api/v3"), format!("{base}/api/graphql")))
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
        Ok(Page {
            repos: repos
                .unwrap_or_default()
                .into_iter()
                .map(Repo::into_listed)
                .collect(),
            next,
        })
    }

    async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
        let queries: Vec<_> = repos
            .chunks(HEAD_BATCH)
            .map(|c| self.head_batch(c))
            .collect();
        let batches: Vec<Vec<Option<String>>> = stream::iter(queries)
            .buffered(HEAD_QUERIES)
            .try_collect()
            .await?;
        Ok(batches.into_iter().flatten().collect())
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

#[derive(Deserialize)]
struct GraphqlAnswer {
    #[serde(default)]
    data: Option<HashMap<String, Option<RepoHead>>>,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RepoHead {
    default_branch_ref: Option<BranchRef>,
}

#[derive(Deserialize)]
struct BranchRef {
    target: Option<Target>,
}

#[derive(Deserialize)]
struct Target {
    oid: String,
}

#[derive(Deserialize)]
struct GraphqlError {
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default)]
    message: String,
    /// The first element is the alias the error belongs to; the rest is inside it.
    #[serde(default)]
    path: Vec<serde_json::Value>,
}

impl Github {
    /// One GraphQL query for up to `HEAD_BATCH` repositories, each under the alias `rN` and
    /// named through variables, so no repository name is ever spliced into the query text.
    async fn head_batch(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
        if repos.is_empty() {
            return Ok(Vec::new());
        }
        let mut params = Vec::with_capacity(repos.len());
        let mut fields = String::new();
        let mut vars = serde_json::Map::new();
        for (i, r) in repos.iter().enumerate() {
            let (owner, name) = r.snapshot.locator.split_once('/').ok_or_else(|| {
                ForgeError::protocol(FORGE, format!("full name {:?}", r.snapshot.locator))
            })?;
            params.push(format!("$o{i}: String!, $n{i}: String!"));
            fields.push_str(&format!(
                " r{i}: repository(owner: $o{i}, name: $n{i}) {{ defaultBranchRef {{ target {{ oid }} }} }}"
            ));
            vars.insert(format!("o{i}"), owner.into());
            vars.insert(format!("n{i}"), name.into());
        }
        let query = format!("query Heads({}) {{{fields} }}", params.join(", "));
        let body = serde_json::json!({ "query": query, "variables": vars });
        let answer = self.http.post_query(&self.graphql, &body).await?;
        let parsed: GraphqlAnswer = self
            .http
            .decode(&self.graphql, &answer)?
            .ok_or_else(|| ForgeError::protocol(FORGE, "GraphQL answered with an empty body"))?;
        read_heads(repos.len(), parsed)
    }
}

/// The HEADs in the query's order. GraphQL reports trouble in `errors` beside whatever `data` it
/// could still produce, so an error only fails the batch when it touches a repository asked for:
/// a `NOT_FOUND` one (deleted or renamed since the listing) is just a repository without a HEAD,
/// and an error elsewhere is logged and the data used.
fn read_heads(n: usize, a: GraphqlAnswer) -> Result<Vec<Option<String>>, ForgeError> {
    let mut data = a.data.unwrap_or_default();
    let mut gone = vec![false; n];
    for e in &a.errors {
        let alias = e.path.first().and_then(|p| p.as_str());
        let index = alias
            .and_then(|p| p.strip_prefix('r'))
            .and_then(|i| i.parse::<usize>().ok());
        match index {
            Some(i) if i < n && e.kind.as_deref() == Some("NOT_FOUND") && e.path.len() == 1 => {
                gone[i] = true
            }
            Some(i) if i < n => {
                return Err(ForgeError::protocol(
                    FORGE,
                    format!(
                        "GraphQL error on repository {i}: {} {}",
                        e.kind.as_deref().unwrap_or(""),
                        e.message
                    ),
                ));
            }
            _ if data.is_empty() => {
                return Err(ForgeError::protocol(
                    FORGE,
                    format!(
                        "GraphQL error: {} {}",
                        e.kind.as_deref().unwrap_or(""),
                        e.message
                    ),
                ));
            }
            _ => tracing::warn!(
                forge = FORGE,
                kind = e.kind.as_deref().unwrap_or(""),
                "GraphQL error outside the repositories asked for: {}",
                e.message
            ),
        }
    }
    (0..n)
        .map(|i| match data.remove(&format!("r{i}")) {
            Some(Some(repo)) => match repo.default_branch_ref.and_then(|b| b.target) {
                Some(t) => commit_id(FORGE, &t.oid).map(Some),
                // No default branch: the repository has no commits.
                None => Ok(None),
            },
            Some(None) if gone[i] => Ok(None),
            Some(None) => Err(ForgeError::protocol(
                FORGE,
                format!("GraphQL answered null for repository {i} without saying why"),
            )),
            None => Err(ForgeError::protocol(
                FORGE,
                format!("GraphQL answer lacks repository {i}"),
            )),
        })
        .collect()
}

/// GitHub's reading of an answer. On a 403 or 429, in the order GitHub documents: `Retry-After`,
/// then an exhausted primary budget (`X-RateLimit-Remaining: 0`, wait until
/// `X-RateLimit-Reset`), then a secondary limit named in the body. Any other 403 is a missing
/// permission, not worth a retry. A 200 carrying a GraphQL `RATE_LIMITED` error is a rate limit
/// too.
pub fn classify(resp: &Response<Bytes>) -> Option<Verdict> {
    let status = resp.status().as_u16();
    if (200..300).contains(&status) {
        return graphql_rate_limited(resp.body()).then(|| Verdict {
            class: Class::RateLimited,
            retry_after: retry_after(resp).or_else(|| until_reset(resp)),
        });
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

/// Whether a 2xx body is a GraphQL answer refused for its rate limit. The substring test keeps
/// every REST page from being parsed twice.
fn graphql_rate_limited(body: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct Errors {
        #[serde(default)]
        errors: Vec<GraphqlError>,
    }
    if !body.windows(12).any(|w| w == b"RATE_LIMITED") {
        return false;
    }
    serde_json::from_slice::<Errors>(body)
        .map(|e| {
            e.errors
                .iter()
                .any(|e| e.kind.as_deref() == Some("RATE_LIMITED"))
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::ScriptedTransport;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SHA: &str = "b8a1a303d868e0bc2ed18bc8a39aaeb6fe8c5147";

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

    fn listed(names: &[&str]) -> Vec<Listed> {
        names
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
                    locator: format!("acme/{n}"),
                    empty: false,
                    web_url: None,
                },
            })
            .collect()
    }

    #[test]
    fn endpoints_for_github_com_and_enterprise() {
        let api = |b| endpoints(b).unwrap();
        assert_eq!(api("https://github.com").0, "https://api.github.com");
        assert_eq!(
            api("https://github.com/").1,
            "https://api.github.com/graphql"
        );
        assert_eq!(
            api("https://ghe.corp.example/"),
            (
                "https://ghe.corp.example/api/v3".into(),
                "https://ghe.corp.example/api/graphql".into()
            )
        );
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

    #[tokio::test]
    async fn heads_are_an_aliased_graphql_query() {
        let bodies = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let b2 = bodies.clone();
        let t = ScriptedTransport::new(move |req| {
            b2.lock()
                .unwrap()
                .push(serde_json::from_slice(req.body()).unwrap());
            ScriptedTransport::reply(
                200,
                serde_json::json!({"data": {
                    "r0": {"defaultBranchRef": {"target": {"oid": SHA}}},
                    "r1": {"defaultBranchRef": null},
                    "r2": {"defaultBranchRef": {"target": {"oid": SHA.to_uppercase()}}}
                }})
                .to_string(),
            )
        });
        let g = github(t.clone());
        let heads = g.heads(&listed(&["a", "no-commits", "c"])).await.unwrap();
        assert_eq!(
            heads,
            vec![Some(SHA.to_string()), None, Some(SHA.to_string())]
        );
        assert_eq!(t.seen(), vec!["POST /graphql"]);
        let body = &bodies.lock().unwrap()[0];
        let q = body["query"].as_str().unwrap();
        assert!(q.contains("r1: repository(owner: $o1, name: $n1)"), "{q}");
        assert!(q.contains("$o2: String!, $n2: String!"), "{q}");
        assert!(!q.contains("no-commits"), "names travel as variables: {q}");
        assert_eq!(body["variables"]["o1"], "acme");
        assert_eq!(body["variables"]["n1"], "no-commits");
    }

    #[tokio::test]
    async fn heads_batch_by_twenty_five_in_order() {
        // Each repository's HEAD is its own number, so any mix-up across batches shows.
        let t = ScriptedTransport::new(|req| {
            let body: serde_json::Value = serde_json::from_slice(req.body()).unwrap();
            let vars = body["variables"].as_object().unwrap();
            let data: serde_json::Map<_, _> = (0..vars.len() / 2)
                .map(|i| {
                    let n: u64 = vars[&format!("n{i}")].as_str().unwrap()[4..]
                        .parse()
                        .unwrap();
                    let oid = format!("{n:040x}");
                    (
                        format!("r{i}"),
                        serde_json::json!({"defaultBranchRef": {"target": {"oid": oid}}}),
                    )
                })
                .collect();
            ScriptedTransport::reply(200, serde_json::json!({ "data": data }).to_string())
        });
        let names: Vec<String> = (0..60).map(|i| format!("repo{i}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let heads = github(t.clone()).heads(&listed(&names)).await.unwrap();
        let want: Vec<_> = (0..60u64).map(|n| Some(format!("{n:040x}"))).collect();
        assert_eq!(heads, want);
        assert_eq!(t.seen(), vec!["POST /graphql"; 3]);
    }

    #[test]
    fn graphql_errors_fail_only_the_repositories_they_touch() {
        let parse = |v: serde_json::Value| serde_json::from_value::<GraphqlAnswer>(v).unwrap();
        // Deleted since the listing: no HEAD, not a failure.
        let gone = parse(serde_json::json!({
            "data": {"r0": null, "r1": {"defaultBranchRef": {"target": {"oid": SHA}}}},
            "errors": [{"type": "NOT_FOUND", "path": ["r0"], "message": "Could not resolve"}]
        }));
        assert_eq!(
            read_heads(2, gone).unwrap(),
            vec![None, Some(SHA.to_string())]
        );
        // Refused for a repository asked for: fail rather than record a wrong None.
        let refused = parse(serde_json::json!({
            "data": {"r0": null},
            "errors": [{"type": "FORBIDDEN", "path": ["r0"], "message": "SAML enforcement"}]
        }));
        assert!(read_heads(1, refused).is_err());
        // An error that names no repository, beside complete data: use the data.
        let aside = parse(serde_json::json!({
            "data": {"r0": {"defaultBranchRef": null}},
            "errors": [{"type": "SOMETHING", "message": "unrelated"}]
        }));
        assert_eq!(read_heads(1, aside).unwrap(), vec![None]);
        // No data at all.
        let none = parse(serde_json::json!({"errors": [{"message": "parse error"}]}));
        assert!(read_heads(1, none).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn graphql_rate_limited_is_waited_out() {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        let t = ScriptedTransport::new(move |_| {
            if n2.fetch_add(1, Ordering::SeqCst) == 0 {
                ScriptedTransport::reply_with(
                    200,
                    r#"{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}"#,
                    &[("retry-after", "5")],
                )
            } else {
                ScriptedTransport::reply(200, serde_json::json!({"data": {"r0": {"defaultBranchRef": {"target": {"oid": SHA}}}}}).to_string())
            }
        });
        let heads = github(t.clone()).heads(&listed(&["a"])).await.unwrap();
        assert_eq!(heads, vec![Some(SHA.to_string())]);
        assert_eq!(n.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn graphql_rate_limited_is_a_rate_limit() {
        let r =
            ScriptedTransport::reply(200, r#"{"errors":[{"type":"RATE_LIMITED","message":"x"}]}"#);
        assert_eq!(classify(&r).map(|v| v.class), Some(Class::RateLimited));
        let fine = ScriptedTransport::reply(200, r#"[{"name":"RATE_LIMITED"}]"#);
        assert_eq!(classify(&fine), None);
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
}
