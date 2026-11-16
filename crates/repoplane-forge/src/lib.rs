//! Listing an organisation's repositories on Forgejo, GitHub, GitLab and Azure DevOps. Only the
//! worker links this crate: the component that can reach a forge cannot reach the database, and
//! the one that holds the database cannot reach a forge (ADR 0001, 0002).
//!
//! The four forges differ in pagination, rate limits, topics and namespaces, and each adapter
//! says so in its own terms rather than through a lowest common denominator. What they share is
//! the `Lister` trait, every method of which is mandatory: a forge that cannot do something
//! answers explicitly, so "cannot" is never confused with "nobody wrote it yet".

pub mod azuredevops;
pub mod error;
pub mod forgejo;
pub mod git;
pub mod github;
pub mod gitlab;
pub mod http;
pub mod link;
pub mod token;

use std::sync::Arc;

use async_trait::async_trait;
use repoplane_proto::inventory::{ConnectionSpec, RepoSnapshot};
use repoplane_proto::{Caps, ForgeKind};
use tokio_util::sync::CancellationToken;

pub use error::{Class, ForgeError};
pub use http::{ReqwestTransport, ScriptedTransport, Transport};
pub use token::Token;

/// One repository as a listing found it. Its `locator` is what `heads` needs to find it again:
/// an API path, a project id, opaque outside the adapter that made it.
#[derive(Debug, Clone, PartialEq)]
pub struct Listed {
    pub snapshot: RepoSnapshot,
}

/// A page of a listing, and where the next one starts.
#[derive(Debug, Clone)]
pub struct Page {
    pub repos: Vec<Listed>,
    /// `None` on the last page.
    pub next: Option<String>,
    /// On the first page only, when the forge says how many pages there are: the cursors of
    /// every other page, so they can be fetched at once rather than one after another. `None`
    /// leaves the listing to be walked through `next`.
    pub rest: Option<Vec<String>>,
}

#[async_trait]
pub trait Lister: Send + Sync {
    fn kind(&self) -> ForgeKind;

    fn caps(&self) -> Caps {
        self.kind().caps()
    }

    /// One page of the organisation's repositories; `cursor` is `None` for the first and the
    /// previous page's `next` after that.
    async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError>;

    /// The default-branch HEAD of each repository, in order, each with its own outcome: a
    /// repository that could not be read does not hide the others'. `Ok(None)` where there is no
    /// HEAD: an empty repository, or one the forge will not read (a disabled Azure DevOps one).
    async fn heads(&self, repos: &[Listed]) -> Vec<Result<Option<String>, ForgeError>>;

    /// Looks one repository up by the forge's own id, outside the listing. `None` when the
    /// forge says it is gone, or it now belongs to another organisation.
    ///
    /// A listing paged by offset can skip a repository when another is deleted while it runs:
    /// every later page shifts back by one. Before a run marks a repository inaccessible, the
    /// worker asks here whether it really is gone (ADR 0005).
    async fn get(&self, external_id: &str) -> Result<Option<Listed>, ForgeError>;
}

/// Every repository a listing returns, its pages walked as a reconcile walks them: the pages a
/// first page names (`rest`) when it names them, otherwise one `next` after another. For tools
/// and tests, one page at a time; a reconcile fetches them at once, as activities.
pub async fn list_all(lister: &dyn Lister) -> Result<Vec<Listed>, ForgeError> {
    let first = lister.page(None).await?;
    let mut repos = first.repos;
    if let Some(rest) = first.rest {
        for cursor in rest {
            repos.extend(lister.page(Some(&cursor)).await?.repos);
        }
        return Ok(repos);
    }
    let (mut next, mut seen) = (first.next, std::collections::BTreeSet::new());
    while let Some(cursor) = next {
        if !seen.insert(cursor.clone()) {
            return Err(ForgeError::protocol(
                lister.kind().as_str(),
                format!("the listing returned page {cursor:?} twice"),
            ));
        }
        let page = lister.page(Some(&cursor)).await?;
        repos.extend(page.repos);
        next = page.next;
    }
    Ok(repos)
}

/// True when `full_name` sits inside the organisation or group `org` (case-insensitive, as the
/// forges treat owner names).
pub fn within(org: &str, full_name: &str) -> bool {
    let prefix = format!("{}/", org.trim_matches('/'));
    full_name.len() > prefix.len() && full_name[..prefix.len()].eq_ignore_ascii_case(&prefix)
}

/// A numeric forge id, as GitHub, GitLab and Forgejo use.
pub fn numeric_id(forge: &'static str, id: &str) -> Result<u64, ForgeError> {
    id.parse()
        .map_err(|_| ForgeError::protocol(forge, format!("{id:?} is not a repository id")))
}

/// The adapter for a connection, with the token the worker chose for it: a connection carries
/// none. The token is never sent anywhere but to the forge.
pub fn lister(
    spec: &ConnectionSpec,
    token: Token,
    transport: Arc<dyn Transport>,
    cancel: CancellationToken,
) -> Result<Box<dyn Lister>, ForgeError> {
    Ok(match spec.forge {
        ForgeKind::Forgejo => Box::new(forgejo::Forgejo::new(
            &spec.base_url,
            &spec.org,
            token,
            transport,
            cancel,
        )?),
        ForgeKind::Github => Box::new(github::Github::new(
            &spec.base_url,
            &spec.org,
            token,
            transport,
            cancel,
        )?),
        ForgeKind::Gitlab => Box::new(gitlab::Gitlab::new(
            &spec.base_url,
            &spec.org,
            token,
            transport,
            cancel,
        )?),
        ForgeKind::Azuredevops => Box::new(azuredevops::AzureDevops::new(
            &spec.base_url,
            &spec.org,
            token,
            transport,
            cancel,
        )?),
    })
}

/// The branch whose tip is a repository's HEAD, if there is one to read: an empty repository,
/// or one with no default branch, has none, and costs no request.
pub(crate) fn readable_branch(s: &RepoSnapshot) -> Option<&str> {
    s.default_branch
        .as_deref()
        .filter(|b| !b.is_empty() && !s.empty)
}

/// Percent-encodes one path segment.
pub fn escape_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Escapes each segment of a ref or path but keeps its slashes: `release/v2` stays two segments
/// rather than becoming `release%2Fv2`, which some forges answer with a 404.
pub fn escape_path(s: &str) -> String {
    s.split('/')
        .map(escape_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// A commit id as the inventory stores it: lowercase hex, 40 or 64 characters.
pub fn commit_id(forge: &'static str, s: &str) -> Result<String, ForgeError> {
    let s = s.to_ascii_lowercase();
    if matches!(s.len(), 40 | 64) && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(s)
    } else {
        Err(ForgeError::protocol(
            forge,
            format!("{s:?} is not a commit id"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A listing scripted page by page, by cursor.
    struct Pages(Vec<(Option<&'static str>, Page)>);

    fn page(names: &[&str], next: Option<&str>, rest: Option<&[&str]>) -> Page {
        Page {
            repos: names
                .iter()
                .map(|n| Listed {
                    snapshot: RepoSnapshot {
                        external_id: n.to_string(),
                        full_name: format!("o/{n}"),
                        name: n.to_string(),
                        default_branch: None,
                        archived: false,
                        topics: None,
                        locator: String::new(),
                        empty: true,
                        web_url: None,
                    },
                })
                .collect(),
            next: next.map(str::to_string),
            rest: rest.map(|r| r.iter().map(|c| c.to_string()).collect()),
        }
    }

    #[async_trait]
    impl Lister for Pages {
        fn kind(&self) -> ForgeKind {
            ForgeKind::Github
        }
        async fn page(&self, cursor: Option<&str>) -> Result<Page, ForgeError> {
            Ok(self.0.iter().find(|(c, _)| *c == cursor).unwrap().1.clone())
        }
        async fn heads(&self, _: &[Listed]) -> Vec<Result<Option<String>, ForgeError>> {
            Vec::new()
        }
        async fn get(&self, _: &str) -> Result<Option<Listed>, ForgeError> {
            Ok(None)
        }
    }

    fn names(repos: Vec<Listed>) -> Vec<String> {
        repos.into_iter().map(|r| r.snapshot.name).collect()
    }

    /// The pages a first page names are the listing, `next` on them notwithstanding (GitHub
    /// names them and still links the next); without them, `next` is followed.
    #[tokio::test]
    async fn a_listing_is_walked_as_a_reconcile_walks_it() {
        let named = Pages(vec![
            (None, page(&["a"], Some("2"), Some(&["2", "3"]))),
            (Some("2"), page(&["b"], Some("3"), None)),
            (Some("3"), page(&["c"], None, None)),
        ]);
        assert_eq!(names(list_all(&named).await.unwrap()), ["a", "b", "c"]);
        let linked = Pages(vec![
            (None, page(&["a"], Some("2"), None)),
            (Some("2"), page(&["b"], Some("3"), None)),
            (Some("3"), page(&["c"], None, None)),
        ]);
        assert_eq!(names(list_all(&linked).await.unwrap()), ["a", "b", "c"]);
        let looping = Pages(vec![
            (None, page(&["a"], Some("2"), None)),
            (Some("2"), page(&["b"], Some("2"), None)),
        ]);
        assert!(matches!(
            list_all(&looping).await,
            Err(ForgeError::Protocol { .. })
        ));
    }

    #[test]
    fn escaping() {
        assert_eq!(escape_segment("a b/c"), "a%20b%2Fc");
        assert_eq!(escape_path("release/v2"), "release/v2");
        assert_eq!(escape_path("feat/a b"), "feat/a%20b");
    }

    #[test]
    fn membership() {
        assert!(within("acme", "acme/api"));
        assert!(within("Acme", "acme/api"));
        assert!(within("acme/platform", "acme/platform/core/api"));
        assert!(!within("acme", "acme-labs/api"));
        assert!(!within("acme", "acme/"));
        assert!(!within("acme/platform", "acme/api"));
    }

    #[test]
    fn commit_ids() {
        assert!(commit_id("t", &"A".repeat(40)).is_ok());
        assert!(commit_id("t", "abc").is_err());
    }
}
