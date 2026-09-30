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
pub mod github;
pub mod gitlab;
pub mod http;
pub mod link;
pub mod token;

use std::sync::Arc;

use async_trait::async_trait;
use repoplane_proto::worker::{ConnectionSpec, RepoSnapshot};
use repoplane_proto::{Caps, ForgeKind};
use tokio_util::sync::CancellationToken;

pub use error::{Class, ForgeError};
pub use http::{ReqwestTransport, ScriptedTransport, Transport};
pub use token::Token;

/// One repository as a listing found it, before its HEAD is read.
#[derive(Debug, Clone, PartialEq)]
pub struct Listed {
    pub snapshot: RepoSnapshot,
    /// What `heads` needs to find this repository again: an API path, a project id. Opaque
    /// outside the adapter that made it.
    pub locator: String,
}

/// A page of a listing, and where the next one starts.
#[derive(Debug, Clone)]
pub struct Page {
    pub repos: Vec<Listed>,
    /// `None` on the last page.
    pub next: Option<String>,
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

    /// The default-branch HEAD of each repository, in order. `None` where there is none: an
    /// empty repository, or one the forge will not read (a disabled Azure DevOps repository).
    /// Batched where the forge allows it.
    async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError>;

    /// Looks one repository up by the forge's own id, outside the listing. `None` when the
    /// forge says it is gone, or it now belongs to another organisation.
    ///
    /// A listing paged by offset can skip a repository when another is deleted while it runs:
    /// every later page shifts back by one. Before a run marks a repository inaccessible, the
    /// worker asks here whether it really is gone (ADR 0005).
    async fn get(&self, external_id: &str) -> Result<Option<Listed>, ForgeError>;
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

/// The adapter for a connection. The token is read here, in the worker, from the variable the
/// connection names; it is never sent anywhere but to the forge.
pub fn lister(
    spec: &ConnectionSpec,
    transport: Arc<dyn Transport>,
    cancel: CancellationToken,
) -> Result<Box<dyn Lister>, ForgeError> {
    let token = Token::from_env(&spec.token_env)?;
    lister_with_token(spec, token, transport, cancel)
}

pub fn lister_with_token(
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

/// How many HEAD lookups an adapter without a batch endpoint keeps in flight.
pub const HEAD_CONCURRENCY: usize = 8;

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
