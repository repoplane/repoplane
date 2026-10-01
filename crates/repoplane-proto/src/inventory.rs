//! What crosses a Temporal activity boundary: a connection as a worker needs it, and the
//! repositories and HEADs it reports. Everything here ends up in workflow history, so nothing
//! here may be a secret — a connection names the variable that holds its token, never the token.

use serde::{Deserialize, Serialize};

use crate::forge::ForgeKind;
use crate::id::ConnectionId;

/// Everything a worker needs to reach a forge, except the secret: that stays in the worker's
/// environment, under the name given here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionSpec {
    pub id: ConnectionId,
    pub forge: ForgeKind,
    pub org: String,
    pub base_url: String,
    pub token_env: String,
    /// The task queue whose workers serve this connection's forge activities.
    pub queue: String,
}

/// One repository as a forge listed it: what exists, not what it holds. The HEAD is not part
/// of it; that is a head refresh's business.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoSnapshot {
    pub external_id: String,
    pub full_name: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub archived: bool,
    pub topics: Option<Vec<String>>,
    pub empty: bool,
    pub web_url: Option<String>,
    /// What the forge adapter needs to find this repository again without listing: an API
    /// path, a project id. Opaque to the API, which stores it and hands it back.
    pub locator: String,
}

/// One repository whose HEAD a refresh reads, as the inventory holds it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeadTarget {
    pub repo: RepoSnapshot,
}

/// A default-branch HEAD as the forge answered it; `None` for an empty or unreadable one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedHead {
    pub external_id: String,
    /// The branch the HEAD was read from. If a reconcile has since recorded another default
    /// branch, this HEAD describes the old one and is dropped.
    pub branch: Option<String>,
    pub head: Option<String>,
}

/// A repository the inventory holds as active that a reconcile's listing did not return. A
/// listing paged by offset skips a repository when another is deleted mid-listing, so each is
/// looked up by id before anything is marked inaccessible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnseenRepo {
    pub external_id: String,
    pub full_name: String,
}
