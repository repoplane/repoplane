//! The `/v1/worker` protocol. A worker leases a task from a queue, reports what it finds, and
//! completes or fails it. Every call after the lease carries the lease id: a worker whose lease
//! expired and was handed to another gets a 409 and stops, so two workers never write the same
//! run.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::forge::ForgeKind;
use crate::id::{ConnectionId, HeadRefreshId, ReconcileId, TaskId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseRequest {
    pub queue: String,
    /// How long the API may hold the request open waiting for a task.
    pub wait_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub task: TaskId,
    pub lease_id: Uuid,
    /// Seconds before the lease expires unless the worker reports progress.
    pub ttl_secs: u64,
    pub work: Work,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Work {
    /// List the organisation and record what exists.
    Reconcile {
        reconcile: ReconcileId,
        connection: ConnectionSpec,
    },
    /// Read the default-branch HEAD of these repositories.
    RefreshHeads {
        refresh: HeadRefreshId,
        connection: ConnectionSpec,
        targets: Vec<HeadTarget>,
    },
}

/// Everything a worker needs to reach a forge, except the secret: that stays in the worker's
/// environment, under the name given here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionSpec {
    pub id: ConnectionId,
    pub forge: ForgeKind,
    pub org: String,
    pub base_url: String,
    pub token_env: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadsUpload {
    pub lease_id: Uuid,
    pub heads: Vec<ObservedHead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadsAck {
    pub changed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageUpload {
    pub lease_id: Uuid,
    pub repos: Vec<RepoSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageAck {
    pub recorded: i64,
}

/// Asks which repositories the inventory holds for this connection that the run has not seen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnseenRequest {
    pub lease_id: Uuid,
}

/// Repositories the inventory holds as active that this run's listing did not return. Before
/// completing, the worker looks each up by id: a listing paged by offset skips a repository
/// when another is deleted mid-listing, and only the ones the forge confirms gone may be marked
/// inaccessible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unseen {
    pub repos: Vec<UnseenRepo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnseenRepo {
    pub external_id: String,
    pub full_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    pub lease_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Complete {
    pub lease_id: Uuid,
    /// Every page of the listing was read and uploaded. Only then may the API mark the
    /// repositories it did not see as inaccessible.
    pub complete_listing: bool,
    pub discovered: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fail {
    pub lease_id: Uuid,
    /// Already scrubbed of anything secret by the worker.
    pub message: String,
    /// Try again later rather than failing the run.
    pub retryable: bool,
}
