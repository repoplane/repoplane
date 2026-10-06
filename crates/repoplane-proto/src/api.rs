//! The public `/v1` API: what the CLI, automation and the enterprise plane send and receive.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::forge::ForgeKind;
use crate::id::{ConnectionId, HeadRefreshId, ReconcileId, RepositoryId, SyncId};

/// How a connection authenticates to its forge. Day 0 knows one way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// A personal or service token, read by the worker from the variable `token_env` names.
    Token,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateConnection {
    pub name: String,
    pub forge: ForgeKind,
    /// The fleet boundary: a GitHub or Forgejo organisation, a GitLab group path (subgroups
    /// included), an Azure DevOps organisation (all projects).
    pub org: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub auth: AuthMode,
    /// The NAME of the variable holding the token, in the worker's environment.
    pub token_env: String,
    /// The task queue whose workers serve this connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Connection {
    pub id: ConnectionId,
    pub name: String,
    pub forge: ForgeKind,
    pub org: String,
    pub base_url: String,
    pub auth: AuthMode,
    pub token_env: String,
    pub queue: String,
    pub created_at: DateTime<Utc>,
    pub repositories: i64,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionList {
    pub items: Vec<Connection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, RunStatus::Succeeded | RunStatus::Failed)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Queued => "queued",
            RunStatus::Running => "running",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Reconcile {
    pub id: ReconcileId,
    pub connection_id: ConnectionId,
    pub status: RunStatus,
    /// Repositories the forge listed.
    pub discovered: i64,
    /// Repositories written to the inventory: new, or changed since the last run.
    pub recorded: i64,
    /// Repositories known before and absent from this complete listing.
    pub inaccessible: i64,
    pub error: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepositoryStatus {
    Active,
    /// Listed before, absent from the latest complete listing. Never deleted: the history
    /// that refers to it stays meaningful.
    Inaccessible,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Repository {
    pub id: RepositoryId,
    pub connection_id: ConnectionId,
    /// The forge's own identifier. Identity follows it, never the name.
    pub external_id: String,
    /// The path as the forge writes it: `org/name`, `group/sub/name`, `org/project/name`.
    pub full_name: String,
    pub name: String,
    pub default_branch: Option<String>,
    pub archived: bool,
    /// `None` when the forge has no topics (Azure DevOps), which is not the same as a
    /// repository with none.
    pub topics: Option<Vec<String>>,
    /// The default-branch HEAD commit as last observed by a head refresh; `None` until one has,
    /// and for an empty repository or one whose HEAD the forge will not tell (a disabled Azure
    /// DevOps repository). A reconcile does not read it, and clears it when the default branch
    /// changes, since it described another branch.
    pub head: Option<String>,
    pub head_observed_at: Option<DateTime<Utc>>,
    pub empty: bool,
    pub web_url: Option<String>,
    pub status: RepositoryStatus,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepositoryList {
    pub items: Vec<Repository>,
    /// Pass back as `cursor` for the next page; absent on the last one.
    pub next_cursor: Option<String>,
}

/// Which repositories of a connection a head refresh reads. Empty means all of them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RefreshHeads {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HeadRefresh {
    pub id: HeadRefreshId,
    pub connection_id: ConnectionId,
    pub status: RunStatus,
    /// Repositories the refresh was asked to read.
    pub targeted: i64,
    /// Repositories whose HEAD the forge answered for (a `None` is an answer).
    pub observed: i64,
    /// Repositories whose HEAD differs from the one recorded before.
    pub changed: i64,
    pub error: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// `connection sync`: a reconcile, then a head refresh of every active repository, as one run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sync {
    pub id: SyncId,
    pub connection_id: ConnectionId,
    pub status: RunStatus,
    pub error: Option<String>,
    pub reconcile: Reconcile,
    /// Absent until the reconcile has succeeded and the refresh is requested.
    pub head_refresh: Option<HeadRefresh>,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// An error answer, on every endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}
