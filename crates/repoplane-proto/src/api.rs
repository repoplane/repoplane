//! The public `/v1` API: what the CLI, automation and the enterprise plane send and receive.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::forge::ForgeKind;
use crate::id::{ConnectionId, HeadRefreshId, ReconcileId, RepositoryId, SyncId};

/// How a connection authenticates to its forge. Day 0 knows one way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// A service token, held by the workers on the connection's queue, which choose it by the
    /// forge's host and the organisation.
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
    /// The task queue whose workers serve this connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Records the connection without first having a worker on its queue list the
    /// organisation: for a connection whose workers are not deployed yet.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub skip_check: bool,
}

/// What adding a connection checked: a worker on its queue listed the first page of the
/// organisation with a token it holds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionCheck {
    /// Repositories on that first page.
    pub repositories: u64,
    /// Whether more pages follow.
    pub more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Connection {
    pub id: ConnectionId,
    pub name: String,
    pub forge: ForgeKind,
    pub org: String,
    pub base_url: String,
    pub auth: AuthMode,
    pub queue: String,
    pub created_at: DateTime<Utc>,
    pub repositories: i64,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    /// Only in the answer to adding the connection, unless the check was skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<ConnectionCheck>,
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
    /// On the first page: what the selector could not decide, per connection.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unselected: Vec<Unselected>,
}

/// Which repositories an operation acts on (ADR 0015): what the inventory records about them,
/// never their contents. Every field is optional and they combine with AND; empty selects all.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Selector {
    /// Repositories with any of these topics. A forge without topics cannot answer: those
    /// repositories are unknown, neither selected nor ruled out.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "one_or_many"
    )]
    pub topic: Vec<String>,
    /// The repository's last path segment, case-insensitively, `*` standing for any run of
    /// characters: `*-starter`, `s4a-roma-*-starter`. No `/`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
}

impl Selector {
    pub fn is_empty(&self) -> bool {
        *self == Selector::default()
    }

    /// Refuses what no repository could match, so a typo is an error rather than an empty answer.
    pub fn validate(&self) -> Result<(), String> {
        if self.topic.iter().any(|t| t.trim().is_empty()) {
            return Err("a topic must not be empty".into());
        }
        match self.name.as_deref() {
            Some("") => Err("a name pattern must not be empty".into()),
            Some(n) if n.contains('/') => Err(format!(
                "name {n:?} contains '/': a name pattern matches a repository's last segment, \
                 so use `*-starter`, not `acme/*-starter`; a connection is already one organisation"
            )),
            _ => Ok(()),
        }
    }
}

/// A string or a list of strings, so `"topic": "node"` and `"topic": ["node", "go"]` both read.
fn one_or_many<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(s) => vec![s],
        OneOrMany::Many(v) => v,
    })
}

/// Repositories a listing left out because the forge cannot say whether they match: the
/// selector asked for a topic and the forge has none. Neither selected nor ruled out.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Unselected {
    pub connection: String,
    pub count: i64,
    pub reason: String,
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
    /// Repositories the forge would not let the token read (a 403 on that repository alone).
    /// The refresh goes on without them; it fails only if it could read none of its targets.
    #[serde(default)]
    pub unread: i64,
    /// Some of them, with the forge's reason: at most [`UNREAD_SHOWN`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unread_repositories: Vec<UnreadRepository>,
    pub error: Option<String>,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Unread repositories a head refresh names; its `unread` counts them all.
pub const UNREAD_SHOWN: usize = 20;

/// A repository whose HEAD a refresh could not read, and the forge's reason.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UnreadRepository {
    pub full_name: String,
    pub reason: String,
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

#[cfg(test)]
mod selector_tests {
    use super::*;

    #[test]
    fn a_topic_is_one_or_a_list() {
        let one: Selector = serde_json::from_str(r#"{"topic": "node"}"#).unwrap();
        let many: Selector = serde_json::from_str(r#"{"topic": ["node", "go"]}"#).unwrap();
        assert_eq!(one.topic, ["node"]);
        assert_eq!(many.topic, ["node", "go"]);
        // An empty body selects everything, and writes back as an empty object.
        let all: Selector = serde_json::from_str("{}").unwrap();
        assert!(all.is_empty());
        assert_eq!(serde_json::to_string(&all).unwrap(), "{}");
    }

    #[test]
    fn what_no_repository_could_match_is_refused() {
        let named = |n: &str| Selector {
            name: Some(n.into()),
            ..Selector::default()
        };
        assert!(named("*-starter").validate().is_ok());
        assert!(named("s4a-roma-*-starter").validate().is_ok());
        let slash = named("acme/*-starter").validate().unwrap_err();
        assert!(slash.contains("last segment"), "{slash}");
        assert!(named("").validate().is_err());
        let blank_topic = Selector {
            topic: vec![" ".into()],
            ..Selector::default()
        };
        assert!(blank_topic.validate().is_err());
    }
}
