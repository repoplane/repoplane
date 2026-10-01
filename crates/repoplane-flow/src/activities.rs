//! Activity definitions: names, inputs and outputs, with no implementation. `repoplane-api`
//! implements [`Control`] and `repoplane-worker` implements [`Forge`], each with
//! `#[activity(definition = …)]`, so a mismatch between a definition and its implementation is
//! a compile error.

#![allow(unused_variables)]

use serde::{Deserialize, Serialize};
use temporalio_macros::activity_definitions;
use temporalio_sdk::activities::ActivityError;

use repoplane_proto::id::{HeadRefreshId, ReconcileId};
use repoplane_proto::inventory::{
    ConnectionSpec, HeadTarget, ObservedHead, RepoSnapshot, UnseenRepo,
};

// --- forge activities: on the connection's queue, in repoplane-worker --------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPage {
    pub connection: ConnectionSpec,
    /// `None` for the first page; then the previous page's `next`.
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListedPage {
    pub repos: Vec<RepoSnapshot>,
    /// `None` on the last page.
    pub next: Option<String>,
    /// On the first page, when the forge says how many pages there are: every other page's
    /// cursor, fetched at once.
    #[serde(default)]
    pub rest: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lookup {
    pub connection: ConnectionSpec,
    /// Forge id of a repository a listing did not return.
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadHead {
    pub connection: ConnectionSpec,
    pub target: HeadTarget,
}

pub struct Forge;

#[activity_definitions]
impl Forge {
    /// One page of the organisation's repositories.
    #[activity]
    pub fn list_page(input: ListPage) -> Result<ListedPage, ActivityError> {
        unimplemented!()
    }

    /// The repository, if it still exists in the organisation; `None` when it is gone.
    #[activity]
    pub fn lookup(input: Lookup) -> Result<Option<RepoSnapshot>, ActivityError> {
        unimplemented!()
    }

    /// The target's default-branch HEAD, naming the branch it was read from.
    #[activity]
    pub fn read_head(input: ReadHead) -> Result<ObservedHead, ActivityError> {
        unimplemented!()
    }
}

// --- control activities: on repoplane-control, in repoplane-api ------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordPage {
    pub reconcile: ReconcileId,
    pub repos: Vec<RepoSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordHeads {
    pub refresh: HeadRefreshId,
    pub heads: Vec<ObservedHead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileFailed {
    pub reconcile: ReconcileId,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshFailed {
    pub refresh: HeadRefreshId,
    pub message: String,
}

pub struct Control;

#[activity_definitions]
impl Control {
    /// Marks the run running and fixes its watermark.
    #[activity]
    pub fn start_reconcile(input: ReconcileId) -> Result<(), ActivityError> {
        unimplemented!()
    }

    /// Records listed repositories under the run's watermark; returns how many were new or
    /// changed.
    #[activity]
    pub fn record_page(input: RecordPage) -> Result<i64, ActivityError> {
        unimplemented!()
    }

    /// The active repositories the run has not seen.
    #[activity]
    pub fn unseen(input: ReconcileId) -> Result<Vec<UnseenRepo>, ActivityError> {
        unimplemented!()
    }

    /// Marks what the run did not see inaccessible and closes the run. Called only after the
    /// whole listing and the lookups of the unseen succeeded.
    #[activity]
    pub fn finish_reconcile(input: ReconcileId) -> Result<(), ActivityError> {
        unimplemented!()
    }

    #[activity]
    pub fn fail_reconcile(input: ReconcileFailed) -> Result<(), ActivityError> {
        unimplemented!()
    }

    /// Marks the refresh running and returns its targets as the inventory holds them now.
    #[activity]
    pub fn start_refresh(input: HeadRefreshId) -> Result<Vec<HeadTarget>, ActivityError> {
        unimplemented!()
    }

    /// Records HEADs; returns how many changed.
    #[activity]
    pub fn record_heads(input: RecordHeads) -> Result<i64, ActivityError> {
        unimplemented!()
    }

    #[activity]
    pub fn finish_refresh(input: HeadRefreshId) -> Result<(), ActivityError> {
        unimplemented!()
    }

    #[activity]
    pub fn fail_refresh(input: RefreshFailed) -> Result<(), ActivityError> {
        unimplemented!()
    }
}

/// The marker types `#[activity(definition = …)]` names. The macro puts them in a private
/// module, so they are re-exported here for the API and the worker to implement against.
pub mod control_defs {
    pub use super::control::*;
}

/// See [`control_defs`].
pub mod forge_defs {
    pub use super::forge::*;
}
