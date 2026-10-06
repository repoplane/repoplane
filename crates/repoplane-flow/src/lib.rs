//! Repoplane's Temporal workflows, and the activities they call.
//!
//! Workflows run in `repoplane-api`'s control worker, on [`CONTROL_QUEUE`]. They call two
//! kinds of activity, each declared here and implemented elsewhere, so that neither side links
//! the other's dependencies:
//!
//! - **control activities** ([`Control`]): everything that touches Postgres, implemented by
//!   `repoplane-api` on [`CONTROL_QUEUE`];
//! - **forge activities** ([`Forge`]): everything that talks to a forge, implemented by
//!   `repoplane-worker` on the connection's own queue, which may run inside a customer's network.
//!
//! Every input and output here is recorded in workflow history, so none may hold a secret: a
//! connection names the variable that holds its token, and only a forge activity reads it.

pub mod activities;
pub mod temporal;
mod workflows;

pub use activities::{Control, Forge};
pub use temporal::{TemporalConfig, TemporalError};
pub use workflows::{
    LookupsWorkflow, ReadHeadsWorkflow, ReconcileWorkflow, RefreshHeadsWorkflow, SyncWorkflow,
};

use serde::{Deserialize, Serialize};

use repoplane_proto::id::{HeadRefreshId, ReconcileId, SyncId};
use repoplane_proto::inventory::{ConnectionSpec, HeadTarget};

/// The task queue of `repoplane-api`'s control worker: workflows and Postgres activities.
pub const CONTROL_QUEUE: &str = "repoplane-control";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileInput {
    pub reconcile: ReconcileId,
    pub connection: ConnectionSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshHeadsInput {
    pub refresh: HeadRefreshId,
    pub connection: ConnectionSpec,
}

/// `connection sync`: the reconcile it runs first was recorded with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncInput {
    pub sync: SyncId,
    pub reconcile: ReconcileId,
    pub connection: ConnectionSpec,
}

/// One slice of a reconcile's lookups by id, run as a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupsInput {
    pub reconcile: ReconcileId,
    pub connection: ConnectionSpec,
    pub ids: Vec<String>,
}

/// One slice of a head refresh's targets, run as a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadHeadsInput {
    pub refresh: HeadRefreshId,
    pub connection: ConnectionSpec,
    pub targets: Vec<HeadTarget>,
}

/// The workflow id of a run: its domain id, so a run is found in Temporal by the id the API
/// already hands out.
pub fn workflow_id(run: &impl std::fmt::Display) -> String {
    run.to_string()
}
