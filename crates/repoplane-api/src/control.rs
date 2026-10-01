//! The control activities: the steps of a workflow that touch Postgres, run by this process's
//! Temporal worker on `repoplane-control`. Each is one store call; Temporal retries it on a
//! database hiccup, and a request the store refuses outright is not retried.

use std::sync::Arc;

use repoplane_flow::activities::{ReconcileFailed, RecordHeads, RecordPage, RefreshFailed};
use repoplane_proto::id::{HeadRefreshId, ReconcileId};
use repoplane_proto::inventory::{HeadTarget, UnseenRepo};
use repoplane_store::{Store, StoreError};
use temporalio_macros::activities;
use temporalio_sdk::ApplicationFailure;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

pub struct ControlActivities {
    pub store: Store,
}

fn failure(e: StoreError) -> ActivityError {
    match e {
        // The store said no: repeating the same request will not change its mind.
        StoreError::Invalid(_)
        | StoreError::NotFound(_)
        | StoreError::Conflict(_)
        | StoreError::Schema { .. } => {
            ActivityError::application(ApplicationFailure::non_retryable(e.to_string()))
        }
        other => ActivityError::application(ApplicationFailure::new(other.to_string())),
    }
}

#[activities]
impl ControlActivities {
    #[activity(definition = repoplane_flow::activities::control_defs::StartReconcile)]
    pub async fn start_reconcile(
        self: Arc<Self>,
        _ctx: ActivityContext,
        id: ReconcileId,
    ) -> Result<(), ActivityError> {
        self.store.start_reconcile(id).await.map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::RecordPage)]
    pub async fn record_page(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RecordPage,
    ) -> Result<i64, ActivityError> {
        self.store
            .record_page(input.reconcile, &input.repos)
            .await
            .map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::Unseen)]
    pub async fn unseen(
        self: Arc<Self>,
        _ctx: ActivityContext,
        id: ReconcileId,
    ) -> Result<Vec<UnseenRepo>, ActivityError> {
        self.store.unseen(id).await.map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::FinishReconcile)]
    pub async fn finish_reconcile(
        self: Arc<Self>,
        _ctx: ActivityContext,
        id: ReconcileId,
    ) -> Result<(), ActivityError> {
        self.store.finish_reconcile(id).await.map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::FailReconcile)]
    pub async fn fail_reconcile(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReconcileFailed,
    ) -> Result<(), ActivityError> {
        self.store
            .fail_reconcile(input.reconcile, &input.message)
            .await
            .map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::StartRefresh)]
    pub async fn start_refresh(
        self: Arc<Self>,
        _ctx: ActivityContext,
        id: HeadRefreshId,
    ) -> Result<Vec<HeadTarget>, ActivityError> {
        self.store.start_refresh(id).await.map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::RecordHeads)]
    pub async fn record_heads(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RecordHeads,
    ) -> Result<i64, ActivityError> {
        self.store
            .record_heads(input.refresh, &input.heads)
            .await
            .map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::FinishRefresh)]
    pub async fn finish_refresh(
        self: Arc<Self>,
        _ctx: ActivityContext,
        id: HeadRefreshId,
    ) -> Result<(), ActivityError> {
        self.store.finish_refresh(id).await.map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::control_defs::FailRefresh)]
    pub async fn fail_refresh(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RefreshFailed,
    ) -> Result<(), ActivityError> {
        self.store
            .fail_refresh(input.refresh, &input.message)
            .await
            .map_err(failure)
    }
}
