//! Running a leased task: keep the lease alive, do the work, then complete or fail it. What the
//! work is — a reconcile or a head refresh — is the only thing that differs.

use std::sync::Arc;
use std::time::Duration;

use repoplane_forge::{ForgeError, Transport};
use repoplane_proto::id::TaskId;
use repoplane_proto::worker::{Complete, Fail, Lease, Work};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::{Api, is_lease_lost};
use crate::{heads, reconcile};

pub async fn run(
    api: Arc<Api>,
    transport: Arc<dyn Transport>,
    lease: Lease,
    shutdown: CancellationToken,
) {
    let task = lease.task;
    let lease_id = lease.lease_id;
    // Cancelled on shutdown, and when the lease is lost: either way, stop talking to the forge.
    let cancel = shutdown.child_token();
    let beat = tokio::spawn(heartbeat(
        api.clone(),
        task,
        lease_id,
        Duration::from_secs(lease.ttl_secs / 3).max(Duration::from_secs(1)),
        cancel.clone(),
    ));

    let (what, outcome) = match &lease.work {
        Work::Reconcile {
            reconcile,
            connection,
        } => {
            tracing::info!(%task, %reconcile, forge = %connection.forge, org = %connection.org, "reconcile started");
            let ctx = Ctx {
                api: &api,
                task,
                lease_id,
            };
            (
                "reconcile",
                reconcile::walk(&ctx, transport, connection, cancel.clone()).await,
            )
        }
        Work::RefreshHeads {
            refresh,
            connection,
            targets,
        } => {
            tracing::info!(%task, %refresh, forge = %connection.forge, targets = targets.len(), "head refresh started");
            let ctx = Ctx {
                api: &api,
                task,
                lease_id,
            };
            (
                "head refresh",
                heads::refresh(&ctx, transport, connection, targets, cancel.clone()).await,
            )
        }
    };
    cancel.cancel();
    let _ = beat.await;

    let result = match outcome {
        Ok(count) => api
            .complete(
                task,
                &Complete {
                    lease_id,
                    complete_listing: true,
                    discovered: count,
                },
            )
            .await
            .map(|_| tracing::info!(%task, count, "{what} succeeded")),
        Err(Failure::LeaseLost) => {
            tracing::warn!(%task, "lease lost; another worker has the task");
            Ok(())
        }
        Err(Failure::Report { message, retryable }) => {
            let retryable = retryable || shutdown.is_cancelled();
            tracing::warn!(%task, retryable, "{what} failed: {message}");
            api.fail(
                task,
                &Fail {
                    lease_id,
                    message,
                    retryable,
                },
            )
            .await
        }
    };
    if let Err(e) = result
        && !is_lease_lost(&e)
    {
        tracing::error!(%task, "could not report the outcome: {e:#}");
    }
}

/// What a piece of work needs to report back.
pub struct Ctx<'a> {
    pub api: &'a Api,
    pub task: TaskId,
    pub lease_id: Uuid,
}

pub enum Failure {
    LeaseLost,
    Report { message: String, retryable: bool },
}

impl From<ForgeError> for Failure {
    fn from(e: ForgeError) -> Self {
        Failure::Report {
            retryable: e.retryable(),
            message: e.to_string(),
        }
    }
}

pub fn from_api(e: anyhow::Error) -> Failure {
    if is_lease_lost(&e) {
        Failure::LeaseLost
    } else {
        Failure::Report {
            message: format!("{e:#}"),
            retryable: true,
        }
    }
}

/// Keeps the lease alive while the forge is slow (a rate-limit pause can last minutes), and
/// cancels the work as soon as the API says the lease went to another worker.
async fn heartbeat(
    api: Arc<Api>,
    task: TaskId,
    lease_id: Uuid,
    every: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(every) => {},
            _ = cancel.cancelled() => return,
        }
        match api.heartbeat(task, lease_id).await {
            Ok(()) => {}
            Err(e) if is_lease_lost(&e) => {
                tracing::warn!(%task, "lease lost during the run; stopping");
                cancel.cancel();
                return;
            }
            Err(e) => tracing::warn!(%task, "heartbeat failed: {e:#}"),
        }
    }
}
