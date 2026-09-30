//! One reconcile: walk the organisation's listing page by page, read each page's HEADs, upload
//! it, and complete the run only when the last page is in. Anything short of that is reported
//! as a failure, so the API never marks repositories inaccessible from a partial listing.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use repoplane_forge::{ForgeError, Lister, Transport};
use repoplane_proto::id::TaskId;
use repoplane_proto::worker::{Complete, ConnectionSpec, Fail, Lease, PageUpload, Work};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::{Api, is_lease_lost};

pub async fn run(
    api: Arc<Api>,
    transport: Arc<dyn Transport>,
    lease: Lease,
    shutdown: CancellationToken,
) {
    let Work::Reconcile {
        reconcile,
        connection,
    } = lease.work;
    let task = lease.task;
    let lease_id = lease.lease_id;
    tracing::info!(%task, %reconcile, forge = %connection.forge, org = %connection.org, "reconcile started");

    // Cancelled on shutdown, and when the lease is lost: either way, stop talking to the forge.
    let cancel = shutdown.child_token();
    let beat = tokio::spawn(heartbeat(
        api.clone(),
        task,
        lease_id,
        Duration::from_secs(lease.ttl_secs / 3).max(Duration::from_secs(1)),
        cancel.clone(),
    ));

    let outcome = walk(&api, transport, task, lease_id, &connection, cancel.clone()).await;
    cancel.cancel();
    let _ = beat.await;

    let result = match outcome {
        Ok(discovered) => api
            .complete(
                task,
                &Complete {
                    lease_id,
                    complete_listing: true,
                    discovered,
                },
            )
            .await
            .map(|_| tracing::info!(%task, %reconcile, discovered, "reconcile succeeded")),
        Err(Failure::LeaseLost) => {
            tracing::warn!(%task, "lease lost; another worker has the task");
            Ok(())
        }
        Err(Failure::Report { message, retryable }) => {
            let retryable = retryable || shutdown.is_cancelled();
            tracing::warn!(%task, %reconcile, retryable, "reconcile failed: {message}");
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

enum Failure {
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

fn from_api(e: anyhow::Error) -> Failure {
    if is_lease_lost(&e) {
        Failure::LeaseLost
    } else {
        Failure::Report {
            message: format!("{e:#}"),
            retryable: true,
        }
    }
}

async fn walk(
    api: &Api,
    transport: Arc<dyn Transport>,
    task: TaskId,
    lease_id: Uuid,
    connection: &ConnectionSpec,
    cancel: CancellationToken,
) -> Result<i64, Failure> {
    let lister: Box<dyn Lister> = repoplane_forge::lister(connection, transport, cancel)?;
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut discovered = 0i64;
    loop {
        let page = lister.page(cursor.as_deref()).await?;
        let heads = lister.heads(&page.repos).await?;
        if heads.len() != page.repos.len() {
            return Err(ForgeError::protocol(
                "worker",
                "heads() answered for a different number of repositories",
            )
            .into());
        }
        let repos: Vec<_> = page
            .repos
            .into_iter()
            .zip(heads)
            .map(|(l, head)| {
                let mut s = l.snapshot;
                s.head = head;
                s
            })
            .collect();
        discovered += repos.len() as i64;
        if !repos.is_empty() {
            api.upload(task, &PageUpload { lease_id, repos })
                .await
                .map_err(from_api)?;
        }
        match page.next {
            None => return Ok(discovered),
            Some(next) => {
                if !seen_cursors.insert(next.clone()) {
                    return Err(ForgeError::protocol(
                        lister.kind().as_str(),
                        "the listing returned a page it had already returned",
                    )
                    .into());
                }
                cursor = Some(next);
            }
        }
    }
}

/// Keeps the lease alive while pages are slow (a rate-limit pause can last minutes), and
/// cancels the run as soon as the API says the lease went to another worker.
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
