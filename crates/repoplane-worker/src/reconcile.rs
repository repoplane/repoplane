//! One reconcile: walk the organisation's listing page by page, read each page's HEADs, upload
//! it, and complete the run only when the last page is in. Anything short of that is reported
//! as a failure, so the API never marks repositories inaccessible from a partial listing.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt, TryStreamExt};
use repoplane_forge::{ForgeError, HEAD_CONCURRENCY, Listed, Lister, Transport};
use repoplane_proto::id::TaskId;
use repoplane_proto::worker::{
    Complete, ConnectionSpec, Fail, Lease, PageUpload, RepoSnapshot, UnseenRepo, Work,
};
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
        let repos = with_heads(lister.as_ref(), page.repos).await?;
        discovered += repos.len() as i64;
        if !repos.is_empty() {
            api.upload(task, &PageUpload { lease_id, repos })
                .await
                .map_err(from_api)?;
        }
        match page.next {
            None => break,
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

    // What the listing did not return is not yet gone: a listing paged by offset skips a
    // repository when another is deleted while it runs. Each one is looked up by id, and only
    // those the forge confirms gone are left for the API to mark inaccessible (ADR 0005).
    let unseen = api.unseen(task, lease_id).await.map_err(from_api)?.repos;
    if !unseen.is_empty() {
        let still_there = confirm(lister.as_ref(), &unseen).await?;
        tracing::info!(
            %task,
            unseen = unseen.len(),
            still_there = still_there.len(),
            "looked up the repositories the listing did not return"
        );
        discovered += still_there.len() as i64;
        if !still_there.is_empty() {
            api.upload(
                task,
                &PageUpload {
                    lease_id,
                    repos: still_there,
                },
            )
            .await
            .map_err(from_api)?;
        }
    }
    Ok(discovered)
}

/// Snapshots with their HEADs filled in.
async fn with_heads(
    lister: &dyn Lister,
    listed: Vec<Listed>,
) -> Result<Vec<RepoSnapshot>, ForgeError> {
    let heads = lister.heads(&listed).await?;
    if heads.len() != listed.len() {
        return Err(ForgeError::protocol(
            "worker",
            "heads() answered for a different number of repositories",
        ));
    }
    Ok(listed
        .into_iter()
        .zip(heads)
        .map(|(l, head)| {
            let mut s = l.snapshot;
            s.head = head;
            s
        })
        .collect())
}

/// The unseen repositories that still exist in the organisation, looked up one by one.
async fn confirm(
    lister: &dyn Lister,
    unseen: &[UnseenRepo],
) -> Result<Vec<RepoSnapshot>, ForgeError> {
    let lookups: Vec<_> = unseen.iter().map(|u| lister.get(&u.external_id)).collect();
    let found: Vec<Option<Listed>> = stream::iter(lookups)
        .buffered(HEAD_CONCURRENCY)
        .try_collect()
        .await?;
    with_heads(lister, found.into_iter().flatten().collect()).await
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use repoplane_forge::Page;
    use repoplane_proto::ForgeKind;

    use super::*;

    /// A forge whose truth is a map from id to repository, answering lookups from it.
    struct Fake {
        repos: HashMap<String, Listed>,
        asked: Mutex<Vec<String>>,
    }

    fn listed(id: &str, name: &str) -> Listed {
        Listed {
            snapshot: RepoSnapshot {
                external_id: id.into(),
                full_name: format!("acme/{name}"),
                name: name.into(),
                default_branch: Some("main".into()),
                archived: false,
                topics: Some(vec![]),
                head: None,
                empty: false,
                web_url: None,
            },
            locator: format!("acme/{name}"),
        }
    }

    #[async_trait]
    impl Lister for Fake {
        fn kind(&self) -> ForgeKind {
            ForgeKind::Forgejo
        }
        async fn page(&self, _cursor: Option<&str>) -> Result<Page, ForgeError> {
            unreachable!("confirm never lists")
        }
        async fn heads(&self, repos: &[Listed]) -> Result<Vec<Option<String>>, ForgeError> {
            Ok(repos.iter().map(|_| Some("a".repeat(40))).collect())
        }
        async fn get(&self, id: &str) -> Result<Option<Listed>, ForgeError> {
            self.asked.lock().unwrap().push(id.into());
            Ok(self.repos.get(id).cloned())
        }
    }

    fn unseen(id: &str, name: &str) -> UnseenRepo {
        UnseenRepo {
            external_id: id.into(),
            full_name: format!("acme/{name}"),
        }
    }

    /// The listing skipped `skipped` (a deletion shifted its page) and `gone` really is gone:
    /// only `skipped` comes back, with its HEAD, and under its current name.
    #[tokio::test]
    async fn only_what_the_forge_confirms_gone_is_left_out() {
        let fake = Fake {
            repos: HashMap::from([("1".to_string(), listed("1", "skipped-renamed"))]),
            asked: Mutex::new(Vec::new()),
        };
        let back = confirm(&fake, &[unseen("1", "skipped"), unseen("2", "gone")])
            .await
            .unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].full_name, "acme/skipped-renamed");
        assert_eq!(back[0].head.as_deref(), Some("a".repeat(40).as_str()));
        assert_eq!(*fake.asked.lock().unwrap(), vec!["1", "2"]);
    }

    #[tokio::test]
    async fn nothing_unseen_asks_nothing() {
        let fake = Fake {
            repos: HashMap::new(),
            asked: Mutex::new(Vec::new()),
        };
        assert!(confirm(&fake, &[]).await.unwrap().is_empty());
        assert!(fake.asked.lock().unwrap().is_empty());
    }
}
