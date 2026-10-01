//! A head refresh reads the default-branch HEAD of the repositories it was given — a scan's
//! targets, or a connection's — and reports them in batches, so a long refresh shows progress
//! and keeps its lease. It lists nothing and changes nothing but HEADs.

use std::sync::Arc;

use repoplane_forge::{ForgeError, Listed, Lister, Transport};
use repoplane_proto::worker::{ConnectionSpec, HeadTarget, HeadsUpload, ObservedHead};
use tokio_util::sync::CancellationToken;

use crate::task::{Ctx, Failure, from_api};

/// Targets read and reported per batch: GitHub answers a hundred in four GraphQL queries.
const BATCH: usize = 100;

/// Returns how many HEADs were read.
pub async fn refresh(
    ctx: &Ctx<'_>,
    transport: Arc<dyn Transport>,
    connection: &ConnectionSpec,
    targets: &[HeadTarget],
    cancel: CancellationToken,
) -> Result<i64, Failure> {
    let lister: Box<dyn Lister> = repoplane_forge::lister(connection, transport, cancel)?;
    let mut observed = 0i64;
    let mut changed = 0i64;
    for batch in targets.chunks(BATCH) {
        let heads = read(lister.as_ref(), batch).await?;
        observed += heads.len() as i64;
        changed += ctx
            .api
            .upload_heads(
                ctx.task,
                &HeadsUpload {
                    lease_id: ctx.lease_id,
                    heads,
                },
            )
            .await
            .map_err(from_api)?
            .changed;
    }
    tracing::info!(task = %ctx.task, observed, changed, "read the HEADs");
    Ok(observed)
}

/// The HEAD of each target, in order.
async fn read(
    lister: &dyn Lister,
    targets: &[HeadTarget],
) -> Result<Vec<ObservedHead>, ForgeError> {
    let listed: Vec<Listed> = targets
        .iter()
        .map(|t| Listed {
            snapshot: t.repo.clone(),
        })
        .collect();
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
        .map(|(l, head)| ObservedHead {
            external_id: l.snapshot.external_id,
            branch: l.snapshot.default_branch,
            head,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::fake::{Fake, listed};

    #[tokio::test]
    async fn reads_every_target_in_order_and_lists_nothing() {
        let fake = Fake::new(HashMap::new());
        let targets: Vec<HeadTarget> = ["1", "2"]
            .iter()
            .map(|id| HeadTarget {
                repo: listed(id, &format!("r{id}")).snapshot,
            })
            .collect();
        let heads = read(&fake, &targets).await.unwrap();
        assert_eq!(
            heads
                .iter()
                .map(|h| h.external_id.as_str())
                .collect::<Vec<_>>(),
            ["1", "2"]
        );
        assert!(
            heads
                .iter()
                .all(|h| h.head.as_deref() == Some("a".repeat(40).as_str()))
        );
        // Each HEAD names the branch it was read from, so a stale one can be told apart.
        assert!(heads.iter().all(|h| h.branch.as_deref() == Some("main")));
        assert_eq!(fake.asked(), vec!["heads 2"]);
    }
}
