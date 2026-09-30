//! A reconcile records what exists: walk the organisation's listing page by page, upload each
//! page, confirm what the listing did not return, and complete only when all of it is in.
//! Anything short of that is a failure, so the API never marks repositories inaccessible from a
//! partial listing. It reads no HEAD: that is a head refresh's business.

use std::collections::HashSet;
use std::sync::Arc;

use futures::stream::{self, StreamExt, TryStreamExt};
use repoplane_forge::{ForgeError, HEAD_CONCURRENCY, Listed, Lister, Transport};
use repoplane_proto::worker::{ConnectionSpec, PageUpload, RepoSnapshot, UnseenRepo};
use tokio_util::sync::CancellationToken;

use crate::task::{Ctx, Failure, from_api};

/// Returns how many repositories the run found.
pub async fn walk(
    ctx: &Ctx<'_>,
    transport: Arc<dyn Transport>,
    connection: &ConnectionSpec,
    cancel: CancellationToken,
) -> Result<i64, Failure> {
    let (api, task, lease_id) = (ctx.api, ctx.task, ctx.lease_id);
    let lister: Box<dyn Lister> = repoplane_forge::lister(connection, transport, cancel)?;
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut discovered = 0i64;
    loop {
        let page = lister.page(cursor.as_deref()).await?;
        let repos: Vec<RepoSnapshot> = page.repos.into_iter().map(|l| l.snapshot).collect();
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
    Ok(found.into_iter().flatten().map(|l| l.snapshot).collect())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::fake::{Fake, listed};

    fn unseen(id: &str, name: &str) -> UnseenRepo {
        UnseenRepo {
            external_id: id.into(),
            full_name: format!("acme/{name}"),
        }
    }

    /// The listing skipped `skipped` (a deletion shifted its page) and `gone` really is gone:
    /// only `skipped` comes back, under its current name.
    #[tokio::test]
    async fn only_what_the_forge_confirms_gone_is_left_out() {
        let fake = Fake::new(HashMap::from([(
            "1".to_string(),
            listed("1", "skipped-renamed"),
        )]));
        let back = confirm(&fake, &[unseen("1", "skipped"), unseen("2", "gone")])
            .await
            .unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].full_name, "acme/skipped-renamed");
        assert_eq!(fake.asked(), vec!["get 1", "get 2"]);
    }

    #[tokio::test]
    async fn nothing_unseen_asks_nothing() {
        let fake = Fake::new(HashMap::new());
        assert!(confirm(&fake, &[]).await.unwrap().is_empty());
        assert!(fake.asked().is_empty());
    }
}
