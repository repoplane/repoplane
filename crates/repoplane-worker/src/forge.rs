//! The forge activities: the steps of a workflow that talk to a forge, run on the connection's
//! queue. Each one reads the connection's token from this process's environment — the name of
//! the variable is all the workflow ever carried — and does one thing: list a page, look
//! repositories up by id, or read HEADs.

use std::sync::Arc;

use futures::stream::{self, StreamExt, TryStreamExt};
use repoplane_flow::activities::{ListPage, ListedPage, Lookup, ReadHeads};
use repoplane_forge::{ForgeError, HEAD_CONCURRENCY, Listed, Lister, Transport};
use repoplane_proto::inventory::{ConnectionSpec, ObservedHead, RepoSnapshot};
use temporalio_macros::activities;
use temporalio_sdk::ApplicationFailure;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tokio_util::sync::CancellationToken;

pub struct ForgeActivities {
    pub transport: Arc<dyn Transport>,
}

/// A forge error as Temporal should treat it: retried when the forge was busy or unreachable,
/// not retried when it will never work (a bad token, a missing variable, a protocol violation).
/// The message has already been scrubbed of the token by the HTTP layer.
pub fn failure(e: ForgeError) -> ActivityError {
    let message = e.to_string();
    if e.retryable() {
        ActivityError::application(ApplicationFailure::new(message))
    } else {
        ActivityError::application(ApplicationFailure::non_retryable(message))
    }
}

impl ForgeActivities {
    fn lister(&self, c: &ConnectionSpec) -> Result<Box<dyn Lister>, ActivityError> {
        repoplane_forge::lister(c, self.transport.clone(), CancellationToken::new())
            .map_err(failure)
    }
}

#[activities]
impl ForgeActivities {
    #[activity(definition = repoplane_flow::activities::forge_defs::ListPage)]
    pub async fn list_page(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ListPage,
    ) -> Result<ListedPage, ActivityError> {
        let lister = self.lister(&input.connection)?;
        list_page(lister.as_ref(), input.cursor.as_deref())
            .await
            .map_err(failure)
    }

    #[activity(definition = repoplane_flow::activities::forge_defs::Lookup)]
    pub async fn lookup(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: Lookup,
    ) -> Result<Vec<RepoSnapshot>, ActivityError> {
        let lister = self.lister(&input.connection)?;
        let found = lookup(lister.as_ref(), &input.ids).await.map_err(failure)?;
        tracing::info!(
            connection = %input.connection.id,
            unseen = input.ids.len(),
            still_there = found.len(),
            "looked up the repositories the listing did not return"
        );
        Ok(found)
    }

    #[activity(definition = repoplane_flow::activities::forge_defs::ReadHeads)]
    pub async fn read_heads(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadHeads,
    ) -> Result<Vec<ObservedHead>, ActivityError> {
        let lister = self.lister(&input.connection)?;
        let repos: Vec<RepoSnapshot> = input.targets.into_iter().map(|t| t.repo).collect();
        read_heads(lister.as_ref(), repos).await.map_err(failure)
    }
}

pub async fn list_page(
    lister: &dyn Lister,
    cursor: Option<&str>,
) -> Result<ListedPage, ForgeError> {
    let page = lister.page(cursor).await?;
    Ok(ListedPage {
        repos: page.repos.into_iter().map(|l| l.snapshot).collect(),
        next: page.next,
    })
}

/// The repositories among `ids` that still exist in the organisation.
pub async fn lookup(lister: &dyn Lister, ids: &[String]) -> Result<Vec<RepoSnapshot>, ForgeError> {
    let lookups: Vec<_> = ids.iter().map(|id| lister.get(id)).collect();
    let found: Vec<Option<Listed>> = stream::iter(lookups)
        .buffered(HEAD_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(found.into_iter().flatten().map(|l| l.snapshot).collect())
}

/// The HEAD of each repository, in order, each naming the branch it was read from.
pub async fn read_heads(
    lister: &dyn Lister,
    repos: Vec<RepoSnapshot>,
) -> Result<Vec<ObservedHead>, ForgeError> {
    let listed: Vec<Listed> = repos
        .into_iter()
        .map(|snapshot| Listed { snapshot })
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

    /// The listing skipped `skipped` (a deletion shifted its page) and `gone` really is gone:
    /// only `skipped` comes back, under its current name.
    #[tokio::test]
    async fn lookup_returns_only_what_still_exists() {
        let fake = Fake::new(HashMap::from([(
            "1".to_string(),
            listed("1", "skipped-renamed"),
        )]));
        let back = lookup(&fake, &["1".into(), "2".into()]).await.unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!(back[0].full_name, "acme/skipped-renamed");
        assert_eq!(fake.asked(), vec!["get 1", "get 2"]);
    }

    #[tokio::test]
    async fn heads_come_back_in_order_with_their_branch() {
        let fake = Fake::new(HashMap::new());
        let repos = vec![listed("1", "a").snapshot, listed("2", "b").snapshot];
        let heads = read_heads(&fake, repos).await.unwrap();
        assert_eq!(
            heads
                .iter()
                .map(|h| h.external_id.as_str())
                .collect::<Vec<_>>(),
            ["1", "2"]
        );
        assert!(heads.iter().all(|h| h.branch.as_deref() == Some("main")));
        assert!(
            heads
                .iter()
                .all(|h| h.head.as_deref() == Some("a".repeat(40).as_str()))
        );
        assert_eq!(fake.asked(), vec!["heads 2"]);
    }
}
