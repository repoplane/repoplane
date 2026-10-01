//! The forge activities: the steps of a workflow that talk to a forge, run on the connection's
//! queue. Each one reads the connection's token from this process's environment — the name of
//! the variable is all the workflow ever carried — and does one thing: list a page, look
//! repositories up by id, or read HEADs.

use std::sync::Arc;

use repoplane_flow::activities::{ListPage, ListedPage, Lookup, ReadHead};
use repoplane_forge::{ForgeError, Listed, Lister, Transport};
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
    ) -> Result<Option<RepoSnapshot>, ActivityError> {
        let lister = self.lister(&input.connection)?;
        let found = lookup(lister.as_ref(), &input.id).await.map_err(failure)?;
        tracing::info!(
            connection = %input.connection.id,
            id = %input.id,
            still_there = found.is_some(),
            "looked up a repository the listing did not return"
        );
        Ok(found)
    }

    #[activity(definition = repoplane_flow::activities::forge_defs::ReadHead)]
    pub async fn read_head(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReadHead,
    ) -> Result<ObservedHead, ActivityError> {
        let lister = self.lister(&input.connection)?;
        read_head(lister.as_ref(), input.target.repo)
            .await
            .map_err(failure)
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
        rest: page.rest,
    })
}

/// The repository, if it still exists in the organisation.
pub async fn lookup(lister: &dyn Lister, id: &str) -> Result<Option<RepoSnapshot>, ForgeError> {
    Ok(lister.get(id).await?.map(|l| l.snapshot))
}

/// The repository's default-branch HEAD, naming the branch it was read from. A failure is the
/// activity's: Temporal retries it if it may pass, and fails the run if it will not.
pub async fn read_head(
    lister: &dyn Lister,
    repo: RepoSnapshot,
) -> Result<ObservedHead, ForgeError> {
    let listed = [Listed { snapshot: repo }];
    let head = lister
        .heads(&listed)
        .await
        .into_iter()
        .next()
        .ok_or_else(|| ForgeError::protocol("worker", "heads() answered for no repository"))??;
    let [Listed { snapshot }] = listed;
    Ok(ObservedHead {
        external_id: snapshot.external_id,
        branch: snapshot.default_branch,
        head,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::fake::{Fake, listed};

    /// A repository the listing skipped still exists, under its current name; one that is
    /// really gone comes back as `None`.
    #[tokio::test]
    async fn lookup_returns_only_what_still_exists() {
        let fake = Fake::new(HashMap::from([(
            "1".to_string(),
            listed("1", "skipped-renamed"),
        )]));
        let back = lookup(&fake, "1").await.unwrap().unwrap();
        assert_eq!(back.full_name, "acme/skipped-renamed");
        assert!(lookup(&fake, "2").await.unwrap().is_none());
        assert_eq!(fake.asked(), vec!["get 1", "get 2"]);
    }

    #[tokio::test]
    async fn a_head_names_its_branch() {
        let fake = Fake::new(HashMap::new());
        let head = read_head(&fake, listed("1", "a").snapshot).await.unwrap();
        assert_eq!(head.external_id, "1");
        assert_eq!(head.branch.as_deref(), Some("main"));
        assert_eq!(head.head, Some("a".repeat(40)));
        assert_eq!(fake.asked(), vec!["heads 1"]);
    }

    /// A transient failure stays retryable, for Temporal to retry; a refusal does not.
    #[tokio::test]
    async fn a_failed_read_keeps_its_retryability() {
        let fake = Fake::new(HashMap::new())
            .failing("2", true)
            .failing("3", false);
        assert!(
            read_head(&fake, listed("2", "b").snapshot)
                .await
                .unwrap_err()
                .retryable()
        );
        assert!(
            !read_head(&fake, listed("3", "c").snapshot)
                .await
                .unwrap_err()
                .retryable()
        );
    }
}
