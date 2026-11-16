//! The forge activities: the steps of a workflow that talk to a forge, run on the connection's
//! queue. Each one takes the token this worker holds for the connection's forge — the workflow
//! carries no credential, not even the name of one — and does one thing: list a page, look
//! repositories up by id, or read HEADs.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use repoplane_flow::activities::{
    FORGE_ATTEMPTS, ListPage, ListedPage, Lookup, MAX_RATE_LIMIT_WAIT, ReadHead,
};
use repoplane_forge::{ForgeError, Listed, Lister, Transport};
use repoplane_proto::inventory::{ConnectionSpec, ObservedHead, RepoSnapshot};
use temporalio_macros::activities;
use temporalio_sdk::ApplicationFailure;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tokio_util::sync::CancellationToken;

use crate::tokens::Tokens;

pub struct ForgeActivities {
    pub transport: Arc<dyn Transport>,
    /// The forge tokens this worker holds; each connection gets the one meant for its forge.
    pub tokens: Arc<Tokens>,
}

/// A forge error as Temporal should treat it on `attempt`, the same on every forge:
/// - the forge says it is rate limiting: retried after the wait it asked for (a minute when it
///   does not say), up to
///   [`RATE_LIMITED_ATTEMPTS`](repoplane_flow::activities::RATE_LIMITED_ATTEMPTS), since retrying
///   within seconds lands in the same window and only spends the quota again;
/// - the forge was busy or unreachable: retried with Temporal's backoff, up to
///   [`FORGE_ATTEMPTS`];
/// - it will never work (a bad token, no token for the forge, a protocol violation): not
///   retried.
///
/// The message has already been scrubbed of the token by the HTTP layer.
pub fn failure(e: ForgeError, attempt: u32) -> ActivityError {
    let message = e.to_string();
    let failure = match e.rate_limit_wait() {
        Some(wait) => ApplicationFailure::builder(message)
            .next_retry_delay(wait.min(MAX_RATE_LIMIT_WAIT))
            .build(),
        None if e.retryable() && attempt < FORGE_ATTEMPTS => ApplicationFailure::new(message),
        None => ApplicationFailure::non_retryable(message),
    };
    ActivityError::application(failure)
}

/// How often a forge call tells Temporal this worker is still on it. The workflow's heartbeat
/// timeout is a few of these, so a worker that died is noticed within that timeout, not after
/// the call's whole start-to-close timeout.
const HEARTBEAT_EVERY: Duration = Duration::from_secs(5);

/// Runs a forge call while heartbeating, and stops it when Temporal cancels the activity: its
/// workflow ended, or this worker is stopping. It then answers cancelled, which Temporal turns
/// into a retry on another worker when this one was stopping.
async fn alive<T>(
    ctx: &ActivityContext,
    cancel: &CancellationToken,
    work: impl Future<Output = Result<T, ActivityError>>,
) -> Result<T, ActivityError> {
    tokio::pin!(work);
    let mut beat = tokio::time::interval(HEARTBEAT_EVERY);
    loop {
        tokio::select! {
            done = &mut work => return done,
            _ = ctx.cancelled() => {
                cancel.cancel();
                return Err(ActivityError::cancelled());
            }
            _ = beat.tick() => {
                let _ = ctx.record_heartbeat(()).await;
            }
        }
    }
}

impl ForgeActivities {
    fn lister(
        &self,
        c: &ConnectionSpec,
        attempt: u32,
        cancel: CancellationToken,
    ) -> Result<Box<dyn Lister>, ActivityError> {
        let token = self
            .tokens
            .for_connection(c)
            .map_err(|e| failure(e, attempt))?;
        repoplane_forge::lister(c, token, self.transport.clone(), cancel)
            .map_err(|e| failure(e, attempt))
    }
}

#[activities]
impl ForgeActivities {
    #[activity(definition = repoplane_flow::activities::forge_defs::ListPage)]
    pub async fn list_page(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: ListPage,
    ) -> Result<ListedPage, ActivityError> {
        let (attempt, cancel) = (ctx.info().attempt, CancellationToken::new());
        let lister = self.lister(&input.connection, attempt, cancel.clone())?;
        alive(&ctx, &cancel, async {
            list_page(lister.as_ref(), input.cursor.as_deref())
                .await
                .map_err(|e| failure(e, attempt))
        })
        .await
    }

    #[activity(definition = repoplane_flow::activities::forge_defs::Lookup)]
    pub async fn lookup(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: Lookup,
    ) -> Result<Option<RepoSnapshot>, ActivityError> {
        let (attempt, cancel) = (ctx.info().attempt, CancellationToken::new());
        let lister = self.lister(&input.connection, attempt, cancel.clone())?;
        let found = alive(&ctx, &cancel, async {
            lookup(lister.as_ref(), &input.id)
                .await
                .map_err(|e| failure(e, attempt))
        })
        .await?;
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
        ctx: ActivityContext,
        input: ReadHead,
    ) -> Result<ObservedHead, ActivityError> {
        let (attempt, cancel) = (ctx.info().attempt, CancellationToken::new());
        let lister = self.lister(&input.connection, attempt, cancel.clone())?;
        alive(&ctx, &cancel, async {
            read_head(lister.as_ref(), input.target.repo)
                .await
                .map_err(|e| failure(e, attempt))
        })
        .await
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

    fn decided(e: ForgeError, attempt: u32) -> (bool, Option<std::time::Duration>) {
        let ActivityError::Application(f) = failure(e, attempt) else {
            panic!("a forge error becomes an application failure");
        };
        (f.is_non_retryable(), f.next_retry_delay())
    }

    /// The same rules on every forge: a rate limit waits as long as the forge asked, past the
    /// attempts an ordinary failure gets; a busy forge stops after them, as it always did; what
    /// will never work is not retried.
    #[test]
    fn a_rate_limit_waits_and_outlasts_ordinary_failures() {
        use std::time::Duration;
        let limited = || ForgeError::Status {
            forge: "t",
            class: repoplane_forge::Class::RateLimited,
            status: 429,
            method: "git ls-remote".into(),
            path: "/o/r.git".into(),
            body: "remote: Rate limit exceeded".into(),
        };
        let busy = || ForgeError::Transport {
            forge: "t",
            method: "GET".into(),
            path: "/x".into(),
            message: "connection reset".into(),
        };
        assert_eq!(
            decided(limited(), 1),
            (false, Some(Duration::from_secs(60)))
        );
        assert_eq!(
            decided(limited(), FORGE_ATTEMPTS + 10),
            (false, Some(Duration::from_secs(60)))
        );
        // A reset an hour and a half away waits at most an hour.
        let reset = ForgeError::RateLimitExceeded {
            forge: "t",
            method: "GET".into(),
            path: "/x".into(),
            wait: Duration::from_secs(90 * 60),
        };
        assert_eq!(decided(reset, 1), (false, Some(MAX_RATE_LIMIT_WAIT)));
        assert_eq!(decided(busy(), 1), (false, None));
        assert_eq!(decided(busy(), FORGE_ATTEMPTS - 1), (false, None));
        assert_eq!(decided(busy(), FORGE_ATTEMPTS), (true, None));
        assert_eq!(
            decided(ForgeError::Config("no token".into()), 1),
            (true, None)
        );
    }

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
