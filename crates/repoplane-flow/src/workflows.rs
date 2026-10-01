//! The workflows. Each step is an activity on an explicit queue: Postgres on the control queue,
//! the forge on the connection's. A step that fails for good — retries exhausted, or a failure
//! the forge activity declared non-retryable — fails the run, and the run's record says why
//! before the workflow ends, so product state never stays `running`.

use std::collections::BTreeSet;
use std::time::Duration;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_workflow::common::error::{ActivityExecutionError, ApplicationFailure};
use temporalio_workflow::{RetryPolicy, WorkflowTermination, join_all};

use repoplane_proto::inventory::ConnectionSpec;

use crate::activities::{
    Control, Forge, ListPage, Lookup, ReadHead, ReconcileFailed, RecordHeads, RecordPage,
    RefreshFailed,
};
use crate::{CONTROL_QUEUE, ReconcileInput, RefreshHeadsInput};

// No batch sizes and no waves: the workflow schedules every page, lookup and HEAD read at once,
// and the worker's activity slots (`concurrency`) decide how many run at a time. Each is retried
// on its own by the forge retry policy below.

/// A forge call. Its HTTP layer already waits out rate limits for up to ten minutes per
/// request, so an attempt may take that long; between attempts Temporal backs off. A failure the
/// worker marks non-retryable (a bad token, a protocol violation) is not retried at all.
fn forge(c: &ConnectionSpec) -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(15 * 60))
        .task_queue(c.queue.clone())
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(60))
                .maximum_attempts(8)
                .build(),
        )
        .build()
}

/// A Postgres call in the API's control worker.
fn control() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(60))
        .task_queue(CONTROL_QUEUE.to_string())
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(30))
                .maximum_attempts(10)
                .build(),
        )
        .build()
}

/// Why a run stopped before finishing.
enum Stop {
    /// A step failed for good; the run is recorded as failed with this message.
    Failed(String),
}

impl From<ActivityExecutionError> for Stop {
    fn from(e: ActivityExecutionError) -> Self {
        Stop::Failed(describe(&e))
    }
}

/// The message of a failure and of every cause under it: the top level only says that an
/// activity failed, and the forge's own words are underneath.
fn describe(e: &ActivityExecutionError) -> String {
    let Some(mut f) = e.failure() else {
        return e.to_string();
    };
    let mut parts = vec![f.message.clone()];
    while let Some(cause) = f.cause.as_deref() {
        if !cause.message.is_empty() && !parts.contains(&cause.message) {
            parts.push(cause.message.clone());
        }
        f = cause;
    }
    parts.join(": ")
}

fn fail(message: String) -> WorkflowTermination {
    ApplicationFailure::non_retryable(message).into()
}

/// `connection reconcile`: brings the inventory in sync with the organisation. Lists every
/// page, records it, looks up by id every known repository the listing did not return, and
/// only then marks the ones the forge confirms gone (ADR 0005).
#[workflow]
#[derive(Default)]
pub struct ReconcileWorkflow;

#[workflow_methods]
impl ReconcileWorkflow {
    #[run(name = "reconcile")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: ReconcileInput) -> WorkflowResult<()> {
        match reconcile(ctx, &input).await {
            Ok(()) => Ok(()),
            Err(Stop::Failed(message)) => {
                let _ = ctx
                    .execute_activity(
                        Control::fail_reconcile,
                        ReconcileFailed {
                            reconcile: input.reconcile,
                            message: message.clone(),
                        },
                        control(),
                    )
                    .await;
                Err(fail(message))
            }
        }
    }
}

async fn reconcile(
    ctx: &mut WorkflowContext<ReconcileWorkflow>,
    input: &ReconcileInput,
) -> Result<(), Stop> {
    let c = &input.connection;
    ctx.execute_activity(Control::start_reconcile, input.reconcile, control())
        .await?;
    let first = ctx
        .execute_activity(
            Forge::list_page,
            ListPage {
                connection: c.clone(),
                cursor: None,
            },
            forge(c),
        )
        .await?;
    let (next, rest) = (first.next.clone(), first.rest.clone());
    record(ctx, input, first.repos).await?;
    match rest {
        // The forge said how many pages there are: fetch them all at once, then record them one
        // after another (pages recorded at once would contend for the same rows).
        Some(cursors) => {
            let pages = join_all(cursors.into_iter().map(|cursor| {
                ctx.execute_activity(
                    Forge::list_page,
                    ListPage {
                        connection: c.clone(),
                        cursor: Some(cursor),
                    },
                    forge(c),
                )
            }))
            .await;
            for page in pages {
                record(ctx, input, page?.repos).await?;
            }
        }
        // It did not: follow the listing one page at a time.
        None => {
            let mut cursor = next;
            let mut cursors = BTreeSet::new();
            while let Some(current) = cursor {
                if !cursors.insert(current.clone()) {
                    return Err(Stop::Failed(format!(
                        "{}: the listing returned a page it had already returned",
                        c.forge
                    )));
                }
                let page = ctx
                    .execute_activity(
                        Forge::list_page,
                        ListPage {
                            connection: c.clone(),
                            cursor: Some(current),
                        },
                        forge(c),
                    )
                    .await?;
                cursor = page.next.clone();
                record(ctx, input, page.repos).await?;
            }
        }
    }
    // What the listing did not return is not yet gone: a listing paged by offset skips a
    // repository when another is deleted while it runs. Each is looked up by id; the ones still
    // there are recorded, and only the rest are left for `finish_reconcile` to mark.
    let unseen = ctx
        .execute_activity(Control::unseen, input.reconcile, control())
        .await?;
    let found = join_all(unseen.into_iter().map(|u| {
        ctx.execute_activity(
            Forge::lookup,
            Lookup {
                connection: c.clone(),
                id: u.external_id,
            },
            forge(c),
        )
    }))
    .await;
    let mut still_there = Vec::new();
    for f in found {
        still_there.extend(f?);
    }
    record(ctx, input, still_there).await?;
    ctx.execute_activity(Control::finish_reconcile, input.reconcile, control())
        .await?;
    Ok(())
}

/// Records listed repositories, if there are any.
async fn record(
    ctx: &WorkflowContext<ReconcileWorkflow>,
    input: &ReconcileInput,
    repos: Vec<repoplane_proto::inventory::RepoSnapshot>,
) -> Result<(), Stop> {
    if !repos.is_empty() {
        ctx.execute_activity(
            Control::record_page,
            RecordPage {
                reconcile: input.reconcile,
                repos,
            },
            control(),
        )
        .await?;
    }
    Ok(())
}

/// `repo refresh-heads`: reads the default-branch HEAD of the repositories the refresh was
/// given, in batches, and records them. Lists nothing, changes nothing but HEADs.
#[workflow]
#[derive(Default)]
pub struct RefreshHeadsWorkflow;

#[workflow_methods]
impl RefreshHeadsWorkflow {
    #[run(name = "refresh-heads")]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: RefreshHeadsInput,
    ) -> WorkflowResult<()> {
        match refresh(ctx, &input).await {
            Ok(()) => Ok(()),
            Err(Stop::Failed(message)) => {
                let _ = ctx
                    .execute_activity(
                        Control::fail_refresh,
                        RefreshFailed {
                            refresh: input.refresh,
                            message: message.clone(),
                        },
                        control(),
                    )
                    .await;
                Err(fail(message))
            }
        }
    }
}

async fn refresh(
    ctx: &mut WorkflowContext<RefreshHeadsWorkflow>,
    input: &RefreshHeadsInput,
) -> Result<(), Stop> {
    let c = &input.connection;
    let targets = ctx
        .execute_activity(Control::start_refresh, input.refresh, control())
        .await?;
    let reads = join_all(targets.into_iter().map(|target| {
        ctx.execute_activity(
            Forge::read_head,
            ReadHead {
                connection: c.clone(),
                target,
            },
            forge(c),
        )
    }))
    .await;
    let heads = reads.into_iter().collect::<Result<Vec<_>, _>>()?;
    if !heads.is_empty() {
        ctx.execute_activity(
            Control::record_heads,
            RecordHeads {
                refresh: input.refresh,
                heads,
            },
            control(),
        )
        .await?;
    }
    ctx.execute_activity(Control::finish_refresh, input.refresh, control())
        .await?;
    Ok(())
}
