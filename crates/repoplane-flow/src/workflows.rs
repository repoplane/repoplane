//! The workflows. Each step is an activity on an explicit queue: Postgres on the control queue,
//! the forge on the connection's. A step that fails for good — retries exhausted, or a failure
//! the forge activity declared non-retryable — fails the run, and the run's record says why
//! before the workflow ends, so product state never stays `running`.

use std::collections::BTreeSet;
use std::time::Duration;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, ChildWorkflowOptions, WorkflowContext, WorkflowResult};
use temporalio_workflow::common::error::{
    ActivityExecutionError, ApplicationFailure, ChildWorkflowExecutionError,
    ChildWorkflowStartError,
};
use temporalio_workflow::common::protos::temporal::api::failure::v1::Failure;
use temporalio_workflow::{RetryPolicy, WorkflowTermination, join_all};

use repoplane_proto::inventory::ConnectionSpec;

use crate::activities::{
    Control, Forge, ListPage, Lookup, ReadHead, ReconcileFailed, RecordHeads, RecordPage,
    RefreshFailed, SyncFailed,
};
use crate::{
    CONTROL_QUEUE, LookupsInput, ReadHeadsInput, ReconcileInput, RefreshHeadsInput, SyncInput,
    workflow_id,
};

// No batch sizes for speed: a workflow schedules its pages, lookups and HEAD reads at once, and
// the worker's activity slots (`concurrency`) decide how many run at a time. Each is retried on
// its own by the forge retry policy below.

/// Most activities one workflow schedules at once. Temporal refuses more than 2,000 pending
/// activities per workflow (its server default, and Temporal Cloud's): a refresh of 2,139
/// repositories scheduled at once failed its workflow task, which Temporal then retried forever.
/// Lookups and HEAD reads beyond it go to child workflows of this many each; it is a limit to
/// stay under, not a speed setting.
const SLICE: usize = 1_000;

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

impl Stop {
    fn message(self) -> String {
        let Stop::Failed(m) = self;
        m
    }
}

impl From<ActivityExecutionError> for Stop {
    fn from(e: ActivityExecutionError) -> Self {
        Stop::Failed(describe(e.failure(), &e))
    }
}

impl From<ChildWorkflowExecutionError> for Stop {
    fn from(e: ChildWorkflowExecutionError) -> Self {
        Stop::Failed(describe(e.failure(), &e))
    }
}

impl From<ChildWorkflowStartError> for Stop {
    fn from(e: ChildWorkflowStartError) -> Self {
        Stop::Failed(e.to_string())
    }
}

/// The message of a failure and of every cause under it: the top level only says that an
/// activity (or a child workflow) failed, and the forge's own words are underneath.
fn describe(top: Option<&Failure>, e: &dyn std::fmt::Display) -> String {
    let Some(mut f) = top else {
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
            for wave in cursors.chunks(SLICE) {
                let pages = join_all(wave.iter().map(|cursor| {
                    ctx.execute_activity(
                        Forge::list_page,
                        ListPage {
                            connection: c.clone(),
                            cursor: Some(cursor.clone()),
                        },
                        forge(c),
                    )
                }))
                .await;
                for page in pages {
                    record(ctx, input, page?.repos).await?;
                }
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
    let ids: Vec<String> = unseen.into_iter().map(|u| u.external_id).collect();
    let slices = ids.chunks(SLICE).enumerate().map(|(k, ids)| {
        ctx.start_child_workflow(
            LookupsWorkflow::run,
            LookupsInput {
                reconcile: input.reconcile,
                connection: c.clone(),
                ids: ids.to_vec(),
            },
            ChildWorkflowOptions::workflow_id(format!("{}-lookups-{}", input.reconcile, k + 1)),
        )
    });
    let mut running = Vec::new();
    for started in join_all(slices).await {
        running.push(started?.result());
    }
    for done in join_all(running).await {
        done?;
    }
    ctx.execute_activity(Control::finish_reconcile, input.reconcile, control())
        .await?;
    Ok(())
}

/// Records listed repositories, if there are any.
async fn record<W>(
    ctx: &WorkflowContext<W>,
    input: &ReconcileInput,
    repos: Vec<repoplane_proto::inventory::RepoSnapshot>,
) -> Result<(), Stop> {
    record_for(ctx, input.reconcile, repos).await
}

async fn record_for<W>(
    ctx: &WorkflowContext<W>,
    reconcile: repoplane_proto::id::ReconcileId,
    repos: Vec<repoplane_proto::inventory::RepoSnapshot>,
) -> Result<(), Stop> {
    if !repos.is_empty() {
        ctx.execute_activity(
            Control::record_page,
            RecordPage { reconcile, repos },
            control(),
        )
        .await?;
    }
    Ok(())
}

/// One slice of a reconcile's lookups (at most [`SLICE`]): each id looked up at once, and the
/// repositories still there recorded. The parent records the failure if it fails.
#[workflow]
#[derive(Default)]
pub struct LookupsWorkflow;

#[workflow_methods]
impl LookupsWorkflow {
    #[run(name = "reconcile-lookups")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: LookupsInput) -> WorkflowResult<()> {
        lookups(ctx, input).await.map_err(|Stop::Failed(m)| fail(m))
    }
}

async fn lookups(
    ctx: &mut WorkflowContext<LookupsWorkflow>,
    input: LookupsInput,
) -> Result<(), Stop> {
    let c = &input.connection;
    let found = join_all(input.ids.iter().map(|id| {
        ctx.execute_activity(
            Forge::lookup,
            Lookup {
                connection: c.clone(),
                id: id.clone(),
            },
            forge(c),
        )
    }))
    .await;
    let mut still_there = Vec::new();
    for f in found {
        still_there.extend(f?);
    }
    record_for(ctx, input.reconcile, still_there).await
}

/// `repo refresh-heads`: reads the default-branch HEAD of the repositories the refresh was
/// given and records them, in child workflows of at most [`SLICE`] repositories that all run at
/// once. Lists nothing, changes nothing but HEADs.
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
    let slices = targets.chunks(SLICE).enumerate().map(|(k, targets)| {
        ctx.start_child_workflow(
            ReadHeadsWorkflow::run,
            ReadHeadsInput {
                refresh: input.refresh,
                connection: c.clone(),
                targets: targets.to_vec(),
            },
            ChildWorkflowOptions::workflow_id(format!("{}-heads-{}", input.refresh, k + 1)),
        )
    });
    let mut running = Vec::new();
    for started in join_all(slices).await {
        running.push(started?.result());
    }
    for done in join_all(running).await {
        done?;
    }
    ctx.execute_activity(Control::finish_refresh, input.refresh, control())
        .await?;
    Ok(())
}

/// One slice of a head refresh (at most [`SLICE`] targets): every HEAD read at once, then
/// recorded. The parent records the failure if it fails.
#[workflow]
#[derive(Default)]
pub struct ReadHeadsWorkflow;

#[workflow_methods]
impl ReadHeadsWorkflow {
    #[run(name = "refresh-heads-slice")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: ReadHeadsInput) -> WorkflowResult<()> {
        read_heads(ctx, input)
            .await
            .map_err(|Stop::Failed(m)| fail(m))
    }
}

async fn read_heads(
    ctx: &mut WorkflowContext<ReadHeadsWorkflow>,
    input: ReadHeadsInput,
) -> Result<(), Stop> {
    let c = &input.connection;
    let reads = join_all(input.targets.into_iter().map(|target| {
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
    Ok(())
}

/// `connection sync`: a reconcile, then a head refresh of every repository it left active, each
/// as a child workflow under its own run id, so each step is a run of its own too, readable and
/// recorded as when it is run alone. A step that fails has recorded why; the sync records it
/// again, as the reason the sync stopped.
#[workflow]
#[derive(Default)]
pub struct SyncWorkflow;

#[workflow_methods]
impl SyncWorkflow {
    #[run(name = "sync")]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: SyncInput) -> WorkflowResult<()> {
        match sync(ctx, &input).await {
            Ok(()) => Ok(()),
            Err(Stop::Failed(message)) => {
                let _ = ctx
                    .execute_activity(
                        Control::fail_sync,
                        SyncFailed {
                            sync: input.sync,
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

async fn sync(ctx: &mut WorkflowContext<SyncWorkflow>, input: &SyncInput) -> Result<(), Stop> {
    let c = &input.connection;
    ctx.execute_activity(Control::start_sync, input.sync, control())
        .await?;
    ctx.start_child_workflow(
        ReconcileWorkflow::run,
        ReconcileInput {
            reconcile: input.reconcile,
            connection: c.clone(),
        },
        ChildWorkflowOptions::workflow_id(workflow_id(&input.reconcile)),
    )
    .await?
    .result()
    .await
    .map_err(|e| Stop::Failed(format!("the reconcile failed: {}", Stop::from(e).message())))?;
    let heads = ctx
        .execute_activity(Control::sync_heads, input.sync, control())
        .await?;
    if heads.queued {
        ctx.start_child_workflow(
            RefreshHeadsWorkflow::run,
            RefreshHeadsInput {
                refresh: heads.refresh,
                connection: c.clone(),
            },
            ChildWorkflowOptions::workflow_id(workflow_id(&heads.refresh)),
        )
        .await?
        .result()
        .await
        .map_err(|e| {
            Stop::Failed(format!(
                "the head refresh failed: {}",
                Stop::from(e).message()
            ))
        })?;
    }
    ctx.execute_activity(Control::finish_sync, input.sync, control())
        .await?;
    Ok(())
}
