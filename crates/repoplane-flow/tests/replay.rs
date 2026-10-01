//! Replays recorded workflow histories against the current workflow code. A change that would
//! make a workflow take a different path through history than it did when it ran — a step
//! added, removed or reordered without a patch — fails here, rather than in production, where
//! it would strand every run in flight.
//!
//! The histories are real runs, recorded by the end-to-end suite:
//!   REPOPLANE_RECORD_HISTORIES=1 make e2e

use repoplane_flow::{ReconcileWorkflow, RefreshHeadsWorkflow};
use temporalio_client::WorkflowHistory;
use temporalio_sdk::workflow_replayer::{WorkflowReplayer, WorkflowReplayerOptions};

#[tokio::test]
async fn recorded_histories_replay() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/histories");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no histories in {}", dir.display());
    let replayer = WorkflowReplayer::new(
        WorkflowReplayerOptions::new()
            .register_workflow::<ReconcileWorkflow>()
            .unwrap()
            .register_workflow::<RefreshHeadsWorkflow>()
            .unwrap()
            .build(),
    )
    .unwrap();
    for f in files {
        let history = WorkflowHistory::from_json(&std::fs::read(&f).unwrap())
            .unwrap_or_else(|e| panic!("{}: {e}", f.display()));
        replayer
            .replay_workflow(history)
            .await
            .unwrap_or_else(|e| panic!("{} no longer replays: {e}", f.display()));
    }
}
