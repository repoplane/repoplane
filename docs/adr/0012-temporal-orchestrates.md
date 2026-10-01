# 12. Temporal orchestrates; Postgres is product state

**Status:** New · 2026-10-01 · supersedes [0008](0008-http-task-queues.md)

## Context

Repoplane's runs are long, can fail and need to be retried, and soon will wait: for a merge, a
review, the next wave of a campaign, a person's go-ahead. That is Temporal's job, and the
official Rust SDK (`temporalio-sdk` / `temporalio-client` 1.0) supports it. A home-made Postgres
task queue (ADR 0008) covered leases and retries for a single short run, and would have had to
grow timers, signals and durable history to go further — badly.

## Decision

- **Every run is a Temporal workflow**, whose id is the run's id (`rec_…`, `hrf_…`). Workflows
  and the activities that touch Postgres run in `repoplane-api`, as a Temporal worker on task
  queue `repoplane-control`. Forge activities run in `repoplane-worker`, on the connection's own
  queue.
- **Postgres holds product state** (what the API shows: connections, inventory, runs and their
  counts). **Temporal holds process state** (which step a run is at, its retries, its timers).
  The API records a run in Postgres before starting its workflow, and a run's failure is recorded
  in Postgres before its workflow ends, so product state never stays `running`.
- Activity definitions live in `repoplane-flow`. The API and the worker implement them with
  `#[activity(definition = …)]`, so a mismatch is a compile error.
- Temporal is reached by `temporal.address` / `namespace` / `api_key_env` (a variable name).
  The client builds without `protoc` (`vendored-protox`) and carries TLS for Temporal Cloud.
- Workflow changes are checked by replaying recorded histories in `make unit`.

## Consequences

- An install needs a Temporal: the CLI dev server for development (`make up`), a Temporal
  cluster or Temporal Cloud in production.
- The worker no longer calls the repoplane API at all; it reaches Temporal and forges only.
- Runs survive any process dying. Retries back off. A non-retryable forge failure (a refused
  token) fails the run at once, with the forge's message.
- Queue-level isolation is Temporal's: workers sharing a namespace can poll each other's queues,
  so a tenant boundary is a namespace.
