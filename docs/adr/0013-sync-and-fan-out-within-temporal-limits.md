# 13. `connection sync` composes the runs; fan-out stays within Temporal's limits

**Status:** New · 2026-10-05

## Context

`connection reconcile` and `repo refresh-heads` are separate runs (ADR 0011). A scan or a
campaign refreshes only its own targets, and later a push webhook will update a single HEAD. Day
to day, though, an operator wants an inventory that is up to date, HEADs included, and had to
run two commands and wait on each.

Every page, lookup and HEAD read is its own activity, all scheduled at once. The worker's
activity slots bound how many run at a time; the workflow sizes no batches. Run on a
2,139-repository GitHub organisation, the head refresh scheduled 2,139 activities at once and
hit Temporal's limit of **2,000 pending activities per workflow**. That is the server default,
and Temporal Cloud's. Temporal refused the workflow task and retried it forever, so the run
stayed `running` with nothing recording why.

## Decision

- **`connection sync`** (`POST /v1/connections/{c}/syncs`) runs a reconcile, then a head refresh
  of every repository it left active. Each runs as a child workflow under its own run id, so each
  stays a run of its own, readable alone. A sync is refused while a reconcile of the connection is
  already in flight. The separate commands stay, as the atomic steps.
- **Fan-out above 1,000 goes to child workflows.** A head refresh's targets and a reconcile's
  lookups by id are split into child workflows of at most 1,000 each (`SLICE`), and the children
  run at once. Within a child, every activity is scheduled at once. The 1,000 keeps each child
  under the 2,000 limit and near 4,000 history events; it is not a speed setting. Throughput is
  still the worker's slots.
- **The API settles runs their workflow will never record.** When it serves a run still in flight,
  it asks Temporal about the workflow (at most every 5 s while a request waits). It records the
  run as failed, with the reason, when:
  - the workflow was closed without completing (terminated, timed out, failed, canceled);
  - the workflow is gone;
  - its workflow task has failed 5 times in a row. The API terminates such a workflow first,
    because Temporal would retry it forever.

  A failed sync fails its step in flight too, since Temporal ends a child when its parent closes.

## Consequences

Measured on the 2,139-repository sandbox (2026-10-05, debug build, 32 worker slots):
- a sync takes 40 s: reconcile 2.8 s, head refresh 37.4 s across 3 slices;
- the slice histories hold about 4,000 events per 1,000 repositories.

The listing is the next limit. A reconcile's history carries every page it recorded, about
1.8 MB for 2,139 repositories, against Temporal's 50 MB per workflow. Organisations past a few
tens of thousands of repositories will need the listing in child workflows or continue-as-new.

Update 2026-10-06: the worker's default rose to 64 slots, which halves HEAD reading on GitHub and
GitLab (a GitHub sync of 2,139 repositories went from about 40 s to about 25 s).
