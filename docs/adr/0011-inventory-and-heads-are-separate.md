# 11. Inventory and HEADs are separate operations

**Status:** Accepted · 2026-10-01

## Context

Listing an organisation costs one request per page of repositories. Reading default-branch
HEADs costs one request per repository, and most of a run's time and rate budget. Yet a HEAD
recorded at reconcile time is stale by the time a scan uses it. A scan has to resolve the exact
commit it checks out anyway, and a scan of seventeen repositories should not pay for the other
hundred.

## Decision

- **`connection reconcile` records what exists:** id, name, default branch, topics, archived,
  empty, and the adapter's locator. It reads no HEAD. When the default branch changes, it clears
  the recorded HEAD and its observation time, because they described another branch.
- **A head refresh reads HEADs:** `repo refresh-heads`, or `POST /v1/connections/{c}/head-refreshes`
  with an optional topic/archived filter. It reads the default-branch HEAD of the repositories it
  targets, which are fixed when it is requested. It records each HEAD with `head_observed_at`
  and reports *targeted / observed / changed*. A scan will run one on its own targets before
  deciding which repositories changed.
- **Each is a Temporal workflow** (ADR 0012), with the same retry and failure handling.

## Consequences

A fresh reconcile leaves the HEAD column empty (`—`) until a refresh has read it. A push makes
no reconcile record a change; a head refresh does. Each operation has one reason to fail, and
one to retry.
