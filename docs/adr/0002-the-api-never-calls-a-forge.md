# 2. The API never calls a forge

**Status:** Revised: forges may call the API · 2026-09-30

## Context

The Go draft said the API could not reach a forge in either direction. GitHub Apps, and the
Cloud/Enterprise plane built on them, need the opposite direction: a forge sends webhooks to the
API.

## Decision

The API never *initiates* a call to a forge. A forge may call the API (a webhook route, not
built yet). Handling such a call means recording it and enqueueing a task for a worker, never
calling the forge back.

## Consequences

All forge I/O stays in the worker, which is what the audit is about. `make boundaries` fails
the build if `repoplane-api` links `repoplane-forge` or an HTTP client.
