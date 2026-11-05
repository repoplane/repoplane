# 2. The API never calls a forge

**Status:** Accepted · 2026-09-30

## Context

Calling a forge takes the forge's credential and its network I/O into the component that holds
every tenant's state. GitHub Apps, and the Cloud/Enterprise plane built on them, still need one
direction: a forge sends webhooks to the API.

## Decision

The API never *initiates* a call to a forge. A forge may call the API (a webhook route, not
built yet). Handling such a call means recording it and starting a workflow, whose forge
activities run in a worker, never calling the forge back.

## Consequences

All forge I/O stays in the worker, which is what the audit is about. `make boundaries` fails
the build if `repoplane-api` links `repoplane-forge` or an HTTP client.
