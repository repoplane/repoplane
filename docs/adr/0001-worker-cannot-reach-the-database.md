# 1. The worker cannot reach the database

**Status:** Kept (carried over from the Go draft) · 2026-09-30

## Context

The worker holds forge credentials and, later, checks out source code. It is the component a
customer runs inside its own network, possibly far from the API (the hybrid deployment).

## Decision

The worker has no database driver and no database credential. Everything it learns goes
through `/v1/worker` on the API.

## Consequences

A compromised worker can report false inventory, but it cannot read or rewrite another
tenant's state. `make boundaries` fails the build if `repoplane-worker` links `sqlx` or
`repoplane-store`.
