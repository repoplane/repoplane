# 1. The worker cannot reach the database

**Status:** Accepted · 2026-10-01

## Context

The worker holds forge credentials and, later, checks out source code. It is the component a
customer runs inside its own network, possibly far from the API (the hybrid deployment).

## Decision

The worker has no database driver and no database credential. It runs the forge activities and
returns what it found to Temporal; the workflows hand that to the Postgres activities, which run
in `repoplane-api`. Nor does the worker talk to the repoplane API: its only peers are Temporal
and the forges.

## Consequences

A compromised worker can report false inventory, but it cannot read or rewrite another
tenant's state. `make boundaries` fails the build if `repoplane-worker` links `sqlx` or
`repoplane-store`.
