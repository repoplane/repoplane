# 3. The API holds no forge credential

**Status:** Accepted · 2026-10-01

## Context

A control plane that holds tokens for every connected organisation is the single most
valuable thing to steal in the system, and a bank will ask where its tokens live.

## Decision

Day 0 authenticates with tokens only, the same way fleets and forgelab do. A connection stores
the *name* of an environment variable (`token_env`), which the schema checks against
`^[A-Z_][A-Z0-9_]*$`. Only the worker resolves it, in its own environment. When GitHub Apps
arrive, this ADR is reopened: who holds the App key, and whether the API may hold an
inbound-only webhook secret.

## Consequences

The token never crosses the API, a log or an error: the worker's HTTP layer scrubs it (and its
base64 forms) from every message it reports.

**No secret enters workflow history either** (ADR 0012). Workflow inputs and activity results are
stored by Temporal. A connection travels in them as its token's *variable name*, and the forge
activity reads the token itself, in the worker. Decoding the recorded histories in
`crates/repoplane-flow/tests/histories` shows only variable names.
