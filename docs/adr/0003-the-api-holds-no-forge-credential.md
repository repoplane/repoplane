# 3. The API holds no forge credential

**Status:** Accepted · 2026-10-01

## Context

A control plane that holds tokens for every connected organisation is the single most
valuable thing to steal in the system, and a bank will ask where its tokens live.

## Decision

Day 0 authenticates with tokens only, the same way fleets and forgelab do. The API, its
database and the CLI hold no forge credential, not even the name of one: the workers hold the
tokens, and choose one for each connection (ADR 0014). When GitHub Apps arrive, this ADR is
reopened: who holds the App key, and whether the API may hold an inbound-only webhook secret.

## Consequences

The token never crosses the API, a log or an error: the worker's HTTP layer scrubs it (and its
base64 forms) from every message it reports.

**No secret enters workflow history either** (ADR 0012). Workflow inputs and activity results are
stored by Temporal. A connection travels in them without any credential, and the forge activity
takes the token the worker holds for it. Decoding the recorded histories in
`crates/repoplane-flow/tests/histories` shows no token.
