# 8. Worker ↔ API is HTTPS long-poll task queues, not Temporal

> **Superseded.** This was a mistake: it replaced Temporal on the belief that its Rust SDK was immature. Temporal orchestrates every run (ADR 0012).

**Status:** Superseded by [0012](0012-temporal-orchestrates.md) · 2026-10-01

## Context

The Go draft orchestrated with Temporal. Temporal's Rust SDK is not a mature, supported option,
and a bank installing repoplane on premises should not have to run a Temporal cluster too. The
worker must also be able to run in a different network from the API.

## Decision

The API owns a Postgres task queue. Workers long-poll `POST /v1/worker/lease` for their queue
over outbound HTTPS, then report pages and complete or fail the task. Leases have a 60s TTL,
which the worker extends by heartbeat. An expired lease goes back to the queue, and the late
holder's calls fail with `lease_lost`. Each connection names its queue, and each queue has its
own token. At most one reconcile per connection is in flight; a second request joins it.

## Consequences

One protocol serves the CLI, the workers and the Enterprise plane, and it passes corporate
proxies and TLS inspection. We give up Temporal's durable workflow history; the reconcile task is
simple enough to restart from the beginning.
