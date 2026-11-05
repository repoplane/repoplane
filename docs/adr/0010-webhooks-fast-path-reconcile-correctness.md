# 10. Webhooks are the fast path; reconciliation is the correctness path

**Status:** Accepted · 2026-09-30

## Context

An on-premises API is often unreachable from github.com, and webhooks are dropped, delayed and
reordered even when they arrive.

## Decision

No state may be reachable *only* through a webhook. A reconcile, started by hand, on a
schedule, or from a webhook, is what makes the inventory correct. Webhooks, when they come, only
make it sooner.

## Consequences

A deployment with no inbound connectivity works fully by polling.
