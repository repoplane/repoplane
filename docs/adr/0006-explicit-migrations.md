# 6. Schema migrations are an explicit deployment step

**Status:** Kept · 2026-09-30

## Context

A server that migrates on start turns a rolling deploy into a race, and gives the operator no
moment to back up first.

## Decision

Migrations are embedded in `repoplane-api` and applied only by `repoplane-api migrate`.
`serve` checks the schema version and refuses to start on any other.

## Consequences

Upgrades are two commands, in an order the operator controls.
