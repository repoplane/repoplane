# 7. `organization_id` on every row from the first migration

**Status:** Kept · 2026-09-30

## Context

The Enterprise plane will be multi-tenant. Adding a tenant column to a populated schema, and to
every query, is the migration that breaks things.

## Decision

Every table carries `organization_id`, and every read filters by it. Open-source repoplane runs
one implicit organisation (`00000000-0000-7000-8000-000000000001`, created by the first
migration).

## Consequences

The Enterprise plane maps its tenants onto organisations without changing the core schema.
