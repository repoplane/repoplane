# 5. Inventory reconciliation is conservative

**Status:** Kept, with a known gap · 2026-09-30

## Context

Marking a repository inaccessible from a listing that silently missed pages would erase real
repositories from every later count.

## Decision

Each run takes a watermark when a worker starts listing, and every repository it sees is
stamped with it. Only a run the worker completes, with every page read and uploaded, may mark
older rows inaccessible. A failed or interrupted run marks nothing. Rows are never deleted. An
older run can never overwrite a row a newer one wrote.

## Consequences

**Known gap:** on forges paged by offset (Forgejo, GitHub, GitLab), a repository deleted
mid-listing shifts later pages back by one, so a complete listing can still miss one repository.
It is then marked inaccessible until the next reconcile sees it. Planned fix: before marking,
confirm each missing repository with a direct lookup by id (a `get` method on `Lister`).
