# 5. Inventory reconciliation is conservative

**Status:** Kept; the offset-pagination gap closed · 2026-09-30

## Context

Marking a repository inaccessible from a listing that silently missed pages would erase real
repositories from every later count.

## Decision

Each run takes a watermark when a worker starts listing, and every repository it sees is
stamped with it. Only a run the worker completes, with every page read and uploaded, may mark
older rows inaccessible. A failed or interrupted run marks nothing. Rows are never deleted. An
older run can never overwrite a row a newer one wrote.

## Consequences

A complete listing is still not proof of absence. On forges paged by offset (Forgejo, GitHub,
GitLab), deleting a repository mid-listing shifts every later page back by one, so a complete
listing can skip a repository that still exists.

So before completing, the worker asks the API which active repositories this run has not seen
(`POST /v1/worker/tasks/{id}/unseen`), and looks each up directly by the forge's id
(`Lister::get`). Found ones are uploaded like any listed repository, under their current name
if they were renamed. Only the rest are left for `complete` to mark. A repository transferred
out of the organisation, or scheduled for deletion on GitLab, counts as gone. Azure DevOps answers
a disabled repository's lookup with a 404, so its `get` checks the listing before it believes
one.

The e2e suite reproduces the race on the local sandbox: a proxy deletes a page-1 repository the
moment page 2 is requested. The test asserts that the skipped repository stays active and that
the next run marks only the deleted one. With the lookup disabled, the same test fails with
`inaccessible 1`.

The cost is one lookup per unseen repository, which is zero on a stable fleet.
