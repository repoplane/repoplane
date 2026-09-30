# 9. Forge differences are modelled, not hidden

**Status:** New · 2026-09-30

## Context

The four forges disagree on things that change the answer. Azure DevOps has no topics, and
"archived" there is a disabled repository that refuses reads. GitLab nests groups. Forgejo writes
`Link` from its own `ROOT_URL`.

## Decision

`ForgeKind::caps()` states what each forge can express (`topics`, `nested_namespaces`,
`archived: archived | disabled`). The inventory stores `topics: null` where the forge has none,
never an empty list. A topic filter on such a connection is refused with a message saying why.
Every `Lister` method is mandatory.

## Consequences

A number repoplane prints never quietly mixes "the forge cannot say" with "the answer is
none".
