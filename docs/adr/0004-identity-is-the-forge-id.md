# 4. Repository identity is the forge's id, never the name

**Status:** Kept · 2026-09-30

## Context

Repositories get renamed and transferred, and deleted and recreated under the same name.
Identity by name gets all three wrong.

## Decision

A repository is `(connection, external_id)`, where `external_id` is the forge's own id
(numeric on GitHub, GitLab and Forgejo; a GUID on Azure DevOps). The name is a mutable attribute.

## Consequences

A rename is an update to the same row. A deleted and recreated repository is a new row, and
the old one is marked inaccessible; the e2e suite checks exactly this on the local sandbox.
