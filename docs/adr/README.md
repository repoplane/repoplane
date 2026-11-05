# Architecture decision records

Each record is a decision in force. Numbers are never reused, so a withdrawn record leaves a gap.

- [1. The worker cannot reach the database](0001-worker-cannot-reach-the-database.md)
- [2. The API never calls a forge](0002-the-api-never-calls-a-forge.md)
- [3. The API holds no forge credential](0003-the-api-holds-no-forge-credential.md)
- [4. Repository identity is the forge's id, never the name](0004-identity-is-the-forge-id.md)
- [5. Inventory reconciliation is conservative](0005-conservative-reconciliation.md)
- [6. Schema migrations are an explicit deployment step](0006-explicit-migrations.md)
- [7. `organization_id` on every row from the first migration](0007-organization-on-every-row.md)
- [9. Forge differences are modelled, not hidden](0009-forge-differences-are-modelled.md)
- [10. Webhooks are the fast path; reconciliation is the correctness path](0010-webhooks-fast-path-reconcile-correctness.md)
- [11. Inventory and HEADs are separate operations](0011-inventory-and-heads-are-separate.md)
- [12. Temporal orchestrates; Postgres is product state](0012-temporal-orchestrates.md)
- [13. `connection sync` composes the runs; fan-out stays within Temporal's limits](0013-sync-and-fan-out-within-temporal-limits.md)
