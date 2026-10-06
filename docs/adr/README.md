# Architecture decision records

- [1. The worker cannot reach the database](0001-worker-cannot-reach-the-database.md): Kept; mechanism rewritten for Temporal
- [2. The API never calls a forge](0002-the-api-never-calls-a-forge.md): Revised: forges may call the API
- [3. The API holds no forge credential](0003-the-api-holds-no-forge-credential.md): Kept, to revisit with GitHub Apps
- [4. Repository identity is the forge's id, never the name](0004-identity-is-the-forge-id.md): Kept
- [5. Inventory reconciliation is conservative](0005-conservative-reconciliation.md): Kept; the offset-pagination gap closed
- [6. Schema migrations are an explicit deployment step](0006-explicit-migrations.md): Kept
- [7. `organization_id` on every row from the first migration](0007-organization-on-every-row.md): Kept
- [8. Worker ↔ API is HTTPS long-poll task queues, not Temporal](0008-http-task-queues.md): Superseded by 12
- [9. Forge differences are modelled, not hidden](0009-forge-differences-are-modelled.md): New
- [10. Webhooks are the fast path; reconciliation is the correctness path](0010-webhooks-fast-path-reconcile-correctness.md): New
- [11. Inventory and HEADs are separate operations](0011-inventory-and-heads-are-separate.md): New
- [12. Temporal orchestrates; Postgres is product state](0012-temporal-orchestrates.md): New
- [13. `connection sync` composes the runs; fan-out stays within Temporal's limits](0013-sync-and-fan-out-within-temporal-limits.md): New
