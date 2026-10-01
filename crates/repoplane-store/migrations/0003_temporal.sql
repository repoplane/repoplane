-- Temporal orchestrates runs now (ADR 0012): it holds what a run is doing — retries, timers,
-- which step comes next — and Postgres holds only what the product shows. The task queue goes.

alter table reconciles drop column task_id;
drop index head_refreshes_task;
alter table head_refreshes drop column task_id;
drop table tasks;
