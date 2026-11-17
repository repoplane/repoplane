-- When the worker read the recorded HEAD, by its own clock, apart from `head_observed_at` (when
-- the API recorded it, by the database's). A read recorded late, a slow head refresh or a step
-- Temporal ran again, must not overwrite a HEAD read after it: record_heads drops it. Null for
-- a HEAD recorded before this column, or from a worker that did not say.
alter table repositories add column head_read_at timestamptz;
