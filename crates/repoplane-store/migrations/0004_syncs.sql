-- `connection sync`: a reconcile, then a head refresh of every active repository, as one run.
-- Each step keeps its own row; a sync points at them and says how the whole went.
create table syncs (
    id              uuid primary key,
    organization_id uuid not null references organizations (id),
    connection_id   uuid not null references connections (id) on delete cascade,
    reconcile_id    uuid not null references reconciles (id),
    -- Null until the reconcile has succeeded and the refresh is requested.
    head_refresh_id uuid references head_refreshes (id),
    status          text not null check (status in ('queued', 'running', 'succeeded', 'failed')),
    error           text,
    requested_at    timestamptz not null default now(),
    started_at      timestamptz,
    finished_at     timestamptz
);

create index syncs_connection on syncs (connection_id, requested_at desc);
