-- HEADs leave the inventory's reconcile. A reconcile records what exists; a head refresh reads
-- the default-branch HEAD of the repositories it targets (a scan's targets, or a connection's),
-- so a scan of seventeen repositories never pays for the other hundred.

-- What the forge adapter needs to find a repository again without listing (an API path, a
-- project id). Opaque here: written by a reconcile, handed back to a head refresh.
alter table repositories add column locator text not null default '';

-- When the HEAD was last read. A HEAD is a fact at a moment, never a property of the repository.
alter table repositories add column head_observed_at timestamptz;
update repositories set head_observed_at = last_seen_at where head_sha is not null;

alter table tasks drop constraint tasks_kind_check;
alter table tasks add constraint tasks_kind_check check (kind in ('reconcile', 'refresh_heads'));

create table head_refreshes (
    id              uuid primary key,
    organization_id uuid not null references organizations (id),
    connection_id   uuid not null references connections (id) on delete cascade,
    -- Null for a refresh with nothing to read, which is finished when it is requested.
    task_id         uuid references tasks (id),
    status          text not null check (status in ('queued', 'running', 'succeeded', 'failed')),
    -- The repositories to read, fixed when the refresh is requested.
    targets         uuid[] not null,
    targeted        bigint not null,
    observed        bigint not null default 0,
    changed         bigint not null default 0,
    error           text,
    requested_at    timestamptz not null default now(),
    started_at      timestamptz,
    finished_at     timestamptz
);

create index head_refreshes_task on head_refreshes (task_id);
