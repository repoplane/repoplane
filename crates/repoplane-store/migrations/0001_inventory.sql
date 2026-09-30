-- Connections and the repository inventory, the reconcile runs that fill it, and the task
-- queue workers lease from. Every table carries organization_id from the first migration:
-- open-source repoplane runs one implicit organisation, the enterprise plane maps its tenants
-- onto them, and no read ever forgets the filter.

create table organizations (
    id         uuid primary key,
    name       text not null unique,
    created_at timestamptz not null default now()
);

-- The implicit organisation of a single-tenant install.
insert into organizations (id, name) values ('00000000-0000-7000-8000-000000000001', 'default');

create table connections (
    id                 uuid primary key,
    organization_id    uuid not null references organizations (id),
    name               text not null,
    forge              text not null check (forge in ('forgejo', 'github', 'gitlab', 'azuredevops')),
    org                text not null check (org <> ''),
    base_url           text not null,
    auth               text not null check (auth in ('token')),
    -- The NAME of the variable holding the token in the worker's environment. Never a token:
    -- the API holds no forge credential (ADR 0003).
    token_env          text not null check (token_env ~ '^[A-Z_][A-Z0-9_]{0,127}$'),
    queue              text not null default 'default' check (queue ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    created_at         timestamptz not null default now(),
    last_reconciled_at timestamptz,
    unique (organization_id, name),
    unique (organization_id, forge, base_url, org)
);

create table repositories (
    id                 uuid primary key,
    organization_id    uuid not null references organizations (id),
    connection_id      uuid not null references connections (id) on delete cascade,
    -- Identity is the forge's own id, never the name: a rename is an update, and a repository
    -- deleted and recreated under the same name is a different repository (ADR 0004).
    external_id        text not null,
    full_name          text not null,
    name               text not null,
    default_branch     text,
    archived           boolean not null,
    -- NULL when the forge has no topics (Azure DevOps); '{}' when the repository has none.
    topics             text[],
    head_sha           text check (head_sha ~ '^[0-9a-f]{40}([0-9a-f]{24})?$'),
    empty              boolean not null default false,
    web_url            text,
    status             text not null check (status in ('active', 'inaccessible')),
    first_seen_at      timestamptz not null,
    last_seen_at       timestamptz not null,
    inaccessible_since timestamptz,
    unique (connection_id, external_id)
);

create index repositories_list on repositories (organization_id, full_name, id);
create index repositories_topics on repositories using gin (topics);

create table tasks (
    id               uuid primary key,
    organization_id  uuid not null references organizations (id),
    queue            text not null,
    kind             text not null check (kind in ('reconcile')),
    state            text not null check (state in ('queued', 'leased', 'done', 'failed')),
    lease_id         uuid,
    lease_expires_at timestamptz,
    attempts         integer not null default 0,
    last_error       text,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now()
);

create index tasks_ready on tasks (queue, created_at) where state in ('queued', 'leased');

create table reconciles (
    id              uuid primary key,
    organization_id uuid not null references organizations (id),
    connection_id   uuid not null references connections (id) on delete cascade,
    task_id         uuid not null references tasks (id),
    status          text not null check (status in ('queued', 'running', 'succeeded', 'failed')),
    -- Stamped when a worker starts the listing: every repository seen by this run gets it as
    -- last_seen_at, and only a complete listing may mark older rows inaccessible (ADR 0005).
    watermark       timestamptz,
    discovered      bigint not null default 0,
    recorded        bigint not null default 0,
    inaccessible    bigint not null default 0,
    error           text,
    requested_at    timestamptz not null default now(),
    started_at      timestamptz,
    finished_at     timestamptz
);

-- At most one reconcile in flight per connection; a second request joins it.
create unique index reconciles_one_active on reconciles (connection_id)
    where status in ('queued', 'running');
