-- A head refresh goes on without a repository the forge would not let its token read (a 403 on
-- that repository alone), and says which and why. Its last known HEAD stays as it was.
create table head_refresh_unread (
    organization_id uuid not null references organizations (id),
    head_refresh_id uuid not null references head_refreshes (id) on delete cascade,
    repository_id   uuid not null references repositories (id) on delete cascade,
    reason          text not null,
    -- A refresh's step repeated by Temporal counts a repository once.
    primary key (head_refresh_id, repository_id)
);

alter table head_refreshes add column unread bigint not null default 0;
