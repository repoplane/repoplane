//! Postgres: the product state (connections, repositories, reconcile runs) and the task queue
//! workers lease from. Only the API links this crate; a worker never opens a database (ADR 0001).

mod queue;
mod repos;

use chrono::{DateTime, Utc};
use repoplane_proto::api::{AuthMode, Connection, CreateConnection, Reconcile, ReconcileStatus};
use repoplane_proto::id::{ConnectionId, OrgId, ReconcileId, TaskId};
use repoplane_proto::{ForgeKind, secret};
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

pub use queue::{Leased, UploadOutcome};
pub use repos::{RepoFilter, RepoPage};

/// The organisation of a single-tenant install, created by the first migration.
pub const DEFAULT_ORG: Uuid = Uuid::from_u128(0x00000000_0000_7000_8000_000000000001);

pub fn default_org() -> OrgId {
    OrgId::from_uuid(DEFAULT_ORG)
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0} not found")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    /// A worker's lease was expired and handed on; it must stop.
    #[error("lease lost: the task was handed to another worker")]
    LeaseLost,
    #[error("schema is at version {have}, this build needs {want}: run `repoplane-api migrate`")]
    Schema { have: i64, want: i64 },
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
    org: OrgId,
}

impl Store {
    pub async fn connect(url: &str) -> Result<Store> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await?;
        Ok(Store {
            pool,
            org: default_org(),
        })
    }

    /// Applies every pending migration. An explicit deployment step (ADR 0006): serving never
    /// changes the schema, it only checks it.
    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    /// Fails unless the database is at exactly the version this build was written for.
    pub async fn check_schema(&self) -> Result<()> {
        let want = MIGRATOR.iter().map(|m| m.version).max().unwrap_or(0);
        let have: Option<i64> =
            sqlx::query_scalar("select max(version) from _sqlx_migrations where success")
                .fetch_one(&self.pool)
                .await
                .or_else(|e| match &e {
                    sqlx::Error::Database(d) if d.code().as_deref() == Some("42P01") => Ok(None),
                    _ => Err(e),
                })?;
        let have = have.unwrap_or(0);
        if have != want {
            return Err(StoreError::Schema { have, want });
        }
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // --- connections -------------------------------------------------------------------

    pub async fn create_connection(&self, c: &CreateConnection) -> Result<Connection> {
        validate_connection(c)?;
        let base_url = normalize_base_url(c.forge, c.base_url.as_deref())?;
        let queue = c.queue.clone().unwrap_or_else(|| "default".into());
        let id = ConnectionId::new();
        let res = sqlx::query(
            "insert into connections (id, organization_id, name, forge, org, base_url, auth, token_env, queue)
             values ($1, $2, $3, $4, $5, $6, 'token', $7, $8)",
        )
        .bind(id.uuid())
        .bind(self.org.uuid())
        .bind(&c.name)
        .bind(c.forge.as_str())
        .bind(&c.org)
        .bind(&base_url)
        .bind(&c.token_env)
        .bind(&queue)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => {}
            Err(sqlx::Error::Database(d)) if d.code().as_deref() == Some("23505") => {
                let what = if d.constraint().is_some_and(|c| c.contains("name")) {
                    format!("a connection named {:?} already exists", c.name)
                } else {
                    format!(
                        "a connection to {} {:?} at {} already exists",
                        c.forge, c.org, base_url
                    )
                };
                return Err(StoreError::Conflict(what));
            }
            Err(sqlx::Error::Database(d)) if d.code().as_deref() == Some("23514") => {
                return Err(StoreError::Invalid(format!(
                    "rejected by the schema: {}",
                    d.constraint().unwrap_or("a check")
                )));
            }
            Err(e) => return Err(e.into()),
        }
        self.connection(&id.to_string()).await
    }

    pub async fn connections(&self) -> Result<Vec<Connection>> {
        let rows: Vec<ConnectionRow> = sqlx::query_as(CONNECTION_SELECT_ALL)
            .bind(self.org.uuid())
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(ConnectionRow::into_api).collect()
    }

    /// Looks a connection up by id or by name.
    pub async fn connection(&self, key: &str) -> Result<Connection> {
        let row: Option<ConnectionRow> = match key.parse::<ConnectionId>() {
            Ok(id) => {
                sqlx::query_as(&format!("{CONNECTION_SELECT} and c.id = $2"))
                    .bind(self.org.uuid())
                    .bind(id.uuid())
                    .fetch_optional(&self.pool)
                    .await?
            }
            Err(_) => {
                sqlx::query_as(&format!("{CONNECTION_SELECT} and c.name = $2"))
                    .bind(self.org.uuid())
                    .bind(key)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };
        row.ok_or_else(|| StoreError::NotFound(format!("connection {key:?}")))?
            .into_api()
    }

    // --- reconciles --------------------------------------------------------------------

    /// Starts a reconcile of a connection, or returns the one already in flight. The boolean
    /// is true when this call created it.
    pub async fn request_reconcile(&self, connection: &Connection) -> Result<(Reconcile, bool)> {
        let mut tx = self.pool.begin().await?;
        // Serialise concurrent requests for one connection; the partial unique index backs it.
        sqlx::query("select id from connections where id = $1 for update")
            .bind(connection.id.uuid())
            .execute(&mut *tx)
            .await?;
        let active: Option<Uuid> = sqlx::query_scalar(
            "select id from reconciles where connection_id = $1 and status in ('queued', 'running')",
        )
        .bind(connection.id.uuid())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(id) = active {
            tx.commit().await?;
            return Ok((self.reconcile(ReconcileId::from_uuid(id)).await?, false));
        }
        let task = TaskId::new();
        let rec = ReconcileId::new();
        sqlx::query(
            "insert into tasks (id, organization_id, queue, kind, state) values ($1, $2, $3, 'reconcile', 'queued')",
        )
        .bind(task.uuid())
        .bind(self.org.uuid())
        .bind(&connection.queue)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "insert into reconciles (id, organization_id, connection_id, task_id, status) values ($1, $2, $3, $4, 'queued')",
        )
        .bind(rec.uuid())
        .bind(self.org.uuid())
        .bind(connection.id.uuid())
        .bind(task.uuid())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok((self.reconcile(rec).await?, true))
    }

    pub async fn reconcile(&self, id: ReconcileId) -> Result<Reconcile> {
        let row: Option<ReconcileRow> = sqlx::query_as(
            "select id, connection_id, status, discovered, recorded, inaccessible, error,
                    requested_at, started_at, finished_at
             from reconciles where organization_id = $1 and id = $2",
        )
        .bind(self.org.uuid())
        .bind(id.uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.ok_or_else(|| StoreError::NotFound(format!("reconcile {id}")))?
            .into_api()
    }
}

// --- validation ------------------------------------------------------------------------

fn validate_connection(c: &CreateConnection) -> Result<()> {
    let bad = |m: String| Err(StoreError::Invalid(m));
    if c.name.is_empty()
        || c.name.len() > 63
        || !c
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return bad(format!(
            "name {:?}: want 1-63 letters, digits, '-', '_' or '.'",
            c.name
        ));
    }
    if repoplane_proto::id::looks_like::<repoplane_proto::id::Con>(&c.name) {
        return bad(format!(
            "name {:?} has the shape of an id; pick another",
            c.name
        ));
    }
    if c.org.trim().is_empty() || c.org.trim() != c.org {
        return bad(format!("org {:?} is empty or padded", c.org));
    }
    if c.forge != ForgeKind::Gitlab && c.org.contains('/') {
        return bad(format!(
            "org {:?}: only a GitLab group path may contain '/'",
            c.org
        ));
    }
    if !secret::valid_env_name(&c.token_env) {
        return bad(secret::describe_env_name_error("token_env", &c.token_env));
    }
    if c.auth != AuthMode::Token {
        return bad("only token auth is supported".into());
    }
    Ok(())
}

/// The base URL a connection is stored with: the forge's default when none is given, no
/// trailing slash, http or https only, and never with credentials in it.
pub fn normalize_base_url(forge: ForgeKind, given: Option<&str>) -> Result<String> {
    let raw = match given.filter(|s| !s.is_empty()) {
        Some(s) => s,
        None => forge.default_base_url().ok_or_else(|| {
            StoreError::Invalid(format!("{forge} is self-hosted: --base-url is required"))
        })?,
    };
    let trimmed = raw.trim_end_matches('/');
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return Err(StoreError::Invalid(format!(
            "base_url {raw:?}: want http:// or https://"
        )));
    };
    if scheme != "http" && scheme != "https" {
        return Err(StoreError::Invalid(format!(
            "base_url {raw:?}: want http:// or https://"
        )));
    }
    if rest
        .split('/')
        .next()
        .is_some_and(|host| host.contains('@'))
    {
        return Err(StoreError::Invalid(
            "base_url must not carry credentials; name the token with token_env".into(),
        ));
    }
    if rest.is_empty() || rest.contains(['?', '#']) {
        return Err(StoreError::Invalid(format!(
            "base_url {raw:?} is not a plain URL"
        )));
    }
    Ok(trimmed.to_string())
}

// --- rows ------------------------------------------------------------------------------

const CONNECTION_SELECT: &str = "
    select c.id, c.name, c.forge, c.org, c.base_url, c.auth, c.token_env, c.queue, c.created_at,
           c.last_reconciled_at,
           (select count(*) from repositories r where r.connection_id = c.id and r.status = 'active') as repositories
    from connections c where c.organization_id = $1";

const CONNECTION_SELECT_ALL: &str = "
    select c.id, c.name, c.forge, c.org, c.base_url, c.auth, c.token_env, c.queue, c.created_at,
           c.last_reconciled_at,
           (select count(*) from repositories r where r.connection_id = c.id and r.status = 'active') as repositories
    from connections c where c.organization_id = $1 order by c.name";

#[derive(sqlx::FromRow)]
struct ConnectionRow {
    id: Uuid,
    name: String,
    forge: String,
    org: String,
    base_url: String,
    auth: String,
    token_env: String,
    queue: String,
    created_at: DateTime<Utc>,
    last_reconciled_at: Option<DateTime<Utc>>,
    repositories: i64,
}

impl ConnectionRow {
    fn into_api(self) -> Result<Connection> {
        Ok(Connection {
            id: ConnectionId::from_uuid(self.id),
            name: self.name,
            forge: self.forge.parse().map_err(StoreError::Invalid)?,
            org: self.org,
            base_url: self.base_url,
            auth: match self.auth.as_str() {
                "token" => AuthMode::Token,
                other => return Err(StoreError::Invalid(format!("unknown auth {other:?}"))),
            },
            token_env: self.token_env,
            queue: self.queue,
            created_at: self.created_at,
            repositories: self.repositories,
            last_reconciled_at: self.last_reconciled_at,
        })
    }
}

#[derive(sqlx::FromRow)]
struct ReconcileRow {
    id: Uuid,
    connection_id: Uuid,
    status: String,
    discovered: i64,
    recorded: i64,
    inaccessible: i64,
    error: Option<String>,
    requested_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl ReconcileRow {
    fn into_api(self) -> Result<Reconcile> {
        Ok(Reconcile {
            id: ReconcileId::from_uuid(self.id),
            connection_id: ConnectionId::from_uuid(self.connection_id),
            status: parse_status(&self.status)?,
            discovered: self.discovered,
            recorded: self.recorded,
            inaccessible: self.inaccessible,
            error: self.error,
            requested_at: self.requested_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

fn parse_status(s: &str) -> Result<ReconcileStatus> {
    Ok(match s {
        "queued" => ReconcileStatus::Queued,
        "running" => ReconcileStatus::Running,
        "succeeded" => ReconcileStatus::Succeeded,
        "failed" => ReconcileStatus::Failed,
        other => {
            return Err(StoreError::Invalid(format!(
                "unknown reconcile status {other:?}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_urls() {
        assert_eq!(
            normalize_base_url(ForgeKind::Github, None).unwrap(),
            "https://github.com"
        );
        assert_eq!(
            normalize_base_url(ForgeKind::Forgejo, Some("http://localhost:3000/")).unwrap(),
            "http://localhost:3000"
        );
        assert!(normalize_base_url(ForgeKind::Forgejo, None).is_err());
        assert!(normalize_base_url(ForgeKind::Gitlab, Some("https://u:p@gitlab.example")).is_err());
        assert!(normalize_base_url(ForgeKind::Gitlab, Some("ftp://gitlab.example")).is_err());
    }

    #[test]
    fn default_org_is_the_migration_s() {
        assert_eq!(
            DEFAULT_ORG.to_string(),
            "00000000-0000-7000-8000-000000000001"
        );
    }
}
