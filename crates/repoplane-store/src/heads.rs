//! Head refreshes: reading the default-branch HEAD of chosen repositories, separately from the
//! inventory. The targets are fixed when the refresh is requested, so what it reads is what was
//! asked for, whatever a reconcile does meanwhile.

use chrono::{DateTime, Utc};
use repoplane_proto::api::{Connection, HeadRefresh, RefreshHeads};
use repoplane_proto::id::{ConnectionId, HeadRefreshId, TaskId};
use uuid::Uuid;

use crate::{Result, Store, StoreError, parse_status};

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    connection_id: Uuid,
    status: String,
    targeted: i64,
    observed: i64,
    changed: i64,
    error: Option<String>,
    requested_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl Row {
    fn into_api(self) -> Result<HeadRefresh> {
        Ok(HeadRefresh {
            id: HeadRefreshId::from_uuid(self.id),
            connection_id: ConnectionId::from_uuid(self.connection_id),
            status: parse_status(&self.status)?,
            targeted: self.targeted,
            observed: self.observed,
            changed: self.changed,
            error: self.error,
            requested_at: self.requested_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

impl Store {
    /// Starts reading the HEADs of a connection's active repositories that match `filter`.
    /// With nothing to read, the refresh is finished as it is created.
    pub async fn request_head_refresh(
        &self,
        connection: &Connection,
        filter: &RefreshHeads,
    ) -> Result<HeadRefresh> {
        if filter.topic.is_some() && !connection.forge.caps().topics {
            return Err(StoreError::Invalid(format!(
                "{} has no repository topics, so connection {:?} cannot be filtered by topic",
                connection.forge, connection.name
            )));
        }
        let mut tx = self.pool.begin().await?;
        let targets: Vec<Uuid> = sqlx::query_scalar(
            "select id from repositories
             where connection_id = $1 and status = 'active'
               -- Without a locator the worker cannot find a repository again; the next
               -- reconcile writes one (rows recorded before migration 0002 have none).
               and locator <> ''
               and ($2::text is null or $2 = any(topics))
               and ($3::boolean is null or archived = $3)
             order by full_name",
        )
        .bind(connection.id.uuid())
        .bind(&filter.topic)
        .bind(filter.archived)
        .fetch_all(&mut *tx)
        .await?;
        let id = HeadRefreshId::new();
        if targets.is_empty() {
            sqlx::query(
                "insert into head_refreshes
                     (id, organization_id, connection_id, status, targets, targeted, started_at, finished_at)
                 values ($1, $2, $3, 'succeeded', '{}', 0, now(), now())",
            )
            .bind(id.uuid())
            .bind(self.org.uuid())
            .bind(connection.id.uuid())
            .execute(&mut *tx)
            .await?;
        } else {
            let task = TaskId::new();
            sqlx::query(
                "insert into tasks (id, organization_id, queue, kind, state)
                 values ($1, $2, $3, 'refresh_heads', 'queued')",
            )
            .bind(task.uuid())
            .bind(self.org.uuid())
            .bind(&connection.queue)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "insert into head_refreshes
                     (id, organization_id, connection_id, task_id, status, targets, targeted)
                 values ($1, $2, $3, $4, 'queued', $5, $6)",
            )
            .bind(id.uuid())
            .bind(self.org.uuid())
            .bind(connection.id.uuid())
            .bind(task.uuid())
            .bind(&targets)
            .bind(targets.len() as i64)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.head_refresh(id).await
    }

    pub async fn head_refresh(&self, id: HeadRefreshId) -> Result<HeadRefresh> {
        let row: Option<Row> = sqlx::query_as(
            "select id, connection_id, status, targeted, observed, changed, error,
                    requested_at, started_at, finished_at
             from head_refreshes where organization_id = $1 and id = $2",
        )
        .bind(self.org.uuid())
        .bind(id.uuid())
        .fetch_optional(&self.pool)
        .await?;
        row.ok_or_else(|| StoreError::NotFound(format!("head refresh {id}")))?
            .into_api()
    }
}
