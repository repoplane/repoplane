//! Syncs: a reconcile, then a head refresh of every active repository, as one run. The two
//! steps keep their own rows; a sync links them and records how the whole went. Each step is
//! safe to repeat, like every run step.

use chrono::{DateTime, Utc};
use repoplane_proto::api::{Connection, RefreshHeads, Sync};
use repoplane_proto::id::{ConnectionId, HeadRefreshId, ReconcileId, SyncId};
use repoplane_proto::inventory::SyncHeads;
use uuid::Uuid;

use crate::{Result, Store, StoreError, parse_status};

#[derive(sqlx::FromRow)]
struct Row {
    id: Uuid,
    connection_id: Uuid,
    reconcile_id: Uuid,
    head_refresh_id: Option<Uuid>,
    status: String,
    error: Option<String>,
    requested_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

impl Store {
    /// Records a sync over a reconcile just requested for it.
    pub async fn create_sync(
        &self,
        connection: &Connection,
        reconcile: ReconcileId,
    ) -> Result<Sync> {
        let id = SyncId::new();
        sqlx::query(
            "insert into syncs (id, organization_id, connection_id, reconcile_id, status)
             values ($1, $2, $3, $4, 'queued')",
        )
        .bind(id.uuid())
        .bind(self.org.uuid())
        .bind(connection.id.uuid())
        .bind(reconcile.uuid())
        .execute(&self.pool)
        .await?;
        self.sync(id).await
    }

    pub async fn sync(&self, id: SyncId) -> Result<Sync> {
        let row: Option<Row> = sqlx::query_as(
            "select id, connection_id, reconcile_id, head_refresh_id, status, error,
                    requested_at, started_at, finished_at
             from syncs where organization_id = $1 and id = $2",
        )
        .bind(self.org.uuid())
        .bind(id.uuid())
        .fetch_optional(&self.pool)
        .await?;
        let row = row.ok_or_else(|| StoreError::NotFound(format!("sync {id}")))?;
        let reconcile = self
            .reconcile(ReconcileId::from_uuid(row.reconcile_id))
            .await?;
        let head_refresh = match row.head_refresh_id {
            Some(h) => Some(self.head_refresh(HeadRefreshId::from_uuid(h)).await?),
            None => None,
        };
        Ok(Sync {
            id: SyncId::from_uuid(row.id),
            connection_id: ConnectionId::from_uuid(row.connection_id),
            status: parse_status(&row.status)?,
            error: row.error,
            reconcile,
            head_refresh,
            requested_at: row.requested_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
        })
    }

    pub async fn start_sync(&self, sync: SyncId) -> Result<()> {
        let n = sqlx::query(
            "update syncs set status = 'running', started_at = coalesce(started_at, now())
             where id = $1 and status in ('queued', 'running')",
        )
        .bind(sync.uuid())
        .execute(&self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(StoreError::Invalid(format!(
                "sync {sync} is not queued or running"
            )));
        }
        Ok(())
    }

    /// Requests the sync's head refresh, of every active repository of its connection, once:
    /// asked again, it returns the one already requested.
    pub async fn sync_heads(&self, sync: SyncId) -> Result<SyncHeads> {
        let (connection_id, existing): (Uuid, Option<Uuid>) =
            sqlx::query_as("select connection_id, head_refresh_id from syncs where id = $1")
                .bind(sync.uuid())
                .fetch_optional(&self.pool)
                .await?
                .ok_or_else(|| StoreError::NotFound(format!("sync {sync}")))?;
        let refresh = match existing {
            Some(h) => self.head_refresh(HeadRefreshId::from_uuid(h)).await?,
            None => {
                let connection = self
                    .connection(&ConnectionId::from_uuid(connection_id).to_string())
                    .await?;
                let r = self
                    .request_head_refresh(&connection, &RefreshHeads::default())
                    .await?;
                sqlx::query(
                    "update syncs set head_refresh_id = $2 where id = $1 and head_refresh_id is null",
                )
                .bind(sync.uuid())
                .bind(r.id.uuid())
                .execute(&self.pool)
                .await?;
                r
            }
        };
        Ok(SyncHeads {
            refresh: refresh.id,
            queued: !refresh.status.is_terminal(),
        })
    }

    pub async fn finish_sync(&self, sync: SyncId) -> Result<()> {
        sqlx::query(
            "update syncs set status = 'succeeded', finished_at = now(), error = null
             where id = $1 and status = 'running'",
        )
        .bind(sync.uuid())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Records the sync failed, and with it a step still in flight: Temporal ends a child
    /// workflow when its parent closes, so that step will never record itself.
    pub async fn fail_sync(&self, sync: SyncId, message: &str) -> Result<()> {
        let message: String = message.chars().take(4000).collect();
        let step = format!("the sync stopped: {message}");
        let step: String = step.chars().take(4000).collect();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "update syncs set status = 'failed', error = $2, finished_at = now()
             where id = $1 and status <> 'succeeded'",
        )
        .bind(sync.uuid())
        .bind(&message)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "update reconciles set status = 'failed', error = $2, finished_at = now()
             where id = (select reconcile_id from syncs where id = $1)
               and status in ('queued', 'running')",
        )
        .bind(sync.uuid())
        .bind(&step)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "update head_refreshes set status = 'failed', error = $2, finished_at = now()
             where id = (select head_refresh_id from syncs where id = $1)
               and status in ('queued', 'running')",
        )
        .bind(sync.uuid())
        .bind(&step)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}
