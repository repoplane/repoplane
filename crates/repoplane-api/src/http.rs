//! The HTTP surface: `/v1` for clients, behind the admin token. Asking for a reconcile or a head
//! refresh records the run, then starts its Temporal workflow; reading a run reads Postgres. The
//! API never calls a forge (ADR 0002).

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use repoplane_flow::{
    CONTROL_QUEUE, ReconcileInput, ReconcileWorkflow, RefreshHeadsInput, RefreshHeadsWorkflow,
    workflow_id,
};
use repoplane_proto::api::{Connection, RunStatus};
use repoplane_proto::api::{
    ConnectionList, CreateConnection, ErrorBody, HeadRefresh, Reconcile, RefreshHeads,
    RepositoryList,
};
use repoplane_proto::id::{HeadRefreshId, ReconcileId};
use repoplane_proto::inventory::ConnectionSpec;
use repoplane_store::{RepoFilter, Store, StoreError};
use serde::Deserialize;
use temporalio_client::{Client, WorkflowIdConflictPolicy, WorkflowStartOptions};

const MAX_RECONCILE_WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub admin_token: Arc<String>,
    pub temporal: Client,
}

pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route(
            "/connections",
            post(create_connection).get(list_connections),
        )
        .route("/connections/{key}", get(get_connection))
        .route("/connections/{key}/reconciles", post(request_reconcile))
        .route("/reconciles/{id}", get(get_reconcile))
        .route(
            "/connections/{key}/head-refreshes",
            post(request_head_refresh),
        )
        .route("/head-refreshes/{id}", get(get_head_refresh))
        .route("/repositories", get(list_repositories));
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .nest("/v1", public)
        .with_state(state)
}

// --- errors ----------------------------------------------------------------------------

pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid", message)
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Invalid(m) => ApiError::new(StatusCode::BAD_REQUEST, "invalid", m),
            StoreError::NotFound(m) => {
                ApiError::new(StatusCode::NOT_FOUND, "not_found", format!("{m} not found"))
            }
            StoreError::Conflict(m) => ApiError::new(StatusCode::CONFLICT, "conflict", m),
            StoreError::Schema { .. } => {
                ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "schema", e.to_string())
            }
            other => {
                tracing::error!(error = %other, "store failure");
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "internal error",
                )
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                code: self.code.into(),
                message: self.message,
            }),
        )
            .into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

// --- auth ------------------------------------------------------------------------------

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Compares without an early exit, so the time taken says nothing about how much matched.
fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= (a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0)) as usize;
    }
    diff == 0
}

fn unauthorized() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "missing or wrong token",
    )
}

fn admin(s: &AppState, h: &HeaderMap) -> ApiResult<()> {
    match bearer(h) {
        Some(t) if same(t, &s.admin_token) => Ok(()),
        _ => Err(unauthorized()),
    }
}

// --- public ----------------------------------------------------------------------------

async fn readyz(State(s): State<AppState>) -> ApiResult<&'static str> {
    s.store.check_schema().await?;
    Ok("ready")
}

async fn create_connection(
    State(s): State<AppState>,
    h: HeaderMap,
    Json(body): Json<CreateConnection>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    let c = s.store.create_connection(&body).await?;
    Ok((StatusCode::CREATED, Json(c)))
}

async fn list_connections(
    State(s): State<AppState>,
    h: HeaderMap,
) -> ApiResult<Json<ConnectionList>> {
    admin(&s, &h)?;
    Ok(Json(ConnectionList {
        items: s.store.connections().await?,
    }))
}

async fn get_connection(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    Ok(Json(s.store.connection(&key).await?))
}

async fn request_reconcile(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    let c = s.store.connection(&key).await?;
    let (r, created) = s.store.request_reconcile(&c).await?;
    if !created {
        // One reconcile per connection at a time: this request joins the one in flight.
        return Ok((StatusCode::OK, Json(r)));
    }
    let started = s
        .temporal
        .start_workflow(
            ReconcileWorkflow::run,
            ReconcileInput {
                reconcile: r.id,
                connection: spec(&c),
            },
            start_options(&r.id),
        )
        .await;
    if let Err(e) = started {
        return Err(not_started(&s, Run::Reconcile(r.id), e.to_string()).await);
    }
    Ok((StatusCode::CREATED, Json(r)))
}

/// What a workflow needs to reach the connection's forge: everything but the secret.
fn spec(c: &Connection) -> ConnectionSpec {
    ConnectionSpec {
        id: c.id,
        forge: c.forge,
        org: c.org.clone(),
        base_url: c.base_url.clone(),
        token_env: c.token_env.clone(),
        queue: c.queue.clone(),
    }
}

/// The workflow id is the run id; starting it twice joins the first.
fn start_options(run: &impl std::fmt::Display) -> WorkflowStartOptions {
    WorkflowStartOptions::new(CONTROL_QUEUE, workflow_id(run))
        .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
        .build()
}

enum Run {
    Reconcile(ReconcileId),
    Refresh(HeadRefreshId),
}

/// Temporal would not take the workflow: the run is recorded as failed, so nothing waits on a
/// run that will never start, and the caller hears why.
async fn not_started(s: &AppState, run: Run, message: String) -> ApiError {
    let message = format!("could not start the workflow: {message}");
    let recorded = match run {
        Run::Reconcile(id) => s.store.fail_reconcile(id, &message).await,
        Run::Refresh(id) => s.store.fail_refresh(id, &message).await,
    };
    if let Err(e) = recorded {
        tracing::error!(error = %e, "could not record a run that never started");
    }
    tracing::error!("{message}");
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "temporal_unavailable",
        message,
    )
}

#[derive(Deserialize)]
struct WaitQuery {
    wait: Option<String>,
}

/// `30`, `30s` or `1m`.
fn parse_wait(s: &str) -> Result<Duration, ApiError> {
    let (n, unit) = match s.strip_suffix('s') {
        Some(n) => (n, 1),
        None => match s.strip_suffix('m') {
            Some(n) => (n, 60),
            None => (s, 1),
        },
    };
    n.parse::<u64>()
        .map(|n| Duration::from_secs(n * unit))
        .map_err(|_| ApiError::bad(format!("wait {s:?}: want seconds, like 30s")))
}

/// Answers at once, or with `?wait=` holds the request until the run ends or the wait is up.
async fn get_reconcile(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<WaitQuery>,
) -> ApiResult<Json<Reconcile>> {
    admin(&s, &h)?;
    let id: ReconcileId = id
        .parse()
        .map_err(|e: repoplane_proto::id::IdError| ApiError::bad(e.to_string()))?;
    let wait = match q.wait.as_deref() {
        Some(w) => parse_wait(w)?.min(MAX_RECONCILE_WAIT),
        None => Duration::ZERO,
    };
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let r = s.store.reconcile(id).await?;
        if r.status.is_terminal() || tokio::time::Instant::now() + POLL > deadline {
            return Ok(Json(r));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Reads the HEADs of a connection's repositories, all of them or those matching the body's
/// filter. Separate from a reconcile, which records what exists and reads no HEAD.
async fn request_head_refresh(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(key): Path<String>,
    body: Option<Json<RefreshHeads>>,
) -> ApiResult<impl IntoResponse> {
    admin(&s, &h)?;
    let c = s.store.connection(&key).await?;
    let filter = body.map(|Json(b)| b).unwrap_or_default();
    let r = s.store.request_head_refresh(&c, &filter).await?;
    // Nothing to read finishes as it is created; there is no workflow to start.
    if r.status == RunStatus::Queued {
        let started = s
            .temporal
            .start_workflow(
                RefreshHeadsWorkflow::run,
                RefreshHeadsInput {
                    refresh: r.id,
                    connection: spec(&c),
                },
                start_options(&r.id),
            )
            .await;
        if let Err(e) = started {
            return Err(not_started(&s, Run::Refresh(r.id), e.to_string()).await);
        }
    }
    Ok((StatusCode::CREATED, Json(r)))
}

async fn get_head_refresh(
    State(s): State<AppState>,
    h: HeaderMap,
    Path(id): Path<String>,
    Query(q): Query<WaitQuery>,
) -> ApiResult<Json<HeadRefresh>> {
    admin(&s, &h)?;
    let id: HeadRefreshId = id
        .parse()
        .map_err(|e: repoplane_proto::id::IdError| ApiError::bad(e.to_string()))?;
    let wait = match q.wait.as_deref() {
        Some(w) => parse_wait(w)?.min(MAX_RECONCILE_WAIT),
        None => Duration::ZERO,
    };
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let r = s.store.head_refresh(id).await?;
        if r.status.is_terminal() || tokio::time::Instant::now() + POLL > deadline {
            return Ok(Json(r));
        }
        tokio::time::sleep(POLL).await;
    }
}

#[derive(Deserialize)]
struct RepoQuery {
    connection: Option<String>,
    topic: Option<String>,
    archived: Option<bool>,
    #[serde(default)]
    include_inaccessible: bool,
    cursor: Option<String>,
    limit: Option<i64>,
}

async fn list_repositories(
    State(s): State<AppState>,
    h: HeaderMap,
    Query(q): Query<RepoQuery>,
) -> ApiResult<Json<RepositoryList>> {
    admin(&s, &h)?;
    let connection = match &q.connection {
        Some(key) => {
            let c = s.store.connection(key).await?;
            if q.topic.is_some() && !c.forge.caps().topics {
                return Err(ApiError::bad(format!(
                    "{} has no repository topics, so connection {:?} cannot be filtered by topic",
                    c.forge, c.name
                )));
            }
            Some(c.id)
        }
        None => None,
    };
    let page = s
        .store
        .repositories(&RepoFilter {
            connection,
            topic: q.topic,
            archived: q.archived,
            include_inaccessible: q.include_inaccessible,
            cursor: q.cursor,
            limit: q.limit.unwrap_or(500),
        })
        .await?;
    Ok(Json(RepositoryList {
        items: page.items,
        next_cursor: page.next_cursor,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison() {
        assert!(same("abc", "abc"));
        assert!(!same("abc", "abd"));
        assert!(!same("abc", "abcd"));
        assert!(!same("", "a"));
    }

    #[test]
    fn waits() {
        assert_eq!(parse_wait("30s").ok().unwrap(), Duration::from_secs(30));
        assert_eq!(parse_wait("2m").ok().unwrap(), Duration::from_secs(120));
        assert_eq!(parse_wait("5").ok().unwrap(), Duration::from_secs(5));
        assert!(parse_wait("soon").is_err());
    }
}
