//! The queue against a real Postgres: the cases a worker's timing decides, which no end-to-end
//! run can be made to hit on purpose. Needs Docker; runs with REPOPLANE_DB_TESTS=1 (`make db`).

use std::time::Duration;

use repoplane_proto::ForgeKind;
use repoplane_proto::api::{AuthMode, Connection, CreateConnection, RefreshHeads, RunStatus};
use repoplane_proto::worker::{ObservedHead, RepoSnapshot, Work};
use repoplane_store::{Leased, RepoFilter, Store};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const TTL: i64 = 60;

#[tokio::test]
async fn queue() {
    if std::env::var("REPOPLANE_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping: set REPOPLANE_DB_TESTS=1 (needs Docker)");
        return;
    }
    let (_pg, store) = start().await;
    repositories_without_a_locator_are_not_refreshed(&store).await;
    a_head_read_from_a_branch_no_longer_default_is_dropped(&store).await;
    a_retried_refresh_counts_each_target_once(&store).await;
    heads_outside_the_targets_are_ignored(&store).await;
}

async fn start() -> (ContainerAsync<GenericImage>, Store) {
    let pg = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "PostgreSQL init process complete",
        ))
        .with_env_var("POSTGRES_USER", "repoplane")
        .with_env_var("POSTGRES_PASSWORD", "repoplane-not-a-secret")
        .with_env_var("POSTGRES_DB", "repoplane")
        .start()
        .await
        .expect("start Postgres");
    let port = pg.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let url = format!("postgres://repoplane:repoplane-not-a-secret@127.0.0.1:{port}/repoplane");
    // Postgres restarts once after init: connect until it answers.
    for _ in 0..60 {
        if let Ok(store) = Store::connect(&url).await
            && store.migrate().await.is_ok()
        {
            return (pg, store);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("Postgres never answered");
}

/// A connection on a queue of its own, so no case leases another's task.
async fn connection(store: &Store, queue: &str) -> Connection {
    store
        .create_connection(&CreateConnection {
            name: queue.into(),
            forge: ForgeKind::Forgejo,
            org: queue.into(),
            base_url: Some("http://forgejo.test".into()),
            auth: AuthMode::Token,
            token_env: "TOKEN".into(),
            queue: Some(queue.into()),
        })
        .await
        .unwrap()
}

fn repo(id: &str, branch: &str, topics: &[&str]) -> RepoSnapshot {
    RepoSnapshot {
        external_id: id.into(),
        full_name: format!("org/{id}"),
        name: id.into(),
        default_branch: Some(branch.into()),
        archived: false,
        topics: Some(topics.iter().map(|t| t.to_string()).collect()),
        empty: false,
        web_url: None,
        locator: format!("org/{id}"),
    }
}

fn head(id: &str, branch: &str, sha: char) -> ObservedHead {
    ObservedHead {
        external_id: id.into(),
        branch: Some(branch.into()),
        head: Some(sha.to_string().repeat(40)),
    }
}

async fn lease(store: &Store, queue: &str) -> Leased {
    store
        .lease(queue, TTL)
        .await
        .unwrap()
        .expect("a task on the queue")
}

/// A complete reconcile that lists exactly `repos`.
async fn reconcile(store: &Store, c: &Connection, repos: &[RepoSnapshot]) {
    store.request_reconcile(c).await.unwrap();
    let l = lease(store, &c.queue).await;
    assert!(matches!(l.work, Work::Reconcile { .. }));
    store
        .upload_page(l.task, l.lease_id, TTL, repos)
        .await
        .unwrap();
    store.complete(l.task, l.lease_id, true).await.unwrap();
}

async fn heads_of(store: &Store, c: &Connection) -> Vec<(String, Option<String>)> {
    let mut v: Vec<_> = store
        .repositories(&RepoFilter {
            connection: Some(c.id),
            limit: 100,
            ..RepoFilter::default()
        })
        .await
        .unwrap()
        .items
        .into_iter()
        .map(|r| (r.external_id, r.head))
        .collect();
    v.sort();
    v
}

fn targets(l: &Leased) -> Vec<String> {
    match &l.work {
        Work::RefreshHeads { targets, .. } => {
            targets.iter().map(|t| t.repo.external_id.clone()).collect()
        }
        _ => panic!("not a head refresh"),
    }
}

/// Rows recorded before migration 0002 have no locator until a reconcile writes one; a
/// refresh must not hand them to a worker, which cannot find them and on GitLab would
/// record "no HEAD" over a real one.
async fn repositories_without_a_locator_are_not_refreshed(store: &Store) {
    let c = connection(store, "q-locator").await;
    reconcile(store, &c, &[repo("a", "main", &[]), repo("b", "main", &[])]).await;
    sqlx::query(
        "update repositories set locator = '' where connection_id = $1 and external_id = 'a'",
    )
    .bind(c.id.uuid())
    .execute(store.pool())
    .await
    .unwrap();
    let r = store
        .request_head_refresh(&c, &RefreshHeads::default())
        .await
        .unwrap();
    assert_eq!(r.targeted, 1);
    assert_eq!(targets(&lease(store, &c.queue).await), ["b"]);
}

/// A refresh reads `main`; meanwhile a reconcile records that the default branch is now
/// `develop`. The HEAD the refresh then reports is `main`'s, and must not land on `develop`.
async fn a_head_read_from_a_branch_no_longer_default_is_dropped(store: &Store) {
    let c = connection(store, "q-branch").await;
    reconcile(store, &c, &[repo("a", "main", &[])]).await;
    store
        .request_head_refresh(&c, &RefreshHeads::default())
        .await
        .unwrap();
    let r = lease(store, &c.queue).await;

    reconcile(store, &c, &[repo("a", "develop", &[])]).await;
    let changed = store
        .upload_heads(r.task, r.lease_id, TTL, &[head("a", "main", '1')])
        .await
        .unwrap();
    assert_eq!(changed, 0);
    assert_eq!(heads_of(store, &c).await, [("a".to_string(), None)]);

    // Read from the branch that is the default now, it is recorded.
    let changed = store
        .upload_heads(r.task, r.lease_id, TTL, &[head("a", "develop", '2')])
        .await
        .unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        heads_of(store, &c).await,
        [("a".to_string(), Some("2".repeat(40)))]
    );
}

/// Attempt 1 records `a`'s HEAD and fails; attempt 2 records `a` again and `b`. The run
/// reports what it changed: each target once, the first attempt's work included.
async fn a_retried_refresh_counts_each_target_once(store: &Store) {
    let c = connection(store, "q-retry").await;
    reconcile(store, &c, &[repo("a", "main", &[]), repo("b", "main", &[])]).await;
    let req = store
        .request_head_refresh(&c, &RefreshHeads::default())
        .await
        .unwrap();

    let first = lease(store, &c.queue).await;
    store
        .upload_heads(first.task, first.lease_id, TTL, &[head("a", "main", '1')])
        .await
        .unwrap();
    store
        .fail(first.task, first.lease_id, "forge answered 503", true)
        .await
        .unwrap();

    let second = lease(store, &c.queue).await;
    assert_eq!(second.task, first.task);
    store
        .upload_heads(
            second.task,
            second.lease_id,
            TTL,
            &[head("a", "main", '1'), head("b", "main", '2')],
        )
        .await
        .unwrap();
    store
        .complete(second.task, second.lease_id, true)
        .await
        .unwrap();

    let r = store.head_refresh(req.id).await.unwrap();
    assert_eq!(r.status, RunStatus::Succeeded);
    assert_eq!((r.targeted, r.observed, r.changed), (2, 2, 2));
}

/// A refresh of the `t` repositories writes HEADs for them only, whatever a worker uploads.
async fn heads_outside_the_targets_are_ignored(store: &Store) {
    let c = connection(store, "q-scope").await;
    reconcile(
        store,
        &c,
        &[repo("a", "main", &["t"]), repo("b", "main", &[])],
    )
    .await;
    let req = store
        .request_head_refresh(
            &c,
            &RefreshHeads {
                topic: Some("t".into()),
                archived: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(req.targeted, 1);
    let l = lease(store, &c.queue).await;
    store
        .upload_heads(
            l.task,
            l.lease_id,
            TTL,
            &[head("a", "main", '1'), head("b", "main", '2')],
        )
        .await
        .unwrap();
    store.complete(l.task, l.lease_id, true).await.unwrap();
    let r = store.head_refresh(req.id).await.unwrap();
    assert_eq!((r.targeted, r.observed, r.changed), (1, 1, 1));
    assert_eq!(
        heads_of(store, &c).await,
        [
            ("a".to_string(), Some("1".repeat(40))),
            ("b".to_string(), None)
        ]
    );
}
