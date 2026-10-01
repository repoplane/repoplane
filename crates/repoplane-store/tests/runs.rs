//! The run steps against a real Postgres: the cases Temporal's timing decides — a step repeated
//! after its commit, a reconcile landing in the middle of a refresh — which no end-to-end run can
//! be made to hit on purpose. Needs Docker; runs with REPOPLANE_DB_TESTS=1 (`make db`).

use std::time::Duration;

use repoplane_proto::ForgeKind;
use repoplane_proto::api::{
    AuthMode, Connection, CreateConnection, RefreshHeads, RepositoryStatus, RunStatus,
};
use repoplane_proto::id::HeadRefreshId;
use repoplane_proto::inventory::{ObservedHead, RepoSnapshot};
use repoplane_store::{RepoFilter, Store};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[tokio::test]
async fn runs() {
    if std::env::var("REPOPLANE_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping: set REPOPLANE_DB_TESTS=1 (needs Docker)");
        return;
    }
    let (_pg, store) = start().await;
    repositories_without_a_locator_are_not_refreshed(&store).await;
    a_head_read_from_a_branch_no_longer_default_is_dropped(&store).await;
    a_repeated_record_counts_each_target_once(&store).await;
    heads_outside_the_targets_are_ignored(&store).await;
    finishing_a_reconcile_marks_what_it_did_not_see_once(&store).await;
    a_late_failure_does_not_undo_a_success(&store).await;
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

/// A complete reconcile that lists exactly `repos`.
async fn reconcile(store: &Store, c: &Connection, repos: &[RepoSnapshot]) {
    let (r, _) = store.request_reconcile(c).await.unwrap();
    store.start_reconcile(r.id).await.unwrap();
    store.record_page(r.id, repos).await.unwrap();
    store.finish_reconcile(r.id).await.unwrap();
}

/// A started refresh of `c`'s repositories: its id and its targets' forge ids.
async fn refresh(
    store: &Store,
    c: &Connection,
    filter: RefreshHeads,
) -> (HeadRefreshId, Vec<String>) {
    let r = store.request_head_refresh(c, &filter).await.unwrap();
    let targets = store.start_refresh(r.id).await.unwrap();
    (
        r.id,
        targets.into_iter().map(|t| t.repo.external_id).collect(),
    )
}

/// Rows recorded before migration 0002 have no locator until a reconcile writes one; a refresh
/// must not hand them to a worker, which cannot find them and on GitLab would record "no HEAD"
/// over a real one.
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
    let (_, targets) = (r.id, store.start_refresh(r.id).await.unwrap());
    assert_eq!(
        targets
            .iter()
            .map(|t| t.repo.external_id.as_str())
            .collect::<Vec<_>>(),
        ["b"]
    );
}

/// A refresh reads `main`; meanwhile a reconcile records that the default branch is now
/// `develop`. The HEAD the refresh then reports is `main`'s, and must not land on `develop`.
async fn a_head_read_from_a_branch_no_longer_default_is_dropped(store: &Store) {
    let c = connection(store, "q-branch").await;
    reconcile(store, &c, &[repo("a", "main", &[])]).await;
    let (r, _) = refresh(store, &c, RefreshHeads::default()).await;

    reconcile(store, &c, &[repo("a", "develop", &[])]).await;
    assert_eq!(
        store
            .record_heads(r, &[head("a", "main", '1')])
            .await
            .unwrap(),
        0
    );
    assert_eq!(heads_of(store, &c).await, [("a".to_string(), None)]);

    // Read from the branch that is the default now, it is recorded.
    assert_eq!(
        store
            .record_heads(r, &[head("a", "develop", '2')])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        heads_of(store, &c).await,
        [("a".to_string(), Some("2".repeat(40)))]
    );
}

/// Temporal repeats a step whose commit it never heard about: `a` is recorded twice, then `b`.
/// The refresh reports what it changed: each target once.
async fn a_repeated_record_counts_each_target_once(store: &Store) {
    let c = connection(store, "q-retry").await;
    reconcile(store, &c, &[repo("a", "main", &[]), repo("b", "main", &[])]).await;
    let (r, _) = refresh(store, &c, RefreshHeads::default()).await;
    store
        .record_heads(r, &[head("a", "main", '1')])
        .await
        .unwrap();
    store
        .record_heads(r, &[head("a", "main", '1'), head("b", "main", '2')])
        .await
        .unwrap();
    store.finish_refresh(r).await.unwrap();
    let r = store.head_refresh(r).await.unwrap();
    assert_eq!(r.status, RunStatus::Succeeded);
    assert_eq!((r.targeted, r.observed, r.changed), (2, 2, 2));
}

/// A refresh of the `t` repositories writes HEADs for them only, whatever a worker reports.
async fn heads_outside_the_targets_are_ignored(store: &Store) {
    let c = connection(store, "q-scope").await;
    reconcile(
        store,
        &c,
        &[repo("a", "main", &["t"]), repo("b", "main", &[])],
    )
    .await;
    let (id, targets) = refresh(
        store,
        &c,
        RefreshHeads {
            topic: Some("t".into()),
            archived: None,
        },
    )
    .await;
    assert_eq!(targets, ["a"]);
    store
        .record_heads(id, &[head("a", "main", '1'), head("b", "main", '2')])
        .await
        .unwrap();
    store.finish_refresh(id).await.unwrap();
    let r = store.head_refresh(id).await.unwrap();
    assert_eq!((r.targeted, r.observed, r.changed), (1, 1, 1));
    assert_eq!(
        heads_of(store, &c).await,
        [
            ("a".to_string(), Some("1".repeat(40))),
            ("b".to_string(), None)
        ]
    );
}

/// `b` is in the inventory, and the second listing (with its lookups) did not find it: finishing
/// marks it inaccessible. Temporal may run a step again; nothing is marked twice.
async fn finishing_a_reconcile_marks_what_it_did_not_see_once(store: &Store) {
    let c = connection(store, "q-finish").await;
    reconcile(store, &c, &[repo("a", "main", &[]), repo("b", "main", &[])]).await;
    let (r, _) = store.request_reconcile(&c).await.unwrap();
    store.start_reconcile(r.id).await.unwrap();
    store.start_reconcile(r.id).await.unwrap(); // repeated: keeps the first watermark
    store
        .record_page(r.id, &[repo("a", "main", &[])])
        .await
        .unwrap();
    let unseen = store.unseen(r.id).await.unwrap();
    assert_eq!(
        unseen
            .iter()
            .map(|u| u.external_id.as_str())
            .collect::<Vec<_>>(),
        ["b"]
    );
    store.finish_reconcile(r.id).await.unwrap();
    store.finish_reconcile(r.id).await.unwrap();
    let done = store.reconcile(r.id).await.unwrap();
    assert_eq!(
        (done.status, done.discovered, done.inaccessible),
        (RunStatus::Succeeded, 1, 1)
    );
    let all = store
        .repositories(&RepoFilter {
            connection: Some(c.id),
            include_inaccessible: true,
            limit: 100,
            ..RepoFilter::default()
        })
        .await
        .unwrap();
    let b = all.items.iter().find(|r| r.external_id == "b").unwrap();
    assert_eq!(b.status, RepositoryStatus::Inaccessible);
}

/// A failure reported after the run already succeeded leaves the success alone.
async fn a_late_failure_does_not_undo_a_success(store: &Store) {
    let c = connection(store, "q-late").await;
    reconcile(store, &c, &[repo("a", "main", &[])]).await;
    let (r, _) = store.request_reconcile(&c).await.unwrap();
    store.start_reconcile(r.id).await.unwrap();
    store
        .record_page(r.id, &[repo("a", "main", &[])])
        .await
        .unwrap();
    store.finish_reconcile(r.id).await.unwrap();
    store.fail_reconcile(r.id, "too late").await.unwrap();
    assert_eq!(
        store.reconcile(r.id).await.unwrap().status,
        RunStatus::Succeeded
    );
}
