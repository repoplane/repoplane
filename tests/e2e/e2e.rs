//! The suite: one sandbox per run (`REPOPLANE_E2E_SANDBOX=local|gh|gl|ado`), both fleets on it,
//! every scenario followed by a forgelab reset. Scenarios run in order against one stack; a
//! failure is recorded, the sandbox is still put back, and the next scenario runs.

use std::future::Future;
use std::pin::Pin;

use forgelab::forge::Settings;
use repoplane_e2e::{
    ForgeProxy, ForgeSide, Stack, compare, expected, note, sandbox, workspace_root,
};
use repoplane_proto::ForgeKind;
use repoplane_proto::api::{HeadRefresh, RepositoryStatus};

type Scenario<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

struct Suite {
    side: ForgeSide,
    stack: Stack,
    connection: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inventory() {
    let Some(sb) = sandbox() else { return };
    // A run's scenarios, not those of the runs before it on this machine.
    let _ = std::fs::remove_file(workspace_root().join("target/e2e-logs/scenarios.log"));
    let side = ForgeSide::start(&sb).await.expect("start the forge side");
    side.prepare().await.expect("prepare the sandbox");
    let stack = Stack::start(side.worker_token().expect("the worker's token"))
        .await
        .expect("start repoplane");
    eprintln!(
        "repoplane at {}; logs in {}",
        stack.api_url,
        stack.logs.display()
    );

    let connection = format!("e2e-{sb}");
    let mut add = vec![
        "connection".to_string(),
        "add".into(),
        side.forge.as_str().into(),
        "--name".into(),
        connection.clone(),
        "--org".into(),
        side.org.clone(),
        "--auth".into(),
        "token".into(),
    ];
    if side.sandbox == "local" {
        add.extend(["--base-url".into(), side.base_url.clone()]);
    }
    let add: Vec<&str> = add.iter().map(String::as_str).collect();
    stack.cli(&add).await.expect("connection add");

    let s = Suite {
        side,
        stack,
        connection,
    };
    let mut failures = Vec::new();
    // These scenarios change the forge only on the local sandbox, and skip themselves on the
    // others: putting a cloud sandbox back and resyncing after a scenario that did nothing took
    // minutes (gl: 7 of its 29).
    let local = s.side.sandbox == "local";
    let scenarios: Vec<(&str, bool, Scenario)> = vec![
        ("baseline", false, Box::pin(baseline(&s))),
        ("idempotent", false, Box::pin(idempotent(&s))),
        (
            "repo list matches the example",
            false,
            Box::pin(table_output(&s)),
        ),
        ("direct lookup by id", false, Box::pin(direct_lookup(&s))),
        (
            "head refresh reads only its targets",
            false,
            Box::pin(refresh_targets(&s)),
        ),
        (
            "sync reconciles, then reads every HEAD",
            false,
            Box::pin(sync(&s)),
        ),
        (
            "a run whose workflow is terminated is recorded as failed",
            false,
            Box::pin(terminated(&s)),
        ),
        (
            "a repository skipped by a shifting listing stays active",
            local,
            Box::pin(shifting_listing(&s)),
        ),
        (
            "the run survives its worker dying",
            false,
            Box::pin(worker_killed(&s)),
        ),
        (
            "a rate-limited forge call waits as long as the forge asked",
            local,
            Box::pin(rate_limited(&s)),
        ),
        (
            "a refused token fails the run fast",
            false,
            Box::pin(refused(&s)),
        ),
        (
            "a repository the token may not read is unread, not a failure",
            false,
            Box::pin(unreadable(&s)),
        ),
        (
            "connection add refuses what no worker can serve",
            false,
            Box::pin(no_token(&s)),
        ),
        ("topic changed", true, Box::pin(topic_changed(&s))),
        ("commit pushed", true, Box::pin(commit_pushed(&s))),
        ("archived", true, Box::pin(archived(&s))),
        (
            "a new default branch clears the recorded HEAD",
            true,
            Box::pin(default_branch_changed(&s)),
        ),
        (
            "deleted repo goes inaccessible",
            local,
            Box::pin(deleted(&s)),
        ),
    ];
    let (total, suite_started) = (scenarios.len(), std::time::Instant::now());
    for (name, mutates, scenario) in scenarios {
        note(format!("--- {name}"));
        let started = std::time::Instant::now();
        let res = scenario.await;
        let ran = started.elapsed();
        let reset = std::time::Instant::now();
        if mutates {
            // Put the forge back, then bring the inventory back with it: the next scenario
            // starts from the locks on both sides, and a reconcile after a reset is itself
            // checked against them.
            if let Err(e) = s.side.put_back("shapes").await {
                failures.push(format!("{name}: putting the sandbox back failed: {e:#}"));
            } else if let Err(e) = resync(&s).await {
                failures.push(format!(
                    "{name}: the inventory did not return to the locks: {e:#}"
                ));
            }
        }
        // How long it ran, and how long putting the sandbox back took: a slow job says where.
        let timing = format!("ran {ran:.1?}, reset {:.1?}", reset.elapsed());
        match res {
            Ok(()) => note(format!("ok: {name} ({timing})")),
            Err(e) => {
                note(format!("FAILED: {name} ({timing}): {e:#}"));
                failures.push(format!("{name}: {e:#}"));
            }
        }
    }
    note(format!(
        "{} scenarios, {} failed, {:.0?} in all",
        total,
        failures.len(),
        suite_started.elapsed()
    ));
    assert!(
        failures.is_empty(),
        "{} scenario(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A reconcile records every declared repository of both fleets exactly as the locks say.
async fn baseline(s: &Suite) -> anyhow::Result<()> {
    settled(s).await?;
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.status.as_str() == "succeeded",
        "reconcile {}: {:?}",
        r.status.as_str(),
        r.error
    );
    // A reconcile records what exists and reads no HEAD.
    let listed = s.stack.repos(&s.connection, false).await?;
    let with_head = listed.iter().filter(|r| r.head.is_some()).count();
    anyhow::ensure!(with_head == 0, "a reconcile recorded {with_head} HEAD(s)");
    let h = s.stack.refresh_heads(&s.connection, &[]).await?;
    anyhow::ensure!(
        h.targeted == listed.len() as i64 && h.observed == h.targeted,
        "head refresh targeted {} and observed {} of {} repositories",
        h.targeted,
        h.observed,
        listed.len()
    );
    record_histories(s, &r.id.to_string(), &h.id.to_string()).await?;
    let want = expected(&s.side)?;
    let repos = s.stack.repos(&s.connection, false).await?;
    let (diffs, extras) = compare(&want, &repos);
    eprintln!(
        "discovered {}, recorded {}, inaccessible {}; {} declared, {} undeclared: {extras:?}",
        r.discovered,
        r.recorded,
        r.inaccessible,
        want.len(),
        extras.len()
    );
    anyhow::ensure!(
        diffs.is_empty(),
        "{} difference(s):\n{}",
        diffs.len(),
        diffs.join("\n")
    );
    anyhow::ensure!(
        r.discovered as usize == repos.len(),
        "discovered {} but listed {}",
        r.discovered,
        repos.len()
    );
    Ok(())
}

/// With REPOPLANE_RECORD_HISTORIES=1 on the local sandbox, saves the baseline's workflow
/// histories for repoplane-flow's replay test, which fails when a workflow change would no
/// longer replay the runs already in flight.
async fn record_histories(s: &Suite, reconcile: &str, refresh: &str) -> anyhow::Result<()> {
    if s.side.sandbox != "local"
        || std::env::var("REPOPLANE_RECORD_HISTORIES").as_deref() != Ok("1")
    {
        return Ok(());
    }
    let dir = repoplane_e2e::workspace_root().join("crates/repoplane-flow/tests/histories");
    std::fs::create_dir_all(&dir)?;
    let rec = s
        .stack
        .history_json::<repoplane_flow::ReconcileWorkflow>(reconcile)
        .await?;
    std::fs::write(dir.join("reconcile.json"), rec)?;
    let hrf = s
        .stack
        .history_json::<repoplane_flow::RefreshHeadsWorkflow>(refresh)
        .await?;
    std::fs::write(dir.join("refresh-heads.json"), hrf)?;
    let slice = s
        .stack
        .history_json::<repoplane_flow::ReadHeadsWorkflow>(&format!("{refresh}-heads-1"))
        .await?;
    std::fs::write(dir.join("refresh-heads-slice.json"), slice)?;
    eprintln!("recorded workflow histories in {}", dir.display());
    Ok(())
}

/// Reconciles and checks the inventory against the locks again.
async fn resync(s: &Suite) -> anyhow::Result<()> {
    settled(s).await?;
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.status.as_str() == "succeeded",
        "reconcile {}: {:?}",
        r.status.as_str(),
        r.error
    );
    s.stack.refresh_heads(&s.connection, &[]).await?;
    let (diffs, _) = compare(
        &expected(&s.side)?,
        &s.stack.repos(&s.connection, false).await?,
    );
    anyhow::ensure!(diffs.is_empty(), "{}", diffs.join("\n"));
    Ok(())
}

/// A second reconcile of an unchanged sandbox records nothing and marks nothing.
async fn idempotent(s: &Suite) -> anyhow::Result<()> {
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.recorded == 0 && r.inaccessible == 0,
        "recorded {}, inaccessible {}",
        r.recorded,
        r.inaccessible
    );
    let h = s.stack.refresh_heads(&s.connection, &[]).await?;
    anyhow::ensure!(
        h.changed == 0,
        "a second head refresh changed {}",
        h.changed
    );
    Ok(())
}

/// A head refresh reads only what its filter selects: a scan of the `node` repositories must
/// not pay for the rest of the organisation.
async fn refresh_targets(s: &Suite) -> anyhow::Result<()> {
    if !s.side.forge.caps().topics {
        let err = s
            .stack
            .refresh_heads(&s.connection, &["--topic", "node"])
            .await
            .expect_err("a topic filter on a forge without topics must be refused");
        anyhow::ensure!(err.to_string().contains("no repository topics"), "{err}");
        return Ok(());
    }
    let node = s
        .stack
        .cli_json::<Vec<repoplane_proto::api::Repository>>(&[
            "repo",
            "list",
            "--connection",
            &s.connection,
            "--topic",
            "node",
        ])
        .await?
        .len() as i64;
    let h = s
        .stack
        .refresh_heads(&s.connection, &["--topic", "node"])
        .await?;
    anyhow::ensure!(
        node > 0 && h.targeted == node && h.observed == node && h.changed == 0,
        "targeted {}, observed {}, changed {}; {node} repositories carry the topic",
        h.targeted,
        h.observed,
        h.changed
    );
    Ok(())
}

/// The table the CLI prints for `repo list --topic node --archived false`.
async fn table_output(s: &Suite) -> anyhow::Result<()> {
    if !s.side.forge.caps().topics {
        let err = s
            .stack
            .cli(&[
                "repo",
                "list",
                "--connection",
                &s.connection,
                "--topic",
                "node",
            ])
            .await
            .expect_err("a topic filter on a forge without topics must be refused");
        anyhow::ensure!(err.to_string().contains("no repository topics"), "{err}");
        return Ok(());
    }
    let out = s
        .stack
        .cli(&[
            "repo",
            "list",
            "--connection",
            &s.connection,
            "--topic",
            "node",
            "--archived",
            "false",
        ])
        .await?;
    let mut lines = out.lines();
    let header = lines.next().unwrap_or_default();
    anyhow::ensure!(
        header.split_whitespace().collect::<Vec<_>>()
            == ["REPOSITORY", "BRANCH", "ARCHIVED", "HEAD", "TOPICS"],
        "header {header:?}"
    );
    let billing = s.side.full_name("billing-api");
    let line = out
        .lines()
        .find(|l| l.starts_with(&format!("{billing} ")))
        .ok_or_else(|| anyhow::anyhow!("{billing} missing from:\n{out}"))?;
    let cols: Vec<&str> = line.split_whitespace().collect();
    anyhow::ensure!(cols[1..4] == ["main", "false", "b8a1a303"], "{line}");
    anyhow::ensure!(cols[4..] == ["node", "service"], "{line}");
    Ok(())
}

/// The lookup a run uses to confirm a repository the listing did not return: by the forge's
/// id, it answers what the listing answered, and an id the forge never issued is gone.
async fn direct_lookup(s: &Suite) -> anyhow::Result<()> {
    let lister = s.side.lister()?;
    let inventory = s.stack.repos(&s.connection, false).await?;
    let mut diffs = Vec::new();
    for path in [
        "billing-api",
        "archived",
        "no-commits",
        "master-branch",
        "platform/core/api",
        "acme/payments/gateway/api",
    ] {
        let name = s.side.full_name(path);
        let r = inventory
            .iter()
            .find(|r| r.full_name == name)
            .ok_or_else(|| anyhow::anyhow!("{name} missing from the inventory"))?;
        let Some(got) = lister.get(&r.external_id).await? else {
            diffs.push(format!("{name}: not found by id {}", r.external_id));
            continue;
        };
        let g = got.snapshot;
        let topics = g.topics.map(|mut t| {
            t.sort();
            t
        });
        let want = (
            &r.full_name,
            &r.default_branch,
            r.archived,
            &r.topics,
            r.empty,
        );
        if (
            &g.full_name,
            &g.default_branch,
            g.archived,
            &topics,
            g.empty,
        ) != want
        {
            diffs.push(format!(
                "{name}: by id {:?}, listed {want:?}",
                (
                    &g.full_name,
                    &g.default_branch,
                    g.archived,
                    &topics,
                    g.empty
                )
            ));
        }
    }
    let never = match s.side.forge {
        ForgeKind::Azuredevops => "5a1f3c2e-8b4d-4e6f-9a0b-1c2d3e4f5a6b",
        _ => "999999999999",
    };
    if let Some(r) = lister.get(never).await? {
        diffs.push(format!(
            "id {never} was never issued, yet found {}",
            r.snapshot.full_name
        ));
    }
    anyhow::ensure!(diffs.is_empty(), "{}", diffs.join("\n"));
    Ok(())
}

/// A repository deleted from page 1 while the worker is between pages shifts page 2 back by
/// one, so the listing skips a repository that still exists. Only the deleted one may be
/// marked inaccessible. Local only: the shift comes from a proxy in front of Forgejo.
async fn shifting_listing(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let proxy = ForgeProxy::start(&s.side).await?;
    let conn = "e2e-local-shifting";
    s.stack
        .cli(&[
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
            "--auth",
            "token",
            "--base-url",
            &proxy.base_url,
        ])
        .await?;
    let first = s.stack.reconcile(conn).await?;
    anyhow::ensure!(
        first.discovered == 123,
        "baseline through the proxy: {}",
        first.discovered
    );

    proxy.arm();
    let log_mark = s.stack.worker_log_len();
    let r = s.stack.reconcile(conn).await;
    let outcome = async {
        let r = r?;
        let victim = proxy
            .deleted
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("the proxy never shifted the listing"))?;
        let still_there =
            s.stack
                .worker_log_count_since(log_mark, "did not return", "still_there=true");
        // The victim sat on page 1, already read: this run saw it. What it did not see is the
        // repository the shift pushed from page 2 onto page 1, which still exists.
        anyhow::ensure!(
            r.inaccessible == 0,
            "inaccessible {}: a repository the listing skipped was marked",
            r.inaccessible
        );
        anyhow::ensure!(
            still_there == 1,
            "want exactly the one skipped repository confirmed by id, got still_there={still_there}"
        );
        // The next run sees the deletion, and only that.
        let next = s.stack.reconcile(conn).await?;
        let all = s.stack.repos(conn, true).await?;
        let gone: Vec<_> = all
            .iter()
            .filter(|r| r.status == RepositoryStatus::Inaccessible)
            .map(|r| r.full_name.clone())
            .collect();
        anyhow::ensure!(
            next.inaccessible == 1 && gone == [victim.clone()],
            "next run marked {} ({gone:?}), want only {victim}",
            next.inaccessible
        );
        eprintln!(
            "deleted {victim}: the listing skipped one live repository, confirmed by id and kept"
        );
        Ok(())
    }
    .await;
    // Both fleets go back whatever happened: the victim may be from either.
    s.side.put_back("scale").await?;
    outcome
}

/// Temporal holds the run while no worker serves the connection's queue: the worker dies, a
/// reconcile is requested and cannot finish, a worker comes back, and that same run completes
/// with the inventory intact. Local only: what is tested is the orchestration, not the forge.
/// `connection sync`: a reconcile, then a head refresh of every active repository, each a run
/// of its own under the sync.
async fn sync(s: &Suite) -> anyhow::Result<()> {
    let r: repoplane_proto::api::Sync = s
        .stack
        .cli_json(&[
            "connection",
            "sync",
            &s.connection,
            "--wait",
            "--timeout",
            "20m",
        ])
        .await?;
    anyhow::ensure!(
        r.status.as_str() == "succeeded",
        "sync {}: {:?}",
        r.status.as_str(),
        r.error
    );
    let (diffs, _) = compare(
        &expected(&s.side)?,
        &s.stack.repos(&s.connection, false).await?,
    );
    anyhow::ensure!(diffs.is_empty(), "{}", diffs.join("\n"));
    let active = s.stack.repos(&s.connection, false).await?.len() as i64;
    let h = r
        .head_refresh
        .ok_or_else(|| anyhow::anyhow!("the sync has no head refresh"))?;
    anyhow::ensure!(
        r.reconcile.status.as_str() == "succeeded" && r.reconcile.discovered == active,
        "reconcile {}: discovered {}, want {active}",
        r.reconcile.status.as_str(),
        r.reconcile.discovered
    );
    anyhow::ensure!(
        h.status.as_str() == "succeeded" && h.targeted == active && h.observed == active,
        "head refresh {}: targeted {}, observed {}, want {active}",
        h.status.as_str(),
        h.targeted,
        h.observed
    );
    if s.side.sandbox == "local"
        && std::env::var("REPOPLANE_RECORD_HISTORIES").as_deref() == Ok("1")
    {
        let dir = repoplane_e2e::workspace_root().join("crates/repoplane-flow/tests/histories");
        let raw = s
            .stack
            .history_json::<repoplane_flow::SyncWorkflow>(&r.id.to_string())
            .await?;
        std::fs::write(dir.join("sync.json"), raw)?;
    }
    Ok(())
}

/// A run whose workflow is ended from outside (here, terminated in Temporal) never records
/// itself; the API sees the workflow closed and records the run as failed, saying why. With no
/// worker, the run cannot finish before it is terminated.
async fn terminated(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    s.stack.kill_worker().await?;
    let outcome = async {
        let started: repoplane_proto::api::Reconcile = s
            .stack
            .cli_json(&["connection", "reconcile", &s.connection])
            .await?;
        let id = started.id.to_string();
        s.stack
            .terminate(&id, "e2e: terminated from outside")
            .await?;
        let done: repoplane_proto::api::Reconcile = s
            .stack
            .cli_json_failing(&["reconcile", "show", &id, "--wait", "--timeout", "1m"])
            .await?;
        anyhow::ensure!(
            done.status.as_str() == "failed"
                && done
                    .error
                    .as_deref()
                    .is_some_and(|e| e.contains("terminated")),
            "want failed, saying it was terminated; got {} {:?}",
            done.status.as_str(),
            done.error
        );
        Ok(())
    }
    .await;
    s.stack.start_worker().await?;
    outcome
}

/// A worker killed in the middle of a forge call: the run waits, and once a worker is back,
/// Temporal gives the call up when its heartbeats stop (30 s) and runs it again there. Without
/// heartbeats it waited out the call's 15-minute start-to-close timeout.
async fn worker_killed(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let proxy = ForgeProxy::start(&s.side).await?;
    let conn = "e2e-local-killed";
    s.stack
        .cli(&[
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
            "--base-url",
            &proxy.base_url,
        ])
        .await?;
    proxy.hold_listings(true);
    let started: repoplane_proto::api::Reconcile =
        s.stack.cli_json(&["connection", "reconcile", conn]).await?;
    let id = started.id.to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while proxy.held() == 0 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    s.stack.kill_worker().await?;
    let killed = std::time::Instant::now();
    let dead = async {
        anyhow::ensure!(proxy.held() > 0, "no forge call was in flight to kill");
        let waiting: repoplane_proto::api::Reconcile =
            s.stack.cli_json(&["reconcile", "show", &id]).await?;
        anyhow::ensure!(
            !waiting.status.is_terminal(),
            "with its worker dead the run ended anyway: {:?}",
            waiting.status
        );
        Ok(())
    }
    .await;
    // Whatever happened above, the forge answers again and a worker comes back.
    proxy.hold_listings(false);
    s.stack.start_worker().await?;
    dead?;
    let done: repoplane_proto::api::Reconcile = s
        .stack
        .cli_json(&["reconcile", "show", &id, "--wait", "--timeout", "5m"])
        .await?;
    let took = killed.elapsed();
    anyhow::ensure!(
        done.status.as_str() == "succeeded" && done.discovered == 123,
        "after the worker came back: {:?}, discovered {}, error {:?}",
        done.status,
        done.discovered,
        done.error
    );
    anyhow::ensure!(
        took < std::time::Duration::from_secs(180),
        "the run took {took:?} after its worker died: the call was not given up on its heartbeat"
    );
    eprintln!("the run finished {took:?} after its worker was killed mid-call");
    Ok(())
}

/// A forge that asks for a wait longer than one call's budget is left alone that long: the run
/// neither fails nor asks again early, since retrying inside the forge's window only spends its
/// quota again. The wait asked for here is an hour, so the run is ended once that is checked.
async fn rate_limited(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let proxy = ForgeProxy::start(&s.side).await?;
    let conn = "e2e-local-ratelimited";
    s.stack
        .cli(&[
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
            "--auth",
            "token",
            "--base-url",
            &proxy.base_url,
        ])
        .await?;
    proxy.rate_limit_next(1);
    let started: repoplane_proto::api::Reconcile =
        s.stack.cli_json(&["connection", "reconcile", conn]).await?;
    let id = started.id.to_string();
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    let waiting: repoplane_proto::api::Reconcile =
        s.stack.cli_json(&["reconcile", "show", &id]).await?;
    let outcome = (|| {
        anyhow::ensure!(
            !waiting.status.is_terminal(),
            "a rate-limited run ended instead of waiting: {:?} {:?}",
            waiting.status,
            waiting.error
        );
        anyhow::ensure!(
            proxy.rate_limited() == 1,
            "the forge was asked {} times within its window, want once",
            proxy.rate_limited()
        );
        Ok(())
    })();
    s.stack
        .terminate(&id, "e2e: the forge asked for an hour")
        .await?;
    outcome
}

/// A token the forge refuses will not get better with retries: the run fails at once, saying
/// why, and marks nothing.
async fn refused(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let proxy = ForgeProxy::start(&s.side).await?;
    let conn = "e2e-local-refused";
    s.stack
        .cli(&[
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
            "--auth",
            "token",
            "--base-url",
            &proxy.base_url,
        ])
        .await?;
    proxy.refuse_token(true);
    let begun = std::time::Instant::now();
    let out = s
        .stack
        .cli(&[
            "connection",
            "reconcile",
            conn,
            "--wait",
            "--output",
            "json",
        ])
        .await;
    let err = out
        .expect_err("a refused token must fail the reconcile")
        .to_string();
    anyhow::ensure!(err.contains("\"failed\"") && err.contains("401"), "{err}");
    anyhow::ensure!(
        begun.elapsed() < std::time::Duration::from_secs(30),
        "failing took {:?}: a 401 must not be retried",
        begun.elapsed()
    );
    Ok(())
}

/// `connection add` has a worker on the connection's queue list the organisation first, and
/// refuses, saying why, a connection no worker can serve: a host no worker holds a token for (a
/// connection cannot have a worker send its token anywhere else), or a queue no worker serves.
/// `--no-check` adds it anyway, and its first run then fails at once.
async fn no_token(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    // The worker holds a token for 127.0.0.1; localhost is the same machine, but not that key.
    let conn = "e2e-local-no-token";
    let add = |extra: &'static [&'static str]| {
        let mut args = vec![
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
        ];
        args.extend_from_slice(extra);
        args
    };
    let refused = |err: anyhow::Error, why: &str| {
        let err = err.to_string();
        anyhow::ensure!(
            err.contains(why) && err.contains("The connection was not added"),
            "{err}"
        );
        Ok(())
    };

    let begun = std::time::Instant::now();
    let Err(err) = s
        .stack
        .cli(&add(&["--base-url", "http://localhost:1"]))
        .await
    else {
        anyhow::bail!("a connection to a host no worker holds a token for was added");
    };
    refused(err, "hold no token for localhost:1/")?;
    anyhow::ensure!(
        begun.elapsed() < std::time::Duration::from_secs(30),
        "refusing took {:?}: a missing token must not be retried",
        begun.elapsed()
    );

    let Err(err) = s
        .stack
        .cli(&add(&[
            "--base-url",
            "http://localhost:1",
            "--queue",
            "nobody",
        ]))
        .await
    else {
        anyhow::bail!("a connection on a queue no worker serves was added");
    };
    refused(err, "no worker on queue \"nobody\"")?;
    anyhow::ensure!(
        s.stack.cli(&["connection", "show", conn]).await.is_err(),
        "a refused connection was recorded"
    );

    s.stack
        .cli(&add(&["--base-url", "http://localhost:1", "--no-check"]))
        .await?;
    let begun = std::time::Instant::now();
    let out = s
        .stack
        .cli(&[
            "connection",
            "reconcile",
            conn,
            "--wait",
            "--timeout",
            "2m",
            "--output",
            "json",
        ])
        .await;
    let Err(err) = out else {
        anyhow::bail!("a reconcile with no token for its forge succeeded");
    };
    let err = err.to_string();
    anyhow::ensure!(
        err.contains("\"failed\"") && err.contains("hold no token for localhost:1/"),
        "{err}"
    );
    anyhow::ensure!(
        begun.elapsed() < std::time::Duration::from_secs(30),
        "failing took {:?}: a missing token must not be retried",
        begun.elapsed()
    );
    Ok(())
}

/// A repository the forge will not let the token read (a 403 on it alone) is reported unread,
/// with the forge's reason, and the refresh succeeds with the others; a token every repository
/// refuses fails the refresh, saying so.
async fn unreadable(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let proxy = ForgeProxy::start(&s.side).await?;
    let conn = "e2e-local-unreadable";
    s.stack
        .cli(&[
            "connection",
            "add",
            "forgejo",
            "--name",
            conn,
            "--org",
            &s.side.org,
            "--base-url",
            &proxy.base_url,
        ])
        .await?;
    s.stack.reconcile(conn).await?;
    let refresh = [
        "repo",
        "refresh-heads",
        "--connection",
        conn,
        "--wait",
        "--timeout",
        "5m",
    ];

    proxy.refuse_git(Some("billing-api"));
    let one = async {
        let mut runs: Vec<HeadRefresh> = s.stack.cli_json(&refresh).await?;
        let r = runs.pop().ok_or_else(|| anyhow::anyhow!("no refresh"))?;
        anyhow::ensure!(
            r.status.as_str() == "succeeded" && r.unread == 1 && r.observed == r.targeted - 1,
            "one repository refused: {} targeted {} observed {} unread {} error {:?}",
            r.status.as_str(),
            r.targeted,
            r.observed,
            r.unread,
            r.error
        );
        let named = &r.unread_repositories;
        anyhow::ensure!(
            named.len() == 1
                && named[0].full_name.ends_with("/billing-api")
                && named[0].reason.contains("403"),
            "unread named {named:?}"
        );
        Ok(())
    }
    .await;

    proxy.refuse_git(Some("*"));
    let all = async {
        let mut runs: Vec<HeadRefresh> = s.stack.cli_json_failing(&refresh).await?;
        let r = runs.pop().ok_or_else(|| anyhow::anyhow!("no refresh"))?;
        anyhow::ensure!(
            r.status.as_str() == "failed"
                && r.error.as_deref().is_some_and(|e| e.contains("refused it")),
            "every repository refused: {} unread {} error {:?}",
            r.status.as_str(),
            r.unread,
            r.error
        );
        Ok(())
    }
    .await;
    proxy.refuse_git(None);
    one.and(all)
}

/// A topic added on the forge shows up after the next reconcile, and nothing else moves.
async fn topic_changed(s: &Suite) -> anyhow::Result<()> {
    if !s.side.forge.caps().topics {
        eprintln!("skipped: {} has no topics", s.side.forge);
        return Ok(());
    }
    let env = s.side.env("shapes")?;
    env.forge
        .set_topics("parser-svc", &["service".into(), "e2e-topic".into()])
        .await?;
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(r.recorded == 1, "recorded {}, want 1", r.recorded);
    let repos = s.stack.repos(&s.connection, false).await?;
    let name = s.side.full_name("parser-svc");
    let p = repos
        .iter()
        .find(|r| r.full_name == name)
        .ok_or_else(|| anyhow::anyhow!("{name} missing"))?;
    anyhow::ensure!(
        p.topics
            .as_ref()
            .is_some_and(|t| t.contains(&"e2e-topic".to_string())),
        "topics {:?}",
        p.topics
    );
    Ok(())
}

/// A commit pushed to a default branch moves that repository's HEAD, and only that one.
async fn commit_pushed(s: &Suite) -> anyhow::Result<()> {
    let sha = s.side.push_commit("shapes", "compliant", "main").await?;
    // A push changes what a repository holds, not whether it exists: the inventory stays put,
    // and the head refresh sees the move.
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(r.recorded == 0, "reconcile recorded {}, want 0", r.recorded);
    let h = s.stack.refresh_heads(&s.connection, &[]).await?;
    anyhow::ensure!(h.changed == 1, "head refresh changed {}, want 1", h.changed);
    let repos = s.stack.repos(&s.connection, false).await?;
    let name = s.side.full_name("compliant");
    let c = repos
        .iter()
        .find(|r| r.full_name == name)
        .ok_or_else(|| anyhow::anyhow!("{name} missing"))?;
    anyhow::ensure!(
        c.head.as_deref() == Some(sha.as_str()),
        "head {:?}, want {sha}",
        c.head
    );
    Ok(())
}

/// Waits until the forge's own listing matches the locks — default branch, archived, topics —
/// before a reconcile is asked to: forges take a moment to show a reset in their listings
/// (Azure DevOps about two seconds for a default branch), and what is tested is repoplane
/// recording what the listing says, not how fast a forge propagates.
async fn settled(s: &Suite) -> anyhow::Result<()> {
    let want = expected(&s.side)?;
    let lister = s.side.lister()?;
    let begun = std::time::Instant::now();
    let mut polls = 0;
    loop {
        polls += 1;
        let listed: std::collections::BTreeMap<_, _> = repoplane_forge::list_all(lister.as_ref())
            .await?
            .into_iter()
            .map(|r| (r.snapshot.full_name.clone(), r.snapshot))
            .collect();
        let differing: Vec<&String> = want
            .iter()
            .filter(|(name, e)| {
                listed.get(*name).is_none_or(|r| {
                    let topics = r.topics.clone().map(|mut t| {
                        t.sort();
                        t
                    });
                    (e.default_branch.is_some() && r.default_branch != e.default_branch)
                        || r.archived != e.archived
                        || topics != e.topics
                })
            })
            .map(|(name, _)| name)
            .collect();
        if differing.is_empty() {
            if polls > 1 {
                eprintln!(
                    "{} listing settled after {:?} ({polls} reads)",
                    s.side.forge,
                    begun.elapsed()
                );
            }
            return Ok(());
        }
        if begun.elapsed() > std::time::Duration::from_secs(120) {
            anyhow::bail!(
                "after {:?} the forge still lists differently: {differing:?}",
                begun.elapsed()
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Pages through the forge's listing, through repoplane's own adapter, until `name` appears
/// matching `want`; returns how long that took. Fails after two minutes, naming what it saw.
async fn until_listed(
    s: &Suite,
    name: &str,
    want: impl Fn(&repoplane_proto::inventory::RepoSnapshot) -> bool,
) -> anyhow::Result<std::time::Duration> {
    let lister = s.side.lister()?;
    let begun = std::time::Instant::now();
    loop {
        let seen = repoplane_forge::list_all(lister.as_ref())
            .await?
            .into_iter()
            .find(|r| r.snapshot.full_name == name)
            .map(|r| r.snapshot);
        if seen.as_ref().is_some_and(&want) {
            return Ok(begun.elapsed());
        }
        if begun.elapsed() > std::time::Duration::from_secs(120) {
            anyhow::bail!(
                "after {:?} the forge still lists {name} as {seen:?}",
                begun.elapsed()
            );
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// A HEAD describes a branch. When the default branch changes, the recorded HEAD described the
/// old one: the next reconcile clears it, and the next head refresh reads the new branch's.
async fn default_branch_changed(s: &Suite) -> anyhow::Result<()> {
    let sha = s
        .side
        .push_commit_to("shapes", "compliant", "main", "e2e-default")
        .await?;
    let env = s.side.env("shapes")?;
    env.forge
        .update_settings(
            "compliant",
            Settings {
                default_branch: Some("e2e-default".into()),
                ..Settings::default()
            },
        )
        .await?;
    let name = s.side.full_name("compliant");
    // Wait for the forge's own listing to show the change before asking repoplane to see it:
    // what is tested is that a reconcile records what the listing says, not how quickly a forge
    // propagates a settings change to its listing.
    let lag = until_listed(s, &name, |r| {
        r.default_branch.as_deref() == Some("e2e-default")
    })
    .await?;
    eprintln!(
        "{} listed the new default branch after {lag:?}",
        s.side.forge
    );
    let find = |repos: Vec<repoplane_proto::api::Repository>| {
        repos
            .into_iter()
            .find(|r| r.full_name == name)
            .ok_or_else(|| anyhow::anyhow!("{name} missing"))
    };
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(r.recorded == 1, "recorded {}, want 1", r.recorded);
    let c = find(s.stack.repos(&s.connection, false).await?)?;
    anyhow::ensure!(
        c.default_branch.as_deref() == Some("e2e-default") && c.head.is_none(),
        "after the reconcile: branch {:?}, head {:?}; want e2e-default and no HEAD",
        c.default_branch,
        c.head
    );
    s.stack.refresh_heads(&s.connection, &[]).await?;
    let c = find(s.stack.repos(&s.connection, false).await?)?;
    anyhow::ensure!(
        c.head.as_deref() == Some(sha.as_str()),
        "head {:?}, want {sha}",
        c.head
    );
    Ok(())
}

/// Archiving on the forge (disabling, on Azure DevOps) shows up after the next reconcile.
async fn archived(s: &Suite) -> anyhow::Result<()> {
    let env = s.side.env("shapes")?;
    env.forge
        .update_settings(
            "scaffold",
            Settings {
                archived: Some(true),
                ..Settings::default()
            },
        )
        .await?;
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(r.recorded == 1, "recorded {}, want 1", r.recorded);
    s.stack.refresh_heads(&s.connection, &[]).await?;
    let repos = s.stack.repos(&s.connection, false).await?;
    let name = s.side.full_name("scaffold");
    let a = repos
        .iter()
        .find(|r| r.full_name == name)
        .ok_or_else(|| anyhow::anyhow!("{name} missing"))?;
    anyhow::ensure!(a.archived, "{name} not archived");
    if s.side.forge == ForgeKind::Azuredevops {
        anyhow::ensure!(
            a.head.is_none(),
            "a disabled repository's HEAD cannot be read, got {:?}",
            a.head
        );
    }
    Ok(())
}

/// A repository deleted on the forge is marked inaccessible, never removed; recreated, it is a
/// new repository with a new identity. Local only: reset cannot recreate a repository, and on a
/// cloud forge `apply` would spend the repository-creation budget.
async fn deleted(s: &Suite) -> anyhow::Result<()> {
    if s.side.sandbox != "local" {
        eprintln!("skipped: only on the local sandbox");
        return Ok(());
    }
    let env = s.side.env("shapes")?;
    let before = s.stack.repos(&s.connection, false).await?;
    let name = s.side.full_name("tagged");
    let old = before
        .iter()
        .find(|r| r.full_name == name)
        .ok_or_else(|| anyhow::anyhow!("{name} missing"))?
        .clone();
    env.forge.delete("tagged").await?;
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.inaccessible == 1,
        "inaccessible {}, want 1",
        r.inaccessible
    );
    let all = s.stack.repos(&s.connection, true).await?;
    let gone = all
        .iter()
        .find(|r| r.id == old.id)
        .ok_or_else(|| anyhow::anyhow!("{name} was removed"))?;
    anyhow::ensure!(
        gone.status == RepositoryStatus::Inaccessible,
        "status {:?}",
        gone.status
    );

    env.apply()
        .await
        .map_err(|e| anyhow::anyhow!("recreate: {e}"))?;
    s.stack.reconcile(&s.connection).await?;
    let all = s.stack.repos(&s.connection, true).await?;
    let same_name: Vec<_> = all.iter().filter(|r| r.full_name == name).collect();
    anyhow::ensure!(
        same_name.len() == 2,
        "want the old and the new {name}, got {}",
        same_name.len()
    );
    anyhow::ensure!(
        same_name.iter().any(|r| r.id != old.id
            && r.status == RepositoryStatus::Active
            && r.external_id != old.external_id),
        "the recreated repository must be a new identity"
    );
    Ok(())
}
