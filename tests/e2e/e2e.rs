//! The suite: one sandbox per run (`REPOPLANE_E2E_SANDBOX=local|gh|gl|ado`), both fleets on it,
//! every scenario followed by a forgelab reset. Scenarios run in order against one stack; a
//! failure is recorded, the sandbox is still put back, and the next scenario runs.

use std::future::Future;
use std::pin::Pin;

use forgelab::forge::Settings;
use repoplane_e2e::{ForgeSide, MARKER_TOPIC, ShiftingProxy, Stack, compare, expected, sandbox};
use repoplane_proto::ForgeKind;
use repoplane_proto::api::RepositoryStatus;

type Scenario<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

struct Suite {
    side: ForgeSide,
    stack: Stack,
    connection: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inventory() {
    let Some(sb) = sandbox() else { return };
    let side = ForgeSide::start(&sb).await.expect("start the forge side");
    side.prepare().await.expect("prepare the sandbox");
    let stack = Stack::start(side.worker_env())
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
        "--token-env".into(),
        side.token_env.clone(),
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
            "a repository skipped by a shifting listing stays active",
            true,
            Box::pin(shifting_listing(&s)),
        ),
        ("topic changed", true, Box::pin(topic_changed(&s))),
        ("commit pushed", true, Box::pin(commit_pushed(&s))),
        ("archived", true, Box::pin(archived(&s))),
        (
            "deleted repo goes inaccessible",
            true,
            Box::pin(deleted(&s)),
        ),
    ];
    for (name, mutates, scenario) in scenarios {
        eprintln!("--- {name}");
        let res = scenario.await;
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
        match res {
            Ok(()) => eprintln!("ok: {name}"),
            Err(e) => {
                eprintln!("FAILED: {name}: {e:#}");
                failures.push(format!("{name}: {e:#}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} scenario(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A reconcile records every declared repository of both fleets exactly as the locks say.
async fn baseline(s: &Suite) -> anyhow::Result<()> {
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.status.as_str() == "succeeded",
        "reconcile {}: {:?}",
        r.status.as_str(),
        r.error
    );
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

/// Reconciles and checks the inventory against the locks again.
async fn resync(s: &Suite) -> anyhow::Result<()> {
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(
        r.status.as_str() == "succeeded",
        "reconcile {}: {:?}",
        r.status.as_str(),
        r.error
    );
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
    anyhow::ensure!(cols[4..] == [MARKER_TOPIC, "node", "service"], "{line}");
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
    let proxy = ShiftingProxy::start(&s.side).await?;
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
            "--token-env",
            &s.side.token_env,
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
    let r = s.stack.reconcile(conn).await;
    let outcome = async {
        let r = r?;
        let victim = proxy
            .deleted
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("the proxy never shifted the listing"))?;
        let still_there: usize = s
            .stack
            .worker_log_field("did not return", "still_there")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
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

/// A topic added on the forge shows up after the next reconcile, and nothing else moves.
async fn topic_changed(s: &Suite) -> anyhow::Result<()> {
    if !s.side.forge.caps().topics {
        eprintln!("skipped: {} has no topics", s.side.forge);
        return Ok(());
    }
    let env = s.side.env("shapes")?;
    env.forge
        .set_topics(
            "parser-svc",
            &[MARKER_TOPIC.into(), "service".into(), "e2e-topic".into()],
        )
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
    let r = s.stack.reconcile(&s.connection).await?;
    anyhow::ensure!(r.recorded == 1, "recorded {}, want 1", r.recorded);
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
