//! The end-to-end harness. It stands up the real `repoplane-api`, `repoplane-worker` and
//! `repoplane` binaries on a throwaway Postgres, points them at a fleets sandbox, and puts that
//! sandbox back with forgelab after every scenario.
//!
//! What a sandbox should hold comes from the fleets' `fleet.lock.json` files, never from this
//! code: the harness only maps a fleet path to the name each forge gives it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, bail};
use axum::response::IntoResponse as _;
use base64::Engine as _;
use forgelab::fleet::read_lock;
use forgelab::forge::GitRemote;
use forgelab::sandbox::{CommandError, Env, Options, load_config};
use repoplane_proto::ForgeKind;
use repoplane_proto::api::{HeadRefresh, Reconcile, Repository};
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::process::{Child, Command};

/// The two fleets every sandbox holds. Their names never collide: `shapes` keeps the root and
/// `platform`/`services`, `scale` everything under `acme/`.
pub const FLEETS: [&str; 2] = ["shapes", "scale"];

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Which sandbox to run against, from `REPOPLANE_E2E_SANDBOX`. Unset means the suite is skipped:
/// it needs Docker, and for the cloud sandboxes, tokens.
pub fn sandbox() -> Option<String> {
    match std::env::var("REPOPLANE_E2E_SANDBOX") {
        Ok(s) if !s.is_empty() => Some(s),
        _ => {
            eprintln!("skipping end-to-end tests: set REPOPLANE_E2E_SANDBOX=local|gh|gl|ado");
            None
        }
    }
}

pub fn fleets_dir() -> PathBuf {
    std::env::var_os("REPOPLANE_FLEETS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("../fleets"))
}

// --- the forge side: a sandbox driven with forgelab ------------------------------------

/// Forgejo 15.0.8 (LTS), pinned by digest like forgelab's own tests and compose.yaml.
const FORGEJO_IMAGE: &str = "codeberg.org/forgejo/forgejo@sha256";
const FORGEJO_DIGEST: &str = "0a2e377fd3c5af3451bfa1f44e6f198f322b6d5e03f04a028b8e672f1ccddc9f";
const FORGEJO_CONFIG: &[(&str, &str)] = &[
    ("FORGEJO__database__DB_TYPE", "sqlite3"),
    ("FORGEJO__database__SQLITE_JOURNAL_MODE", "WAL"),
    ("FORGEJO__database__SQLITE_TIMEOUT", "20000"),
    ("FORGEJO__security__INSTALL_LOCK", "true"),
    (
        "FORGEJO__security__SECRET_KEY",
        "repoplane-e2e-not-a-secret",
    ),
    ("FORGEJO__repository__DEFAULT_BRANCH", "main"),
    ("FORGEJO__indexer__REPO_INDEXER_ENABLED", "false"),
    ("FORGEJO__actions__ENABLED", "false"),
    ("FORGEJO__cron__ENABLED", "false"),
    ("FORGEJO__mailer__ENABLED", "false"),
    ("FORGEJO__log__LEVEL", "warn"),
    ("FORGEJO__server__OFFLINE_MODE", "true"),
    ("FORGEJO__service__DISABLE_REGISTRATION", "true"),
];

pub struct ForgeSide {
    pub sandbox: String,
    pub forge: ForgeKind,
    pub base_url: String,
    pub org: String,
    /// Azure DevOps: the project that repositories without a namespace go to.
    pub default_project: String,
    /// The variable the repoplane connection names; the worker gets it in its environment.
    pub token_env: String,
    token: SecretString,
    config_path: PathBuf,
    _tmp: Option<tempfile::TempDir>,
    _forgejo: Option<ContainerAsync<GenericImage>>,
}

impl ForgeSide {
    pub async fn start(sandbox: &str) -> anyhow::Result<ForgeSide> {
        if sandbox == "local" {
            return Self::start_local().await;
        }
        let config_path = fleets_dir().join("sandboxes.yaml");
        let cfg = load_config(&config_path).map_err(|e| anyhow::anyhow!("{e}"))?;
        let sb = cfg.sandbox(sandbox).map_err(|e| anyhow::anyhow!("{e}"))?;
        let token = std::env::var(&sb.token_env)
            .ok()
            .filter(|t| !t.is_empty())
            .with_context(|| {
                format!(
                    "sandbox {sandbox}: {} is not set (source fleets/.env)",
                    sb.token_env
                )
            })?;
        let forge: ForgeKind = sb.forge.parse().map_err(anyhow::Error::msg)?;
        Ok(ForgeSide {
            sandbox: sandbox.to_string(),
            forge,
            base_url: if sb.base_url.is_empty() {
                forge.default_base_url().unwrap_or_default().to_string()
            } else {
                sb.base_url.clone()
            },
            org: sb.org.clone(),
            default_project: sb.default_project.clone(),
            token_env: sb.token_env.clone(),
            token: token.into(),
            config_path,
            _tmp: None,
            _forgejo: None,
        })
    }

    /// A Forgejo of our own, with an admin, a token and a sandboxes.yaml pointing at it.
    async fn start_local() -> anyhow::Result<ForgeSide> {
        let mut image = GenericImage::new(FORGEJO_IMAGE, FORGEJO_DIGEST)
            .with_exposed_port(3000.tcp())
            .with_wait_for(WaitFor::http(
                HttpWaitStrategy::new("/api/healthz")
                    .with_port(3000.tcp())
                    .with_expected_status_code(200u16),
            ))
            .with_startup_timeout(Duration::from_secs(120));
        for (k, v) in FORGEJO_CONFIG {
            image = image.with_env_var(*k, *v);
        }
        let container = image.start().await.context("start Forgejo")?;
        let port = container.get_host_port_ipv4(3000.tcp()).await?;
        let base_url = format!("http://127.0.0.1:{port}");
        let (user, password) = ("labadmin", "labadmin-not-a-secret");
        let mut res = container
            .exec(ExecCommand::new([
                "su",
                "git",
                "-c",
                &format!(
                    "forgejo admin user create --username {user} --password {password} --email admin@repoplane.test --admin --must-change-password=false"
                ),
            ]))
            .await?;
        let out = String::from_utf8_lossy(&res.stdout_to_vec().await?).to_string();
        if !out.contains("successfully created") {
            bail!("create the Forgejo admin: {out}");
        }
        let v: serde_json::Value = reqwest::Client::new()
            .post(format!("{base_url}/api/v1/users/{user}/tokens"))
            .basic_auth(user, Some(password))
            .json(&serde_json::json!({"name": "e2e", "scopes": ["write:organization", "write:repository", "write:user"]}))
            .send()
            .await?
            .json()
            .await?;
        let token = v["sha1"]
            .as_str()
            .context("mint a Forgejo token")?
            .to_string();
        let tmp = tempfile::tempdir()?;
        let config_path = tmp.path().join("sandboxes.yaml");
        std::fs::write(
            &config_path,
            format!(
                "version: 1\nsandboxes:\n  local:\n    forge: forgejo\n    base_url: {base_url}\n    org: forgelab-sandbox\n    token_env: FORGELAB_LOCAL_TOKEN\n"
            ),
        )?;
        Ok(ForgeSide {
            sandbox: "local".into(),
            forge: ForgeKind::Forgejo,
            base_url,
            org: "forgelab-sandbox".into(),
            default_project: String::new(),
            token_env: "FORGELAB_LOCAL_TOKEN".into(),
            token: token.into(),
            config_path,
            _tmp: Some(tmp),
            _forgejo: Some(container),
        })
    }

    /// A repoplane forge client for this sandbox, for checking an adapter directly.
    pub fn lister(&self) -> anyhow::Result<Box<dyn repoplane_forge::Lister>> {
        let spec = repoplane_proto::inventory::ConnectionSpec {
            id: repoplane_proto::id::ConnectionId::new(),
            forge: self.forge,
            org: self.org.clone(),
            base_url: self.base_url.clone(),
            token_env: self.token_env.clone(),
            queue: "default".into(),
        };
        Ok(repoplane_forge::lister_with_token(
            &spec,
            repoplane_forge::Token::new(self.token.expose_secret())?,
            repoplane_forge::ReqwestTransport::new()?,
            tokio_util::sync::CancellationToken::new(),
        )?)
    }

    /// The token, for the worker's environment only.
    pub fn worker_env(&self) -> (String, String) {
        (
            self.token_env.clone(),
            self.token.expose_secret().to_string(),
        )
    }

    pub fn env(&self, fleet: &str) -> anyhow::Result<Env> {
        Env::open(Options {
            fleet_dir: fleets_dir().join(fleet).to_string_lossy().into_owned(),
            config_path: self.config_path.to_string_lossy().into_owned(),
            sandbox: self.sandbox.clone(),
            yes: true,
            token: Some(self.token.clone()),
            out: Some(std::sync::Arc::new(
                std::sync::Mutex::new(std::io::stderr()),
            )),
            ..Options::default()
        })
        .map_err(|e| anyhow::anyhow!("open {fleet} on {}: {e}", self.sandbox))
    }

    /// Before the suite: a local sandbox is built; a cloud one must already hold both fleets.
    /// Drift is put back; a guard failure stops the suite, because resetting against the wrong
    /// thing is the one mistake that cannot be taken back.
    pub async fn prepare(&self) -> anyhow::Result<()> {
        for fleet in FLEETS {
            let env = self.env(fleet)?;
            if self.sandbox == "local" {
                env.apply()
                    .await
                    .map_err(|e| anyhow::anyhow!("apply {fleet}: {e}"))?;
                continue;
            }
            match env.verify().await {
                Ok(()) => {}
                Err(CommandError::Drift(d)) => {
                    eprintln!("{fleet} drifted on {}, resetting:\n{d}", self.sandbox);
                    self.put_back(fleet).await?;
                }
                Err(e) => bail!(
                    "{fleet} on {}: {e}\nThe sandbox is not in a state reset can repair: run `forgelab apply --sandbox {} --fleet {fleet} --config sandboxes.yaml` in the fleets checkout",
                    self.sandbox,
                    self.sandbox
                ),
            }
        }
        Ok(())
    }

    /// After a scenario: reset, then verify. Reset cannot recreate a repository a scenario
    /// deleted; on the local sandbox, where `apply` costs nothing, a reset that refuses is
    /// followed by `apply` and a second reset. `apply` never comes first: it waits for every
    /// repository to be at its baseline, which only reset restores.
    pub async fn put_back(&self, fleet: &str) -> anyhow::Result<()> {
        let env = self.env(fleet)?;
        if let Err(e) = env.reset().await {
            if self.sandbox != "local" {
                bail!("reset {fleet}: {e}");
            }
            eprintln!("reset {fleet} refused ({e}); applying first");
            env.apply()
                .await
                .map_err(|e| anyhow::anyhow!("apply {fleet}: {e}"))?;
            env.reset()
                .await
                .map_err(|e| anyhow::anyhow!("reset {fleet}: {e}"))?;
        }
        env.verify()
            .await
            .map_err(|e| anyhow::anyhow!("verify {fleet} after reset: {e}"))
    }

    /// The full name a forge gives a fleet path.
    pub fn full_name(&self, fleet_path: &str) -> String {
        match self.forge {
            ForgeKind::Forgejo | ForgeKind::Github => {
                format!("{}/{}", self.org, fleet_path.replace('/', "-"))
            }
            ForgeKind::Gitlab => format!("{}/{fleet_path}", self.org),
            ForgeKind::Azuredevops => match fleet_path.split_once('/') {
                Some((project, rest)) => {
                    format!("{}/{project}/{}", self.org, rest.replace('/', "-"))
                }
                None => format!("{}/{}/{fleet_path}", self.org, self.default_project),
            },
        }
    }

    /// Pushes one new commit onto a repository's default branch and returns its id.
    pub async fn push_commit(
        &self,
        fleet: &str,
        fleet_path: &str,
        branch: &str,
    ) -> anyhow::Result<String> {
        self.push_commit_to(fleet, fleet_path, branch, branch).await
    }

    /// Pushes one new commit, made on top of `from`, to the branch `to` (created if need be).
    pub async fn push_commit_to(
        &self,
        fleet: &str,
        fleet_path: &str,
        from: &str,
        to: &str,
    ) -> anyhow::Result<String> {
        let env = self.env(fleet)?;
        let remote = env
            .forge
            .git_remote(fleet_path)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let dir = tempfile::tempdir()?;
        let work = dir.path().join("w");
        git(
            &remote,
            dir.path(),
            &[
                "clone",
                "--depth",
                "1",
                "--branch",
                from,
                remote.url.as_str(),
                "w",
            ],
        )
        .await?;
        std::fs::write(
            work.join("E2E.md"),
            format!(
                "pushed by the repoplane e2e suite\n{:?}\n",
                std::time::SystemTime::now()
            ),
        )?;
        git(&remote, &work, &["add", "E2E.md"]).await?;
        git(
            &remote,
            &work,
            &[
                "-c",
                "user.name=repoplane e2e",
                "-c",
                "user.email=e2e@repoplane.test",
                "commit",
                "-q",
                "-m",
                "e2e: move HEAD",
            ],
        )
        .await?;
        git(
            &remote,
            &work,
            &["push", "-q", "origin", &format!("HEAD:refs/heads/{to}")],
        )
        .await?;
        let sha = git(&remote, &work, &["rev-parse", "HEAD"]).await?;
        Ok(sha.trim().to_string())
    }
}

/// Runs git with the forge credential as an Authorization header scoped to the forge's origin,
/// passed through the environment so it never appears on a command line.
async fn git(remote: &GitRemote, dir: &Path, args: &[&str]) -> anyhow::Result<String> {
    let mut cmd = Command::new("git");
    cmd.args(args)
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0");
    if let Some(auth) = &remote.auth {
        let basic = base64::engine::general_purpose::STANDARD.encode(format!(
            "{}:{}",
            auth.username,
            auth.secret.expose_secret()
        ));
        cmd.env("GIT_CONFIG_COUNT", "1")
            .env(
                "GIT_CONFIG_KEY_0",
                format!("http.{}/.extraHeader", remote.origin()),
            )
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Basic {basic}"),
            );
    }
    let out = cmd.output().await?;
    if !out.status.success() {
        bail!(
            "git {}: {}",
            args.first().unwrap_or(&""),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

// --- the repoplane side: API, worker and CLI -----------------------------------------------

pub struct Stack {
    pub api_url: String,
    api_token: String,
    bin: PathBuf,
    pub logs: PathBuf,
    /// The Temporal frontend, as `http://127.0.0.1:<port>`.
    pub temporal: String,
    forge_token: (String, String),
    _api: Child,
    worker: tokio::sync::Mutex<Option<Child>>,
    _temporal: ContainerAsync<GenericImage>,
    _pg: ContainerAsync<GenericImage>,
}

/// The Temporal CLI's dev server, the version of the local `temporal`, pinned by digest.
const TEMPORAL_IMAGE: &str = "temporalio/temporal@sha256";
const TEMPORAL_DIGEST: &str = "ad4c82c97bd12b417d1ea942610dbcd511afb250c4d5ed26c694009533df447e";

/// A Temporal dev server, answering once its frontend reports SERVING.
async fn start_temporal() -> anyhow::Result<(ContainerAsync<GenericImage>, String)> {
    let c = GenericImage::new(TEMPORAL_IMAGE, TEMPORAL_DIGEST)
        .with_exposed_port(7233.tcp())
        .with_cmd(["server", "start-dev", "--ip", "0.0.0.0", "--headless"])
        .start()
        .await
        .context("start Temporal")?;
    let port = c.get_host_port_ipv4(7233.tcp()).await?;
    for _ in 0..120 {
        let mut res = c
            .exec(ExecCommand::new([
                "temporal",
                "operator",
                "cluster",
                "health",
                "--address",
                "127.0.0.1:7233",
            ]))
            .await?;
        let out = String::from_utf8_lossy(&res.stdout_to_vec().await?).to_string();
        if out.contains("SERVING") {
            return Ok((c, format!("http://127.0.0.1:{port}")));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("Temporal never reported SERVING")
}

fn bin_dir() -> PathBuf {
    std::env::var_os("REPOPLANE_BIN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target/debug"))
}

fn free_port() -> anyhow::Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

impl Stack {
    /// The fields of the worker's latest log line containing `message`, ANSI colour removed.
    /// How much the worker has logged so far, to read only what comes after.
    pub fn worker_log_len(&self) -> usize {
        std::fs::read_to_string(self.logs.join("worker.log"))
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Worker log lines after `mark` containing both `message` and `needle`.
    pub fn worker_log_count_since(&self, mark: usize, message: &str, needle: &str) -> usize {
        let raw = std::fs::read_to_string(self.logs.join("worker.log")).unwrap_or_default();
        let plain = strip_ansi(raw.get(mark..).unwrap_or(""));
        plain
            .lines()
            .filter(|l| l.contains(message) && l.contains(needle))
            .count()
    }

    /// Postgres and Temporal in containers; the API and the worker as real processes, the
    /// worker with the forge token in its environment and the API without it.
    pub async fn start(forge_token: (String, String)) -> anyhow::Result<Stack> {
        let bin = bin_dir();
        for b in ["repoplane-api", "repoplane-worker", "repoplane"] {
            if !bin.join(b).exists() {
                bail!(
                    "{} is missing: build first (make e2e does)",
                    bin.join(b).display()
                );
            }
        }
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
            .context("start Postgres")?;
        let pg_port = pg.get_host_port_ipv4(5432.tcp()).await?;
        let (temporal_container, temporal) = start_temporal().await?;
        let db_url =
            format!("postgres://repoplane:repoplane-not-a-secret@127.0.0.1:{pg_port}/repoplane");
        let api_token = format!("token-{}", uuid_like());
        let port = free_port()?;
        let api_url = format!("http://127.0.0.1:{port}");
        let logs = workspace_root().join("target/e2e-logs");
        std::fs::create_dir_all(&logs)?;
        let cfg_dir = tempfile::tempdir()?;

        let api_env = |cmd: &mut Command| {
            cmd.env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("REPOPLANE_DATABASE_URL", &db_url)
                .env("REPOPLANE_TOKEN", &api_token)
                .env("RUST_LOG", "info,sqlx=warn")
                .current_dir(cfg_dir.path());
        };
        // Postgres may still be restarting after init: migrate until it answers.
        let mut migrated = false;
        for _ in 0..60 {
            let mut m = Command::new(bin.join("repoplane-api"));
            api_env(&mut m);
            if m.arg("migrate")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .await?
                .success()
            {
                migrated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !migrated {
            bail!("repoplane-api migrate never succeeded");
        }
        std::fs::write(
            cfg_dir.path().join("repoplane-api.yaml"),
            format!("listen: 127.0.0.1:{port}\ntemporal:\n  address: {temporal}\n"),
        )?;
        let mut api = Command::new(bin.join("repoplane-api"));
        api_env(&mut api);
        let api = api
            .arg("serve")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(logs.join("api.log"))?)
            .kill_on_drop(true)
            .spawn()?;

        let client = reqwest::Client::new();
        let mut ready = false;
        for _ in 0..100 {
            if client
                .get(format!("{api_url}/readyz"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !ready {
            bail!(
                "the API never became ready; see {}",
                logs.join("api.log").display()
            );
        }

        // The config directory must outlive the API's start only.
        drop(cfg_dir);
        let stack = Stack {
            api_url,
            api_token,
            bin,
            logs,
            temporal,
            forge_token,
            _api: api,
            worker: tokio::sync::Mutex::new(None),
            _temporal: temporal_container,
            _pg: pg,
        };
        std::fs::File::create(stack.logs.join("worker.log"))?;
        stack.start_worker().await?;
        Ok(stack)
    }

    /// A finished workflow's history as JSON, for the replay tests in repoplane-flow.
    pub async fn history_json<W: temporalio_common::HasWorkflowDefinition>(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let client = repoplane_flow::TemporalConfig {
            address: self.temporal.clone(),
            ..Default::default()
        }
        .connect("repoplane-e2e")
        .await?;
        let handle = client.get_workflow_handle::<W>(workflow_id);
        Ok(handle.fetch_history(Default::default()).to_json().await?)
    }

    /// Terminates a workflow, as an operator would from the Temporal UI.
    pub async fn terminate(&self, workflow_id: &str, reason: &str) -> anyhow::Result<()> {
        let client = repoplane_flow::TemporalConfig {
            address: self.temporal.clone(),
            ..Default::default()
        }
        .connect("repoplane-e2e")
        .await?;
        client
            .get_workflow_handle::<temporalio_client::UntypedWorkflow>(workflow_id)
            .terminate(
                temporalio_client::WorkflowTerminateOptions::builder()
                    .reason(reason)
                    .build(),
            )
            .await?;
        Ok(())
    }

    /// Starts the worker: the forge token in its environment, Temporal as its only peer.
    pub async fn start_worker(&self) -> anyhow::Result<()> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.logs.join("worker.log"))?;
        let mut worker = std::process::Command::new(self.bin.join("repoplane-worker"));
        worker
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env(&self.forge_token.0, &self.forge_token.1)
            .env("RUST_LOG", "info")
            .args(["--temporal-address", &self.temporal])
            .stdout(Stdio::null())
            .stderr(log);
        let mut cmd = Command::from(worker);
        *self.worker.lock().await = Some(cmd.kill_on_drop(true).spawn()?);
        Ok(())
    }

    /// Stops the worker the way a crash would: no chance to finish anything.
    pub async fn kill_worker(&self) -> anyhow::Result<()> {
        if let Some(mut w) = self.worker.lock().await.take() {
            w.kill().await?;
        }
        Ok(())
    }

    /// Runs the CLI; its stdout on success, its stderr as the error otherwise.
    pub async fn cli(&self, args: &[&str]) -> anyhow::Result<String> {
        let out = Command::new(self.bin.join("repoplane"))
            .args(args)
            .env("REPOPLANE_CONFIG", "/nonexistent/repoplane-e2e.yaml")
            .env("REPOPLANE_API_URL", &self.api_url)
            .env("REPOPLANE_TOKEN", &self.api_token)
            .output()
            .await?;
        if !out.status.success() {
            bail!(
                "repoplane {}: {}{}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).to_string())
    }

    /// Like [`Stack::cli_json`], for a command expected to exit non-zero (`--wait` on a run that
    /// fails): its output is read whatever the exit status.
    pub async fn cli_json_failing<T: DeserializeOwned>(&self, args: &[&str]) -> anyhow::Result<T> {
        let mut a = args.to_vec();
        a.extend(["--output", "json"]);
        let out = Command::new(self.bin.join("repoplane"))
            .args(&a)
            .env("REPOPLANE_CONFIG", "/nonexistent/repoplane-e2e.yaml")
            .env("REPOPLANE_API_URL", &self.api_url)
            .env("REPOPLANE_TOKEN", &self.api_token)
            .output()
            .await?;
        let raw = String::from_utf8_lossy(&out.stdout);
        serde_json::from_str(&raw)
            .with_context(|| format!("decode: {raw}{}", String::from_utf8_lossy(&out.stderr)))
    }

    pub async fn cli_json<T: DeserializeOwned>(&self, args: &[&str]) -> anyhow::Result<T> {
        let mut a = args.to_vec();
        a.extend(["--output", "json"]);
        let raw = self.cli(&a).await?;
        serde_json::from_str(&raw).with_context(|| format!("decode: {raw}"))
    }

    pub async fn reconcile(&self, connection: &str) -> anyhow::Result<Reconcile> {
        self.cli_json(&[
            "connection",
            "reconcile",
            connection,
            "--wait",
            "--timeout",
            "20m",
        ])
        .await
    }

    /// Reads HEADs of a connection's repositories (all, or those `filter` selects) and waits.
    pub async fn refresh_heads(
        &self,
        connection: &str,
        filter: &[&str],
    ) -> anyhow::Result<HeadRefresh> {
        let mut args = vec![
            "repo",
            "refresh-heads",
            "--connection",
            connection,
            "--wait",
            "--timeout",
            "20m",
        ];
        args.extend(filter);
        let mut runs: Vec<HeadRefresh> = self.cli_json(&args).await?;
        anyhow::ensure!(runs.len() == 1, "want one refresh, got {}", runs.len());
        let r = runs.remove(0);
        anyhow::ensure!(
            r.status.as_str() == "succeeded",
            "head refresh {}: {:?}",
            r.status.as_str(),
            r.error
        );
        Ok(r)
    }

    pub async fn repos(
        &self,
        connection: &str,
        include_inaccessible: bool,
    ) -> anyhow::Result<Vec<Repository>> {
        let mut args = vec!["repo", "list", "--connection", connection];
        if include_inaccessible {
            args.push("--include-inaccessible");
        }
        self.cli_json(&args).await
    }
}

fn uuid_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{n:x}{:x}", std::process::id())
}

// --- expectations ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Expected {
    pub fleet: &'static str,
    pub fleet_path: String,
    pub default_branch: Option<String>,
    pub archived: bool,
    pub topics: Option<Vec<String>>,
    pub head: Option<String>,
}

/// Every declared repository of both fleets, keyed by the full name the forge gives it.
pub fn expected(side: &ForgeSide) -> anyhow::Result<BTreeMap<String, Expected>> {
    let caps = side.forge.caps();
    let mut out = BTreeMap::new();
    for fleet in FLEETS {
        let lock = read_lock(&fleets_dir().join(fleet).join("fleet.lock.json"))
            .map_err(|e| anyhow::anyhow!("{fleet}: {e}"))?;
        for r in lock.repos {
            // A disabled Azure DevOps repository refuses reads: its HEAD is unknown.
            let unreadable =
                r.archived && caps.archived == repoplane_proto::forge::Archived::Disabled;
            // Exactly the fleet's: forgelab's marker is in the description, not the topics.
            let topics = caps.topics.then(|| r.topics.clone());
            out.insert(
                side.full_name(&r.name),
                Expected {
                    fleet,
                    fleet_path: r.name.clone(),
                    // An empty repository's default branch is the forge's to report or not.
                    default_branch: (!r.empty).then(|| r.default_branch.clone()),
                    archived: r.archived,
                    topics,
                    head: (!r.empty && !unreadable).then(|| r.baseline.clone()),
                },
            );
        }
    }
    Ok(out)
}

/// What differs between the locks and the inventory, one line per difference, plus the
/// repositories the forge has that no fleet declares.
pub fn compare(
    expected: &BTreeMap<String, Expected>,
    actual: &[Repository],
) -> (Vec<String>, Vec<String>) {
    let by_name: BTreeMap<&str, &Repository> =
        actual.iter().map(|r| (r.full_name.as_str(), r)).collect();
    let mut diffs = Vec::new();
    for (name, e) in expected {
        let Some(a) = by_name.get(name.as_str()) else {
            diffs.push(format!("{name}: missing from the inventory"));
            continue;
        };
        if e.default_branch.is_some() && a.default_branch != e.default_branch {
            diffs.push(format!(
                "{name}: default branch {:?}, want {:?}",
                a.default_branch, e.default_branch
            ));
        }
        if a.archived != e.archived {
            diffs.push(format!(
                "{name}: archived {}, want {}",
                a.archived, e.archived
            ));
        }
        let topics = a.topics.clone().map(|mut t| {
            t.sort();
            t
        });
        if topics != e.topics {
            diffs.push(format!("{name}: topics {topics:?}, want {:?}", e.topics));
        }
        if a.head != e.head {
            diffs.push(format!("{name}: head {:?}, want {:?}", a.head, e.head));
        }
    }
    let extras = actual
        .iter()
        .filter(|r| !expected.contains_key(&r.full_name))
        .map(|r| r.full_name.clone())
        .collect();
    (diffs, extras)
}

// --- a forge that shifts under a listing ---------------------------------------------------

/// A proxy in front of Forgejo that misbehaves on demand, on the organisation listing only:
///
/// - **shift** (`arm`): deletes a repository from page 1 the moment page 2 is asked for, so
///   every later page shifts back by one and the listing skips a repository that still exists —
///   the race ADR 0005 guards against, made deterministic;
/// - **rate limit** (`rate_limit_next`): answers 429 with a `Retry-After` longer than a forge
///   call's budget, which the activity reports as retryable and Temporal retries;
/// - **refuse** (`refuse_token`): answers 401, which no retry will fix.
pub struct ForgeProxy {
    pub base_url: String,
    /// The repository deleted, once it has been.
    pub deleted: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    faults: Faults,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Default)]
struct Faults {
    rate_limit: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    rate_limited: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    refuse: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for ForgeProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    listing: String,
    token: String,
    http: reqwest::Client,
    page_one: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    deleted: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    faults: Faults,
}

impl ForgeProxy {
    pub async fn start(side: &ForgeSide) -> anyhow::Result<ForgeProxy> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let deleted = std::sync::Arc::new(std::sync::Mutex::new(None));
        let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let faults = Faults::default();
        let state = ProxyState {
            upstream: side.base_url.clone(),
            listing: format!("/api/v1/orgs/{}/repos", side.org),
            token: side.token.expose_secret().to_string(),
            http: reqwest::Client::new(),
            page_one: Default::default(),
            deleted: deleted.clone(),
            armed: armed.clone(),
            faults: faults.clone(),
        };
        let app = axum::Router::new().fallback(forward).with_state(state);
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(ForgeProxy {
            base_url,
            deleted,
            armed,
            faults,
            task,
        })
    }

    /// The next `n` listing requests are answered 429, asking for an hour.
    pub fn rate_limit_next(&self, n: usize) {
        self.faults
            .rate_limit
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }

    /// How many listing requests were answered 429 so far.
    pub fn rate_limited(&self) -> usize {
        self.faults
            .rate_limited
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// While on, every listing request is answered 401.
    pub fn refuse_token(&self, on: bool) {
        self.faults
            .refuse
            .store(on, std::sync::atomic::Ordering::SeqCst);
    }

    /// From now on, the next listing that reaches page 2 shifts.
    pub fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

async fn forward(
    axum::extract::State(s): axum::extract::State<ProxyState>,
    method: axum::http::Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    body: bytes::Bytes,
) -> axum::response::Response {
    let pq = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/")
        .to_string();
    let listing = method == axum::http::Method::GET && uri.path() == s.listing;
    let page = uri
        .query()
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("page=")))
        .unwrap_or("1")
        .to_string();
    use std::sync::atomic::Ordering::SeqCst;
    if listing && s.faults.refuse.load(SeqCst) {
        return (
            axum::http::StatusCode::UNAUTHORIZED,
            r#"{"message":"token is required"}"#,
        )
            .into_response();
    }
    if listing
        && s.faults
            .rate_limit
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
    {
        s.faults.rate_limited.fetch_add(1, SeqCst);
        return (
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "3600")],
            r#"{"message":"rate limited"}"#,
        )
            .into_response();
    }
    if listing && page == "2" && s.armed.load(SeqCst) {
        let victim = s.page_one.lock().unwrap().first().cloned();
        let first_time = s.deleted.lock().unwrap().is_none();
        if let (Some(victim), true) = (victim, first_time) {
            let status = s
                .http
                .delete(format!("{}/api/v1/repos/{victim}", s.upstream))
                .header("authorization", format!("token {}", s.token))
                .send()
                .await
                .map(|r| r.status().as_u16());
            eprintln!("shifting proxy: deleted {victim} before page 2: {status:?}");
            *s.deleted.lock().unwrap() = Some(victim);
        }
    }
    let mut req = s
        .http
        .request(method, format!("{}{pq}", s.upstream))
        .body(body);
    for (k, v) in &headers {
        if !matches!(k.as_str(), "host" | "accept-encoding" | "content-length") {
            req = req.header(k, v);
        }
    }
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => return (axum::http::StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    };
    let status = resp.status();
    let mut out_headers = axum::http::HeaderMap::new();
    for (k, v) in resp.headers() {
        if !matches!(
            k.as_str(),
            "transfer-encoding" | "connection" | "content-length" | "content-encoding"
        ) {
            out_headers.insert(k, v.clone());
        }
    }
    let bytes = resp.bytes().await.unwrap_or_default();
    if listing && page == "1" {
        let names: Vec<String> = serde_json::from_slice::<Vec<serde_json::Value>>(&bytes)
            .unwrap_or_default()
            .iter()
            .filter_map(|r| r["full_name"].as_str().map(str::to_string))
            .collect();
        *s.page_one.lock().unwrap() = names;
    }
    (status, out_headers, bytes).into_response()
}

/// `s` without its ANSI colour codes.
fn strip_ansi(s: &str) -> String {
    let mut plain = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for d in chars.by_ref() {
                if d == 'm' {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }
    plain
}
