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
use base64::Engine as _;
use forgelab::fleet::read_lock;
use forgelab::forge::GitRemote;
use forgelab::sandbox::{CommandError, Env, Options, load_config};
use repoplane_proto::ForgeKind;
use repoplane_proto::api::{Reconcile, Repository};
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
/// forgelab's marker: on every repository it made, and absent from the locks.
pub const MARKER_TOPIC: &str = "forgelab-managed";

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
                branch,
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
            &["push", "-q", "origin", &format!("HEAD:refs/heads/{branch}")],
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
    admin_token: String,
    bin: PathBuf,
    pub logs: PathBuf,
    _api: Child,
    _worker: Child,
    _pg: ContainerAsync<GenericImage>,
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
    /// Postgres in a container; the API and the worker as real processes, the worker with the
    /// forge token in its environment and the API without it.
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
        let db_url =
            format!("postgres://repoplane:repoplane-not-a-secret@127.0.0.1:{pg_port}/repoplane");
        let admin_token = format!("admin-{}", uuid_like());
        let worker_token = format!("worker-{}", uuid_like());
        let port = free_port()?;
        let api_url = format!("http://127.0.0.1:{port}");
        let logs = workspace_root().join("target/e2e-logs");
        std::fs::create_dir_all(&logs)?;
        let cfg_dir = tempfile::tempdir()?;

        let api_env = |cmd: &mut Command| {
            cmd.env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .env("REPOPLANE_DATABASE_URL", &db_url)
                .env("REPOPLANE_ADMIN_TOKEN", &admin_token)
                .env("REPOPLANE_WORKER_TOKEN", &worker_token)
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
            format!("listen: 127.0.0.1:{port}\n"),
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

        let worker = Command::new(bin.join("repoplane-worker"))
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("REPOPLANE_WORKER_TOKEN", &worker_token)
            .env(&forge_token.0, &forge_token.1)
            .env("RUST_LOG", "info")
            .args(["--api-url", &api_url])
            .current_dir(cfg_dir.path())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(logs.join("worker.log"))?)
            .kill_on_drop(true)
            .spawn()?;
        // The config directory must outlive the processes' start only.
        drop(cfg_dir);
        Ok(Stack {
            api_url,
            admin_token,
            bin,
            logs,
            _api: api,
            _worker: worker,
            _pg: pg,
        })
    }

    /// Runs the CLI; its stdout on success, its stderr as the error otherwise.
    pub async fn cli(&self, args: &[&str]) -> anyhow::Result<String> {
        let out = Command::new(self.bin.join("repoplane"))
            .args(args)
            .env("REPOPLANE_CONFIG", "/nonexistent/repoplane-e2e.yaml")
            .env("REPOPLANE_API_URL", &self.api_url)
            .env("REPOPLANE_TOKEN", &self.admin_token)
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
            let topics = caps.topics.then(|| {
                let mut t = r.topics.clone();
                t.push(MARKER_TOPIC.to_string());
                t.sort();
                t
            });
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
