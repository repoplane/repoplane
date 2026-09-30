//! `repoplane`: the command line. A client of the `/v1` API and nothing else — it links neither
//! a database driver nor a forge client.

mod client;
mod context;
mod render;

use std::io::Write as _;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::bail;
use clap::{Args, Parser, Subcommand, ValueEnum};
use repoplane_proto::api::{AuthMode, CreateConnection, ReconcileStatus};
use repoplane_proto::{ForgeKind, secret};

use crate::client::{Client, RepoQuery};

#[derive(Parser)]
#[command(
    name = "repoplane",
    version,
    about = "Keep an accurate inventory of your repository fleet"
)]
struct Cli {
    /// The context from contexts.yaml to use.
    #[arg(long, global = true, env = "REPOPLANE_CONTEXT")]
    context: Option<String>,
    /// Overrides the context's API URL.
    #[arg(long, global = true)]
    api_url: Option<String>,
    #[arg(long, short, global = true, value_enum, default_value = "table")]
    output: Output,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Output {
    Table,
    Json,
}

#[derive(Subcommand)]
enum Command {
    /// Connections to forges: one organisation each.
    #[command(subcommand)]
    Connection(ConnectionCmd),
    /// The repository inventory.
    #[command(subcommand)]
    Repo(RepoCmd),
    /// Reconcile runs.
    #[command(subcommand)]
    Reconcile(ReconcileCmd),
}

#[derive(Subcommand)]
enum ConnectionCmd {
    /// Connect one organisation on a forge.
    Add(AddArgs),
    /// List connections.
    List,
    /// Show one connection, by name or id.
    Show { connection: String },
    /// List the organisation's repositories now and record them.
    Reconcile {
        connection: String,
        /// Wait for the run to finish, and exit non-zero if it fails.
        #[arg(long)]
        wait: bool,
        /// How long --wait waits.
        #[arg(long, default_value = "30m", value_parser = parse_duration)]
        timeout: Duration,
    },
}

#[derive(Args)]
struct AddArgs {
    /// forgejo, github, gitlab or azuredevops.
    forge: ForgeKind,
    #[arg(long)]
    name: String,
    /// A GitHub or Forgejo organisation, a GitLab group path (subgroups included), or an Azure
    /// DevOps organisation (all projects).
    #[arg(long)]
    org: String,
    #[arg(long, value_enum, default_value = "token")]
    auth: Auth,
    /// The NAME of the environment variable holding the token, in the worker's environment.
    #[arg(long)]
    token_env: String,
    /// The forge's URL. Required for Forgejo; defaults to the public service otherwise.
    #[arg(long)]
    base_url: Option<String>,
    /// The task queue whose workers serve this connection.
    #[arg(long)]
    queue: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Auth {
    Token,
}

#[derive(Subcommand)]
enum RepoCmd {
    /// List repositories, across connections unless --connection is given.
    List {
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        archived: Option<bool>,
        /// Include repositories absent from the latest complete listing.
        #[arg(long)]
        include_inaccessible: bool,
    },
}

#[derive(Subcommand)]
enum ReconcileCmd {
    /// Show a run.
    Show {
        id: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value = "30m", value_parser = parse_duration)]
        timeout: Duration,
    },
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let (n, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: u64 = n
        .parse()
        .map_err(|_| format!("{s:?}: want a duration like 90s, 30m or 1h"))?;
    Ok(Duration::from_secs(match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(format!("{s:?}: want a duration like 90s, 30m or 1h")),
    }))
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("repoplane: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn print(out: Output, table: String, json: &impl serde::Serialize) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    match out {
        Output::Table => stdout.write_all(table.as_bytes())?,
        Output::Json => {
            serde_json::to_writer_pretty(&mut stdout, json)?;
            stdout.write_all(b"\n")?;
        }
    }
    Ok(())
}

async fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    // Validate what can be validated before reading any configuration.
    if let Command::Connection(ConnectionCmd::Add(a)) = &cli.command
        && !secret::valid_env_name(&a.token_env)
    {
        bail!(secret::describe_env_name_error("--token-env", &a.token_env));
    }
    let ctx = context::resolve(cli.context.as_deref(), cli.api_url.as_deref())?;
    let api = Client::new(&ctx)?;
    let out = cli.output;
    match cli.command {
        Command::Connection(ConnectionCmd::Add(a)) => {
            let c = api
                .create_connection(&CreateConnection {
                    name: a.name,
                    forge: a.forge,
                    org: a.org,
                    base_url: a.base_url,
                    auth: match a.auth {
                        Auth::Token => AuthMode::Token,
                    },
                    token_env: a.token_env,
                    queue: a.queue,
                })
                .await?;
            print(out, render::connection(&c), &c)?;
        }
        Command::Connection(ConnectionCmd::List) => {
            let l = api.connections().await?;
            print(out, render::connections(&l.items), &l)?;
        }
        Command::Connection(ConnectionCmd::Show { connection }) => {
            let c = api.connection(&connection).await?;
            print(out, render::connection(&c), &c)?;
        }
        Command::Connection(ConnectionCmd::Reconcile {
            connection,
            wait,
            timeout,
        }) => {
            let r = api.reconcile(&connection).await?;
            let r = if wait {
                wait_for(&api, &r.id.to_string(), timeout).await?
            } else {
                r
            };
            print(out, render::reconcile(&r), &r)?;
            return Ok(exit_for(r.status, wait));
        }
        Command::Reconcile(ReconcileCmd::Show { id, wait, timeout }) => {
            let r = if wait {
                wait_for(&api, &id, timeout).await?
            } else {
                api.reconcile_status(&id, None).await?
            };
            print(out, render::reconcile(&r), &r)?;
            return Ok(exit_for(r.status, wait));
        }
        Command::Repo(RepoCmd::List {
            connection,
            topic,
            archived,
            include_inaccessible,
        }) => {
            let mut q = RepoQuery {
                connection,
                topic,
                archived,
                include_inaccessible,
                cursor: None,
            };
            let mut items = Vec::new();
            loop {
                let page = api.repositories(&q).await?;
                items.extend(page.items);
                match page.next_cursor {
                    Some(c) => q.cursor = Some(c),
                    None => break,
                }
            }
            print(out, render::repositories(&items), &items)?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn exit_for(status: ReconcileStatus, waited: bool) -> ExitCode {
    match status {
        ReconcileStatus::Failed => ExitCode::FAILURE,
        s if waited && !s.is_terminal() => ExitCode::from(3),
        _ => ExitCode::SUCCESS,
    }
}

/// Polls with long waits until the run ends or `timeout` passes.
async fn wait_for(
    api: &Client,
    id: &str,
    timeout: Duration,
) -> anyhow::Result<repoplane_proto::api::Reconcile> {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let r = api
            .reconcile_status(
                id,
                Some(
                    left.min(Duration::from_secs(30))
                        .max(Duration::from_secs(1)),
                ),
            )
            .await?;
        if r.status.is_terminal() || Instant::now() >= deadline {
            if !r.status.is_terminal() {
                eprintln!(
                    "repoplane: still {} after {}s",
                    r.status.as_str(),
                    timeout.as_secs()
                );
            }
            return Ok(r);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("2").unwrap(), Duration::from_secs(2));
        assert!(parse_duration("soon").is_err());
    }

    #[test]
    fn cli_parses_the_spec_example() {
        let cli = Cli::try_parse_from([
            "repoplane",
            "connection",
            "add",
            "forgejo",
            "--name",
            "lab",
            "--org",
            "acme-services",
            "--auth",
            "token",
            "--token-env",
            "FORGEJO_TOKEN",
            "--base-url",
            "http://localhost:3000",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Connection(ConnectionCmd::Add(_))
        ));
        Cli::try_parse_from([
            "repoplane",
            "repo",
            "list",
            "--topic",
            "node",
            "--archived",
            "false",
        ])
        .unwrap();
        Cli::try_parse_from(["repoplane", "connection", "reconcile", "lab", "--wait"]).unwrap();
    }
}
