//! Lists an organisation straight through a `Lister`, with no API or worker around it — for
//! developing an adapter against a real forge.
//!
//!   cargo run -p repoplane-forge --example list -- <forge> <base_url> <org> <TOKEN_ENV>
//!
//! Prints one line per repository (full name, default branch, archived, HEAD, topics), then a
//! summary line with the repository count and how long listing and reading HEADs took.

use std::collections::HashSet;

use repoplane_forge::{ReqwestTransport, Token, list_all, lister};
use repoplane_proto::id::ConnectionId;
use repoplane_proto::inventory::ConnectionSpec;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [forge, base_url, org, token_env] = args.as_slice() else {
        eprintln!("usage: list <forge> <base_url> <org> <TOKEN_ENV>");
        std::process::exit(2);
    };
    let spec = ConnectionSpec {
        id: ConnectionId::new(),
        forge: forge.parse()?,
        org: org.clone(),
        base_url: base_url.trim_end_matches('/').to_string(),
        queue: "default".into(),
    };
    let l = lister(
        &spec,
        Token::from_env(token_env)?,
        ReqwestTransport::new()?,
        CancellationToken::new(),
    )?;
    let started = std::time::Instant::now();
    let repos = list_all(l.as_ref()).await?;
    let listed_in = started.elapsed();
    let heads = l.heads(&repos).await;
    let mut ids = HashSet::new();
    for (r, h) in repos.iter().zip(heads) {
        let s = &r.snapshot;
        if !ids.insert(s.external_id.clone()) {
            eprintln!("duplicate external id {} ({})", s.external_id, s.full_name);
        }
        let head = match h {
            Ok(h) => h.unwrap_or_else(|| "—".into()),
            Err(e) => format!("unread: {e}"),
        };
        println!(
            "{}\t{}\t{}\t{}\t{}",
            s.full_name,
            s.default_branch.as_deref().unwrap_or("—"),
            s.archived,
            head,
            s.topics
                .as_ref()
                .map(|t| {
                    let mut t = t.clone();
                    t.sort();
                    t.join(" ")
                })
                .unwrap_or_else(|| "—".into()),
        );
    }
    eprintln!(
        "{} repositories, {} distinct, listed in {:.1}s, HEADs read in {:.1}s",
        repos.len(),
        ids.len(),
        listed_in.as_secs_f64(),
        (started.elapsed() - listed_in).as_secs_f64()
    );
    Ok(())
}
