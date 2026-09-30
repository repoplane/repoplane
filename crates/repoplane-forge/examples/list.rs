//! Lists an organisation straight through a `Lister`, with no API or worker around it — for
//! developing an adapter against a real forge.
//!
//!   cargo run -p repoplane-forge --example list -- <forge> <base_url> <org> <TOKEN_ENV>
//!
//! Prints one line per repository (full name, default branch, archived, HEAD, topics), then a
//! summary line with the page and repository counts.

use std::collections::HashSet;

use repoplane_forge::{ReqwestTransport, lister};
use repoplane_proto::id::ConnectionId;
use repoplane_proto::worker::ConnectionSpec;
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
        token_env: token_env.clone(),
    };
    let l = lister(&spec, ReqwestTransport::new()?, CancellationToken::new())?;
    let started = std::time::Instant::now();
    let (mut cursor, mut pages, mut total) = (None::<String>, 0, 0);
    let mut ids = HashSet::new();
    loop {
        let page = l.page(cursor.as_deref()).await?;
        let heads = l.heads(&page.repos).await?;
        pages += 1;
        for (r, h) in page.repos.iter().zip(heads) {
            let s = &r.snapshot;
            if !ids.insert(s.external_id.clone()) {
                eprintln!("duplicate external id {} ({})", s.external_id, s.full_name);
            }
            total += 1;
            println!(
                "{}\t{}\t{}\t{}\t{}",
                s.full_name,
                s.default_branch.as_deref().unwrap_or("—"),
                s.archived,
                h.as_deref().unwrap_or("—"),
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
        match page.next {
            Some(n) => cursor = Some(n),
            None => break,
        }
    }
    eprintln!(
        "{total} repositories, {} distinct, {pages} pages, {:.1}s",
        ids.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
