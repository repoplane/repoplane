//! What the CLI prints. Tables are padded to their widest cell plus three spaces; absent values
//! print as `—`, which is never confused with an empty string or a zero.

use repoplane_proto::api::{Connection, Reconcile, Repository};

const ABSENT: &str = "—";

pub fn table(header: &[&str], rows: &[Vec<String>]) -> String {
    let width = |i: usize| {
        rows.iter()
            .map(|r| r[i].chars().count())
            .chain([header[i].chars().count()])
            .max()
            .unwrap_or(0)
    };
    let widths: Vec<usize> = (0..header.len()).map(width).collect();
    let line = |cells: Vec<&str>| {
        let mut s = String::new();
        for (i, c) in cells.iter().enumerate() {
            if i + 1 == cells.len() {
                s.push_str(c);
            } else {
                s.push_str(c);
                s.push_str(&" ".repeat(widths[i] - c.chars().count() + 3));
            }
        }
        s.trim_end().to_string()
    };
    let mut out = vec![line(header.to_vec())];
    for r in rows {
        out.push(line(r.iter().map(String::as_str).collect()));
    }
    out.join("\n") + "\n"
}

pub fn repositories(items: &[Repository]) -> String {
    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|r| {
            vec![
                r.full_name.clone(),
                r.default_branch.clone().unwrap_or_else(|| ABSENT.into()),
                r.archived.to_string(),
                r.head
                    .as_deref()
                    .map(|h| h[..8.min(h.len())].to_string())
                    .unwrap_or_else(|| ABSENT.into()),
                match &r.topics {
                    Some(t) if !t.is_empty() => {
                        let mut t = t.clone();
                        t.sort();
                        t.join(" ")
                    }
                    Some(_) => String::new(),
                    None => ABSENT.into(),
                },
            ]
        })
        .collect();
    table(
        &["REPOSITORY", "BRANCH", "ARCHIVED", "HEAD", "TOPICS"],
        &rows,
    )
}

pub fn connections(items: &[Connection]) -> String {
    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|c| {
            vec![
                c.id.to_string(),
                c.name.clone(),
                c.forge.to_string(),
                c.org.clone(),
                c.repositories.to_string(),
                c.last_reconciled_at
                    .map(|t| t.format("%Y-%m-%d %H:%M:%SZ").to_string())
                    .unwrap_or_else(|| "never".into()),
            ]
        })
        .collect();
    table(
        &[
            "ID",
            "NAME",
            "FORGE",
            "ORG",
            "REPOSITORIES",
            "LAST RECONCILED",
        ],
        &rows,
    )
}

pub fn connection(c: &Connection) -> String {
    let fields = [
        ("ID", c.id.to_string()),
        ("Name", c.name.clone()),
        ("Forge", c.forge.to_string()),
        ("Org", c.org.clone()),
        ("Base URL", c.base_url.clone()),
        ("Auth", "token".into()),
        ("Token env", c.token_env.clone()),
        ("Queue", c.queue.clone()),
        ("", String::new()),
        ("Repositories", c.repositories.to_string()),
        (
            "Last reconciled",
            c.last_reconciled_at
                .map(|t| t.format("%Y-%m-%d %H:%M:%SZ").to_string())
                .unwrap_or_else(|| "never".into()),
        ),
    ];
    let mut out = String::new();
    for (k, v) in fields {
        if k.is_empty() {
            out.push('\n');
        } else {
            out.push_str(&format!("{k:<17}{v}\n"));
        }
    }
    out
}

/// ```text
/// Reconcile   rec_06G9N084KXVH9BQD3DW01R4TA4
/// Status      succeeded
///
/// Discovered     6
/// Recorded       6
/// Inaccessible   0
/// ```
pub fn reconcile(r: &Reconcile) -> String {
    let mut out = format!(
        "{:<12}{}\n{:<12}{}\n",
        "Reconcile",
        r.id,
        "Status",
        r.status.as_str()
    );
    if let Some(e) = &r.error {
        out.push_str(&format!("{:<12}{e}\n", "Error"));
    }
    let n = [r.discovered, r.recorded, r.inaccessible];
    let w = n
        .iter()
        .map(|v| v.to_string().len())
        .max()
        .unwrap_or(1)
        .max(3);
    out.push('\n');
    for (k, v) in [
        ("Discovered", n[0]),
        ("Recorded", n[1]),
        ("Inaccessible", n[2]),
    ] {
        out.push_str(&format!("{k:<13}{v:>w$}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use repoplane_proto::api::{ReconcileStatus, RepositoryStatus};
    use repoplane_proto::id::{ConnectionId, ReconcileId, RepositoryId};

    fn repo(name: &str, head: &str, topics: &[&str]) -> Repository {
        Repository {
            id: RepositoryId::new(),
            connection_id: ConnectionId::new(),
            external_id: "1".into(),
            full_name: name.into(),
            name: name.rsplit('/').next().unwrap().into(),
            default_branch: Some("main".into()),
            archived: false,
            topics: Some(topics.iter().map(|s| s.to_string()).collect()),
            head: Some(head.into()),
            empty: false,
            web_url: None,
            status: RepositoryStatus::Active,
            first_seen_at: Utc::now(),
            last_seen_at: Utc::now(),
        }
    }

    #[test]
    fn repo_list_matches_the_spec() {
        let out = repositories(&[
            repo(
                "acme-services/billing-api",
                "22d003020c1f0000000000000000000000000000",
                &["sonar", "node", "service"],
            ),
            repo(
                "acme-services/ledger-worker",
                "1833865b00000000000000000000000000000000",
                &["node", "service"],
            ),
            repo(
                "acme-services/node-gateway",
                "568a26e900000000000000000000000000000000",
                &["service", "gateway", "node"],
            ),
        ]);
        assert_eq!(
            out,
            "REPOSITORY                    BRANCH   ARCHIVED   HEAD       TOPICS\n\
             acme-services/billing-api     main     false      22d00302   node service sonar\n\
             acme-services/ledger-worker   main     false      1833865b   node service\n\
             acme-services/node-gateway    main     false      568a26e9   gateway node service\n"
        );
    }

    #[test]
    fn reconcile_matches_the_spec() {
        let r = Reconcile {
            id: "rec_06G9N084KXVH9BQD3DW01R4TA4"
                .parse::<ReconcileId>()
                .unwrap(),
            connection_id: ConnectionId::new(),
            status: ReconcileStatus::Succeeded,
            discovered: 6,
            recorded: 6,
            inaccessible: 0,
            error: None,
            requested_at: Utc::now(),
            started_at: None,
            finished_at: None,
        };
        assert_eq!(
            reconcile(&r),
            "Reconcile   rec_06G9N084KXVH9BQD3DW01R4TA4\n\
             Status      succeeded\n\
             \n\
             Discovered     6\n\
             Recorded       6\n\
             Inaccessible   0\n"
        );
    }

    #[test]
    fn absent_values() {
        let mut r = repo("o/empty", "x", &[]);
        r.head = None;
        r.topics = None;
        let out = repositories(&[r]);
        assert!(out.lines().nth(1).unwrap().contains("—"), "{out}");
    }
}
