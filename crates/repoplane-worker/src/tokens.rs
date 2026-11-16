//! Which token goes to which forge. The worker holds the forge tokens and chooses one for each
//! connection; a connection carries no credential, not even the name of one (ADR 0014). A token
//! is keyed by where it may be sent, and only ever goes to a connection its key matches.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::bail;
use repoplane_forge::{ForgeError, Token};
use repoplane_proto::inventory::ConnectionSpec;
use repoplane_proto::secret;

/// Where a token may go: `host`, `host:port`, `host/org` or `host:port/org`. Without a port the
/// key matches its host on any port; without an organisation, every organisation there. An
/// organisation matches itself and, for a GitLab group, its subgroups.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Key {
    host: String,
    port: Option<u16>,
    org: Option<String>,
}

impl Key {
    fn parse(raw: &str) -> Result<Key, String> {
        let s = raw.trim().to_ascii_lowercase();
        if s.contains("://") {
            return Err(format!(
                "{raw:?}: give the host without a scheme, as in github.com"
            ));
        }
        let (authority, org) = match s.split_once('/') {
            Some((a, o)) => (a, Some(o.trim_end_matches('/'))),
            None => (s.as_str(), None),
        };
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
                let port = p
                    .parse::<u16>()
                    .map_err(|_| format!("{raw:?}: {p} is not a port"))?;
                (h, Some(port))
            }
            _ => (authority, None),
        };
        let bracketed = host.starts_with('[') && host.ends_with(']'); // an IPv6 address
        if host.is_empty() || (host.contains(['@', ':', '?', '#', ' ']) && !bracketed) {
            return Err(format!(
                "{raw:?}: want host, host:port, host/org or host:port/org"
            ));
        }
        let org = match org {
            Some(o) if o.is_empty() || o.split('/').any(str::is_empty) => {
                return Err(format!("{raw:?}: the organisation after the host is empty"));
            }
            Some(o) => Some(o.to_string()),
            None => None,
        };
        Ok(Key {
            host: host.to_string(),
            port,
            org,
        })
    }

    /// How closely the key matches a connection, when it does: a longer organisation path
    /// first, then a port over none.
    fn matches(&self, host: &str, port: u16, org: &str) -> Option<(usize, bool)> {
        if self.host != host || self.port.is_some_and(|p| p != port) {
            return None;
        }
        let depth = match &self.org {
            None => 0,
            Some(o) if org == o || org.starts_with(&format!("{o}/")) => o.split('/').count(),
            Some(_) => return None,
        };
        Some((depth, self.port.is_some()))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.host)?;
        if let Some(p) = self.port {
            write!(f, ":{p}")?;
        }
        if let Some(o) = &self.org {
            write!(f, "/{o}")?;
        }
        Ok(())
    }
}

/// The forge tokens this worker holds, each with where it may be sent.
pub struct Tokens(Vec<(Key, Token)>);

impl Tokens {
    /// Reads the configured tokens (where → the variable holding the token) through `env`. A
    /// worker does not start with a token it cannot use or must never send to a forge.
    pub fn load(
        configured: &BTreeMap<String, String>,
        temporal_api_key_env: Option<&str>,
        env: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Tokens> {
        if configured.is_empty() {
            bail!(
                "this worker holds no forge token: list them under `tokens` in its config, or \
                 pass --token HOST=VARIABLE (for example --token github.com=GITHUB_TOKEN)"
            );
        }
        let mut tokens: Vec<(Key, Token)> = Vec::with_capacity(configured.len());
        for (raw, var) in configured {
            let key = Key::parse(raw).map_err(|e| anyhow::anyhow!("tokens: {e}"))?;
            if let Some((seen, _)) = tokens.iter().find(|(k, _)| *k == key) {
                bail!("tokens: {raw:?} and {seen} name the same place");
            }
            if !secret::valid_env_name(var) {
                bail!(secret::describe_env_name_error(
                    &format!("tokens.{key}"),
                    var
                ));
            }
            if temporal_api_key_env == Some(var.as_str()) {
                bail!(
                    "tokens.{key}: {var} holds the Temporal API key, which never goes to a forge"
                );
            }
            let Some(value) = env(var.as_str()).filter(|v| !v.trim().is_empty()) else {
                bail!("tokens.{key}: the environment variable {var} is not set");
            };
            let token = Token::new(value).map_err(|e| anyhow::anyhow!("tokens.{key}: {e}"))?;
            tokens.push((key, token));
        }
        Ok(Tokens(tokens))
    }

    /// Where each token may go, for the log: never the tokens themselves.
    pub fn keys(&self) -> Vec<String> {
        self.0.iter().map(|(k, _)| k.to_string()).collect()
    }

    /// The token for a connection: the one whose key matches it most closely. None matching is
    /// a configuration to change, not a failure to retry.
    pub fn for_connection(&self, c: &ConnectionSpec) -> Result<Token, ForgeError> {
        let url = url::Url::parse(&c.base_url)
            .map_err(|e| ForgeError::Config(format!("base_url {:?}: {e}", c.base_url)))?;
        let (Some(host), Some(port)) = (url.host_str(), url.port_or_known_default()) else {
            return Err(ForgeError::Config(format!(
                "base_url {:?} names no host",
                c.base_url
            )));
        };
        let host = host.to_ascii_lowercase();
        let org = c.org.to_ascii_lowercase();
        self.0
            .iter()
            .filter_map(|(k, t)| k.matches(&host, port, &org).map(|rank| (rank, t)))
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, t)| t.clone())
            .ok_or_else(|| {
                let at = match url.port() {
                    Some(p) => format!("{host}:{p}"),
                    None => host.clone(),
                };
                ForgeError::Config(format!(
                    "the workers on queue {:?} hold no token for {at}/{}: add one to their `tokens`",
                    c.queue, c.org
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use repoplane_proto::forge::ForgeKind;
    use repoplane_proto::id::ConnectionId;

    fn load(pairs: &[(&str, &str)]) -> anyhow::Result<Tokens> {
        let configured = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        // Each variable holds its own name, so a test reads which one was chosen.
        Tokens::load(&configured, Some("TEMPORAL_API_KEY"), |v| {
            (v != "UNSET").then(|| format!("value-of-{v}"))
        })
    }

    fn spec(base_url: &str, org: &str) -> ConnectionSpec {
        ConnectionSpec {
            id: ConnectionId::new(),
            forge: ForgeKind::Gitlab,
            org: org.into(),
            base_url: base_url.into(),
            queue: "default".into(),
        }
    }

    fn chosen(t: &Tokens, base_url: &str, org: &str) -> String {
        t.for_connection(&spec(base_url, org))
            .unwrap()
            .expose()
            .trim_start_matches("value-of-")
            .to_string()
    }

    #[test]
    fn keys_parse_to_host_port_and_organisation() {
        let k = Key::parse("GitLab.example.com:8443/Platform/").unwrap();
        assert_eq!(k.to_string(), "gitlab.example.com:8443/platform");
        assert_eq!(Key::parse("github.com").unwrap().to_string(), "github.com");
        for bad in [
            "",
            "https://github.com",
            "token@github.com",
            "github.com/",
            "github.com//acme",
            "host:99999",
        ] {
            assert!(Key::parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn the_closest_key_wins() {
        let t = load(&[
            ("gitlab.com", "ANY_GROUP"),
            ("gitlab.com/acme", "ACME"),
            ("gitlab.com/acme/secure", "ACME_SECURE"),
        ])
        .unwrap();
        assert_eq!(chosen(&t, "https://gitlab.com", "other"), "ANY_GROUP");
        assert_eq!(chosen(&t, "https://gitlab.com", "ACME"), "ACME");
        assert_eq!(chosen(&t, "https://gitlab.com", "acme/platform"), "ACME");
        assert_eq!(
            chosen(&t, "https://gitlab.com", "acme/secure/x"),
            "ACME_SECURE"
        );
        // A group is a path segment, not a prefix of a name.
        assert_eq!(chosen(&t, "https://gitlab.com", "acmecorp"), "ANY_GROUP");
    }

    #[test]
    fn a_key_without_a_port_matches_every_port() {
        let t = load(&[("127.0.0.1", "LOCAL"), ("forge.test:3000", "PINNED")]).unwrap();
        assert_eq!(chosen(&t, "http://127.0.0.1:41234", "acme"), "LOCAL");
        assert_eq!(chosen(&t, "http://forge.test:3000", "acme"), "PINNED");
        assert!(
            t.for_connection(&spec("http://forge.test:3001", "acme"))
                .is_err()
        );
        // The scheme's own port is the port: forge.test:443 is https://forge.test.
        let t = load(&[("forge.test:443", "TLS")]).unwrap();
        assert_eq!(chosen(&t, "https://forge.test", "acme"), "TLS");
    }

    #[test]
    fn a_port_breaks_a_tie() {
        let t = load(&[
            ("forge.test/acme", "ANY_PORT"),
            ("forge.test:3000/acme", "PORT"),
        ])
        .unwrap();
        assert_eq!(chosen(&t, "http://forge.test:3000", "acme"), "PORT");
        assert_eq!(chosen(&t, "http://forge.test:4000", "acme"), "ANY_PORT");
    }

    /// A connection cannot send a worker's token anywhere its key does not name, and is told
    /// at once, without a retry, which queue lacks a token for which host.
    #[test]
    fn no_key_no_token() {
        let t = load(&[("github.com", "GITHUB")]).unwrap();
        let e = t
            .for_connection(&spec("https://attacker.example", "acme"))
            .unwrap_err();
        assert!(!e.retryable());
        assert!(
            e.to_string()
                .contains("queue \"default\" hold no token for attacker.example/acme"),
            "{e}"
        );
        assert_eq!(t.keys(), vec!["github.com"]);
    }

    #[test]
    fn a_worker_refuses_to_start_with_a_token_it_cannot_use_or_must_not_send() {
        let refused = |pairs: &[(&str, &str)], why: &str| {
            let e = load(pairs).err().expect("loaded").to_string();
            assert!(e.contains(why), "{e}");
        };
        refused(&[], "holds no forge token");
        refused(&[("github.com", "UNSET")], "UNSET is not set");
        refused(&[("github.com", "TEMPORAL_API_KEY")], "Temporal API key");
        refused(
            &[("github.com", "github_token")],
            "not an environment variable name",
        );
        refused(
            &[("github.com", "A"), ("GitHub.com", "B")],
            "name the same place",
        );
        refused(&[("https://github.com", "A")], "without a scheme");
    }
}
