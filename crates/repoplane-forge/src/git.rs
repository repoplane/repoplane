//! Default-branch HEADs over git rather than over a forge's API: `git ls-remote <url>
//! refs/heads/<branch>`, one small request per repository. Git traffic does not count against the
//! REST and GraphQL budgets — GitHub's GraphQL CPU limit caps HEADs read that way near 600 a
//! minute whatever the concurrency, and gitlab.com's Free plan allows 100 API requests a minute —
//! and it is the same call on every forge.
//!
//! git runs in an environment built from scratch: the caller's `GIT_DIR` or global configuration
//! must not redirect it, and only proxy and CA settings pass through, which a forge behind a
//! corporate proxy needs. It runs from `/`, so the configuration of a repository the worker
//! happens to start inside is not read either: `actions/checkout` leaves an `Authorization` header
//! for github.com in its checkout's, and GitHub refuses a request that carries two (CI,
//! 2026-11-05). The token travels as an `Authorization` header scoped to the forge's
//! origin through `GIT_CONFIG_*`, never in the URL, where every user of the machine could read it
//! in `ps`. Everything git prints is scrubbed of it.

use std::time::Duration;

use base64::Engine as _;
use futures::stream::{self, StreamExt};
use tokio::process::Command;

use crate::error::{Class, ForgeError};
use crate::token::Token;

/// Variables handed to git from the worker's environment.
const PASSTHROUGH: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
    "ALL_PROXY",
    "all_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "GIT_SSL_CAINFO",
    "GIT_SSL_CAPATH",
];

/// One `ls-remote` may take this long before it counts as a transient failure.
const TIMEOUT: Duration = Duration::from_secs(60);

/// git processes run at once by one `heads` call over several repositories (the `list`
/// example). The worker reads one repository per activity, so there it is the worker's
/// activity slots that bound git, not this.
pub const CONCURRENCY: usize = 8;

/// How git authenticates to a forge: Basic, with the token as the password under the username
/// the forge expects.
#[derive(Clone)]
pub struct GitAuth {
    pub username: &'static str,
    pub token: Token,
}

/// One HEAD to read: the remote, and the branch whose tip is wanted.
pub struct Remote {
    pub url: url::Url,
    pub branch: String,
}

/// The tip of `branch` on `url`; `None` when the repository or the branch is not there.
pub async fn ls_remote(
    forge: &'static str,
    remote: &Remote,
    auth: Option<&GitAuth>,
) -> Result<Option<String>, ForgeError> {
    let refname = format!("refs/heads/{}", remote.branch);
    let mut cmd = command(&remote.url, &refname, auth);
    let scrub = |s: &str| match auth {
        Some(a) => a.token.scrub(s),
        None => s.to_string(),
    };
    let path = remote.url.path().to_string();
    let out = match tokio::time::timeout(TIMEOUT, cmd.output()).await {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            return Err(ForgeError::Config(format!(
                "cannot run git (is it installed?): {e}"
            )));
        }
        Err(_) => {
            return Err(ForgeError::Transport {
                forge,
                method: "git ls-remote".into(),
                path,
                message: format!("no answer after {}s", TIMEOUT.as_secs()),
            });
        }
    };
    let stderr = scrub(String::from_utf8_lossy(&out.stderr).trim());
    if !out.status.success() {
        return classify(forge, path, &stderr);
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        if let Some((sha, name)) = line.split_once('\t')
            && name == refname
        {
            return crate::commit_id(forge, sha).map(Some);
        }
    }
    // The branch is not on the remote: an empty repository, or one renamed since listing.
    Ok(None)
}

/// The `ls-remote` process: the environment below, and `/` as its directory so that no
/// repository around the worker lends git its configuration.
fn command(url: &url::Url, refname: &str, auth: Option<&GitAuth>) -> Command {
    let mut cmd = Command::new("git");
    cmd.env_clear()
        .envs(env(url, auth))
        .current_dir("/")
        .args(["ls-remote", "--quiet", url.as_str(), refname])
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    cmd
}

/// A failed `ls-remote`, read from what git said: a repository gone since the listing has no
/// HEAD; a refused credential will not get better; anything else may.
fn classify(forge: &'static str, path: String, stderr: &str) -> Result<Option<String>, ForgeError> {
    let refused = |status: u16| ForgeError::Status {
        forge,
        class: Class::Auth,
        status,
        method: "git ls-remote".into(),
        path: path.clone(),
        body: stderr.to_string(),
    };
    // The HTTP status, when git says it, decides: "The requested URL returned error: 503".
    match http_status(stderr) {
        Some(404) => return Ok(None),
        Some(s @ (401 | 403)) => return Err(refused(s)),
        Some(_) => return Err(transient(forge, path, stderr)),
        None => {}
    }
    // Otherwise the words, read without the URLs in them: a repository named
    // `identity-permissions-api` answered 503, and "permission" in its URL read as a refusal.
    let words = without_urls(&stderr.to_ascii_lowercase());
    let refusal = [
        "authentication failed",
        "access denied",
        "permission denied",
        "not authorized",
    ];
    if refusal.iter().any(|p| words.contains(p)) {
        return Err(refused(401));
    }
    let gone = [
        "not found",
        "does not exist",
        "could not be found",
        "does not appear to be a git repository",
    ];
    if gone.iter().any(|p| words.contains(p)) {
        return Ok(None);
    }
    Err(transient(forge, path, stderr))
}

/// A failure that may pass: the forge was busy or unreachable. Temporal retries it.
fn transient(forge: &'static str, path: String, stderr: &str) -> ForgeError {
    ForgeError::Transport {
        forge,
        method: "git ls-remote".into(),
        path,
        message: if stderr.is_empty() {
            "git failed".into()
        } else {
            stderr.to_string()
        },
    }
}

/// The status in git's "The requested URL returned error: 503".
fn http_status(stderr: &str) -> Option<u16> {
    let rest = &stderr[stderr.find("returned error: ")? + "returned error: ".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// `s` with every URL and every quoted part (git quotes the URL it failed on) blanked, so that
/// a repository's name is never read as what happened to it.
fn without_urls(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut quoted = false;
    for word in s.split_inclusive(char::is_whitespace) {
        let bare = word.trim_end();
        if bare.contains("://") {
            out.push(' ');
            continue;
        }
        for c in word.chars() {
            if c == '\'' {
                quoted = !quoted;
                out.push(' ');
            } else if !quoted {
                out.push(c);
            }
        }
    }
    out
}

/// The environment git runs in: nothing from the caller but the passthrough variables, no
/// global or system configuration, no prompts, and the credential as a header for this origin.
fn env(url: &url::Url, auth: Option<&GitAuth>) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = PASSTHROUGH
        .iter()
        .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v)))
        .collect();
    env.push(("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()));
    env.push(("GIT_CONFIG_NOSYSTEM".into(), "1".into()));
    env.push(("GIT_TERMINAL_PROMPT".into(), "0".into()));
    if let Some(a) = auth {
        let basic = base64::engine::general_purpose::STANDARD.encode(format!(
            "{}:{}",
            a.username,
            a.token.expose()
        ));
        env.push(("GIT_CONFIG_COUNT".into(), "1".into()));
        env.push((
            "GIT_CONFIG_KEY_0".into(),
            format!("http.{}/.extraHeader", origin(url)),
        ));
        env.push((
            "GIT_CONFIG_VALUE_0".into(),
            format!("Authorization: Basic {basic}"),
        ));
    }
    env
}

/// `scheme://host[:port]`: the only place the credential header is sent.
fn origin(url: &url::Url) -> String {
    let mut o = format!("{}://{}", url.scheme(), url.host_str().unwrap_or(""));
    if let Some(p) = url.port() {
        o.push_str(&format!(":{p}"));
    }
    o
}

/// The HEAD of each remote, in order, each with its own outcome: `Ok(None)` where there is
/// nothing to read, an error where git could not tell. At most [`CONCURRENCY`] git processes at
/// once.
pub async fn heads(
    forge: &'static str,
    remotes: Vec<Result<Option<Remote>, ForgeError>>,
    auth: &GitAuth,
) -> Vec<Result<Option<String>, ForgeError>> {
    let lookups: Vec<_> = remotes
        .into_iter()
        .map(|r| async move {
            match r {
                Ok(Some(r)) => ls_remote(forge, &r, Some(auth)).await,
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            }
        })
        .collect();
    stream::iter(lookups).buffered(CONCURRENCY).collect().await
}

/// The forge's base URL with path segments appended, each escaped: a repository called
/// `my repo` is `my%20repo`, and a `/` inside one segment stays inside it.
pub fn remote_url(base: &str, segments: &[&str]) -> Result<url::Url, ForgeError> {
    let mut u =
        url::Url::parse(base).map_err(|e| ForgeError::Config(format!("base URL {base:?}: {e}")))?;
    if !matches!(u.scheme(), "http" | "https") {
        return Err(ForgeError::Config(format!(
            "base URL {base:?}: want http or https"
        )));
    }
    u.path_segments_mut()
        .map_err(|_| ForgeError::Config(format!("base URL {base:?} cannot take a path")))?
        .pop_if_empty()
        .extend(segments);
    Ok(u)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &std::path::Path, args: &[&str]) {
        let ok = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .status()
            .unwrap()
            .success();
        assert!(ok, "git {args:?}");
    }

    /// A bare repository with one commit on `main`, served over `file://`.
    fn repository() -> (tempfile::TempDir, url::Url, String) {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        std::fs::write(work.join("README.md"), "hi\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-q", "-m", "first"]);
        let sha = String::from_utf8(
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&work)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_string();
        git(dir.path(), &["clone", "-q", "--bare", "work", "bare.git"]);
        let url = url::Url::from_file_path(dir.path().join("bare.git")).unwrap();
        (dir, url, sha)
    }

    #[tokio::test]
    async fn reads_a_branch_tip() {
        let (_dir, url, sha) = repository();
        let head = ls_remote(
            "t",
            &Remote {
                url: url.clone(),
                branch: "main".into(),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(head, Some(sha));
        let missing = ls_remote(
            "t",
            &Remote {
                url,
                branch: "nope".into(),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn a_missing_repository_has_no_head() {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_file_path(dir.path().join("gone.git")).unwrap();
        let head = ls_remote(
            "t",
            &Remote {
                url,
                branch: "main".into(),
            },
            None,
        )
        .await
        .unwrap();
        assert_eq!(head, None);
    }

    /// What happened is read from the status git reports, or from its words, never from the
    /// repository's name or URL.
    #[test]
    fn a_failure_is_read_from_what_git_says_not_the_url() {
        let read = |stderr: &str| classify("t", "/o/r".into(), stderr);
        // Azure DevOps, 2026-10-06: a 503 on a repository named for permissions.
        let busy = read(
            "fatal: unable to access 'https://dev.azure.com/o/p/_git/identity-permissions-api/': The requested URL returned error: 503",
        )
        .unwrap_err();
        assert!(busy.retryable(), "{busy}");
        let busy = read(
            "fatal: unable to access 'https://x/o/not-found-handler-404/': The requested URL returned error: 502",
        )
        .unwrap_err();
        assert!(busy.retryable());
        assert!(
            read("fatal: unable to access 'https://x/o/r/': The requested URL returned error: 429")
                .unwrap_err()
                .retryable()
        );
        assert_eq!(
            read("fatal: unable to access 'https://x/o/r/': The requested URL returned error: 403")
                .unwrap_err()
                .class(),
            Some(Class::Auth)
        );
        assert_eq!(
            read("fatal: unable to access 'https://x/o/r/': The requested URL returned error: 404")
                .unwrap(),
            None
        );
        // GitHub says a missing repository in words, quoting the URL.
        assert_eq!(
            read("remote: Repository not found.\nfatal: repository 'https://github.com/o/r.git/' not found")
                .unwrap(),
            None
        );
        // A repository named like a refusal, failing to resolve: transient.
        assert!(
            read("fatal: unable to access 'https://x/o/access-denied-page/': Could not resolve host: x")
                .unwrap_err()
                .retryable()
        );
    }

    #[test]
    fn refusals_are_not_retried_and_never_carry_the_token() {
        let err = classify(
            "t",
            "/o/r.git".into(),
            "fatal: Authentication failed for 'https://x/o/r.git/'",
        )
        .unwrap_err();
        assert_eq!(err.class(), Some(Class::Auth));
        assert!(!err.retryable());
        let err = classify(
            "t",
            "/o/r.git".into(),
            "fatal: unable to access: Could not resolve host: x",
        )
        .unwrap_err();
        assert!(err.retryable());
        let a = GitAuth {
            username: "x-access-token",
            token: Token::new("s3cr3t").unwrap(),
        };
        let e = env(
            &url::Url::parse("https://github.com/o/r.git").unwrap(),
            Some(&a),
        );
        let key = e.iter().find(|(k, _)| k == "GIT_CONFIG_KEY_0").unwrap();
        assert_eq!(key.1, "http.https://github.com/.extraHeader");
        assert!(
            !e.iter().any(|(_, v)| v.contains("s3cr3t")),
            "the token travels base64-encoded in a header only"
        );
        assert!(!e.iter().any(|(k, _)| k == "GIT_DIR"));
    }

    /// Started inside a checkout, git would read that repository's configuration too: in CI it
    /// added the workflow's own `Authorization` header to ours, and GitHub answered 400.
    #[test]
    fn git_runs_outside_any_repository() {
        let url = url::Url::parse("https://github.com/o/r.git").unwrap();
        let cmd = command(&url, "refs/heads/main", None);
        assert_eq!(
            cmd.as_std().get_current_dir(),
            Some(std::path::Path::new("/"))
        );
    }

    #[test]
    fn remote_urls_escape_each_segment() {
        let u = remote_url("https://dev.azure.com/", &["my org", "proj", "_git", "a b"]).unwrap();
        assert_eq!(u.as_str(), "https://dev.azure.com/my%20org/proj/_git/a%20b");
        let u = remote_url("https://gitlab.example/gl", &["group", "sub", "api.git"]).unwrap();
        assert_eq!(u.as_str(), "https://gitlab.example/gl/group/sub/api.git");
        assert!(remote_url("ftp://x", &["a"]).is_err());
    }
}
