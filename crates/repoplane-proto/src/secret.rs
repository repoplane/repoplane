//! Secrets are named, never written. Every place a credential is configured carries the name
//! of an environment variable; these checks keep a pasted token from travelling in its place.

/// A variable name as the configuration expects one: `REPOPLANE_TOKEN`, `FORGELAB_GH_TOKEN`.
pub fn valid_env_name(s: &str) -> bool {
    let mut bytes = s.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'_'))
        && s.len() <= 128
        && bytes.all(|b| matches!(b, b'A'..=b'Z' | b'0'..=b'9' | b'_'))
}

/// True for values that look like a credential rather than a variable name: the known token
/// prefixes, or a long run with no underscore and mixed case. Used to refuse, with a message
/// that says why, instead of storing a token as if it were a name.
pub fn looks_like_secret(s: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "glpat-",
        "gloas-",
        "glptt-",
    ];
    if PREFIXES.iter().any(|p| s.starts_with(p)) {
        return true;
    }
    let has_lower = s.bytes().any(|b| b.is_ascii_lowercase());
    let has_digit = s.bytes().any(|b| b.is_ascii_digit());
    s.len() >= 32 && has_lower && has_digit && !s.contains('_')
}

/// The message a refused value gets.
pub fn describe_env_name_error(field: &str, value: &str) -> String {
    if looks_like_secret(value) {
        format!(
            "{field} takes the NAME of an environment variable, and this looks like a token. \
             Export it (export MY_TOKEN=…) and pass MY_TOKEN; a secret never goes in a flag or a file"
        )
    } else {
        format!(
            "{field}: {value:?} is not an environment variable name (want A-Z, 0-9 and _, not starting with a digit)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(valid_env_name("FORGELAB_GH_TOKEN"));
        assert!(valid_env_name("_X"));
        assert!(!valid_env_name("1X"));
        assert!(!valid_env_name("gh_token"));
        assert!(!valid_env_name(""));
    }

    #[test]
    fn secrets() {
        assert!(looks_like_secret(
            "ghp_0123456789abcdefABCDEF0123456789abcd"
        ));
        assert!(looks_like_secret("glpat-xxxxxxxxxxxxxxxxxxxx"));
        assert!(looks_like_secret(
            "4f1c9a0b2e7d4c3a9b8e7f6d5c4b3a2f1e0d9c8b"
        ));
        assert!(!looks_like_secret("FORGELAB_GH_TOKEN"));
        assert!(describe_env_name_error("--token-env", "ghp_abc").contains("looks like a token"));
    }
}
