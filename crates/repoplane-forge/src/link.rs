//! The RFC 8288 `Link` header, which GitHub and Forgejo use to say where the next page is. It is
//! the only authoritative signal: a short page is not necessarily the last, and a full page is
//! not necessarily followed by another.

/// The `rel="next"` target, if any.
pub fn next(header: Option<&str>) -> Option<String> {
    let header = header?;
    for part in header.split(',') {
        let mut pieces = part.split(';');
        let url = pieces.next()?.trim();
        let url = url.strip_prefix('<')?.strip_suffix('>')?;
        if pieces.any(|p| {
            let p = p.trim();
            p == "rel=\"next\"" || p == "rel=next"
        }) {
            return Some(url.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_next() {
        let h = r#"<http://x/api/v1/orgs/o/repos?limit=50&page=2>; rel="next",<http://x/api/v1/orgs/o/repos?limit=50&page=3>; rel="last""#;
        assert_eq!(
            next(Some(h)).as_deref(),
            Some("http://x/api/v1/orgs/o/repos?limit=50&page=2")
        );
        let last = r#"<http://x/?page=1>; rel="first", <http://x/?page=2>; rel="prev""#;
        assert_eq!(next(Some(last)), None);
        assert_eq!(next(None), None);
    }
}
