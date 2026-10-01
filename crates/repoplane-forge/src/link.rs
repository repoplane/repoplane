//! The RFC 8288 `Link` header, which GitHub and Forgejo use to say where the next page is. It is
//! the only authoritative signal: a short page is not necessarily the last, and a full page is
//! not necessarily followed by another.

/// The `rel="next"` target, if any.
pub fn next(header: Option<&str>) -> Option<String> {
    rel(header, "next")
}

/// The `rel="last"` target, if any: it says how many pages there are, so the rest of a listing
/// can be fetched at once rather than one page after another.
pub fn last(header: Option<&str>) -> Option<String> {
    rel(header, "last")
}

fn rel(header: Option<&str>, want: &str) -> Option<String> {
    let header = header?;
    for part in header.split(',') {
        let mut pieces = part.split(';');
        let url = pieces.next()?.trim();
        let url = url.strip_prefix('<')?.strip_suffix('>')?;
        if pieces.any(|p| {
            let p = p.trim();
            p == format!("rel=\"{want}\"") || p == format!("rel={want}")
        }) {
            return Some(url.to_string());
        }
    }
    None
}

/// The `page` query parameter of a URL; 1 when it does not say.
pub fn page_of(url: &str) -> Option<u64> {
    let u = url::Url::parse(url).ok()?;
    match u.query_pairs().find(|(k, _)| k == "page") {
        Some((_, v)) => v.parse().ok(),
        None => Some(1),
    }
}

/// `url` with its `page` set to `page`.
pub fn with_page(url: &str, page: u64) -> Option<String> {
    let mut u = url::Url::parse(url).ok()?;
    let pairs: Vec<(String, String)> = u
        .query_pairs()
        .filter(|(k, _)| k != "page")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    u.query_pairs_mut()
        .clear()
        .extend_pairs(pairs)
        .append_pair("page", &page.to_string());
    Some(u.to_string())
}

/// Most pages a listing is fanned out to at once; a `last` beyond it is not believed.
pub const MAX_PAGES: u64 = 10_000;

/// The URLs of pages 2..=N of a listing whose first page's `Link` names page N as the last.
/// `None` when the header names no last page, which leaves the listing to be walked.
pub fn rest_of(header: Option<&str>) -> Option<Vec<String>> {
    let last = last(header)?;
    let n = page_of(&last)?;
    if !(2..=MAX_PAGES).contains(&n) {
        return (n == 1).then(Vec::new);
    }
    (2..=n).map(|k| with_page(&last, k)).collect()
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

    #[test]
    fn rest_of_a_listing_from_its_last_page() {
        let h = r#"<http://x/r?per_page=100&page=2>; rel="next", <http://x/r?per_page=100&page=4>; rel="last""#;
        assert_eq!(
            rest_of(Some(h)).unwrap(),
            vec![
                "http://x/r?per_page=100&page=2",
                "http://x/r?per_page=100&page=3",
                "http://x/r?per_page=100&page=4"
            ]
        );
        assert_eq!(rest_of(Some(r#"<http://x/r?page=2>; rel="next""#)), None);
        assert_eq!(
            rest_of(Some(r#"<http://x/r?page=1>; rel="last""#)),
            Some(vec![])
        );
        assert_eq!(
            rest_of(Some(r#"<http://x/r?page=999999>; rel="last""#)),
            None
        );
    }
}
