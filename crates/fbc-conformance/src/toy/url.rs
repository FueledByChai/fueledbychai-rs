//! The toy's configured URLs (FBC-ja3, Codex r4172917294): a base URL from a [`VenueConfig`]
//! key, its credential spans from the key named after it with `.redact` appended, carried into
//! the [`WireUrl`] it plans or asks under, and into every URL the toy builds by appending a
//! path or query to it, so the journal hashes them and no `Debug` shows them (decisions 0009,
//! 0014 item 8).
//!
//! The `.redact` value is a comma-separated list of `start..end` byte ranges of the URL, each
//! inside its path, in order and not overlapping (an empty value or none marks nothing). The
//! toy marks credentials in the path only, so it refuses a URL with user information (which the
//! runtime never sends either, decision 0029) and one with a query or fragment (the toy appends
//! its own paths and query to the base), rather than plan an address with a credential it
//! cannot mark.

use core::ops::Range;

use fbc_core::{ConfigError, VenueConfig, WireUrl};

/// A configured URL's keys and the schemes it takes.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(super) struct UrlKey {
    /// The URL.
    pub(super) url: &'static str,
    /// Its credential spans.
    pub(super) redact: &'static str,
    /// A socket URL (`ws://`, `wss://`) or an HTTP base (`http://`, `https://`).
    pub(super) socket: bool,
}

/// The URL `key` configures in `cfg`, with the spans its `.redact` key marks.
pub(super) fn configured(cfg: &VenueConfig, key: UrlKey) -> Result<WireUrl, ConfigError> {
    let text = cfg.get(key.url).ok_or(ConfigError::Missing(key.url))?;
    let invalid = |reason| ConfigError::Invalid {
        key: key.url,
        reason,
    };
    let schemes: &[&str] = match key.socket {
        true => &["ws://", "wss://"],
        false => &["http://", "https://"],
    };
    let rest = schemes.iter().find_map(|s| text.strip_prefix(s));
    let rest = rest.ok_or(invalid(match key.socket {
        true => "not a ws:// or wss:// URL",
        false => "not an http:// or https:// URL",
    }))?;
    if text.contains(['?', '#']) {
        return Err(invalid(
            "a query or fragment, which the toy cannot mark: it appends its own paths and query",
        ));
    }
    let authority = &rest[..rest.find('/').unwrap_or(rest.len())];
    if authority.contains('@') {
        return Err(invalid(
            "user information, which the toy cannot mark and the runtime never sends: put the \
             credential in the path and mark it",
        ));
    }
    if authority.is_empty() {
        return Err(invalid("no host"));
    }
    let path_at = text.len() - rest.len() + authority.len();
    let spans = spans(cfg.get(key.redact).unwrap_or(""), key.redact, path_at)?;
    WireUrl::redacted(text.to_owned(), spans).map_err(|_| ConfigError::Invalid {
        key: key.redact,
        reason: "a span past the URL's end, out of order, overlapping or splitting a character",
    })
}

/// `base` with `suffix` appended, its spans kept where they were.
pub(super) fn join(base: &WireUrl, suffix: &str) -> WireUrl {
    let text = format!("{}{suffix}", base.as_str());
    let kept = WireUrl::redacted(text, base.redactions().to_vec());
    kept.expect("spans inside the base stay inside it, on the same boundaries")
}

/// The `start..end` ranges `text` lists, each starting in the path, at `path_at` or after.
fn spans(text: &str, key: &'static str, path_at: usize) -> Result<Vec<Range<u32>>, ConfigError> {
    let invalid = |reason| ConfigError::Invalid { key, reason };
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let span = |part: &str| {
        let (start, end) = part.split_once("..")?;
        Some(start.trim().parse().ok()?..end.trim().parse().ok()?)
    };
    let spans = text
        .split(',')
        .map(|part| span(part).ok_or(invalid("not start..end byte ranges")));
    let spans = spans.collect::<Result<Vec<Range<u32>>, _>>()?;
    let outside = spans
        .iter()
        .any(|s| usize::try_from(s.start).map_or(true, |at| at < path_at));
    match outside {
        true => Err(invalid(
            "a span outside the URL's path, which alone the toy marks",
        )),
        false => Ok(spans),
    }
}
