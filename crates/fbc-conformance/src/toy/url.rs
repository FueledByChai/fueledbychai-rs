//! The toy's configured URLs (FBC-ja3, Codex r4172917294): a base URL from a [`VenueConfig`]
//! key, its credential spans from the key named after it with `.redact` appended, carried into
//! the [`WireUrl`] it plans or asks under, and into every URL the toy builds by appending a
//! path or query to it, so the journal hashes them and no `Debug` shows them (decisions 0009,
//! 0014 item 8).
//!
//! The `.redact` value is a comma-separated list of `start..end` byte ranges of the URL, each
//! inside its path, in order, not overlapping and at least [`MIN_CREDENTIAL_LEN`] bytes long
//! (an empty value or none marks nothing). A codec names a credential wherever a frame or
//! response holds it, so a shorter one would mark unrelated bytes, such as prices and sequence
//! numbers, that the journal then keeps only as hashes (Codex r4220116602). The
//! toy marks credentials in the path only, so it refuses a URL with user information (which the
//! runtime never sends either, decision 0029) and one with a query or fragment (the toy appends
//! its own paths and query to the base), rather than plan an address with a credential it
//! cannot mark. It also refuses, at configuration time rather than at the first connection or
//! request, an authority without a usable host or port, a path with a character a URI never
//! holds, and an HTTP base ending in `/`, which the paths it appends would double (Codex
//! r4219602934, r4219753503, r4219929307, r4219602924). A codec given a configured URL names
//! its credentials wherever a frame or an HTTP response echoes them (r4219753519,
//! r4219929319).

use core::ops::Range;
use std::net::{Ipv4Addr, Ipv6Addr};

use fbc_core::{ConfigError, HeaderMark, HttpResponse, InboundSpans, VenueConfig, WireUrl};

use super::MIN_CREDENTIAL_LEN;

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
    if !usable(authority) {
        return Err(invalid(
            "no usable host: a name or an IPv4 or bracketed IPv6 literal, then at most a port \
             from 1 to 65535",
        ));
    }
    let path_at = text.len() - rest.len() + authority.len();
    if !path(&text[path_at..]) {
        return Err(invalid(
            "a path with a character a URI never holds: escape it as %XX",
        ));
    }
    if !key.socket && text.ends_with('/') {
        return Err(invalid(
            "a trailing /: the toy appends its own paths, each starting with one",
        ));
    }
    let spans = spans(cfg.get(key.redact).unwrap_or(""), key.redact, path_at)?;
    let url = WireUrl::redacted(text.to_owned(), spans).map_err(|_| ConfigError::Invalid {
        key: key.redact,
        reason: "a span past the URL's end, out of order, overlapping or splitting a character",
    })?;
    let short = |r: &Range<u32>| ((r.end - r.start) as usize) < MIN_CREDENTIAL_LEN;
    match url.redactions().iter().any(short) {
        true => Err(ConfigError::Invalid {
            key: key.redact,
            reason: "a span shorter than 16 bytes, which the toy would name wherever a frame or \
                     response holds it, unrelated bytes included",
        }),
        false => Ok(url),
    }
}

/// Whether `authority` is a host, then at most `:` and a port from 1 to 65535 (Codex
/// r4219602934): a name or IPv4 literal of letters, digits, `.`, `-` and `_`, or an IPv6
/// literal in brackets.
fn usable(authority: &str) -> bool {
    let (host_ok, port) = match authority.strip_prefix('[') {
        Some(v6_and_port) => {
            let Some((inside, after)) = v6_and_port.split_once(']') else {
                return false;
            };
            let port = match after {
                "" => None,
                after => match after.strip_prefix(':') {
                    Some(port) => Some(port),
                    None => return false,
                },
            };
            (inside.parse::<Ipv6Addr>().is_ok(), port)
        }
        None => {
            let (host, port) = match authority.rsplit_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            };
            (named_host(host), port)
        }
    };
    let port_ok =
        |p: &str| p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok_and(|p| p > 0);
    host_ok && port.is_none_or(port_ok)
}

/// A name of letters, digits, `.`, `-` and `_`, or, all digits and dots, an IPv4 address
/// (Codex r4219753503: a literal is parsed, not only its characters checked).
fn named_host(host: &str) -> bool {
    let named = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_');
    match host.chars().all(|c| c.is_ascii_digit() || c == '.') {
        true => host.parse::<Ipv4Addr>().is_ok(),
        false => !host.is_empty() && host.chars().all(named),
    }
}

/// Whether `path` is RFC 3986 path characters only (Codex r4219929307): unreserved, sub-delims,
/// `:`, `@`, `/` and `%` escapes of two hex digits, so the runtime's URI parser takes it.
fn path(path: &str) -> bool {
    let mut bytes = path.bytes();
    while let Some(b) = bytes.next() {
        let ok = match b {
            b'%' => {
                let hex = |b: Option<u8>| b.is_some_and(|b| b.is_ascii_hexdigit());
                hex(bytes.next()) && hex(bytes.next())
            }
            b if b.is_ascii_alphanumeric() => true,
            _ => b"-._~!$&'()*+,;=:@/".contains(&b),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// The credentials `url`'s spans hold.
pub(super) fn secrets(url: &WireUrl) -> Vec<String> {
    let text = url.as_str();
    let span = |r: &Range<u32>| {
        text.get(r.start as usize..r.end as usize)
            .map(str::to_owned)
    };
    url.redactions().iter().filter_map(span).collect()
}

/// Every occurrence of any of `secrets` in `bytes` (each at least [`MIN_CREDENTIAL_LEN`] bytes
/// when configured; a codec built directly is given such spans by its caller) and every span of `also`, overlapping or
/// adjacent ones as one span, in order.
pub(super) fn occurrences(
    bytes: &[u8],
    secrets: &[String],
    also: &[Range<u32>],
) -> Vec<Range<u32>> {
    let to_u32 = |at: usize| u32::try_from(at).unwrap_or(u32::MAX);
    let mut found: Vec<Range<u32>> = also.to_vec();
    for secret in secrets
        .iter()
        .map(String::as_bytes)
        .filter(|s| !s.is_empty())
    {
        let hits = bytes
            .windows(secret.len())
            .enumerate()
            .filter(|&(_, w)| w == secret);
        found.extend(hits.map(|(i, _)| to_u32(i)..to_u32(i + secret.len())));
    }
    found.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u32>> = Vec::new();
    for r in found {
        match merged.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    merged
}

/// What of an HTTP response holds any of `secrets` (Codex r4219753519): every occurrence in
/// its body, overlapping or adjacent ones as one span, and each header whose value holds one,
/// or whose name does, a proxy echoing it there.
pub(super) fn echoed(resp: &HttpResponse<'_>, secrets: &[String]) -> InboundSpans {
    let holds = |text: &str| secrets.iter().any(|s| text.contains(s.as_str()));
    let headers = (0u32..)
        .zip(resp.headers)
        .filter_map(|(at, &(name, value))| match (holds(name), holds(value)) {
            (true, _) => Some((at, HeaderMark::NameAndValue)),
            (false, true) => Some((at, HeaderMark::Value)),
            (false, false) => None,
        });
    let headers: Vec<_> = headers.collect();
    let body = occurrences(resp.body, secrets, &[]);
    InboundSpans::response(headers, body)
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
