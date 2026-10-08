//! Reading a URL's authority as the runtime opens it: a DNS name, an IPv4 address or a bracketed
//! IPv6 address, then optionally `:` and a port from 1 to 65535. Discovery reads the REST base
//! with it ([`crate::discover::markets_url`]), so a base the runtime could not open is refused
//! when it is configured, not found as a request never sent (Codex r4218015211).
//!
//! src/auth reads the login's REST base with its own copy of the same rules (a review path,
//! decision 0009); FBC-uxib moves it onto this one.

use core::net::{Ipv4Addr, Ipv6Addr};

/// A host an authority names.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Host<'a> {
    Name(&'a str),
    V4(Ipv4Addr),
    V6(Ipv6Addr),
}

impl Host<'_> {
    /// Whether the host is this machine: `127.0.0.0/8`, `::1` or the name `localhost`.
    pub(crate) fn is_loopback(self) -> bool {
        match self {
            Host::Name(name) => name.eq_ignore_ascii_case("localhost"),
            Host::V4(address) => address.is_loopback(),
            Host::V6(address) => address.is_loopback(),
        }
    }
}

/// The host `authority` names, or `None` when it is not a DNS name, an IPv4 address or a
/// bracketed IPv6 address followed by nothing or `:` and a port from 1 to 65535.
pub(crate) fn host(authority: &str) -> Option<Host<'_>> {
    let (host, port) = match authority.strip_prefix('[') {
        Some(literal) => {
            let (address, port) = literal.split_once(']')?;
            (Host::V6(address.parse().ok()?), port)
        }
        None => {
            let (name, port) = authority.split_at(authority.find(':').unwrap_or(authority.len()));
            let host = match name.parse() {
                Ok(address) => Host::V4(address),
                Err(_) => Host::Name(dns_name(name)?),
            };
            (host, port)
        }
    };
    if let Some(digits) = port.strip_prefix(':') {
        let numeric = (1..=5).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit());
        let port: u16 = numeric.then(|| digits.parse().ok()).flatten()?;
        (port != 0).then_some(())?;
    } else if !port.is_empty() {
        return None;
    }
    Some(host)
}

/// `name` when it is a DNS name: at most 253 bytes of dot-separated labels, each 1 to 63
/// letters, digits and inner hyphens, the last not all digits (so it is no IPv4 address
/// written another way).
fn dns_name(name: &str) -> Option<&str> {
    let label = |label: &str| {
        let inner = |b: u8| b.is_ascii_alphanumeric() || b == b'-';
        (1..=63).contains(&label.len())
            && label.bytes().all(inner)
            && !label.starts_with('-')
            && !label.ends_with('-')
    };
    let last = name.rsplit('.').next().unwrap_or(name);
    let numeric = last.bytes().all(|b| b.is_ascii_digit());
    (name.len() <= 253 && name.split('.').all(label) && !numeric).then_some(name)
}
