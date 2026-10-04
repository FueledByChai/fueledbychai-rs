//! Where a URL points: the host and port the connector opens, read from an absolute URL whose
//! scheme the calling protocol speaks. Errors say what is wrong, never what the URL was.

use hyper::Uri;

use crate::error::{NetError, Step};

/// The host (an IPv6 literal without its brackets) and port a URL names, and whether its
/// scheme asks for TLS.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Target {
    pub host: String,
    pub port: u16,
    pub tls: bool,
}

/// Parses `url`, refusing anything that is not a URL.
pub(crate) fn parse(url: &str) -> Result<Uri, NetError> {
    url.parse()
        .map_err(|_| NetError::protocol(Step::Url, "not a valid absolute URL"))
}

/// The target of `uri` when its scheme is the calling protocol's `plain` one (`ws`, `http`;
/// the port defaults to 80) or its `secure` one (`wss`, `https`; TLS, the port defaults to 443).
pub(crate) fn target(
    uri: &Uri,
    plain: &'static str,
    secure: &'static str,
) -> Result<Target, NetError> {
    let tls = match uri.scheme_str() {
        Some(scheme) if scheme == plain => false,
        Some(scheme) if scheme == secure => true,
        _ => {
            return Err(NetError::protocol(
                Step::Url,
                "the scheme is not one this call speaks",
            ));
        }
    };
    let host = uri.host().unwrap_or_default();
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() {
        return Err(NetError::protocol(Step::Url, "the URL has no host"));
    }
    Ok(Target {
        host: host.to_owned(),
        port: uri.port_u16().unwrap_or(if tls { 443 } else { 80 }),
        tls,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(url: &str, plain: &'static str) -> Result<Target, NetError> {
        let secure = if plain == "ws" { "wss" } else { "https" };
        target(&parse(url)?, plain, secure)
    }

    fn at(host: &str, port: u16, tls: bool) -> Target {
        Target {
            host: host.into(),
            port,
            tls,
        }
    }

    #[test]
    fn the_host_and_port_come_from_the_authority_without_user_information() {
        assert_eq!(
            of("ws://ws.test/s", "ws").unwrap(),
            at("ws.test", 80, false)
        );
        assert_eq!(
            of("http://u:p@api.test:8080/x?q=1", "http").unwrap(),
            at("api.test", 8080, false)
        );
        assert_eq!(of("ws://[::1]:9/", "ws").unwrap(), at("::1", 9, false));
    }

    #[test]
    fn a_tls_scheme_asks_for_tls_and_defaults_to_port_443() {
        assert_eq!(
            of("wss://ws.test/s", "ws").unwrap(),
            at("ws.test", 443, true)
        );
        assert_eq!(
            of("https://api.test:8443/x", "http").unwrap(),
            at("api.test", 8443, true)
        );
    }

    #[test]
    fn a_url_this_call_cannot_open_fails_at_the_url_step_saying_why() {
        let why = |url, plain| match of(url, plain) {
            Err(e) if e.step() == Step::Url => e.to_string(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            why("https://x.test/", "ws"),
            "URL failed: the scheme is not one this call speaks"
        );
        assert_eq!(
            why("wss://x.test/", "http"),
            "URL failed: the scheme is not one this call speaks"
        );
        assert_eq!(
            why("ftp://x.test/", "http"),
            "URL failed: the scheme is not one this call speaks"
        );
        assert_eq!(
            why("/relative", "http"),
            "URL failed: the scheme is not one this call speaks"
        );
        assert_eq!(
            why("http://:80/", "http"),
            "URL failed: the URL has no host"
        );
        assert_eq!(
            why("http://a b/", "http"),
            "URL failed: not a valid absolute URL"
        );
    }
}
