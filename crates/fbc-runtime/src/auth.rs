//! The credential headers an order-entry connection's opening handshake carries (decision
//! 0116): the one place the runtime reads a credential's value (0009), on the
//! `crates/*/src/auth` review path.
//!
//! A venue names them in its [`ExecEndpoint`]s ([`HandshakeHeader`]: a header and the
//! [`Secrets`] key whose value it carries). As a session is built, before the credentials are
//! handed to the venue's codec, [`HandshakeHeaders::for_endpoint`] checks each name and copies
//! each value into a [`Secret`] of the runtime's own, zeroed when the session drops; the codec
//! still gets the whole set. Every connection attempt to that endpoint, a reconnect's and one
//! through a SOCKS5 proxy included, then adds them to its upgrade request
//! ([`HandshakeHeaders::add_to`]), each value marked sensitive. A market-data socket has none.
//!
//! Nothing here shows a value: [`HandshakeHeaders`]' `Debug` names each header with
//! `<redacted>`, the request's header values `Debug` as `Sensitive`, an error names the header
//! or the key and never the value, and the journal records no handshake. The bytes hyper
//! writes the request into are not zeroed after the write (FBC-96h, deferred).

use core::fmt;

use fbc_core::{ConfigError, ExecEndpoint, HandshakeHeader, Secret, Secrets, VenueError};
use hyper::header::{HeaderName, HeaderValue};
use hyper::http::request::Builder;

use crate::exec_session::ExecSessionError;

/// The headers an opening handshake sets itself, or that would frame a body it does not have:
/// a venue may not name one (decision 0116).
const OWN: [&str; 5] = [
    "host",
    "connection",
    "upgrade",
    "content-length",
    "transfer-encoding",
];

/// One endpoint's credential headers, each value a copy the runtime keeps, zeroed on drop. Not
/// `Clone`: one set per endpoint, borrowed by each connection attempt.
#[derive(Default)]
pub(crate) struct HandshakeHeaders {
    headers: Vec<(HeaderName, Secret)>,
}

impl HandshakeHeaders {
    /// No headers: a market-data socket's, or an endpoint that names none.
    pub(crate) fn none() -> HandshakeHeaders {
        HandshakeHeaders::default()
    }

    /// The headers `ep` names, their values read from `creds`. Refused when `ep` names a header
    /// no handshake can carry, or names one twice ([`ExecSessionError::HandshakeHeader`]), when
    /// `creds` lack a key it names ([`ConfigError::Missing`]), or when a value is not one a
    /// header can carry, such as one holding a line break ([`ConfigError::Invalid`]); none of
    /// these carries a value.
    pub(crate) fn for_endpoint(
        ep: &ExecEndpoint,
        creds: &Secrets,
    ) -> Result<HandshakeHeaders, ExecSessionError> {
        let mut headers: Vec<(HeaderName, Secret)> = Vec::with_capacity(ep.handshake.len());
        for &HandshakeHeader { name, secret } in &ep.handshake {
            let refused = ExecSessionError::HandshakeHeader {
                stream: ep.stream,
                name,
            };
            let parsed = HeaderName::from_bytes(name.as_bytes()).map_err(|_| refused.clone())?;
            let own =
                OWN.contains(&parsed.as_str()) || parsed.as_str().starts_with("sec-websocket-");
            if own || headers.iter().any(|(n, _)| *n == parsed) {
                return Err(refused);
            }
            let value = creds
                .get(secret)
                .ok_or(VenueError::Config(ConfigError::Missing(secret)))
                .map_err(ExecSessionError::Venue)?;
            if HeaderValue::from_str(value.expose()).is_err() {
                let reason = "not a value an HTTP header can carry";
                let invalid = ConfigError::Invalid {
                    key: secret,
                    reason,
                };
                return Err(ExecSessionError::Venue(VenueError::Config(invalid)));
            }
            headers.push((parsed, Secret::new(value.expose().to_owned())));
        }
        Ok(HandshakeHeaders { headers })
    }

    /// `request` with every header added, each value marked sensitive so the request's `Debug`
    /// shows none of it. A value [`for_endpoint`](HandshakeHeaders::for_endpoint) accepted
    /// always parses; were one not to, the builder is handed [`UNSENDABLE`] in its place, so it
    /// fails with `http`'s invalid-value error, which names nothing.
    pub(crate) fn add_to(&self, mut request: Builder) -> Builder {
        for (name, secret) in &self.headers {
            request = match HeaderValue::from_str(secret.expose()) {
                Ok(mut value) => {
                    value.set_sensitive(true);
                    request.header(name, value)
                }
                Err(_) => request.header(name, UNSENDABLE),
            };
        }
        request
    }
}

/// No header can carry a line feed.
const UNSENDABLE: &[u8] = b"\n";

impl fmt::Debug for HandshakeHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for (name, _) in &self.headers {
            list.entry(&format_args!("{name}: <redacted>"));
        }
        list.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{StreamId, WireUrl};
    use hyper::Request;

    const KEY: &str = "toy.key";

    fn value() -> String {
        format!("SYNTH{}-auth-unit", "apikey")
    }

    fn endpoint(names: &[&'static str]) -> ExecEndpoint {
        ExecEndpoint {
            stream: StreamId(3),
            url: WireUrl::plain("ws://toy.invalid/"),
            handshake: names
                .iter()
                .map(|&name| HandshakeHeader { name, secret: KEY })
                .collect(),
        }
    }

    fn creds(value: String) -> Secrets {
        let mut creds = Secrets::new();
        creds.insert(KEY, Secret::new(value));
        creds
    }

    #[test]
    fn the_headers_go_on_the_request_as_they_are_and_marked_sensitive() {
        let headers =
            HandshakeHeaders::for_endpoint(&endpoint(&["Authorization"]), &creds(value())).unwrap();
        let request = headers.add_to(Request::get("/")).body(()).unwrap();
        let sent = request.headers().get_all("authorization");
        let sent: Vec<&HeaderValue> = sent.iter().collect();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].as_bytes(), value().as_bytes());
        assert!(sent[0].is_sensitive());
        for shown in [format!("{request:?}"), format!("{headers:?}")] {
            assert!(!shown.contains(&value()), "{shown}");
        }
        assert_eq!(format!("{headers:?}"), "[authorization: <redacted>]");
        assert_eq!(format!("{:?}", HandshakeHeaders::none()), "[]");
    }

    #[test]
    fn an_endpoint_naming_none_adds_nothing() {
        let headers = HandshakeHeaders::for_endpoint(&endpoint(&[]), &Secrets::new()).unwrap();
        let request = headers.add_to(Request::get("/")).body(()).unwrap();
        assert!(request.headers().is_empty());
    }

    #[test]
    fn a_name_that_is_no_header_s_or_the_handshake_s_own_is_refused() {
        for name in [
            "Bad Name",
            "",
            "Host",
            "Connection",
            "upgrade",
            "Sec-WebSocket-Protocol",
            "Content-Length",
            "Transfer-Encoding",
        ] {
            let err = HandshakeHeaders::for_endpoint(&endpoint(&[name]), &creds(value()));
            let want = ExecSessionError::HandshakeHeader {
                stream: StreamId(3),
                name,
            };
            assert_eq!(err.err(), Some(want), "{name}");
        }
    }

    #[test]
    fn a_value_with_a_line_break_is_refused_by_its_key() {
        for bad in [format!("{}\r\nX: 1", value()), format!("{}\0", value())] {
            let err = HandshakeHeaders::for_endpoint(&endpoint(&["X-Key"]), &creds(bad));
            let err = err.err().unwrap();
            assert!(
                matches!(
                    err,
                    ExecSessionError::Venue(VenueError::Config(ConfigError::Invalid {
                        key: KEY,
                        ..
                    }))
                ),
                "{err:?}"
            );
            assert!(!format!("{err} {err:?}").contains(&value()));
        }
    }

    #[test]
    fn an_unsendable_value_fails_the_request_without_naming_it() {
        // Not reachable through `for_endpoint`, which refuses such a value first.
        let headers = HandshakeHeaders {
            headers: vec![(
                HeaderName::from_static("x-key"),
                Secret::new(format!("{}\n", value())),
            )],
        };
        let err = headers.add_to(Request::get("/")).body(()).unwrap_err();
        assert!(!format!("{err} {err:?}").contains(&value()));
    }
}
