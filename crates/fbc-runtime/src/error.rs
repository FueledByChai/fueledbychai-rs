//! The runtime's one error type: which step failed and why, never the URL (0014 item 8).

use std::fmt;
use std::io;

/// The step of opening or using a connection that failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Reading the URL: not a URL, a scheme this call does not speak, or no usable host.
    Url,
    /// Opening TCP to the SOCKS5 proxy.
    ProxyTcp,
    /// Opening TCP straight to the target (no proxy).
    TargetTcp,
    /// The SOCKS5 method negotiation (RFC 1928 §3).
    ProxyGreeting,
    /// The SOCKS5 CONNECT request and its reply (RFC 1928 §4, §6).
    ProxyConnect,
    /// Adding a consumer's TLS trust anchor: the bytes are not a usable certificate.
    TlsTrust,
    /// The TLS handshake with the target: its certificate, its name, or the TLS exchange.
    TlsHandshake,
    /// The WebSocket opening handshake.
    WebSocketUpgrade,
    /// The HTTP/1.1 exchange: sending the request or reading the response.
    Http,
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Step::Url => "URL",
            Step::ProxyTcp => "TCP connection to the SOCKS5 proxy",
            Step::TargetTcp => "TCP connection to the target",
            Step::ProxyGreeting => "SOCKS5 greeting",
            Step::ProxyConnect => "SOCKS5 CONNECT",
            Step::TlsTrust => "TLS trust anchor",
            Step::TlsHandshake => "TLS handshake",
            Step::WebSocketUpgrade => "WebSocket upgrade",
            Step::Http => "HTTP/1.1 request",
        })
    }
}

/// Why the step failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cause {
    /// An I/O error, by kind only.
    Io(io::ErrorKind),
    /// A rule of the protocol or of this runtime that the input or the peer broke.
    Protocol(&'static str),
    /// The proxy answered CONNECT with this failure code (RFC 1928 §6).
    Reply(u8),
    /// The server answered the WebSocket upgrade with this HTTP status instead of 101.
    Status(u16),
    /// The protocol library's description of the failure; it never holds the URL.
    Detail(String),
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cause::Io(kind) => write!(f, "{kind}"),
            Cause::Protocol(why) => f.write_str(why),
            Cause::Reply(code) => write!(f, "reply code 0x{code:02x} ({})", reply_name(*code)),
            Cause::Status(status) => write!(f, "status {status}"),
            Cause::Detail(detail) => f.write_str(detail),
        }
    }
}

/// RFC 1928 §6's name for a CONNECT reply code.
fn reply_name(code: u8) -> &'static str {
    match code {
        0 => "succeeded",
        1 => "general SOCKS server failure",
        2 => "connection not allowed by ruleset",
        3 => "network unreachable",
        4 => "host unreachable",
        5 => "connection refused",
        6 => "TTL expired",
        7 => "command not supported",
        8 => "address type not supported",
        _ => "unassigned",
    }
}

/// A failed connection or exchange: the [`Step`] and its [`Cause`]. Neither holds a URL, so
/// `Display` and `Debug` cannot show a URL's user information or query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetError {
    step: Step,
    cause: Cause,
}

impl NetError {
    pub(crate) fn new(step: Step, cause: Cause) -> Self {
        NetError { step, cause }
    }

    pub(crate) fn protocol(step: Step, why: &'static str) -> Self {
        NetError::new(step, Cause::Protocol(why))
    }

    /// The step that failed.
    pub fn step(&self) -> Step {
        self.step
    }

    /// Why it failed.
    pub fn cause(&self) -> &Cause {
        &self.cause
    }
}

/// Maps an I/O error at `step` to a [`NetError`] that keeps only the error's kind.
pub(crate) fn io(step: Step) -> impl FnOnce(io::Error) -> NetError {
    move |e| NetError::new(step, Cause::Io(e.kind()))
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed: {}", self.step, self.cause)
    }
}

impl std::error::Error for NetError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_step_and_cause_reads_as_a_sentence() {
        let shown = |step, cause| NetError::new(step, cause).to_string();
        assert_eq!(
            shown(Step::Url, Cause::Protocol("no host")),
            "URL failed: no host"
        );
        assert_eq!(
            shown(Step::ProxyTcp, Cause::Io(io::ErrorKind::ConnectionRefused)),
            "TCP connection to the SOCKS5 proxy failed: connection refused"
        );
        assert_eq!(
            shown(Step::TargetTcp, Cause::Io(io::ErrorKind::TimedOut)),
            "TCP connection to the target failed: timed out"
        );
        assert_eq!(
            shown(Step::TlsTrust, Cause::Detail("bad DER".into())),
            "TLS trust anchor failed: bad DER"
        );
        assert_eq!(
            shown(Step::TlsHandshake, Cause::Io(io::ErrorKind::UnexpectedEof)),
            "TLS handshake failed: unexpected end of file"
        );
        assert_eq!(
            shown(Step::WebSocketUpgrade, Cause::Status(404)),
            "WebSocket upgrade failed: status 404"
        );
        assert_eq!(
            shown(Step::Http, Cause::Detail("closed".into())),
            "HTTP/1.1 request failed: closed"
        );
        assert_eq!(
            shown(Step::ProxyGreeting, Cause::Reply(0)),
            "SOCKS5 greeting failed: reply code 0x00 (succeeded)"
        );
    }

    #[test]
    fn every_reply_code_has_its_rfc_1928_name() {
        let names: Vec<String> = (1..=9).map(|c| Cause::Reply(c).to_string()).collect();
        assert_eq!(
            names,
            [
                "reply code 0x01 (general SOCKS server failure)",
                "reply code 0x02 (connection not allowed by ruleset)",
                "reply code 0x03 (network unreachable)",
                "reply code 0x04 (host unreachable)",
                "reply code 0x05 (connection refused)",
                "reply code 0x06 (TTL expired)",
                "reply code 0x07 (command not supported)",
                "reply code 0x08 (address type not supported)",
                "reply code 0x09 (unassigned)",
            ]
        );
        assert_eq!(
            NetError::new(Step::ProxyConnect, Cause::Reply(2)).cause(),
            &Cause::Reply(2)
        );
    }
}
