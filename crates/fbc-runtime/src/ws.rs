//! The WebSocket client (`ws://`, and `wss://` over TLS), on the one connector.
//!
//! The opening handshake is the runtime's own (FBC-17h): hyper's HTTP/1.1 client sends the
//! upgrade request and reads the response head, the response is checked against RFC 6455 §4.1
//! here, and tokio-tungstenite takes the stream over from there, with any bytes hyper read past
//! the head. tungstenite's own client handshake compares the whole `Connection` header to
//! `Upgrade`, so it refuses `Connection: keep-alive, Upgrade`, which the RFC allows.

use std::convert::Infallible;
use std::future::pending;
use std::io;

use futures_util::FutureExt;
use http_body_util::Empty;
use hyper::client::conn::http1;
use hyper::header::{
    CONNECTION, HOST, HeaderMap, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_EXTENSIONS, SEC_WEBSOCKET_KEY,
    SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

pub use tungstenite::Message;

use crate::connector::Connector;
use crate::error::{Cause, NetError, Step};
use crate::http::{Bytes, host_header};
use crate::target;
use crate::transport::Transport;

/// An open WebSocket: a `Stream` of incoming [`Message`]s and a `Sink` for outgoing ones.
pub type WebSocket = tokio_tungstenite::WebSocketStream<Transport>;

impl Connector {
    /// Opens `url` (`ws://`, or `wss://` with TLS) through this connector and completes the
    /// WebSocket upgrade. The request goes in origin form (path and query) with a Host header
    /// from the URL; user information in the URL is never sent.
    pub async fn websocket(&self, url: &str) -> Result<WebSocket, NetError> {
        let uri = target::parse(url)?;
        let to = target::target(&uri, "ws", "wss")?;
        let host = host_header(&uri)?;
        let path = uri.path_and_query().map_or("/", |p| p.as_str());
        let key = generate_key();
        let request = Request::get(path)
            .header(HOST, host)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, &key)
            .body(Empty::<Bytes>::new())
            .map_err(|_| NetError::protocol(Step::Url, BAD_TARGET))?;
        let stream = self.open(&to).await?;
        upgrade(stream, request, &derive_accept_key(key.as_bytes())).await
    }
}

const BAD_TARGET: &str = "the path and query are not a valid request target";

/// Sends the upgrade `request` on `stream`, checks the response, and hands the stream, with
/// any bytes read past the response head, to tokio-tungstenite.
async fn upgrade(
    stream: Transport,
    request: Request<Empty<Bytes>>,
    accept: &str,
) -> Result<WebSocket, NetError> {
    let (mut sender, connection) = http1::Builder::new()
        .title_case_headers(true)
        .handshake(TokioIo::new(stream))
        .await
        .map_err(upgrade_error)?;
    let exchange = async move {
        let mut response = sender.send_request(request).await.map_err(upgrade_error)?;
        check_response(response.status(), response.headers(), accept)?;
        let upgraded = hyper::upgrade::on(&mut response)
            .await
            .map_err(upgrade_error)?;
        let parts = upgraded
            .downcast::<TokioIo<Transport>>()
            .map_err(|_| NetError::protocol(Step::WebSocketUpgrade, NOT_OURS))?;
        let (stream, read) = (parts.io.into_inner(), parts.read_buf.to_vec());
        Ok(WebSocket::from_partially_read(stream, read, Role::Client, None).await)
    };
    // The connection runs beside the exchange until it hands the stream over (or fails, which
    // the exchange then reports); the exchange's result is the call's, whatever the response.
    let drive = connection.with_upgrades().then(|_| pending::<Infallible>());
    tokio::select! {
        result = exchange => result,
        never = drive => match never {},
    }
}

const NOT_OURS: &str = "the upgraded connection is not the one the connector opened";
const NO_UPGRADE: &str = "the response has no Upgrade: websocket header";
const NO_CONNECTION_UPGRADE: &str = "the response's Connection header has no Upgrade token";
const BAD_ACCEPT: &str = "the response's Sec-WebSocket-Accept does not match the request's key";
const UNASKED: &str =
    "the response names a subprotocol or an extension the request did not ask for";

/// Checks an upgrade response as RFC 6455 §4.1 says a client must: status 101, `Upgrade:
/// websocket`, a `Connection` header with an `Upgrade` token among its comma-separated tokens
/// (any of its lines, case aside), the `Sec-WebSocket-Accept` that `accept` is, and no
/// subprotocol or extension, since the request asks for none.
fn check_response(status: StatusCode, headers: &HeaderMap, accept: &str) -> Result<(), NetError> {
    let fail = |why| Err(NetError::protocol(Step::WebSocketUpgrade, why));
    if status != StatusCode::SWITCHING_PROTOCOLS {
        return Err(NetError::new(
            Step::WebSocketUpgrade,
            Cause::Status(status.as_u16()),
        ));
    }
    let upgrade = headers.get(UPGRADE).and_then(|v| v.to_str().ok());
    if !upgrade.is_some_and(|v| v.eq_ignore_ascii_case("websocket")) {
        return fail(NO_UPGRADE);
    }
    let tokens = headers
        .get_all(CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','));
    if !tokens
        .map(str::trim)
        .any(|t| t.eq_ignore_ascii_case("upgrade"))
    {
        return fail(NO_CONNECTION_UPGRADE);
    }
    if headers
        .get(SEC_WEBSOCKET_ACCEPT)
        .is_none_or(|v| v != accept)
    {
        return fail(BAD_ACCEPT);
    }
    if headers.contains_key(SEC_WEBSOCKET_PROTOCOL)
        || headers.contains_key(SEC_WEBSOCKET_EXTENSIONS)
    {
        return fail(UNASKED);
    }
    Ok(())
}

/// Refuses, before any connection, a URL no attempt could open: not a `ws://` or `wss://` URL,
/// no usable host, or a `wss://` host that is not a TLS server name.
pub(crate) fn check_url(url: &str) -> Result<(), NetError> {
    let to = target::target(&target::parse(url)?, "ws", "wss")?;
    if to.tls {
        crate::tls::server_name(&to.host)?;
    }
    Ok(())
}

/// hyper's failure to send the request or read the response: the I/O error's kind when there is
/// one, otherwise hyper's description (which never holds the URL).
fn upgrade_error(e: hyper::Error) -> NetError {
    let io = std::error::Error::source(&e).and_then(|s| s.downcast_ref::<io::Error>());
    let cause = io.map_or_else(|| Cause::Detail(e.to_string()), |io| Cause::Io(io.kind()));
    NetError::new(Step::WebSocketUpgrade, cause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    const KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
    /// RFC 6455 §1.3's example: the accept value for `KEY`.
    const ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

    fn headers(lines: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in lines {
            map.append(*name, HeaderValue::from_static(value));
        }
        map
    }

    fn good() -> Vec<(&'static str, &'static str)> {
        vec![
            ("upgrade", "websocket"),
            ("connection", "Upgrade"),
            ("sec-websocket-accept", ACCEPT),
        ]
    }

    fn why(status: u16, lines: &[(&'static str, &'static str)]) -> Result<(), Cause> {
        let status = StatusCode::from_u16(status).unwrap();
        check_response(status, &headers(lines), ACCEPT).map_err(|e| {
            assert_eq!(e.step(), Step::WebSocketUpgrade);
            e.cause().clone()
        })
    }

    fn with(name: &'static str, value: &'static str) -> Vec<(&'static str, &'static str)> {
        let mut lines: Vec<_> = good().into_iter().filter(|(n, _)| *n != name).collect();
        lines.push((name, value));
        lines
    }

    #[test]
    fn the_accept_value_is_rfc_6455s_example() {
        assert_eq!(derive_accept_key(KEY.as_bytes()), ACCEPT);
    }

    #[test]
    fn an_upgrade_token_anywhere_in_the_connection_header_is_accepted() {
        assert_eq!(why(101, &good()), Ok(()));
        for value in [
            "keep-alive, Upgrade",
            "upgrade",
            "  UPGRADE  ,close",
            "a,,upgrade",
        ] {
            assert_eq!(why(101, &with("connection", value)), Ok(()), "{value}");
        }
        let mut two_lines = with("connection", "keep-alive");
        two_lines.push(("connection", "Upgrade"));
        assert_eq!(why(101, &two_lines), Ok(()));
    }

    #[test]
    fn a_connection_header_without_an_upgrade_token_is_refused() {
        let refused = Err(Cause::Protocol(NO_CONNECTION_UPGRADE));
        for value in ["keep-alive", "Upgrade-Insecure", "keep-alive Upgrade", ""] {
            assert_eq!(why(101, &with("connection", value)), refused, "{value}");
        }
        let none: Vec<_> = good()
            .into_iter()
            .filter(|(n, _)| *n != "connection")
            .collect();
        assert_eq!(why(101, &none), refused);
    }

    #[test]
    fn another_status_is_reported_by_its_code() {
        assert_eq!(why(200, &good()), Err(Cause::Status(200)));
        assert_eq!(why(404, &[]), Err(Cause::Status(404)));
    }

    #[test]
    fn the_upgrade_header_must_name_websocket() {
        let refused = Err(Cause::Protocol(NO_UPGRADE));
        assert_eq!(why(101, &with("upgrade", "WebSocket")), Ok(()));
        assert_eq!(why(101, &with("upgrade", "h2c")), refused);
        let none: Vec<_> = good()
            .into_iter()
            .filter(|(n, _)| *n != "upgrade")
            .collect();
        assert_eq!(why(101, &none), refused);
    }

    #[test]
    fn the_accept_value_must_be_the_one_the_key_gives() {
        let refused = Err(Cause::Protocol(BAD_ACCEPT));
        let other = "dGhlIHNhbXBsZSBub25jZQ==";
        assert_eq!(why(101, &with("sec-websocket-accept", other)), refused);
        let none: Vec<_> = good()
            .into_iter()
            .filter(|(n, _)| !n.ends_with("accept"))
            .collect();
        assert_eq!(why(101, &none), refused);
    }

    #[test]
    fn a_subprotocol_or_extension_the_request_did_not_ask_for_is_refused() {
        let refused = Err(Cause::Protocol(UNASKED));
        assert_eq!(why(101, &with("sec-websocket-protocol", "chat")), refused);
        let deflate = "permessage-deflate";
        assert_eq!(
            why(101, &with("sec-websocket-extensions", deflate)),
            refused
        );
    }

    /// Decision 0079: tungstenite logs each frame it writes and reads at TRACE, the auth
    /// frame's session token included, so the log facade's TRACE is compiled out of every build
    /// that links this crate.
    #[test]
    fn the_log_facade_s_trace_level_is_compiled_out() {
        assert!(log::STATIC_MAX_LEVEL <= log::LevelFilter::Debug);
    }
}
