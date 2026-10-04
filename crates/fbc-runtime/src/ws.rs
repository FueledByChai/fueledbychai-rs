//! The WebSocket client (`ws://`, and `wss://` over TLS), on the one connector.

use tokio_tungstenite::tungstenite;

pub use tungstenite::Message;

use crate::connector::Connector;
use crate::error::{Cause, NetError, Step};
use crate::target;
use crate::transport::Transport;

/// An open WebSocket: a `Stream` of incoming [`Message`]s and a `Sink` for outgoing ones.
pub type WebSocket = tokio_tungstenite::WebSocketStream<Transport>;

impl Connector {
    /// Opens `url` (`ws://`, or `wss://` with TLS) through this connector and completes the
    /// WebSocket upgrade.
    pub async fn websocket(&self, url: &str) -> Result<WebSocket, NetError> {
        let uri = target::parse(url)?;
        let to = target::target(&uri, "ws", "wss")?;
        let stream = self.open(&to).await?;
        let (socket, _response) = tokio_tungstenite::client_async(uri, stream)
            .await
            .map_err(upgrade_error)?;
        Ok(socket)
    }
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

fn upgrade_error(e: tungstenite::Error) -> NetError {
    let cause = match e {
        tungstenite::Error::Http(response) => Cause::Status(response.status().as_u16()),
        tungstenite::Error::Io(io) => Cause::Io(io.kind()),
        other => Cause::Detail(other.to_string()),
    };
    NetError::new(Step::WebSocketUpgrade, cause)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn an_upgrade_failure_keeps_the_status_or_the_io_kind() {
        let io = tungstenite::Error::Io(io::Error::from(io::ErrorKind::ConnectionReset));
        assert_eq!(
            upgrade_error(io),
            NetError::new(
                Step::WebSocketUpgrade,
                Cause::Io(io::ErrorKind::ConnectionReset)
            )
        );
        let closed = upgrade_error(tungstenite::Error::ConnectionClosed);
        assert!(matches!(closed.cause(), Cause::Detail(_)));
    }
}
