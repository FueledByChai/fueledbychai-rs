//! The WebSocket client (`ws://`), on the one connector.

use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite;

pub use tungstenite::Message;

use crate::connector::Connector;
use crate::error::{Cause, NetError, Step};
use crate::target;

/// An open WebSocket: a `Stream` of incoming [`Message`]s and a `Sink` for outgoing ones.
pub type WebSocket = tokio_tungstenite::WebSocketStream<TcpStream>;

impl Connector {
    /// Opens `url` (`ws://` only) through this connector and completes the WebSocket upgrade.
    pub async fn websocket(&self, url: &str) -> Result<WebSocket, NetError> {
        let uri = target::parse(url)?;
        let to = target::target(&uri, "ws")?;
        let stream = self.connect(&to.host, to.port).await?;
        let (socket, _response) = tokio_tungstenite::client_async(uri, stream)
            .await
            .map_err(upgrade_error)?;
        Ok(socket)
    }
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
