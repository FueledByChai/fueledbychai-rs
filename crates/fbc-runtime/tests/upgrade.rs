//! FBC-17h: the WebSocket upgrade accepts a `Connection` header that lists the `Upgrade` token
//! among others, as RFC 6455 §4.1 allows, and still fails one without that token at the upgrade
//! step. Every server is local (tests/common); nothing reaches the internet.

mod common;

use common::upgrading;
use fbc_runtime::ws::Message;
use fbc_runtime::{Cause, Connector, ProxyConfig, Step};
use futures_util::{SinkExt, StreamExt};

#[tokio::test]
async fn a_connection_token_list_with_upgrade_is_accepted_and_a_message_is_exchanged() {
    let server = upgrading("keep-alive, Upgrade", "hello").await;
    let direct = Connector::new(ProxyConfig::Direct);

    let mut ws = direct
        .websocket(&format!("ws://{server}/stream"))
        .await
        .unwrap();

    // The greeting came in the same write as the 101: none of its bytes is lost to the
    // handshake's read.
    assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("hello"));
    ws.send(Message::text("ping")).await.unwrap();
    assert_eq!(ws.next().await.unwrap().unwrap(), Message::text("ping"));
}

#[tokio::test]
async fn a_connection_header_without_an_upgrade_token_fails_at_the_upgrade_step() {
    let direct = Connector::new(ProxyConfig::Direct);
    for connection in ["keep-alive", "keep-alive, Upgraded", "close"] {
        let server = upgrading(connection, "hello").await;
        let err = direct
            .websocket(&format!("ws://{server}/stream?token=secret"))
            .await
            .unwrap_err();
        assert_eq!(err.step(), Step::WebSocketUpgrade, "{connection}");
        assert!(
            matches!(err.cause(), Cause::Protocol(_)),
            "{connection}: {err}"
        );
        assert!(!err.to_string().contains("secret"), "{err}");
    }
}
