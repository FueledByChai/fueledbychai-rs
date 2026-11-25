//! FBC-rq2l: the port [`common::closed_port`] gives refuses every connection, and no listener of
//! these tests can take it, for as long as the test holds it. A port that was bound and then
//! released was free again: in CI a WebSocket server another test of `md_journal` started took
//! one, and a plain HTTP request meant to be refused reached it, whose upgrade then failed.

mod common;

use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr};

use tokio::net::{TcpSocket, TcpStream};

#[tokio::test]
async fn a_closed_port_refuses_connections_and_no_listener_takes_it_while_held() {
    let closed = common::closed_port().await;
    let at = SocketAddr::from((Ipv4Addr::LOCALHOST, closed.port()));

    let refused = TcpStream::connect(at).await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);

    // A socket that does not set SO_REUSEADDR is refused the port.
    let socket = TcpSocket::new_v4().unwrap();
    assert_eq!(socket.bind(at).unwrap_err().kind(), ErrorKind::AddrInUse);
    // On Linux a listener that sets it is refused too. macOS admits an explicit bind that sets
    // it beside a connected socket, but no listener of these tests binds a port other than 0.
    #[cfg(target_os = "linux")]
    {
        let bound = tokio::net::TcpListener::bind(at).await.map(drop);
        assert_eq!(bound.unwrap_err().kind(), ErrorKind::AddrInUse);
        let bound = std::net::TcpListener::bind(at).map(drop);
        assert_eq!(bound.unwrap_err().kind(), ErrorKind::AddrInUse);
    }

    // The port still refuses connections after the refused binds.
    let refused = TcpStream::connect(at).await.unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::ConnectionRefused);
}
