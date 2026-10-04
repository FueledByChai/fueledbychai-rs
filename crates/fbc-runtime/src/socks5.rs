//! The SOCKS5 CONNECT handshake (RFC 1928), no-authentication method only: the owner's proxy
//! is a host and a port, with no credentials (owner decision 2026-10-03). Written by hand over
//! any byte stream, so the tests drive every reply from memory.

use std::net::IpAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Cause, NetError, Step, io};

const VERSION: u8 = 5;
const NO_AUTHENTICATION: u8 = 0;
const NO_ACCEPTABLE_METHOD: u8 = 0xff;
const CONNECT: u8 = 1;
const IPV4: u8 = 1;
const DOMAINNAME: u8 = 3;
const IPV6: u8 = 4;

/// The CONNECT request for `host:port`. A name goes as DOMAINNAME, so the proxy resolves it;
/// an IP literal goes as IPv4 or IPv6. Built before the proxy is dialled, so a name that does
/// not fit fails at [`Step::Url`] without a connection.
pub(crate) fn connect_request(host: &str, port: u16) -> Result<Vec<u8>, NetError> {
    let mut request = vec![VERSION, CONNECT, 0];
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            request.push(IPV4);
            request.extend(ip.octets());
        }
        Ok(IpAddr::V6(ip)) => {
            request.push(IPV6);
            request.extend(ip.octets());
        }
        Err(_) => {
            let len = u8::try_from(host.len())
                .map_err(|_| NetError::protocol(Step::Url, "the host name is over 255 bytes"))?;
            request.extend([DOMAINNAME, len]);
            request.extend(host.as_bytes());
        }
    }
    request.extend(port.to_be_bytes());
    Ok(request)
}

/// Negotiates no-authentication, sends `request` (from [`connect_request`]) and reads the
/// whole reply, so the stream then carries the target's bytes only.
pub(crate) async fn handshake<S>(stream: &mut S, request: &[u8]) -> Result<(), NetError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let step = Step::ProxyGreeting;
    stream
        .write_all(&[VERSION, 1, NO_AUTHENTICATION])
        .await
        .map_err(io(step))?;
    let mut choice = [0u8; 2];
    stream.read_exact(&mut choice).await.map_err(io(step))?;
    match choice {
        [VERSION, NO_AUTHENTICATION] => {}
        [VERSION, NO_ACCEPTABLE_METHOD] => {
            return Err(NetError::protocol(
                step,
                "the proxy accepts no offered method (only no-authentication is offered)",
            ));
        }
        [VERSION, _] => {
            return Err(NetError::protocol(
                step,
                "the proxy chose a method not offered",
            ));
        }
        _ => return Err(NetError::protocol(step, "the reply is not SOCKS version 5")),
    }

    let step = Step::ProxyConnect;
    stream.write_all(request).await.map_err(io(step))?;
    let mut head = [0u8; 4];
    stream.read_exact(&mut head).await.map_err(io(step))?;
    if head[0] != VERSION {
        return Err(NetError::protocol(step, "the reply is not SOCKS version 5"));
    }
    if head[1] != 0 {
        return Err(NetError::new(step, Cause::Reply(head[1])));
    }
    // The reply ends with the proxy's bound address and port; read past them.
    let bound = match head[3] {
        IPV4 => 4,
        IPV6 => 16,
        DOMAINNAME => usize::from(stream.read_u8().await.map_err(io(step))?),
        _ => {
            return Err(NetError::protocol(
                step,
                "the reply has an unknown address type",
            ));
        }
    };
    let mut rest = vec![0u8; bound + 2];
    stream.read_exact(&mut rest).await.map_err(io(step))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;
    use tokio::io::duplex;

    #[test]
    fn a_name_goes_as_domainname_and_an_ip_literal_as_an_address() {
        assert_eq!(
            connect_request("ab.test", 443).unwrap(),
            [
                5, 1, 0, 3, 7, b'a', b'b', b'.', b't', b'e', b's', b't', 1, 187
            ]
        );
        assert_eq!(
            connect_request("10.0.0.1", 80).unwrap(),
            [5, 1, 0, 1, 10, 0, 0, 1, 0, 80]
        );
        let v6 = connect_request("::1", 80).unwrap();
        assert_eq!((v6[3], v6.len(), v6[19]), (4, 22, 1));
        let long = "a".repeat(256);
        assert_eq!(connect_request(&long, 80).unwrap_err().step(), Step::Url);
        assert_eq!(
            connect_request(&long[..255], 80).unwrap().len(),
            4 + 1 + 255 + 2
        );
    }

    /// Runs the handshake against a proxy that answers with `reply` whatever it is sent and
    /// then stops sending, and returns the result and the bytes the client sent.
    async fn against(reply: &[u8]) -> (Result<(), NetError>, Vec<u8>) {
        let (mut client, mut proxy) = duplex(1024);
        proxy.write_all(reply).await.unwrap();
        proxy.shutdown().await.unwrap();
        let request = connect_request("ab.test", 443).unwrap();
        let result = handshake(&mut client, &request).await;
        drop(client);
        let mut sent = Vec::new();
        proxy.read_to_end(&mut sent).await.unwrap();
        (result, sent)
    }

    const OK: [u8; 2] = [5, 0];

    #[tokio::test]
    async fn a_successful_handshake_reads_every_bound_address_form() {
        for bound in [
            &[5, 0, 0, 1, 1, 2, 3, 4, 0, 80][..],
            &[5, 0, 0, 3, 2, b'p', b'x', 0, 80],
            &[
                5, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 80,
            ],
        ] {
            let (result, sent) = against(&[&OK[..], bound].concat()).await;
            result.unwrap();
            assert_eq!(
                sent[..3],
                [5, 1, 0],
                "greeting offers no-authentication only"
            );
            assert_eq!(sent[3..], connect_request("ab.test", 443).unwrap());
        }
    }

    async fn fails(reply: &[u8]) -> NetError {
        against(reply).await.0.unwrap_err()
    }

    #[tokio::test]
    async fn each_bad_greeting_fails_at_the_greeting_step() {
        let protocol = |why| NetError::protocol(Step::ProxyGreeting, why);
        assert_eq!(
            fails(&[5, 0xff]).await,
            protocol("the proxy accepts no offered method (only no-authentication is offered)")
        );
        assert_eq!(
            fails(&[5, 2]).await,
            protocol("the proxy chose a method not offered")
        );
        assert_eq!(
            fails(&[4, 0]).await,
            protocol("the reply is not SOCKS version 5")
        );
        assert_eq!(
            fails(&[5]).await,
            NetError::new(Step::ProxyGreeting, Cause::Io(ErrorKind::UnexpectedEof))
        );
    }

    #[tokio::test]
    async fn each_bad_connect_reply_fails_at_the_connect_step() {
        let step = Step::ProxyConnect;
        let with = |rest: &[u8]| [&OK[..], rest].concat();
        assert_eq!(
            fails(&with(&[5, 2, 0, 1])).await,
            NetError::new(step, Cause::Reply(2))
        );
        assert_eq!(
            fails(&with(&[4, 0, 0, 1])).await,
            NetError::protocol(step, "the reply is not SOCKS version 5")
        );
        assert_eq!(
            fails(&with(&[5, 0, 0, 9])).await,
            NetError::protocol(step, "the reply has an unknown address type")
        );
        let eof = NetError::new(step, Cause::Io(ErrorKind::UnexpectedEof));
        assert_eq!(fails(&with(&[5, 0])).await, eof);
        assert_eq!(fails(&with(&[5, 0, 0, 3])).await, eof);
        assert_eq!(fails(&with(&[5, 0, 0, 1, 1, 2])).await, eof);
    }

    #[tokio::test]
    async fn a_proxy_that_stops_reading_fails_the_write_at_its_step() {
        let request = connect_request("ab.test", 443).unwrap();
        let (mut client, proxy) = duplex(64);
        drop(proxy);
        let err = handshake(&mut client, &request).await.unwrap_err();
        assert_eq!(
            err,
            NetError::new(Step::ProxyGreeting, Cause::Io(ErrorKind::BrokenPipe))
        );

        let (mut client, mut proxy) = duplex(64);
        proxy.write_all(&OK).await.unwrap();
        let mut greeting = [0u8; 3];
        let reader = async move {
            proxy.read_exact(&mut greeting).await.unwrap();
            drop(proxy);
        };
        let (_, result) = tokio::join!(reader, handshake(&mut client, &request));
        assert_eq!(result.unwrap_err().step(), Step::ProxyConnect);
    }
}
