//! The TCP stream under every connection the runtime opens, and the kernel receive timestamps
//! read beneath TLS and WebSocket (FBC-2y3, decision 0031).
//!
//! On Linux, [`Tcp`] turns on `SO_TIMESTAMPNS` as it wraps the stream and reads with `recvmsg`,
//! keeping the kernel's receive time (`CLOCK_REALTIME`) of the last packet each read consumed.
//! Whatever is read through it, TLS records or WebSocket frames, the latest such time is the
//! receive time of the last packet read before the layer above completed its frame: that frame's
//! last packet, or a later one that came in the same read. Elsewhere it reads as a plain stream
//! and has no timestamp. The calls go through nix's safe wrappers, so there is no unsafe code
//! here.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use fbc_core::KernelRxNs;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// A TCP stream the [`crate::Connector`] opened, directly or through the proxy, that keeps the
/// kernel receive time of what it last read where the platform has one (Linux).
#[derive(Debug)]
pub struct Tcp {
    stream: TcpStream,
    /// The kernel receive time of the last packet the latest read consumed, if it had one.
    #[cfg(target_os = "linux")]
    last: Option<KernelRxNs>,
    /// Room for one timestamp control message, reused by every read.
    #[cfg(target_os = "linux")]
    space: Vec<u8>,
}

impl Tcp {
    /// Wraps `stream`, asking the kernel to timestamp what it receives (Linux). A kernel that
    /// refuses leaves the stream without timestamps rather than unusable.
    pub(crate) fn new(stream: TcpStream) -> Tcp {
        #[cfg(target_os = "linux")]
        {
            use nix::sys::socket::{setsockopt, sockopt::ReceiveTimestampns};
            let _ = setsockopt(&stream, ReceiveTimestampns, &true);
            Tcp {
                stream,
                last: None,
                space: nix::cmsg_space!(nix::sys::time::TimeSpec),
            }
        }
        #[cfg(not(target_os = "linux"))]
        Tcp { stream }
    }

    /// The kernel receive time of the last packet the latest read consumed; always `None` off
    /// Linux, and `None` on Linux before the first read or when the latest read had none.
    pub fn kernel_rx(&self) -> Option<KernelRxNs> {
        #[cfg(target_os = "linux")]
        return self.last;
        #[cfg(not(target_os = "linux"))]
        None
    }
}

/// One `recvmsg` into `buf`: the bytes read and the kernel receive time it carried, if any.
#[cfg(target_os = "linux")]
fn recv(
    stream: &TcpStream,
    buf: &mut [u8],
    space: &mut [u8],
) -> io::Result<(usize, Option<KernelRxNs>)> {
    use nix::sys::socket::{ControlMessageOwned, MsgFlags, recvmsg};
    use std::os::fd::AsRawFd;
    let mut iov = [io::IoSliceMut::new(buf)];
    let msg = recvmsg::<()>(stream.as_raw_fd(), &mut iov, Some(space), MsgFlags::empty())?;
    // A truncated control buffer only loses the timestamp, never the bytes.
    let rx = msg.cmsgs().ok().and_then(|mut all| {
        all.find_map(|c| match c {
            ControlMessageOwned::ScmTimestampns(t) => Some(KernelRxNs(
                t.tv_sec()
                    .saturating_mul(1_000_000_000)
                    .saturating_add(t.tv_nsec()),
            )),
            _ => None,
        })
    });
    Ok((msg.bytes, rx))
}

impl AsyncRead for Tcp {
    #[cfg(target_os = "linux")]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        use tokio::io::Interest;
        let this = self.get_mut();
        loop {
            std::task::ready!(this.stream.poll_read_ready(cx))?;
            let (stream, space) = (&this.stream, &mut this.space);
            let unfilled = buf.initialize_unfilled();
            // A spurious readiness is cleared by try_io's WouldBlock, and the loop waits again.
            match stream.try_io(Interest::READABLE, || recv(stream, unfilled, space)) {
                Ok((n, rx)) => {
                    // A read without a timestamp leaves none, never an older read's.
                    this.last = rx;
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Tcp {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn a_read_keeps_its_kernel_receive_time_and_a_reset_fails_the_read() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap());
        let (client, accepted) = tokio::join!(client, listener.accept());
        let (mut server, _) = accepted.unwrap();
        let mut tcp = Tcp::new(client.unwrap());
        assert_eq!(tcp.kernel_rx(), None);

        server.write_all(b"tick").await.unwrap();
        let mut buf = [0u8; 4];
        tcp.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"tick");
        assert!(tcp.kernel_rx().is_some());
        tcp.write_all(b"ok").await.unwrap();
        tcp.flush().await.unwrap();

        // A reset reaches the reader as the error it is, not as the end of the stream.
        server.set_zero_linger().unwrap();
        drop(server);
        let err = tcp.read(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        tcp.shutdown().await.unwrap_or(());
    }
}
