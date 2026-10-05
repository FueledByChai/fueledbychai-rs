//! The byte stream a WebSocket or HTTP exchange runs on: plain TCP from the connector, or TLS
//! over that same TCP stream.

use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use fbc_core::KernelRxNs;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream;

use crate::tcp::Tcp;

/// A connection the [`crate::Connector`] opened: plain (`ws://`, `http://`) or TLS (`wss://`,
/// `https://`), directly or through the proxy either way.
#[derive(Debug)]
pub enum Transport {
    Plain(Tcp),
    Tls(Box<TlsStream<Tcp>>),
}

impl Transport {
    /// The kernel receive time of the last packet read beneath any TLS ([`Tcp::kernel_rx`]):
    /// `None` off Linux.
    pub fn kernel_rx(&self) -> Option<KernelRxNs> {
        match self {
            Transport::Plain(s) => s.kernel_rx(),
            Transport::Tls(s) => s.get_ref().0.kernel_rx(),
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Transport::Plain(s) => s.is_write_vectored(),
            Transport::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_flush(cx),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
