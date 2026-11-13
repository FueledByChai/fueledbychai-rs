//! The owner's interrupt in a run that holds (decision 0146 item 4): each press of Ctrl-C, from
//! a source of presses (`main.rs`: tokio's `ctrl_c`; a test: presses of its own), goes to the
//! hold's connection while that connection listens, and any other is handed to `outside`, which
//! in `main.rs` ends the program at once with status 130, writing no report, as an interrupt has
//! always ended the probe ([`wire`]).
//!
//! The hold's connection listens from its opening until it is over: `stage.rs` polls the
//! interrupt once as the connection opens, before it writes anything, and drops it only once
//! its close and the read of the server's echo are over. A press in between, a second one
//! included, is the hold's: one before the hold ended ends it, and one after it, while it
//! closes, is read by the hold as it drops the interrupt.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::stage::Interrupt;

/// The hold's side of the interrupt: listening from its first poll until it is dropped, and
/// ready at each press taken while it listens.
struct Pressed {
    listening: Arc<AtomicBool>,
    presses: mpsc::UnboundedReceiver<()>,
}

impl Future for Pressed {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.listening.store(true, Ordering::SeqCst);
        match self.presses.poll_recv(cx) {
            Poll::Ready(Some(())) => Poll::Ready(()),
            // The source of presses ended, so no press will come.
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for Pressed {
    fn drop(&mut self) {
        self.listening.store(false, Ordering::SeqCst);
    }
}

/// The task that takes each press from `presses`, to be spawned on the run's runtime, and the
/// interrupt the hold waits on ([`crate::stage::Interrupt`]): a press goes to the hold while
/// its interrupt listens (from its first poll until it is dropped), and otherwise to `outside`.
pub fn wire<S, X>(presses: S, outside: X) -> (impl Future<Output = ()> + Send + 'static, Interrupt)
where
    S: Stream<Item = ()> + Send + 'static,
    X: FnMut() + Send + 'static,
{
    let listening = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::unbounded_channel();
    let flag = Arc::clone(&listening);
    let task = async move {
        let (mut presses, mut outside) = (Box::pin(presses), outside);
        while presses.next().await.is_some() {
            // The hold clears the flag as it drops its side, before the channel closes, so a
            // press that finds the flag set reaches it.
            if !(flag.load(Ordering::SeqCst) && tx.send(()).is_ok()) {
                outside();
            }
        }
    };
    let pressed: Interrupt = Box::pin(Pressed {
        listening,
        presses: rx,
    });
    (task, pressed)
}
