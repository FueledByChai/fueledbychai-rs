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
//!
//! A place-stage run (FBC-23ch; decision 0148 item 8) never ends the program at a press:
//! `main.rs` wires the same presses with an `outside` that hands each to the run's [`Presses`]
//! ([`Presses::channel`]), so a press during the preflight, the reads, a session or a stop's
//! cancels, a second one included, stops the run with its report (`places::Stop::Interrupted`).
//! A press while the hold's connection listens is the hold's, as before. One the run takes from
//! the hold's first read to its last is the hold's too (`stage.rs`, `Run::hold`): before the
//! connection opens it keeps it from opening, and during the reads after it it lets them end;
//! either stops the run before its preflight, writing no stop marker.

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

/// The presses a place-stage run takes ([`Presses::pressed`]): each one `main.rs` hands it that
/// the hold did not take, counted. Without a source of presses ([`Presses::none`]) it is never
/// pressed.
#[derive(Default)]
pub struct Presses {
    presses: Option<mpsc::UnboundedReceiver<()>>,
    taken: u32,
}

impl Presses {
    /// Presses that never come: a stage-one run's, and a test's that presses nothing.
    pub fn none() -> Presses {
        Presses::default()
    }

    /// The `outside` [`wire`] hands each press it does not give the hold, and the presses it
    /// hands them to: a press is never lost and never ends the program.
    pub fn channel() -> (impl FnMut() + Send + 'static, Presses) {
        let (tx, rx) = mpsc::unbounded_channel();
        let outside = move || {
            let _ = tx.send(());
        };
        let presses = Presses {
            presses: Some(rx),
            taken: 0,
        };
        (outside, presses)
    }

    /// Resolves at the next press, one made before it is polled included, and counts it; never
    /// once the source of presses has ended. Cancelled before a press, it takes none.
    pub async fn pressed(&mut self) {
        match &mut self.presses {
            Some(rx) => match rx.recv().await {
                Some(()) => self.taken += 1,
                None => std::future::pending().await,
            },
            None => std::future::pending().await,
        }
    }

    /// Takes a press already made, if one is waiting, and counts it, without waiting for one:
    /// whether it took one.
    pub fn try_pressed(&mut self) -> bool {
        let taken = (self.presses.as_mut()).is_some_and(|rx| rx.try_recv().is_ok());
        self.taken += u32::from(taken);
        taken
    }

    /// How many presses it has taken.
    #[allow(dead_code)] // the presses' tests read it; a stop's cancels count their own
    pub fn taken(&self) -> u32 {
        self.taken
    }
}
