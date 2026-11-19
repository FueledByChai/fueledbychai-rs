//! Serving scripts (decision 0150): the second kind of [`WsScript`](crate::WsScript), beside
//! 0025's timeline of steps, for a venue model whose frame count and order no timeline can know.
//! The stub accepts every connection and answers each data frame of connection `n` with the
//! frames the [`Responder`] made for `n` computes from it, several connections at once, until
//! the stub is dropped; a [`Pusher`] sends a connection unsolicited frames and closes it, and
//! waits on a barrier, the client's pong, that orders by the client's reads (FBC-4ptk). A
//! responder may hold its connection's first answer until another connection's close or end is
//! recorded ([`Responder::hold_first`]).

use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

use crate::script::{Frame, Responder, Step};
use crate::server::{
    Cmd, Conn, ConnRecord, Ready, Report, ScriptError, Shared, done, lock, panic_text, respond,
};

type MakeFn = dyn Fn(usize) -> Responder + Send + Sync;

/// What a serving script carries: the function that makes each connection's responder, and the
/// link its [`Pusher`] reaches the stub's connections through. Clones share both, and two
/// compare equal only when they share the link, so a script compares as it always did.
#[derive(Clone)]
pub(crate) struct Serving {
    make: Arc<MakeFn>,
    link: Arc<Link>,
}

impl Serving {
    /// A serving script's parts: what it carries and the pusher bound to it.
    pub(crate) fn new(
        make: impl Fn(usize) -> Responder + Send + Sync + 'static,
    ) -> (Serving, Pusher) {
        let link = Arc::new(Link::default());
        let pusher = Pusher { link: link.clone() };
        let serving = Serving {
            make: Arc::new(make),
            link,
        };
        (serving, pusher)
    }

    /// Binds this script to the stub about to play it, once that stub's endpoints are bound;
    /// `steps` (the script's own) must be empty. A serving script plays no step, and is started
    /// on one stub only, ever, so each push names one connection: the binding is never undone,
    /// not even when that stub is dropped.
    pub(crate) fn bind(&self, steps: &[Step]) -> io::Result<Arc<Link>> {
        let refuse = |why: &str| io::Error::new(io::ErrorKind::InvalidInput, why.to_owned());
        if !steps.is_empty() {
            return Err(refuse("a serving script plays no steps"));
        }
        if self.link.bound.swap(true, Ordering::SeqCst) {
            return Err(refuse("this serving script was already started on a stub"));
        }
        Ok(self.link.clone())
    }
}

impl PartialEq for Serving {
    fn eq(&self, other: &Serving) -> bool {
        Arc::ptr_eq(&self.link, &other.link)
    }
}

impl Eq for Serving {}

impl fmt::Debug for Serving {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Serving(..)")
    }
}

/// The stub's connections as a [`Pusher`] reaches them: each upgraded connection's commands, by
/// its number, entered before its record says it is upgraded.
#[derive(Default)]
pub(crate) struct Link {
    conns: Mutex<Vec<Option<mpsc::UnboundedSender<Cmd>>>>,
    bound: AtomicBool,
}

impl Link {
    /// Enters connection `conn`'s commands, once its upgrade completed.
    pub(crate) fn enter(&self, conn: usize, cmds: mpsc::UnboundedSender<Cmd>) {
        let mut conns = lock(&self.conns);
        if conns.len() <= conn {
            conns.resize(conn + 1, None);
        }
        conns[conn] = Some(cmds);
    }
}

/// Sends a serving script's connections unsolicited frames (an order update, a fill) and closes
/// them, each on demand, in the order asked among the answers its responder sends. It can be
/// cloned and sent between threads; a responder may hold one.
#[derive(Clone)]
pub struct Pusher {
    link: Arc<Link>,
}

impl Pusher {
    /// Sends `frame` on connection `conn`, once the frames asked before it on that connection
    /// were written. Refused on a connection closed or never upgraded.
    pub async fn push(&self, conn: usize, frame: Frame) -> Result<(), PushError> {
        self.ask(conn, |sent| Cmd::Push(frame, sent)).await
    }

    /// Sends connection `conn` a close frame, as [`Step::Close`] does; the stub keeps recording
    /// until the client's close reply. Refused on a connection closed or never upgraded.
    pub async fn close(&self, conn: usize) -> Result<(), PushError> {
        self.ask(conn, |sent| Cmd::Close(None, sent)).await
    }

    /// Sends connection `conn` a close frame carrying `code` and `reason`, as
    /// [`Step::CloseWith`] does; otherwise as [`Pusher::close`].
    pub async fn close_with(
        &self,
        conn: usize,
        code: u16,
        reason: impl Into<String>,
    ) -> Result<(), PushError> {
        let frame = CloseFrame {
            code: code.into(),
            reason: reason.into().into(),
        };
        self.ask(conn, |sent| Cmd::Close(Some(frame), sent)).await
    }

    /// Sends connection `conn` a WebSocket ping, once the frames asked before it on that
    /// connection (answers and pushes) were written, and resolves true when the client's pong
    /// to it arrives: an event, never a time, that says the client has read every one of those
    /// frames, as [`Step::Barrier`] does in a step script. False on a connection closed or never
    /// upgraded, when the connection ends before the pong, and when its ping cannot be written
    /// (the stub closed the connection), once every barrier asked before it has resolved.
    ///
    /// A barrier orders only the frames asked before it, so it is asked from the task that
    /// pushed the last frame it orders, after that push, never from a task spawned beside it.
    /// Several may wait on one connection at once: each resolves in the order asked, and a pong
    /// to a later ping resolves every barrier asked before it too, since the client read past
    /// their pings (a client may answer only the last of several pings, RFC 6455 section 5.5.3).
    pub async fn barrier(&self, conn: usize) -> bool {
        self.ask(conn, Cmd::Ping).await.is_ok()
    }

    async fn ask(
        &self,
        conn: usize,
        cmd: impl FnOnce(tokio::sync::oneshot::Sender<bool>) -> Cmd,
    ) -> Result<(), PushError> {
        let cmds = lock(&self.link.conns).get(conn).cloned().flatten();
        let cmds = cmds.ok_or(PushError::NoConnection { conn })?;
        match done(&cmds, cmd).await {
            true => Ok(()),
            false => Err(PushError::Closed { conn }),
        }
    }
}

impl fmt::Debug for Pusher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Pusher(..)")
    }
}

/// Why a [`Pusher`] refused a push or a close; nothing was sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushError {
    /// The stub playing the pusher's script has upgraded no connection numbered `conn`: none
    /// was accepted under that number, its upgrade failed, or the script runs on no stub.
    NoConnection { conn: usize },
    /// Connection `conn` closed, or the write did not go.
    Closed { conn: usize },
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::NoConnection { conn } => {
                write!(f, "the stub upgraded no connection {conn}")
            }
            PushError::Closed { conn } => write!(f, "connection {conn} is closed"),
        }
    }
}

impl std::error::Error for PushError {}

/// Serves every connection `accepted` yields, numbered in accept order as the stub's records
/// are, each in a task of its own so that connections are answered at once, until the stub's
/// listener stops.
pub(crate) async fn serve(
    serving: Serving,
    mut accepted: mpsc::UnboundedReceiver<Ready>,
    report: Report,
    records: Records,
) {
    let mut conn = 0;
    while let Some(ready) = accepted.recv().await {
        let (make, report) = (serving.make.clone(), report.clone());
        tokio::spawn(answer(conn, ready, make, report, records.clone()));
        conn += 1;
    }
}

/// The stub's records of its connections as a serving script's answers read them, word of each
/// change to them, which ends once the stub's connections and listener have stopped, and the
/// stub's stop, which its drop closes.
#[derive(Clone)]
pub(crate) struct Records {
    pub(crate) conns: Shared<Vec<ConnRecord>>,
    pub(crate) changed: watch::Receiver<()>,
    pub(crate) stop: watch::Receiver<()>,
}

impl Records {
    /// Waits, before a connection's first answer is computed, for the connection that `with`'s
    /// hold names for `frame`, its first data frame, if any, to show its client's close frame
    /// or its end in the record. `Some(Err)` refuses the answer; `None` says the stub stopped
    /// first.
    async fn hold(&mut self, with: &Responder, frame: &Frame) -> Option<Result<(), String>> {
        let named = catch_unwind(AssertUnwindSafe(|| with.held_for(frame)));
        let named = named.map_err(|panic| format!("its hold panicked: {}", panic_text(&*panic)));
        let other = match named {
            Ok(None) => return Some(Ok(())),
            Ok(Some(other)) => other,
            Err(reason) => return Some(Err(reason)),
        };
        if lock(&self.conns).len() <= other {
            let reason = format!("its first answer waits for connection {other}, not accepted");
            return Some(Err(reason));
        }
        let conns = &self.conns;
        let ended = |c: &ConnRecord| c.client_closed || !c.open;
        // It ends when the record shows it, or when the stub's tasks have all stopped.
        drop(self.changed.wait_for(|()| ended(&lock(conns)[other])).await);
        // The stub's drop, which closes its stop before any connection sees it, ends every
        // connection; an end it caused releases nothing.
        let stopped = self.stop.has_changed().is_err();
        (!stopped).then_some(Ok(()))
    }
}

/// Answers each data frame of connection `conn`, in the order it arrived, with what `make(conn)`
/// computes from it, until the connection ends. A refusal, or a panic in the responder, fails
/// that answer alone and is reported; a panic in `make(conn)` fails every answer of that
/// connection, each reported, while the connection is still read and recorded. A write that
/// does not go (the connection closing, or the client gone) fails nothing and the frame still
/// counts as answered; the connection's end then ends its answers.
///
/// A responder that holds its first answer ([`Responder::hold_first`]) has it computed only once
/// the record shows what the hold waits for; when the stub stops first, nothing more is answered.
async fn answer(
    conn: usize,
    ready: Ready,
    make: Arc<MakeFn>,
    report: Report,
    mut records: Records,
) {
    // A connection whose upgrade failed sent no frame to answer.
    let Some(Conn { mut frames, cmds }) = ready.await.ok().flatten() else {
        return;
    };
    let made = catch_unwind(AssertUnwindSafe(|| make(conn)));
    let made = made.map_err(|panic| {
        let text = panic_text(&*panic);
        format!("making its responder panicked: {text}")
    });
    let mut frame = 0;
    while let Some(received) = frames.recv().await {
        let with = made.as_ref().map_err(String::clone);
        let held = match (&with, frame) {
            (Ok(with), 0) => records.hold(with, &received).await,
            _ => Some(Ok(())),
        };
        let Some(held) = held else { return };
        match held.and(with).and_then(|with| respond(with, &received)) {
            Ok(out) => {
                for sent in out {
                    let _ = done(&cmds, |ok| Cmd::Push(sent, ok)).await;
                }
            }
            Err(reason) => report.fail(ScriptError::Answer {
                conn,
                frame,
                reason,
            }),
        }
        frame += 1;
        report.played.send_modify(|n| *n += 1);
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use tokio::time::Instant;

    use super::*;

    fn poll_once<F: Future>(f: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(Waker::noop()))
    }

    /// `serve_ws` records a client's close frame and the connection's end in one turn unless
    /// its close reply cannot be written at once, so this shows each apart on a record built by
    /// hand.
    #[tokio::test]
    async fn a_hold_is_released_by_the_clients_close_frame_alone_and_by_the_end_alone() {
        let with = Responder::new(|_| Ok(Vec::new())).hold_first(|_| Some(0));
        let shows: [fn(&mut ConnRecord); 2] = [|c| c.client_closed = true, |c| c.open = false];
        for show in shows {
            let conns: Shared<Vec<ConnRecord>> = Shared::default();
            lock(&conns).push(ConnRecord {
                accepted_at: Instant::now(),
                upgraded: true,
                received: Vec::new(),
                open: true,
                client_closed: false,
            });
            let (changes, changed) = watch::channel(());
            let (_held, stop) = watch::channel(());
            let mut records = Records {
                conns: conns.clone(),
                changed,
                stop,
            };
            let frame = Frame::text("auth");
            let mut hold = pin!(records.hold(&with, &frame));
            assert!(poll_once(hold.as_mut()).is_pending());
            // A change that shows neither releases nothing.
            lock(&conns)[0].received.push(Frame::text("page"));
            changes.send_replace(());
            assert!(poll_once(hold.as_mut()).is_pending());
            show(&mut lock(&conns)[0]);
            changes.send_replace(());
            assert_eq!(poll_once(hold.as_mut()), Poll::Ready(Some(Ok(()))));
        }
    }
}
