//! Serving scripts (decision 0150): the second kind of [`WsScript`](crate::WsScript), beside
//! 0025's timeline of steps, for a venue model whose frame count and order no timeline can know.
//! The stub accepts every connection and answers each data frame of connection `n` with the
//! frames the [`Responder`] made for `n` computes from it, several connections at once, until
//! the stub is dropped; a [`Pusher`] sends a connection unsolicited frames and closes it.

use std::fmt;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

use crate::script::{Frame, Responder, Step};
use crate::server::{Cmd, Conn, Ready, Report, ScriptError, done, lock, panic_text, respond};

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
) {
    let mut conn = 0;
    while let Some(ready) = accepted.recv().await {
        tokio::spawn(answer(conn, ready, serving.make.clone(), report.clone()));
        conn += 1;
    }
}

/// Answers each data frame of connection `conn`, in the order it arrived, with what `make(conn)`
/// computes from it, until the connection ends. A refusal, or a panic in the responder, fails
/// that answer alone and is reported; a panic in `make(conn)` fails every answer of that
/// connection, each reported, while the connection is still read and recorded. A write that
/// does not go (the connection closing, or the client gone) fails nothing and the frame still
/// counts as answered; the connection's end then ends its answers.
async fn answer(conn: usize, ready: Ready, make: Arc<MakeFn>, report: Report) {
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
        match with.and_then(|with| respond(with, &received)) {
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
