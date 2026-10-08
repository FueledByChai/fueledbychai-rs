//! Everything the stack logs, captured at TRACE. This workspace emits no diagnostics of its own
//! (it has no `tracing` or `log` call); what runs beneath it reports through the `log` facade
//! (tungstenite and tokio-tungstenite, whose TRACE records would show every frame written,
//! decision 0079). One logger for the test binary keeps every record with the thread that made
//! it, so a test reads its own session's records: each test runs its session on its own thread,
//! and its stub on another ([`super::venue::Stub`]).

use std::sync::{Mutex, Once};
use std::thread::ThreadId;

use log::{Level, LevelFilter, Log, Metadata, Record};

/// One record: the thread that logged it, its level, target and text.
#[derive(Clone, Debug)]
pub struct Line {
    pub thread: ThreadId,
    pub level: Level,
    pub target: String,
    pub text: String,
}

static LINES: Mutex<Vec<Line>> = Mutex::new(Vec::new());

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        let line = Line {
            thread: std::thread::current().id(),
            level: record.level(),
            target: record.target().to_owned(),
            text: record.args().to_string(),
        };
        LINES.lock().unwrap_or_else(|e| e.into_inner()).push(line);
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture;

/// Installs the capture once for the test binary, at TRACE: every level any crate asks for.
pub fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        log::set_logger(&CAPTURE).expect("no other logger in this test binary");
        log::set_max_level(LevelFilter::Trace);
    });
    assert_eq!(log::max_level(), LevelFilter::Trace);
}

/// Every record the calling thread logged so far.
pub fn of_this_thread() -> Vec<Line> {
    let me = std::thread::current().id();
    let lines = LINES.lock().unwrap_or_else(|e| e.into_inner());
    lines.iter().filter(|l| l.thread == me).cloned().collect()
}
