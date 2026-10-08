//! What the checks that read fixture frames share (FBC-whw): the case files they read, each
//! decoded by an order-entry codec built fresh from the factory, and a runner for the checks
//! that hold every event of a kind in a case to what the case's name states.
//!
//! # A case file
//!
//! `<fixtures>/<check>/<case>.frames` is one case: what one fresh codec is handed, in order,
//! one line each. A line is
//!
//! - `text <frame>`: a text frame, everything after the first space up to the end of the line;
//! - `hex <bytes>`: a binary frame, its bytes in hexadecimal (spaces between them allowed);
//! - `resync`: the codec's [`resync`](fbc_core::ExecCodec::resync) asked for at the suite's
//!   fixed encode time ([`ENCODE_WALL`]), so a venue that answers a resync in frames can be
//!   handed its answer after it;
//!
//! and a blank line or one starting with `#` is a comment. Frames are handed to
//! [`on_frame`](fbc_core::ExecCodec::on_frame) on [`Setup::exec_stream`](super::Setup), in the
//! decode scope the core lends for the venue's caps; the codec is not opened first. A venue
//! whose fills or positions arrive only in HTTP responses is not modelled here.
//!
//! What a check reports of a frame is its file and line, never its bytes: a frame can carry a
//! credential.

use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use fbc_core::{CtxCall, DecodeError, Effects, ExecEvent, ExecSink, RawFrame, VenueMeta, WallNs};

use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

/// A case file's extension.
pub(crate) const EXT: &str = "frames";

/// The wall time a `resync` line asks the codec's resync at: the suite's fixed encode time.
pub const ENCODE_WALL: WallNs = super::harness::WALL;

/// One line of a case that hands the codec something.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Line {
    Text(String),
    Binary(Vec<u8>),
    Resync,
}

/// A case: its lines that hand the codec something, each with its line number.
pub(crate) type Case = Vec<(usize, Line)>;

/// What handing the codec one line gave: the call's result (a resync's is always `Ok`), the
/// events it pushed and the effects it asked for.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Step {
    pub line: usize,
    /// The codec call the line made: `ExecCodec::on_frame` or `ExecCodec::resync`.
    pub call: &'static str,
    pub result: Result<(), DecodeError>,
    pub events: Vec<(VenueMeta, ExecEvent)>,
    pub fx: Effects,
}

/// The case `text` holds, or why it is not one.
pub(crate) fn parse(text: &str) -> Result<Case, String> {
    let mut case = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let n = i + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let line = if trimmed == "resync" {
            Line::Resync
        } else if let Some(frame) = raw.strip_prefix("text ") {
            Line::Text(frame.to_owned())
        } else if let Some(bytes) = raw.strip_prefix("hex ") {
            Line::Binary(unhex(bytes).ok_or_else(|| format!("line {n}: not hexadecimal bytes"))?)
        } else {
            return Err(format!(
                "line {n}: starts with neither `text `, `hex ` nor `resync`"
            ));
        };
        case.push((n, line));
    }
    Ok(case)
}

/// The bytes `hex` spells, spaces between them allowed; `None` when it spells none, has an odd
/// number of digits or anything but hexadecimal digits and spaces.
pub(crate) fn unhex(hex: &str) -> Option<Vec<u8>> {
    let digits: Vec<u8> = hex.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return None;
    }
    let digit = |b: u8| char::from(b).to_digit(16);
    digits
        .chunks(2)
        .map(|pair| u8::try_from(digit(pair[0])? * 16 + digit(pair[1])?).ok())
        .collect()
}

/// The case in `file`, or why it cannot be read.
pub(crate) fn read(file: &Path) -> Result<Case, String> {
    let text = fs::read_to_string(file).map_err(|e| format!("cannot read: {e}"))?;
    parse(&text)
}

/// The stems of the case files in `dir`; `None` when `dir` does not exist.
pub(crate) fn cases_in(dir: &Path) -> Result<Option<BTreeSet<String>>, String> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {}: {e}", dir.display())),
    };
    let mut stems = BTreeSet::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == EXT) {
            let stem = path.file_stem().unwrap_or_default();
            stems.insert(stem.to_string_lossy().into_owned());
        }
    }
    Ok(Some(stems))
}

/// The events pushed by one call.
struct Collect(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// `case` handed, line by line, to an order-entry codec built fresh from the factory.
pub(crate) fn decode(h: &Harness<'_>, case: &Case) -> Result<Vec<Step>, Failure> {
    let mut codec = h.exec_codec()?;
    let (specs, stream) = (h.specs(), h.exec_stream);
    Ok(h.decode_scope(|scope| {
        let mut steps = Vec::with_capacity(case.len());
        for (line, input) in case {
            let mut sink = Collect(Vec::new());
            let mut fx = Effects::new();
            let call = match input {
                Line::Resync => "ExecCodec::resync",
                Line::Text(_) | Line::Binary(_) => "ExecCodec::on_frame",
            };
            let result = match input {
                Line::Text(text) => {
                    let frame = RawFrame::Text(text);
                    codec.on_frame(stream, frame, scope, specs, &mut sink, &mut fx)
                }
                Line::Binary(bytes) => {
                    let frame = RawFrame::Binary(bytes);
                    codec.on_frame(stream, frame, scope, specs, &mut sink, &mut fx)
                }
                Line::Resync => {
                    let ctx = h.ctx(codec.nonces_for(CtxCall::Resync));
                    codec.resync(&ctx, &mut fx);
                    Ok(())
                }
            };
            steps.push(Step {
                line: *line,
                call,
                result,
                events: sink.0,
                fx,
            });
        }
        steps
    }))
}

/// A case a check reads and what every event of its kind in it must be.
pub(crate) struct Expect {
    /// The case file's stem.
    pub stem: &'static str,
    /// Judges an event: `None` when it is not of the kind the case states, else whether it is
    /// what the case states, and if not what it is.
    pub judge: fn(&ExecEvent) -> Option<Result<(), String>>,
}

/// What a check that reads cases holds them to.
pub(crate) struct Cases<'a> {
    /// The check, whose subdirectory holds the cases.
    pub check: &'static str,
    /// The capability a judged event breaks, as the caps or the types spell it.
    pub capability: &'a str,
    /// The kind of event judged, in the singular (`fill`).
    pub noun: &'static str,
    /// The cases, every one required.
    pub expect: &'a [Expect],
}

impl Cases<'_> {
    /// Reads and decodes every case, holding each event of the kind to what its case states:
    /// a case missing or unreadable, a frame the codec refuses, a case with no event of the
    /// kind, and a case file no expectation names are breaches too.
    pub(crate) fn run(&self, h: &Harness<'_>, subject: &Subject<'_>) -> Result<Verdict, Failure> {
        let check = self.check;
        let dir = subject.fixtures().join(check);
        let on_disk = match cases_in(&dir) {
            Ok(Some(stems)) => stems,
            Ok(None) => {
                let fixtures = subject.fixtures().display();
                let what = format!("missing: {fixtures} holds no {check}/ directory");
                return Err(h.fail(check, what));
            }
            Err(what) => return Err(h.fail(check, what)),
        };
        let mut breaches: Vec<Breach> = Vec::new();
        let mut probed = Vec::new();
        for expect in self.expect {
            let file = format!("{check}/{}.{EXT}", expect.stem);
            let case = match read(&subject.fixtures().join(&file)) {
                Ok(case) => case,
                Err(what) => {
                    breaches.push(Breach::new(&file, what));
                    continue;
                }
            };
            let mut judged = 0usize;
            for step in decode(h, &case)? {
                if let Err(e) = step.result {
                    breaches.push(Breach::new(
                        &file,
                        format!("line {} refused: {e}", step.line),
                    ));
                }
                for (_, event) in &step.events {
                    let Some(verdict) = (expect.judge)(event) else {
                        continue;
                    };
                    judged += 1;
                    if let Err(what) = verdict {
                        let what = format!("{file} line {}: {what}", step.line);
                        breaches.push(Breach::new(self.capability, what));
                    }
                }
            }
            match judged {
                0 => breaches.push(Breach::new(
                    &file,
                    format!(
                        "decodes to no {}: a case that states none proves nothing",
                        self.noun
                    ),
                )),
                1 => probed.push(format!("{file}: 1 {}", self.noun)),
                n => probed.push(format!("{file}: {n} {}s", self.noun)),
            }
        }
        let named: BTreeSet<&str> = self.expect.iter().map(|e| e.stem).collect();
        for stem in on_disk.iter().filter(|s| !named.contains(s.as_str())) {
            breaches.push(Breach::new(
                &format!("{check}/{stem}.{EXT}"),
                format!("{check} reads no such case, so it is never checked"),
            ));
        }
        if breaches.is_empty() {
            Ok(Verdict::Passed { check, probed })
        } else {
            Err(Failure { check, breaches })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_case_reads_text_hex_and_resync_lines_and_skips_comments() {
        let case = parse("# a comment\n\ntext a|b=1\nresync\n  # indented\nhex 0a ff\n").unwrap();
        assert_eq!(
            case,
            [
                (3, Line::Text("a|b=1".to_owned())),
                (4, Line::Resync),
                (6, Line::Binary(vec![0x0a, 0xff])),
            ]
        );
    }

    #[test]
    fn a_text_frame_keeps_its_spaces() {
        let case = parse("text  {\"a\": 1} ").unwrap();
        assert_eq!(case, [(1, Line::Text(" {\"a\": 1} ".to_owned()))]);
    }

    #[test]
    fn a_line_of_no_kind_or_bad_hex_is_refused_by_number() {
        assert_eq!(
            parse("text a\nframe b").unwrap_err(),
            "line 2: starts with neither `text `, `hex ` nor `resync`"
        );
        for bad in ["hex ", "hex abc", "hex zz", "hex 0x"] {
            assert_eq!(
                parse(bad).unwrap_err(),
                "line 1: not hexadecimal bytes",
                "{bad}"
            );
        }
    }

    #[test]
    fn hex_reads_upper_and_lower_case() {
        assert_eq!(unhex("DEad beEF"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }
}
