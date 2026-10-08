//! What the market-data checks share (FBC-vmw): the book channels they drive, the case files
//! of a channel's frames they read, each decoded by a market-data codec built fresh from the
//! factory, and a runner for the checks that judge one case per channel.
//!
//! # The book channels driven
//!
//! Each book channel the caps declare ([`MdCaps::books`](fbc_core::MdCaps), by
//! [`BookId`] its index) is driven on its own: one codec, standing for one connection,
//! subscribed to that channel on as many instruments of the [`Setup`](super::Setup)'s spec
//! table, in order, as one connection carries under the venue's
//! [`ConnTopology`](fbc_core::ConnTopology): the first alone for `PerInstrument`, the first
//! `max_subscriptions` for a capped shared connection, every one otherwise. A case's frames
//! name those instruments only. A channel whose
//! [`BookCaps::rest_anchor`] is true is skipped by name, saying why: its snapshot comes in an
//! HTTP answer, which these case files do not carry (FBC-fhk4).
//!
//! # A market-data case file
//!
//! `<fixtures>/<check>/<channel>.frames` is one channel's case, `<channel>` being its name as
//! [`BookCaps::channel`] spells it: what one fresh codec is handed, in order, after it is
//! opened ([`on_open`](fbc_core::MdCodec::on_open)) and subscribed to the channel
//! ([`subscribe`](fbc_core::MdCodec::subscribe), one call). A line is a frame, as in an
//! order-entry case ([`frames`](super::frames)), after the tags that state what the fixture
//! knows of it:
//!
//! ```text
//! [<tag> ...] text <frame>
//! [<tag> ...] hex <bytes>
//! ```
//!
//! - `gap=<symbol>`: the frame breaks the sequence of the instrument whose venue symbol is
//!   `<symbol>` on its book ([`continuity`](super::continuity));
//! - `ts`: the frame carries the venue's timestamp
//!   ([`no_exch_ts_synthesized`](super::no_exch_ts_synthesized));
//! - `public`, `rpi`: the order channels whose liquidity the frame shows
//!   ([`book_channels`](super::book_channels)).
//!
//! A blank line or one starting with `#` is a comment. Frames are handed to
//! [`on_frame`](fbc_core::MdCodec::on_frame) in the decode scope the core lends a market-data
//! session ([`dispatch_market_data`](fbc_core::dispatch_market_data)); a frame the codec
//! refuses is a breach, and so is a case missing, unreadable or malformed, and a case file no
//! driven channel reads. What a check reports of a frame is its file and line, never its bytes.

use std::collections::BTreeSet;
use std::path::Path;

use fbc_core::{
    BookCaps, BookId, Channel, DecodeError, Effects, MdEvent, MdSink, RawFrame, SpecTable,
    VenueMeta,
};

use super::frames::{self, EXT, unhex};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

/// Why a channel anchored on REST is skipped.
pub(crate) const ANCHORED: &str = "BookCaps.rest_anchor is true: its snapshot comes in an HTTP \
                                   answer, which the suite does not carry yet (FBC-fhk4)";

/// What the fixture states of one frame.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub(crate) struct Tags {
    /// It breaks the sequence of the instrument this venue symbol names.
    pub gap: Option<String>,
    /// It carries the venue's timestamp.
    pub ts: bool,
    /// The order channels whose liquidity it shows.
    pub shows: BTreeSet<Channel>,
}

impl Tags {
    /// Adds the tag `word`; `false` when it is none.
    fn add(&mut self, word: &str) -> bool {
        if let Some(symbol) = word.strip_prefix("gap=").filter(|s| !s.is_empty()) {
            self.gap = Some(symbol.to_owned());
            return true;
        }
        match word {
            "ts" => self.ts = true,
            "public" => _ = self.shows.insert(Channel::Public),
            "rpi" => _ = self.shows.insert(Channel::Rpi),
            _ => return false,
        }
        true
    }
}

/// One frame of a case.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

/// One line of a case: its number, what the fixture states of its frame, and the frame.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Line {
    pub line: usize,
    pub tags: Tags,
    pub frame: Frame,
}

/// The case `text` holds, or why it is not one.
pub(crate) fn parse(text: &str) -> Result<Vec<Line>, String> {
    let mut case = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let n = i + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let mut tags = Tags::default();
        let mut rest = raw.trim_start();
        let frame = loop {
            if let Some(frame) = rest.strip_prefix("text ") {
                break Frame::Text(frame.to_owned());
            }
            if let Some(bytes) = rest.strip_prefix("hex ") {
                let bytes =
                    unhex(bytes).ok_or_else(|| format!("line {n}: not hexadecimal bytes"))?;
                break Frame::Binary(bytes);
            }
            let Some((word, after)) = rest.split_once(' ') else {
                return Err(format!("line {n}: holds no `text ` or `hex ` frame"));
            };
            if !tags.add(word) {
                return Err(format!(
                    "line {n}: `{word}` is neither a frame nor a tag (gap=<symbol>, ts, public, rpi)"
                ));
            }
            rest = after.trim_start();
        };
        case.push(Line {
            line: n,
            tags,
            frame,
        });
    }
    Ok(case)
}

/// The case in `file`, or why it cannot be read.
pub(crate) fn read(file: &Path) -> Result<Vec<Line>, String> {
    let text = std::fs::read_to_string(file).map_err(|e| format!("cannot read: {e}"))?;
    parse(&text)
}

/// One book channel the caps declare.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Book {
    pub id: BookId,
    pub caps: BookCaps,
}

/// The book channels the caps declare, split into those a check drives and, for the rest, a
/// line naming each and why it is skipped: `skip` says why a channel the suite could drive is
/// skipped by this check, if it is; one anchored on REST is skipped by every check.
pub(crate) fn books(
    h: &Harness<'_>,
    skip: impl Fn(&BookCaps) -> Option<String>,
) -> (Vec<Book>, Vec<String>) {
    let mut driven = Vec::new();
    let mut skipped = Vec::new();
    for (i, caps) in h.caps.md.books.iter().enumerate() {
        let why = match caps.rest_anchor {
            true => Some(ANCHORED.to_owned()),
            false => skip(caps),
        };
        match why {
            Some(why) => skipped.push(format!("{}: skipped: {why}", caps.channel)),
            None => driven.push(Book {
                id: BookId(u8::try_from(i).expect("at most 256 book channels")),
                caps: *caps,
            }),
        }
    }
    (driven, skipped)
}

/// What handing the codec one line gave: the call's result and the events it pushed.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Step {
    pub line: usize,
    pub tags: Tags,
    pub result: Result<(), DecodeError>,
    pub events: Vec<(VenueMeta, MdEvent)>,
}

/// The events pushed by one call.
struct Collect(Vec<(VenueMeta, MdEvent)>);

impl MdSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

/// `case` handed, line by line, to a market-data codec built fresh from the factory, opened and
/// subscribed to `book` on every instrument; a subscription the codec refuses fails the check.
pub(crate) fn decode(h: &Harness<'_>, book: &Book, case: &[Line]) -> Result<Vec<Step>, Failure> {
    let subs = h.book_subs(book.id);
    let mut codec = h.md_codec(subs.clone());
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    if let Err(e) = codec.subscribe(&subs, &[], h.specs(), &mut fx) {
        let what = format!("refused {} on every instrument: {e}", book.caps.channel);
        return Err(h.fail("MdCodec::subscribe", what));
    }
    let specs = h.specs();
    Ok(h.md_scope(|scope| {
        let mut steps = Vec::with_capacity(case.len());
        for line in case {
            let mut sink = Collect(Vec::new());
            let frame = match &line.frame {
                Frame::Text(text) => RawFrame::Text(text),
                Frame::Binary(bytes) => RawFrame::Binary(bytes),
            };
            let result = codec.on_frame(frame, scope, specs, &mut sink, &mut fx);
            steps.push(Step {
                line: line.line,
                tags: line.tags.clone(),
                result,
                events: sink.0,
            });
        }
        steps
    }))
}

/// Judges one channel's decoded case, read from the file named, under the setup's spec table:
/// pushes what breaks the check and gives what it probed.
pub(crate) type Judge = dyn Fn(&Book, &str, &[Step], &SpecTable, &mut Vec<Breach>) -> String;

/// A check that reads one case per driven book channel from `<fixtures>/<dir>/`.
pub(crate) struct PerBook<'a> {
    /// The check.
    pub check: &'static str,
    /// The directory, relative to the fixtures, its cases are in.
    pub dir: &'a str,
    /// Judges one channel's decoded case, read from `file`: pushes what breaks the check and
    /// gives what it probed.
    pub judge: &'a Judge,
}

impl PerBook<'_> {
    /// Reads and decodes each driven channel's case and judges it: a directory or case missing
    /// or unreadable, a frame the codec refuses and a case file no driven channel reads are
    /// breaches too. The skipped channels are listed among what was probed.
    pub(crate) fn run(
        &self,
        h: &Harness<'_>,
        subject: &Subject<'_>,
        books: &[Book],
        skipped: Vec<String>,
    ) -> Result<(Vec<String>, Vec<Breach>), Failure> {
        let dir = self.dir;
        let on_disk = match frames::cases_in(&subject.fixtures().join(dir)) {
            Ok(Some(stems)) => stems,
            Ok(None) => {
                let fixtures = subject.fixtures().display();
                let what = format!("missing: {fixtures} holds no {dir}/ directory");
                return Err(h.fail(self.check, what));
            }
            Err(what) => return Err(h.fail(self.check, what)),
        };
        let mut breaches = Vec::new();
        let mut probed = skipped;
        for book in books {
            let file = format!("{dir}/{}.{EXT}", book.caps.channel);
            let case = match read(&subject.fixtures().join(&file)) {
                Ok(case) => case,
                Err(what) => {
                    breaches.push(Breach::new(&file, what));
                    continue;
                }
            };
            let steps = decode(h, book, &case)?;
            for step in &steps {
                if let Err(e) = &step.result {
                    let what = format!("line {} refused: {e}", step.line);
                    breaches.push(Breach::new(&file, what));
                }
            }
            probed.push((self.judge)(book, &file, &steps, h.specs(), &mut breaches));
        }
        let driven: BTreeSet<&str> = books.iter().map(|b| b.caps.channel).collect();
        for stem in on_disk.iter().filter(|s| !driven.contains(s.as_str())) {
            breaches.push(Breach::new(
                &format!("{dir}/{stem}.{EXT}"),
                format!(
                    "{} drives no book channel of that name, so it is never checked",
                    self.check
                ),
            ));
        }
        Ok((probed, breaches))
    }
}

/// What a check that drove `driven` channels gave: skipped with `why` when it drove none,
/// passed when nothing broke it, failed otherwise.
pub(crate) fn verdict(
    check: &'static str,
    driven: &[Book],
    why: &'static str,
    outcome: Result<(Vec<String>, Vec<Breach>), Failure>,
) -> Result<Verdict, Failure> {
    if driven.is_empty() {
        return Ok(Verdict::Skipped { check, why });
    }
    let (probed, breaches) = outcome?;
    if breaches.is_empty() {
        Ok(Verdict::Passed { check, probed })
    } else {
        Err(Failure { check, breaches })
    }
}

/// Why a check that drives every channel not anchored on REST is skipped when it drives none.
pub(crate) const NO_BOOK: &str = "the caps declare no book channel the suite drives: none, or \
                                  each anchored on REST (BookCaps.rest_anchor), whose snapshot \
                                  comes in an HTTP answer the suite does not carry yet (FBC-fhk4)";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_case_reads_tags_before_text_and_hex_frames_and_skips_comments() {
        let case =
            parse("# c\n\ntext a|b=1\ngap=X ts text  c d\n public  rpi hex 0a ff\n").unwrap();
        let rpi = Tags {
            shows: [Channel::Public, Channel::Rpi].into(),
            ..Tags::default()
        };
        assert_eq!(
            case,
            [
                Line {
                    line: 3,
                    tags: Tags::default(),
                    frame: Frame::Text("a|b=1".to_owned())
                },
                Line {
                    line: 4,
                    tags: Tags {
                        gap: Some("X".to_owned()),
                        ts: true,
                        ..Tags::default()
                    },
                    frame: Frame::Text(" c d".to_owned())
                },
                Line {
                    line: 5,
                    tags: rpi,
                    frame: Frame::Binary(vec![0x0a, 0xff])
                },
            ]
        );
    }

    #[test]
    fn a_line_of_no_frame_an_unknown_tag_or_bad_hex_is_refused_by_number() {
        assert_eq!(
            parse("text a\ngap=X").unwrap_err(),
            "line 2: holds no `text ` or `hex ` frame"
        );
        for bad in ["late", "gap", "gap="] {
            assert_eq!(
                parse(&format!("{bad} text a")).unwrap_err(),
                format!(
                    "line 1: `{bad}` is neither a frame nor a tag (gap=<symbol>, ts, public, rpi)"
                )
            );
        }
        assert_eq!(
            parse("gap=X hex zz").unwrap_err(),
            "line 1: not hexadecimal bytes"
        );
    }
}
