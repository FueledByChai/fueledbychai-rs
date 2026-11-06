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
//! name those instruments only: an event on another instrument, or a `gap=` tag naming one, is
//! a breach (Codex r4217991939). So is a trading status ([`MdEvent::Status`]) from a venue whose
//! [`MdCaps::status`](fbc_core::MdCaps) declares it reports none, `FeedSource::None`
//! (decision 0130). A channel whose [`BookCaps::rest_anchor`] is true is driven like any other
//! (FBC-fhk4): its snapshot comes in an HTTP answer to a request the codec asks for, which its
//! case hands the codec in an `http` line.
//!
//! # A market-data case file
//!
//! `<fixtures>/<check>/<channel>.frames` is one channel's case, `<channel>` being its name as
//! [`BookCaps::channel`] spells it: what one fresh codec is handed, in order, after it is
//! opened ([`on_open`](fbc_core::MdCodec::on_open)) and subscribed to the channel
//! ([`subscribe`](fbc_core::MdCodec::subscribe), one call). A line is a frame, as in an
//! order-entry case ([`super::frames`]), or an HTTP answer, after the tags that state what the
//! fixture knows of it:
//!
//! ```text
//! [<tag> ...] text <frame>
//! [<tag> ...] hex <bytes>
//! [<tag> ...] http [tag=<n>] <status> text <body>
//! [<tag> ...] http [tag=<n>] <status> hex <bytes>
//! ```
//!
//! - `gap=<symbol>`: the line breaks the sequence of the instrument whose venue symbol is
//!   `<symbol>` on its book ([`continuity`](super::continuity)); a frame batching several
//!   instruments may carry one such tag for each whose sequence it breaks;
//! - `ts`: the line carries the venue's timestamp
//!   ([`no_exch_ts_synthesized`](super::no_exch_ts_synthesized));
//! - `public`, `rpi`: the order channels whose liquidity the line shows
//!   ([`book_channels`](super::book_channels)).
//!
//! An `http` line (FBC-fhk4) is the response, with status `<status>` (100 to 599), no header and
//! the body everything after `text ` up to the end of the line or the bytes `hex ` spells, to an
//! HTTP request ([`Effect::Http`]) the codec asked for, opening, subscribing or on an earlier
//! line, that no `http` line has answered yet: the one whose tag ([`HttpTag`]) is `<n>`, or,
//! without `tag=`, the earliest. It is handed to [`on_http`](fbc_core::MdCodec::on_http) under
//! that request's tag, as the runtime hands a codec the answer to the request it asked for
//! (decision 0027): a channel anchored on REST is given its snapshot this way. An `http` line
//! with no such request to answer is a breach naming the case and line, and the rest of that
//! case is not judged: the case is wrong, not the codec. A request's failure
//! ([`HttpFailure`](fbc_core::HttpFailure)), its headers and the timers the codec asks for (a
//! retry) are not modelled.
//!
//! The suite cannot tell a request the codec has given up on from one it still waits on: a
//! request stays outstanding until a line answers it, and an answer is never matched against
//! the request's URL. Once a codec asks again in place of an earlier request (the toy does,
//! holding more deltas than [`MAX_HELD`](crate::toy::MAX_HELD) while it waits for an anchor),
//! a line answering the newer request must name it with `tag=`: without one it answers the
//! earliest, the request given up on.
//!
//! A blank line or one starting with `#` is a comment. Frames are handed to
//! [`on_frame`](fbc_core::MdCodec::on_frame), and answers to `on_http`, in the decode scope the
//! core lends a market-data session ([`dispatch_market_data`](fbc_core::dispatch_market_data));
//! a line the codec refuses is a breach, and so is a case missing, unreadable or malformed, and
//! a case file no driven channel reads. What a check reports of a line is its file and number,
//! never its bytes.

use std::collections::{BTreeSet, VecDeque};
use std::path::Path;

use fbc_core::{
    BookCaps, BookId, Channel, DecodeError, Effect, Effects, FeedSource, HttpResponse, HttpTag,
    InstrumentId, MdEvent, MdSink, RawFrame, SpecTable, VenueMeta,
};

use super::frames::{self, EXT, unhex};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

/// What the fixture states of one line.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub(crate) struct Tags {
    /// It breaks the sequences of the instruments these venue symbols name.
    pub gaps: BTreeSet<String>,
    /// It carries the venue's timestamp.
    pub ts: bool,
    /// The order channels whose liquidity it shows.
    pub shows: BTreeSet<Channel>,
}

impl Tags {
    /// Adds the tag `word`; `false` when it is none.
    fn add(&mut self, word: &str) -> bool {
        if let Some(symbol) = word.strip_prefix("gap=").filter(|s| !s.is_empty()) {
            self.gaps.insert(symbol.to_owned());
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

/// What one line of a case hands the codec.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Input {
    Text(String),
    Binary(Vec<u8>),
    /// The response to the request whose tag is `tag`, or, for `None`, to the earliest the
    /// codec asked for that no line answered.
    Http {
        tag: Option<HttpTag>,
        status: u16,
        body: Vec<u8>,
    },
}

/// What kind of input a line hands the codec.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Kind {
    /// A text frame.
    Text,
    /// A binary frame.
    Binary,
    /// The answer to an HTTP request: no frame.
    Http,
}

impl Input {
    fn kind(&self) -> Kind {
        match self {
            Input::Text(_) => Kind::Text,
            Input::Binary(_) => Kind::Binary,
            Input::Http { .. } => Kind::Http,
        }
    }
}

/// One line of a case: its number, what the fixture states of it, and what it hands the codec.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Line {
    pub line: usize,
    pub tags: Tags,
    pub input: Input,
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
        let input = loop {
            if let Some(frame) = rest.strip_prefix("text ") {
                break Input::Text(frame.to_owned());
            }
            if let Some(bytes) = rest.strip_prefix("hex ") {
                let bytes =
                    unhex(bytes).ok_or_else(|| format!("line {n}: not hexadecimal bytes"))?;
                break Input::Binary(bytes);
            }
            if let Some(answer) = rest.strip_prefix("http ") {
                break http(answer, n)?;
            }
            let Some((word, after)) = rest.split_once(' ') else {
                return Err(format!(
                    "line {n}: holds no `text ` or `hex ` frame and no `http ` answer"
                ));
            };
            if !tags.add(word) {
                let tags = "gap=<symbol>, ts, public, rpi";
                return Err(format!(
                    "line {n}: `{word}` is neither a frame nor a tag ({tags})"
                ));
            }
            rest = after.trim_start();
        };
        case.push(Line {
            line: n,
            tags,
            input,
        });
    }
    Ok(case)
}

/// The `http` line `n` whose text after `http ` is `answer`: `tag=<n>` naming the request it
/// answers, if it names one, then the response as an order-entry case spells it.
fn http(answer: &str, n: usize) -> Result<Input, String> {
    let (tag, response) = match answer.strip_prefix("tag=") {
        None => (None, answer),
        Some(rest) => {
            let (tag, response) = rest.split_once(' ').unwrap_or((rest, ""));
            let tag = tag.parse().map_err(|_| {
                format!("line {n}: `tag={tag}` names no request: a request's tag is a number")
            })?;
            (Some(HttpTag(tag)), response)
        }
    };
    let (status, body) = frames::response(response, n)?;
    Ok(Input::Http { tag, status, body })
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
/// line naming each and why it is skipped: `skip` says why a channel is skipped by this check,
/// if it is.
pub(crate) fn books(
    h: &Harness<'_>,
    skip: impl Fn(&BookCaps) -> Option<String>,
) -> (Vec<Book>, Vec<String>) {
    let mut driven = Vec::new();
    let mut skipped = Vec::new();
    for (i, caps) in h.caps.md.books.iter().enumerate() {
        match skip(caps) {
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
    /// What the line handed the codec.
    pub kind: Kind,
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

/// The HTTP requests `fx` asks for, their tags appended to `asked` in order. The rest is
/// dropped: the suite opens no connection and runs no timer.
fn note(fx: &mut Effects, asked: &mut VecDeque<HttpTag>) {
    for effect in fx.take() {
        if let Effect::Http { tag, .. } = effect {
            asked.push_back(tag);
        }
    }
}

/// The request an `http` line answers, taken from `asked`: the one `tag` names, or the earliest;
/// what is wrong with the line `n` when there is none.
fn answered(
    asked: &mut VecDeque<HttpTag>,
    tag: Option<HttpTag>,
    n: usize,
) -> Result<HttpTag, String> {
    match tag {
        None => asked.pop_front().ok_or_else(|| {
            format!(
                "line {n} answers an HTTP request, but the codec has asked for none it still \
                 waits on"
            )
        }),
        Some(tag) => match asked.iter().position(|t| *t == tag) {
            Some(at) => Ok(asked.remove(at).expect("a position held")),
            None => Err(format!(
                "line {n} answers HTTP request {}, which the codec has not asked for or a line \
                 has answered",
                tag.0
            )),
        },
    }
}

/// `case` handed line by line to a market-data codec built fresh from the factory, opened and
/// subscribed to `book` on every instrument: a frame to `on_frame`, an answer to `on_http` under
/// the tag of the request it answers. A subscription the codec refuses fails the check; an
/// `http` line with no request to answer is what is wrong with the case (`Ok(Err(_))`).
pub(crate) fn decode(
    h: &Harness<'_>,
    book: &Book,
    case: &[Line],
) -> Result<Result<Vec<Step>, String>, Failure> {
    let subs = h.book_subs(book.id);
    let mut codec = h.md_codec(subs.clone());
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    if let Err(e) = codec.subscribe(&subs, &[], h.specs(), &mut fx) {
        let what = format!("refused {} on every instrument: {e}", book.caps.channel);
        return Err(h.fail("MdCodec::subscribe", what));
    }
    // The tags of the HTTP requests asked for and not yet answered, earliest first.
    let mut asked = VecDeque::new();
    note(&mut fx, &mut asked);
    let specs = h.specs();
    Ok(h.md_scope(|scope| {
        let mut steps = Vec::with_capacity(case.len());
        for line in case {
            let mut sink = Collect(Vec::new());
            let result = match &line.input {
                Input::Text(text) => {
                    codec.on_frame(RawFrame::Text(text), scope, specs, &mut sink, &mut fx)
                }
                Input::Binary(bytes) => {
                    codec.on_frame(RawFrame::Binary(bytes), scope, specs, &mut sink, &mut fx)
                }
                Input::Http { tag, status, body } => {
                    let tag = answered(&mut asked, *tag, line.line)?;
                    let resp = HttpResponse {
                        status: *status,
                        headers: &[],
                        body,
                    };
                    codec.on_http(tag, Ok(resp), scope, specs, &mut sink, &mut fx)
                }
            };
            note(&mut fx, &mut asked);
            steps.push(Step {
                line: line.line,
                kind: line.input.kind(),
                tags: line.tags.clone(),
                result,
                events: sink.0,
            });
        }
        Ok(steps)
    }))
}

/// What `step`, of the case read from `file`, breaks of the status source the venue declares,
/// `declared` ([`MdCaps::status`](fbc_core::MdCaps)): one breach per trading status it pushed
/// when that is `FeedSource::None`, none otherwise (decision 0130).
fn undeclared_statuses(declared: FeedSource, file: &str, step: &Step) -> Vec<Breach> {
    if declared != FeedSource::None {
        return Vec::new();
    }
    let statuses = step.events.iter().map(|(_, ev)| ev);
    statuses
        .filter(|ev| matches!(ev, MdEvent::Status { .. }))
        .map(|ev| {
            let what = format!(
                "{file} line {}: {ev:?}, a trading status the venue declares it does not report",
                step.line
            );
            Breach::new("MdCaps.status is FeedSource::None", what)
        })
        .collect()
}

/// The instrument `ev` is on.
fn inst_of(ev: &MdEvent) -> InstrumentId {
    match *ev {
        MdEvent::Touch { inst, .. }
        | MdEvent::BookSnapshotBegin { inst, .. }
        | MdEvent::BookSnapshotEnd { inst, .. }
        | MdEvent::Level { inst, .. }
        | MdEvent::Window { inst, .. }
        | MdEvent::SidePrune { inst, .. }
        | MdEvent::SideClear { inst, .. }
        | MdEvent::Trade { inst, .. }
        | MdEvent::Mark { inst, .. }
        | MdEvent::Index { inst, .. }
        | MdEvent::Funding { inst, .. }
        | MdEvent::Stats { inst, .. }
        | MdEvent::Health { inst, .. }
        | MdEvent::Status { inst, .. } => inst,
    }
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
    /// or unreadable, an `http` line with no request to answer (the case not judged further), a
    /// line the codec refuses and a case file no driven channel reads are breaches too. The
    /// skipped channels are listed among what was probed.
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
            let steps = match decode(h, book, &case)? {
                Ok(steps) => steps,
                Err(what) => {
                    breaches.push(Breach::new(&file, what));
                    continue;
                }
            };
            let subscribed: BTreeSet<InstrumentId> =
                h.book_subs(book.id).iter().map(|s| s.inst).collect();
            let specs = h.specs();
            for step in &steps {
                let line = step.line;
                if let Err(e) = &step.result {
                    breaches.push(Breach::new(&file, format!("line {line} refused: {e}")));
                }
                // Only what this connection subscribed (Codex r4217991939).
                let gapped = step.tags.gaps.iter().map(|sym| (sym, specs.by_symbol(sym)));
                for (sym, _) in
                    gapped.filter(|(_, s)| s.is_some_and(|s| !subscribed.contains(&s.id)))
                {
                    let what = format!(
                        "line {line}: `gap={sym}` names an instrument this \
                                        connection did not subscribe"
                    );
                    breaches.push(Breach::new(&file, what));
                }
                let outside = step.events.iter().map(|(_, ev)| inst_of(ev));
                for inst in outside.filter(|i| !subscribed.contains(i)) {
                    let what = format!(
                        "line {line}: an event on {inst:?}, which this connection \
                                        did not subscribe"
                    );
                    breaches.push(Breach::new(&file, what));
                }
                // Only a status the caps say the venue reports (decision 0130).
                breaches.extend(undeclared_statuses(h.caps.md.status, &file, step));
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

/// Why a check that drives every book channel is skipped when it drives none.
pub(crate) const NO_BOOK: &str = "the caps declare no book channel";

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
                    input: Input::Text("a|b=1".to_owned())
                },
                Line {
                    line: 4,
                    tags: Tags {
                        gaps: ["X".to_owned()].into(),
                        ts: true,
                        ..Tags::default()
                    },
                    input: Input::Text(" c d".to_owned())
                },
                Line {
                    line: 5,
                    tags: rpi,
                    input: Input::Binary(vec![0x0a, 0xff])
                },
            ]
        );
    }

    /// FBC-fhk4: an `http` line, after its tags, answers the request its `tag=` names or the
    /// earliest, with a text or hex body read as an order-entry case reads one.
    #[test]
    fn an_http_line_reads_its_tags_the_request_it_names_its_status_and_body() {
        let case = parse(
            "public rpi http tag=2 200 text anchor|sym=A|seq=1\n\
             http 503 hex 6f 6b\n\
             ts http tag=7 204 text \n",
        )
        .unwrap();
        let http = |tag: Option<u64>, status, body: &[u8]| Input::Http {
            tag: tag.map(HttpTag),
            status,
            body: body.to_vec(),
        };
        let shows = Tags {
            shows: [Channel::Public, Channel::Rpi].into(),
            ..Tags::default()
        };
        let ts = Tags {
            ts: true,
            ..Tags::default()
        };
        assert_eq!(
            case,
            [
                Line {
                    line: 1,
                    tags: shows,
                    input: http(Some(2), 200, b"anchor|sym=A|seq=1"),
                },
                Line {
                    line: 2,
                    tags: Tags::default(),
                    input: http(None, 503, b"ok"),
                },
                Line {
                    line: 3,
                    tags: ts,
                    input: http(Some(7), 204, b""),
                },
            ]
        );
        assert_eq!(
            case.iter().map(|l| l.input.kind()).collect::<Vec<_>>(),
            [Kind::Http; 3]
        );
    }

    #[test]
    fn an_http_line_naming_no_request_or_spelling_no_response_is_refused_by_number() {
        for bad in [
            "http tag=x 200 text a",
            "http tag= 200 text a",
            "http tag=-1 200 text a",
        ] {
            let tag = bad.split(' ').nth(1).unwrap();
            assert_eq!(
                parse(bad).unwrap_err(),
                format!("line 1: `{tag}` names no request: a request's tag is a number"),
                "{bad}"
            );
        }
        let shape = "line 1: not `http <status> text <body>` or `http <status> hex <bytes>`, \
                     <status> from 100 to 599";
        for bad in [
            "http tag=1",
            "http tag=1 200",
            "http 99 text a",
            "http 200 body a",
        ] {
            assert_eq!(parse(bad).unwrap_err(), shape, "{bad}");
        }
        assert_eq!(
            parse("http 200 hex 0").unwrap_err(),
            "line 1: not hexadecimal bytes"
        );
    }

    /// FBC-fhk4: an answer goes to the request its tag names, or to the earliest one asked for
    /// and not yet answered; a line with none to answer is the case's error.
    #[test]
    fn an_answer_takes_the_request_its_tag_names_or_the_earliest_and_none_twice() {
        let mut asked: VecDeque<HttpTag> = [1, 2, 3].map(HttpTag).into();
        assert_eq!(answered(&mut asked, Some(HttpTag(2)), 4), Ok(HttpTag(2)));
        assert_eq!(answered(&mut asked, None, 5), Ok(HttpTag(1)));
        assert_eq!(
            answered(&mut asked, Some(HttpTag(2)), 6),
            Err(
                "line 6 answers HTTP request 2, which the codec has not asked for or a line has \
                 answered"
                    .to_owned()
            )
        );
        assert_eq!(answered(&mut asked, None, 7), Ok(HttpTag(3)));
        assert_eq!(
            answered(&mut asked, None, 8),
            Err(
                "line 8 answers an HTTP request, but the codec has asked for none it still waits \
                 on"
                .to_owned()
            )
        );
    }

    #[test]
    fn a_trading_status_breaches_only_a_venue_whose_caps_declare_it_reports_none() {
        use fbc_core::TradingStatus;
        // The toy declares none, so a status from it is a breach, one per status; a venue
        // declaring a source may report one, and no other event is a status.
        assert_eq!(crate::toy::caps().md.status, FeedSource::None);
        let status = MdEvent::Status {
            inst: crate::toy::INST_A,
            status: TradingStatus::Halted,
        };
        let health = MdEvent::Health {
            inst: crate::toy::INST_A,
            feed: fbc_core::Feed::Trades,
            h: fbc_core::FeedHealth::Gap,
        };
        let step = Step {
            line: 4,
            kind: Kind::Text,
            tags: Tags::default(),
            result: Ok(()),
            events: vec![(VenueMeta::NONE, health), (VenueMeta::NONE, status)],
        };
        let breaches = undeclared_statuses(FeedSource::None, "continuity/book.frames", &step);
        assert_eq!(breaches.len(), 1, "{breaches:?}");
        assert_eq!(breaches[0].capability, "MdCaps.status is FeedSource::None");
        assert!(
            (breaches[0].what).starts_with("continuity/book.frames line 4: Status {"),
            "{breaches:?}"
        );
        for declared in [FeedSource::Poll, FeedSource::Stream] {
            let none = undeclared_statuses(declared, "continuity/book.frames", &step);
            assert!(none.is_empty(), "{declared:?}: {none:?}");
        }
    }

    #[test]
    fn a_line_of_no_frame_an_unknown_tag_or_bad_hex_is_refused_by_number() {
        assert_eq!(
            parse("text a\ngap=X").unwrap_err(),
            "line 2: holds no `text ` or `hex ` frame and no `http ` answer"
        );
        assert_eq!(
            parse("ts http").unwrap_err(),
            "line 1: holds no `text ` or `hex ` frame and no `http ` answer"
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
