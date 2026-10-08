//! `continuity` (design §6, decisions 0003 and 0022): the fixture's sequence breaks are
//! detected as the caps declare. For every book channel the suite drives
//! ([`book_cases`](super::book_cases)) whose [`BookCaps::continuity`] chains its frames
//! ([`Continuity::PlusOne`] or [`Continuity::PrevId`]), `continuity/<channel>.frames` marks
//! with the `gap` tag each frame that breaks the book's sequence (a number skipped, repeated or
//! gone back). The codec must push `Health { feed: Book(<that channel>), h: Gap }` from each
//! marked frame, and no gap from any other frame: a break missed leaves a broken book trusted,
//! and a gap reported in sequence throws a good one away. A case that marks no break proves
//! nothing and fails. A channel declared [`Continuity::Windowed`] or [`Continuity::Unsequenced`]
//! has nothing to chain and is skipped by name.
//!
//! # The longer-block sub-case
//!
//! A venue whose market data is binary ([`Encoding::Sbe`] or [`Encoding::Protobuf`]) also
//! gives `continuity/longer_block/<channel>.frames` for each channel: frames whose binary block
//! is longer than the schema the codec was written against (a field the venue added). The codec
//! must decode every one, pushing something, and report a gap exactly where the case marks one,
//! if anywhere: a longer block is read by its declared length, never taken for a break or
//! refused. A text venue ([`Encoding::Json`] or [`Encoding::Text`]) has no binary block: the
//! sub-case is skipped by name, and a `longer_block/` directory in its fixtures fails, as a
//! case never read.

use fbc_core::{BookCaps, Continuity, Encoding, Feed, FeedHealth, MdEvent};

use super::book_cases::{self, Book, PerBook, Step};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "continuity";
/// The sub-case's directory, under `continuity/`.
const LONGER: &str = "longer_block";

/// Why the check is skipped for a venue it drives no channel of.
const NOTHING_CHAINED: &str = "the caps declare no book channel the suite drives whose \
     continuity chains its frames: none, each anchored on REST (BookCaps.rest_anchor, FBC-fhk4), \
     or each Windowed or Unsequenced (BookCaps.continuity)";

/// Runs `continuity` against `subject`.
pub fn continuity(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let (books, skipped) = book_cases::books(&h, unchained);
    let outcome = run(&h, subject, &books, skipped);
    book_cases::verdict(CHECK, &books, NOTHING_CHAINED, outcome)
}

/// Why a channel declaring `caps` has no sequence to break, if it has none.
fn unchained(caps: &BookCaps) -> Option<String> {
    match caps.continuity {
        Continuity::PlusOne | Continuity::PrevId => None,
        other => Some(format!(
            "BookCaps.continuity is {other:?}: nothing chains its frames"
        )),
    }
}

/// Both cases of every driven channel: the breaks, then the longer blocks.
fn run(
    h: &Harness<'_>,
    subject: &Subject<'_>,
    books: &[Book],
    skipped: Vec<String>,
) -> Result<(Vec<String>, Vec<Breach>), Failure> {
    let breaks = PerBook {
        check: CHECK,
        dir: CHECK,
        judge: &|book, file, steps, breaches| judge(book, file, steps, breaches, true),
    };
    let (mut probed, mut breaches) = breaks.run(h, subject, books, skipped)?;
    let dir = format!("{CHECK}/{LONGER}");
    match h.caps.md.encoding {
        Encoding::Sbe | Encoding::Protobuf => {
            let longer = PerBook {
                check: CHECK,
                dir: &dir,
                judge: &|book, file, steps, breaches| judge(book, file, steps, breaches, false),
            };
            let (more, broken) = longer.run(h, subject, books, Vec::new())?;
            probed.extend(more);
            breaches.extend(broken);
        }
        text @ (Encoding::Json | Encoding::Text) => {
            probed.push(format!(
                "{LONGER}: skipped: MdCaps.encoding is {text:?}: its frames carry no binary block"
            ));
            if subject.fixtures().join(&dir).exists() {
                breaches.push(Breach::new(
                    &format!("{dir}/"),
                    format!(
                        "MdCaps.encoding is {text:?}: a text frame has no binary block, so \
                         {LONGER} is never read"
                    ),
                ));
            }
        }
    }
    Ok((probed, breaches))
}

/// Whether `step` reported a gap on `book`'s channel, and whether it reported one on any feed.
fn gaps(book: &Book, step: &Step) -> (bool, bool) {
    let mut on_book = false;
    let mut any = false;
    for (_, ev) in &step.events {
        if let MdEvent::Health {
            feed,
            h: FeedHealth::Gap,
            ..
        } = ev
        {
            any = true;
            on_book |= *feed == Feed::Book(book.id);
        }
    }
    (on_book, any)
}

/// Holds `book`'s case, read from `file`, to the gaps it marks: one reported from each marked
/// frame and none from the others. `breaks` is the main case, which must mark one; the
/// longer-block case must instead push something.
fn judge(
    book: &Book,
    file: &str,
    steps: &[Step],
    breaches: &mut Vec<Breach>,
    breaks: bool,
) -> String {
    let capability = format!("BookCaps.continuity is {:?}", book.caps.continuity);
    let channel = book.caps.channel;
    let mut marked = 0usize;
    for step in steps {
        let (on_book, any) = gaps(book, step);
        let line = step.line;
        if step.tags.gap {
            marked += 1;
            if !on_book {
                let what = format!(
                    "{file} line {line} breaks the sequence, yet no gap was reported on {channel}"
                );
                breaches.push(Breach::new(&capability, what));
            }
        } else if any {
            let what = format!("{file} line {line} is in sequence, yet a gap was reported");
            breaches.push(Breach::new(&capability, what));
        }
    }
    let frames = steps.len();
    if breaks {
        if marked == 0 {
            let what = "marks no frame `gap`: a case that breaks no sequence proves nothing";
            breaches.push(Breach::new(file, what));
        }
        return format!("{file}: {marked} of {frames} frames break the sequence");
    }
    if steps.iter().all(|s| s.events.is_empty()) {
        let what = "decodes to no event: a case that decodes nothing proves nothing";
        breaches.push(Breach::new(file, what));
    }
    format!("{file}: {frames} frames, {marked} breaking the sequence")
}
