//! `continuity` (design §6, decisions 0003 and 0022): the fixture's sequence breaks are
//! detected as the caps declare. For every book channel the suite drives
//! ([`book_cases`](super::book_cases)) whose [`BookCaps::continuity`] chains its frames
//! ([`Continuity::PlusOne`] or [`Continuity::PrevId`]), `continuity/<channel>.frames` marks
//! with the `gap=<symbol>` tag each frame that breaks the sequence of that instrument's book (a
//! number skipped, repeated or gone back). The codec must push `Health { inst: <that
//! instrument>, feed: Book(<that channel>), h: Gap }` from each marked frame, a gap on no other
//! instrument or feed from it (Codex r4217682431, r4217839174), and no gap from any other
//! frame: a break missed or put elsewhere leaves a broken book trusted, and a gap reported in
//! sequence throws a good one away. From the frame that breaks a book, its first event
//! included, until a snapshot of that book begins, the codec must push no level or window of it
//! (Codex r4218167354): the consumer applies events as they come, so a delta of a broken book
//! is trusted. Nor may it report the book [`FeedHealth::Live`] before that snapshot ends (Codex
//! r4218492690): `Live` says no gap since the last snapshot, and a gap discards a snapshot
//! begun before it in its frame (Codex r4218744557). A case that marks no break proves nothing
//! and fails, and so does one none of whose frames in sequence pushes an event of the book
//! (Codex r4218744571): it must show the codec taking the sequence too. A channel declared
//! [`Continuity::Windowed`] or [`Continuity::Unsequenced`] has nothing to chain and is skipped
//! by name.
//!
//! # The longer-block sub-case
//!
//! A venue whose market data is binary ([`Encoding::Sbe`] or [`Encoding::Protobuf`]) also
//! gives `continuity/longer_block/<channel>.frames` for each channel the suite drives, its
//! continuity chained or not (Codex r4218492670): frames whose binary block is longer than the
//! schema the codec was written against (a field the venue added). The case must hold a frame,
//! every one binary (Codex r4217991970), and the codec must decode every one, each pushing
//! something (Codex r4217682441, r4217839165), and report a gap exactly where the case marks
//! one, if anywhere: a longer block is read by its declared length, never taken for a break,
//! refused or dropped. A text venue ([`Encoding::Json`] or [`Encoding::Text`]) has no binary
//! block: the sub-case is skipped by name, and a `longer_block/` directory in its fixtures
//! fails, as a case never read.

use std::collections::BTreeSet;

use fbc_core::{
    BookCaps, Continuity, Encoding, Feed, FeedHealth, InstrumentId, MdEvent, SpecTable,
};

use super::book_cases::{self, Book, NO_BOOK, PerBook, Step};
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
    let (chained, skipped) = book_cases::books(&h, unchained);
    // A binary venue's longer blocks are read on every channel the suite drives, chained or
    // not (Codex r4218492670): the check runs while there is one.
    let (driven, why) = match h.caps.md.encoding {
        Encoding::Sbe | Encoding::Protobuf => (book_cases::books(&h, |_| None).0, NO_BOOK),
        Encoding::Json | Encoding::Text => (chained.clone(), NOTHING_CHAINED),
    };
    let outcome = run(&h, subject, (&chained, &driven), skipped);
    book_cases::verdict(CHECK, &driven, why, outcome)
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

/// The breaks of every `chained` channel, then the longer blocks of every `driven` one.
fn run(
    h: &Harness<'_>,
    subject: &Subject<'_>,
    (chained, driven): (&[Book], &[Book]),
    skipped: Vec<String>,
) -> Result<(Vec<String>, Vec<Breach>), Failure> {
    let breaks = PerBook {
        check: CHECK,
        dir: CHECK,
        judge: &|book, file, steps, specs, breaches| {
            judge(book, file, steps, specs, breaches, true)
        },
    };
    let (mut probed, mut breaches) = breaks.run(h, subject, chained, skipped)?;
    let dir = format!("{CHECK}/{LONGER}");
    match h.caps.md.encoding {
        Encoding::Sbe | Encoding::Protobuf => {
            let longer = PerBook {
                check: CHECK,
                dir: &dir,
                judge: &|book, file, steps, specs, breaches| {
                    judge(book, file, steps, specs, breaches, false)
                },
            };
            let (more, broken) = longer.run(h, subject, driven, Vec::new())?;
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

/// Holds `book`'s case, read from `file`, to the gaps it marks: from each marked frame a gap on
/// the instrument it names and no other, and none from the other frames. `breaks` is the main
/// case, which must mark one; in the longer-block case every frame must instead push something.
fn judge(
    book: &Book,
    file: &str,
    steps: &[Step],
    specs: &SpecTable,
    breaches: &mut Vec<Breach>,
    breaks: bool,
) -> String {
    let capability = format!("BookCaps.continuity is {:?}", book.caps.continuity);
    let channel = book.caps.channel;
    let symbol = |inst: InstrumentId| {
        specs.get(inst).map_or_else(
            || format!("{inst:?}"),
            |s| s.venue_symbol.as_wire().to_owned(),
        )
    };
    let mut marked = 0usize;
    // The instruments whose book a frame broke, until a snapshot of it begins, then ends.
    let mut broken = Broken::default();
    // Whether a frame in sequence pushed an event of the book.
    let mut in_sequence = false;
    for step in steps {
        let line = step.line;
        // The instruments and feeds a gap was reported on.
        let gapped: Vec<(InstrumentId, Feed)> = step
            .events
            .iter()
            .filter_map(|(_, ev)| match ev {
                MdEvent::Health {
                    inst,
                    feed,
                    h: FeedHealth::Gap,
                } => Some((*inst, *feed)),
                _ => None,
            })
            .collect();
        if !breaks && !step.binary {
            // Codex r4217991970: a longer block is a binary frame's.
            let what = format!("line {line} is a text frame: a longer block is binary");
            breaches.push(Breach::new(file, what));
        }
        if step.tags.gaps.is_empty() {
            if !gapped.is_empty() {
                let what = format!("{file} line {line} is in sequence, yet a gap was reported");
                breaches.push(Breach::new(&capability, what));
            }
            if !breaks && step.events.is_empty() {
                let what = format!(
                    "line {line} decodes to no event: a longer block must be read, not dropped"
                );
                breaches.push(Breach::new(file, what));
            }
            // Codex r4218744571: a frame in sequence that the codec takes.
            in_sequence |= step.events.iter().any(|(_, ev)| on_book(ev, book));
            let held = Held { book, broke: &[] };
            kept_live(
                &mut broken,
                &held,
                file,
                step,
                &capability,
                &symbol,
                breaches,
            );
            continue;
        }
        marked += 1;
        // Each instrument whose sequence the frame breaks, by its own book (Codex
        // r4217991957: a batched frame may break several).
        let mut own = Vec::new();
        for sym in &step.tags.gaps {
            let Some(inst) = specs.by_symbol(sym).map(|s| s.id) else {
                let what = format!("line {line}: `gap={sym}` names no instrument of the setup");
                breaches.push(Breach::new(file, what));
                continue;
            };
            own.push((inst, Feed::Book(book.id)));
            if !gapped.contains(&(inst, Feed::Book(book.id))) {
                let what = format!(
                    "{file} line {line} breaks {sym}'s sequence, yet no gap on {channel} was \
                     reported for it"
                );
                breaches.push(Breach::new(&capability, what));
            }
        }
        // A gap on another instrument, or on another feed of one (Codex r4217839174).
        let broke = step
            .tags
            .gaps
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        for &(other, feed) in gapped.iter().filter(|g| !own.contains(g)) {
            let on = match feed == Feed::Book(book.id) {
                true => symbol(other),
                false => format!("{}'s {feed:?}", symbol(other)),
            };
            let what = format!(
                "{file} line {line} breaks {broke}'s sequence, yet a gap was reported on {on}"
            );
            breaches.push(Breach::new(&capability, what));
        }
        let broke: Vec<InstrumentId> = own.iter().map(|&(inst, _)| inst).collect();
        let held = Held {
            book,
            broke: &broke,
        };
        kept_live(
            &mut broken,
            &held,
            file,
            step,
            &capability,
            &symbol,
            breaches,
        );
    }
    let frames = steps.len();
    if !breaks {
        if frames == 0 {
            // Codex r4217839165.
            let what = "hands the codec no frame: a case that decodes nothing proves nothing";
            breaches.push(Breach::new(file, what));
        }
        return format!("{file}: {frames} frames, {marked} breaking the sequence");
    }
    if marked == 0 {
        let what = "marks no frame `gap=<symbol>`: a case that breaks no sequence proves nothing";
        breaches.push(Breach::new(file, what));
    }
    if !in_sequence {
        let what = format!(
            "no frame in sequence pushes an event of {channel}: a case must show the codec \
             taking the sequence, not only breaking it"
        );
        breaches.push(Breach::new(file, what));
    }
    format!("{file}: {marked} of {frames} frames break the sequence")
}

/// The instruments whose book is broken: `held` until a snapshot of it begins, `healing` until
/// that snapshot ends.
#[derive(Default)]
struct Broken {
    held: BTreeSet<InstrumentId>,
    healing: BTreeSet<InstrumentId>,
}

/// Whether `ev` is an event of `book`'s levels: a snapshot's begin or end, a level or a window.
fn on_book(ev: &MdEvent, book: &Book) -> bool {
    match *ev {
        MdEvent::BookSnapshotBegin { book: b, .. }
        | MdEvent::BookSnapshotEnd { book: b, .. }
        | MdEvent::Level { book: b, .. }
        | MdEvent::Window { book: b, .. } => b == book.id,
        _ => false,
    }
}

/// The book a step is held to, and the instruments whose sequence the step breaks.
struct Held<'a> {
    book: &'a Book,
    broke: &'a [InstrumentId],
}

/// Holds `step` to the books broken before it or by it: none takes a level or window until a
/// snapshot of it begins, the snapshot's own levels being the recovery (Codex r4218167354), nor
/// is reported `Live` until that snapshot ends (Codex r4218492690). The frame that breaks a book
/// is held from its first event, as a level pushed before the gap reaches the consumer first,
/// and held again at its gap, which discards a snapshot begun before it (Codex r4218744557).
fn kept_live(
    broken: &mut Broken,
    held: &Held<'_>,
    file: &str,
    step: &Step,
    capability: &str,
    symbol: &dyn Fn(InstrumentId) -> String,
    breaches: &mut Vec<Breach>,
) {
    let (book, line) = (held.book, step.line);
    for &inst in held.broke {
        broken.held.insert(inst);
    }
    for (_, ev) in &step.events {
        match *ev {
            MdEvent::Health {
                inst,
                feed: Feed::Book(b),
                h: FeedHealth::Gap,
            } if b == book.id && held.broke.contains(&inst) => {
                broken.healing.remove(&inst);
                broken.held.insert(inst);
            }
            MdEvent::BookSnapshotBegin { inst, book: b, .. }
                if b == book.id && broken.held.remove(&inst) =>
            {
                broken.healing.insert(inst);
            }
            MdEvent::BookSnapshotEnd { inst, book: b } if b == book.id => {
                broken.healing.remove(&inst);
            }
            MdEvent::Level { inst, book: b, .. } | MdEvent::Window { inst, book: b, .. }
                if b == book.id && broken.held.contains(&inst) =>
            {
                let what = format!(
                    "{file} line {line} pushes a level of {}'s book while its sequence is \
                     broken: a broken book takes nothing until its next snapshot",
                    symbol(inst)
                );
                breaches.push(Breach::new(capability, what));
            }
            MdEvent::Health {
                inst,
                feed: Feed::Book(b),
                h: FeedHealth::Live,
            } if b == book.id
                && (broken.held.contains(&inst) || broken.healing.contains(&inst)) =>
            {
                let what = format!(
                    "{file} line {line} reports {}'s book Live while its sequence is broken: \
                     it is Live again only once its next snapshot ends",
                    symbol(inst)
                );
                breaches.push(Breach::new(capability, what));
            }
            _ => {}
        }
    }
}
