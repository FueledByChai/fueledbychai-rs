//! `no_exch_ts_synthesized` (design §6, decision 0006): an event carries the venue's
//! timestamp ([`VenueMeta::exch_ts`](fbc_core::VenueMeta)) only when the frame it was decoded
//! from does. A codec that fills a missing timestamp from a clock of its own would make the
//! journal's latency figures and a replay's ordering rest on a time the venue never sent.
//!
//! For every book channel the suite drives ([`book_cases`](super::book_cases)),
//! `no_exch_ts_synthesized/<channel>.frames` marks with the `ts` tag each frame, or HTTP answer
//! (an anchored channel's snapshot, FBC-fhk4), that carries the venue's timestamp. The events
//! pushed from a line so marked are not judged. Every event pushed from a line not marked must
//! carry none, except a book delta the codec held from an earlier frame and the line releases,
//! as an anchor's answer releases the deltas held until it came (Codex r4489870341).
//!
//! # Held frames
//!
//! A line names each held frame it releases by the timestamp that frame's line states with
//! `ts=<ns>`: `held=<ns>`, or `held=<ns>:<count>` when the case states how many of the line's
//! book deltas carry it. On a line not marked, an event may carry exactly such a timestamp only
//! when it is a book delta (a `Level`, `Window`, `SidePrune` or `SideClear`) outside a book
//! snapshot. Every other event carrying it breaches, each named: one of a snapshot, from its
//! begin to its end, which is the line's own, and any other, a health, a touch, a trade or a
//! status among them. With a count, more of the line's book deltas carrying the timestamp than
//! the count is a breach naming each of them; without one, any number may carry it, so a codec
//! that puts the held frame's timestamp on another delta it releases, one held without a
//! timestamp, is not told apart. The count bounds how many of the line's book deltas carry the
//! timestamp, not which: a codec that moves it from the delta the held frame carried to another
//! it releases, or onto another instrument's delta, is not told apart, and a `held=` whose frame
//! releases nothing is not flagged. Which of the frames a line releases an event came from the
//! case does not state either, so an event released carrying another of those frames'
//! timestamps is not told apart.
//!
//! Each held frame is released once. A `held=<ns>` is the case's error, and an event carrying
//! its timestamp is judged as if the line released no frame stamped so, when no earlier line
//! states the timestamp; when every earlier line stating it pushed a book delta carrying it at
//! once, outside a snapshot, and so held none stamped so (a frame that pushes some of its book
//! deltas at once and holds others cannot be named; a gap's health or another event it pushes at
//! once with its timestamp does not count, so a breaking delta held for a re-anchor is named);
//! or when each of those lines that held is released already, by an
//! earlier line or an earlier `held=` of the same line: the error names the line stating the
//! timestamp and the line that released its frame. Of several earlier lines stating one
//! timestamp, a `held=` naming it releases every one still held.
//!
//! A held book snapshot cannot be stated: the snapshot an `http` or frame line pushes is that
//! line's own, carrying the line's timestamp or, on a line not marked, none, whichever earlier
//! frame the codec may have taken it from. A codec that holds a stamped snapshot and pushes it
//! from a line not marked breaches as one that dates the line's own snapshot.
//!
//! A case none of whose unmarked lines pushes an event other than those released from a held
//! frame with its timestamp proves nothing and fails. Market data only: the order-entry events
//! are not held to this check yet.

use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{BookId, ExchNs, InstrumentId, MdEvent};

use super::book_cases::{self, Kind, NO_BOOK, PerBook, Step};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "no_exch_ts_synthesized";

/// The book snapshots a case has begun and not yet ended, by instrument and book.
type Open = BTreeSet<(InstrumentId, BookId)>;

/// Whether `ev` is an event of a book snapshot, from its begin to its end, `open` holding the
/// snapshots begun and not ended before it, and updated.
fn of_snapshot(open: &mut Open, ev: &MdEvent) -> bool {
    match *ev {
        MdEvent::BookSnapshotBegin { inst, book, .. } => {
            open.insert((inst, book));
            true
        }
        MdEvent::BookSnapshotEnd { inst, book } => {
            open.remove(&(inst, book));
            true
        }
        MdEvent::Level { inst, book, .. }
        | MdEvent::Window { inst, book, .. }
        | MdEvent::SidePrune { inst, book, .. }
        | MdEvent::SideClear { inst, book, .. } => open.contains(&(inst, book)),
        _ => false,
    }
}

/// Whether `ev`, when no snapshot holds it, is a book delta: the only event a line may release
/// from a held frame carrying that frame's timestamp.
fn is_delta(ev: &MdEvent) -> bool {
    matches!(
        ev,
        MdEvent::Level { .. }
            | MdEvent::Window { .. }
            | MdEvent::SidePrune { .. }
            | MdEvent::SideClear { .. }
    )
}

/// A line stating a timestamp (`ts=<ns>`): a frame a later line may release.
struct Stated {
    line: usize,
    /// It pushed a book delta carrying the timestamp at once, outside a snapshot, so held none
    /// stamped so.
    pushed: bool,
    /// The line that released it, once one has.
    released: Option<usize>,
}

/// A held frame one line releases.
struct Release {
    /// The earliest line stating its timestamp that the release takes.
    at: usize,
    /// How many of the line's book deltas may carry its timestamp, at most, if the line says.
    count: Option<usize>,
    /// The line's book deltas carrying it.
    carried: Vec<MdEvent>,
}

/// What judging one case counted.
#[derive(Default)]
struct Counts {
    /// Events pushed from lines without a timestamp.
    judged: usize,
    /// Of those, the events a line released carrying a held frame's timestamp.
    released: usize,
}

/// The `held=` tags of the line `n` taken against `stated`, the frames earlier lines state: the
/// frames it releases, by timestamp, each taken from `stated`; a breach of the case, read from
/// `file`, for each tag naming no frame still held.
fn releases(
    file: &str,
    n: usize,
    step: &Step,
    stated: &mut BTreeMap<i64, Vec<Stated>>,
    breaches: &mut Vec<Breach>,
) -> BTreeMap<i64, Release> {
    let mut releases = BTreeMap::new();
    for held in &step.tags.held {
        let ns = held.ns;
        let frames: &mut [Stated] = match stated.get_mut(&ns) {
            Some(frames) => frames,
            None => &mut [],
        };
        let mut at = None;
        for frame in frames
            .iter_mut()
            .filter(|f| !f.pushed && f.released.is_none())
        {
            at.get_or_insert(frame.line);
            frame.released = Some(n);
        }
        if let Some(at) = at {
            let count = held.count;
            let carried = Vec::new();
            releases.insert(ns, Release { at, count, carried });
            continue;
        }
        let released = frames
            .iter()
            .rev()
            .find_map(|f| Some((f.line, f.released?)));
        let what = match (released, frames.last()) {
            (Some((at, by)), _) => format!(
                "line {n}: `held={ns}` names line {at}'s frame, which line {by} released: a held \
                 frame is released once"
            ),
            (None, Some(frame)) => format!(
                "line {n}: `held={ns}` names line {}'s frame, which pushed an event carrying \
                 exch_ts {ns} at once: it held none stamped so",
                frame.line
            ),
            (None, None) => format!(
                "line {n}: `held={ns}` names no timestamp an earlier line states with `ts={ns}`"
            ),
        };
        breaches.push(Breach::new(file, what));
    }
    releases
}

/// Judges the events of `steps`, the case read from `file`: pushes a breach for each event
/// carrying a timestamp its line neither carries nor releases with a held frame's book deltas,
/// for each line releasing more of them than it states, for each `held=` naming no frame still
/// held, and for a case judging no event but those released.
fn judge(file: &str, steps: &[Step], breaches: &mut Vec<Breach>) -> Counts {
    let mut counts = Counts::default();
    // The lines stating each timestamp (`ts=<ns>`).
    let mut stated: BTreeMap<i64, Vec<Stated>> = BTreeMap::new();
    let mut open = Open::new();
    for step in steps {
        let n = step.line;
        let line = match step.kind {
            Kind::Http => "HTTP answer",
            Kind::Text | Kind::Binary => "frame",
        };
        let mut held = releases(file, n, step, &mut stated, breaches);
        // The timestamps the line's book deltas carry outside a snapshot: the frames it pushed
        // at once, which a later `held=` cannot name.
        let mut pushed = BTreeSet::new();
        for (meta, ev) in &step.events {
            let snapshot = of_snapshot(&mut open, ev);
            if let (false, true, Some(ExchNs(ns))) = (snapshot, is_delta(ev), meta.exch_ts) {
                pushed.insert(ns);
            }
            if step.tags.ts {
                continue;
            }
            counts.judged += 1;
            let Some(ExchNs(ns)) = meta.exch_ts else {
                continue;
            };
            let what = match held.get_mut(&ns) {
                Some(release) if snapshot => format!(
                    "{file} line {n}: the {line} carries no timestamp, yet {ev:?}, of a book \
                     snapshot it pushes, carries exch_ts {ns}, line {}'s: a snapshot is the \
                     line's own, never released from a held frame",
                    release.at
                ),
                Some(release) if !is_delta(ev) => format!(
                    "{file} line {n}: the {line} carries no timestamp, yet {ev:?}, no book delta, \
                     carries exch_ts {ns}, line {}'s: only a book delta is released from a held \
                     frame",
                    release.at
                ),
                Some(release) => {
                    release.carried.push(*ev);
                    continue;
                }
                None if step.tags.held.is_empty() => format!(
                    "{file} line {n}: the {line} carries no timestamp, yet {ev:?} carries \
                     exch_ts {ns}"
                ),
                None => format!(
                    "{file} line {n}: the {line} carries no timestamp and releases no held \
                     frame stamped {ns}, yet {ev:?} carries exch_ts {ns}"
                ),
            };
            breaches.push(Breach::new("VenueMeta.exch_ts", what));
        }
        for (ns, release) in held {
            let carried = release.carried.len();
            match release.count {
                Some(count) if carried > count => {
                    let events: Vec<String> =
                        release.carried.iter().map(|ev| format!("{ev:?}")).collect();
                    let what = format!(
                        "{file} line {n}: the {line} carries no timestamp and releases line {}'s \
                         held frame with `held={ns}:{count}`, yet {carried} of its book deltas \
                         carry exch_ts {ns}: {}",
                        release.at,
                        events.join(", ")
                    );
                    breaches.push(Breach::new("VenueMeta.exch_ts", what));
                    counts.released += count;
                }
                _ => counts.released += carried,
            }
        }
        for &ns in &step.tags.stamps {
            let frame = Stated {
                line: n,
                pushed: pushed.contains(&ns),
                released: None,
            };
            stated.entry(ns).or_default().push(frame);
        }
    }
    if counts.judged == counts.released {
        let what = match counts.judged {
            0 => {
                "no event is decoded from a line without a timestamp: a case that states none \
                 proves nothing"
            }
            _ => {
                "every event decoded from a line without a timestamp is released from a held \
                 frame with its timestamp: a case that states none of its own proves nothing"
            }
        };
        breaches.push(Breach::new(file, what));
    }
    counts
}

/// Runs `no_exch_ts_synthesized` against `subject`.
pub fn no_exch_ts_synthesized(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let (books, skipped) = book_cases::books(&h, |_| None);
    let per_book = PerBook {
        check: CHECK,
        dir: CHECK,
        judge: &|_, file, steps, _, breaches| {
            let Counts { judged, released } = judge(file, steps, breaches);
            let lines = match steps.iter().any(|s| s.kind == Kind::Http) {
                true => "frames and HTTP answers",
                false => "frames",
            };
            let probed = format!("{file}: {judged} events from {lines} without a timestamp");
            match released {
                0 => probed,
                _ => format!("{probed}, {released} released from a held frame with its timestamp"),
            }
        },
    };
    let outcome = per_book.run(&h, subject, &books, skipped);
    book_cases::verdict(CHECK, &books, NO_BOOK, outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{BookSide, Feed, FeedHealth, Lots, Ticks, TradingStatus, VenueMeta};

    use super::book_cases::{Held, Tags};

    const INST: InstrumentId = InstrumentId::new(1);
    const BOOK: BookId = BookId(1);

    fn at(ns: Option<i64>) -> VenueMeta {
        VenueMeta {
            exch_ts: ns.map(ExchNs),
            ..VenueMeta::NONE
        }
    }

    fn level(px: i64) -> MdEvent {
        MdEvent::Level {
            inst: INST,
            book: BOOK,
            side: BookSide::Bid,
            px: Ticks(px),
            qty: Lots::new(1).unwrap(),
        }
    }

    fn step(line: usize, tags: Tags, events: Vec<(VenueMeta, MdEvent)>) -> Step {
        Step {
            line,
            kind: Kind::Http,
            tags,
            result: Ok(()),
            events,
        }
    }

    /// A line stating `ns` with `ts=`.
    fn stating(ns: i64) -> Tags {
        Tags {
            ts: true,
            stamps: [ns].into(),
            ..Tags::default()
        }
    }

    /// A line releasing the held frames `held` names, as `(ns, count)`.
    fn releasing(held: &[(i64, Option<usize>)]) -> Tags {
        Tags {
            held: held.iter().map(|&(ns, count)| Held { ns, count }).collect(),
            ..Tags::default()
        }
    }

    /// A stamped delta held (line 1 pushes nothing), then an unstamped answer releasing it:
    /// its snapshot carrying `snap`, the held delta's level carrying `flushed`.
    fn held_then_answered(snap: Option<i64>, flushed: Option<i64>, held: &[i64]) -> Vec<Step> {
        let held: Vec<_> = held.iter().map(|&ns| (ns, None)).collect();
        let begin = MdEvent::BookSnapshotBegin {
            inst: INST,
            book: BOOK,
            epoch: 0,
        };
        let end = MdEvent::BookSnapshotEnd {
            inst: INST,
            book: BOOK,
        };
        vec![
            step(1, stating(900), Vec::new()),
            step(
                2,
                releasing(&held),
                vec![
                    (at(snap), begin),
                    (at(snap), level(100)),
                    (at(snap), end),
                    (at(flushed), level(99)),
                ],
            ),
        ]
    }

    /// What each breach of `steps` names and says.
    fn judged(steps: &[Step]) -> (Vec<(String, String)>, Counts) {
        let mut breaches = Vec::new();
        let counts = judge("f", steps, &mut breaches);
        let said = breaches
            .into_iter()
            .map(|b| (b.capability, b.what))
            .collect();
        (said, counts)
    }

    #[test]
    fn a_breaking_delta_whose_gap_health_carries_its_timestamp_is_held_and_released() {
        // The toy's gap path: the breaking delta pushes a gap Health carrying its own meta, and
        // holds its level until the re-anchor; only a book delta pushed at once counts as one
        // the frame did not hold (the second round's skeptic, on the Codex r4489870341 fix).
        let gap = MdEvent::Health {
            inst: INST,
            feed: Feed::Book(BOOK),
            h: FeedHealth::Gap,
        };
        let steps = vec![
            step(1, stating(2000), vec![(at(Some(2000)), gap)]),
            step(
                2,
                releasing(&[(2000, Some(1))]),
                vec![(at(Some(2000)), level(97))],
            ),
            step(3, Tags::default(), vec![(at(None), level(96))]),
        ];
        let (said, counts) = judged(&steps);
        assert!(said.is_empty(), "{said:?}");
        assert_eq!((counts.judged, counts.released), (2, 1));
    }

    #[test]
    fn a_released_delta_carries_the_held_frames_timestamp_and_its_snapshot_none() {
        let mut breaches = Vec::new();
        let counts = judge(
            "f",
            &held_then_answered(None, Some(900), &[900]),
            &mut breaches,
        );
        assert!(breaches.is_empty(), "{breaches:?}");
        assert_eq!((counts.judged, counts.released), (4, 1));
    }

    #[test]
    fn a_snapshot_carrying_a_held_frames_timestamp_breaches_naming_each_event() {
        let mut breaches = Vec::new();
        judge(
            "f",
            &held_then_answered(Some(900), Some(900), &[900]),
            &mut breaches,
        );
        let said: Vec<&str> = breaches.iter().map(|b| b.what.as_str()).collect();
        assert_eq!(said.len(), 3, "{said:?}");
        assert!(said[0].starts_with(
            "f line 2: the HTTP answer carries no timestamp, yet \
                                     BookSnapshotBegin {"
        ));
        assert!(said[1].starts_with("f line 2: the HTTP answer carries no timestamp, yet Level {"));
        assert!(said[2].starts_with(
            "f line 2: the HTTP answer carries no timestamp, yet \
                                     BookSnapshotEnd {"
        ));
        for s in said {
            assert!(s.ends_with(
                "of a book snapshot it pushes, carries exch_ts 900, line 1's: \
                                 a snapshot is the line's own, never released from a held frame"
            ));
        }
    }

    #[test]
    fn a_timestamp_the_line_releases_from_no_held_frame_breaches() {
        // Invented on the flushed delta.
        let mut breaches = Vec::new();
        judge(
            "f",
            &held_then_answered(None, Some(7), &[900]),
            &mut breaches,
        );
        assert_eq!(breaches.len(), 1, "{breaches:?}");
        assert!(breaches[0].what.starts_with(
            "f line 2: the HTTP answer carries no timestamp and releases no held frame stamped \
             7, yet Level {"
        ));
        // Released by a line that names no held frame: the rule before Codex r4489870341.
        let mut breaches = Vec::new();
        judge(
            "f",
            &held_then_answered(None, Some(900), &[]),
            &mut breaches,
        );
        assert_eq!(breaches.len(), 1, "{breaches:?}");
        assert!(
            breaches[0]
                .what
                .starts_with("f line 2: the HTTP answer carries no timestamp, yet Level {")
        );
    }

    #[test]
    fn a_held_timestamp_no_earlier_line_states_is_the_cases_error() {
        let mut breaches = Vec::new();
        judge(
            "f",
            &held_then_answered(None, Some(800), &[800]),
            &mut breaches,
        );
        let said: Vec<(&str, &str)> = breaches
            .iter()
            .map(|b| (b.capability.as_str(), b.what.as_str()))
            .collect();
        assert_eq!(said.len(), 2, "{said:?}");
        assert_eq!(
            said[0],
            (
                "f",
                "line 2: `held=800` names no timestamp an earlier line states with `ts=800`"
            )
        );
        assert_eq!(said[1].0, "VenueMeta.exch_ts");
        assert!(said[1].1.contains("releases no held frame stamped 800"));
    }

    /// Codex r4489870341, the skeptic's first P3: on a line releasing a held frame, only a book
    /// delta outside a snapshot may carry its timestamp. A health, a trade or a status carrying
    /// it breaches, each named, where the rule before let them all through.
    #[test]
    fn only_a_book_delta_is_released_with_a_held_frames_timestamp() {
        let live = MdEvent::Health {
            inst: INST,
            feed: Feed::Book(BOOK),
            h: FeedHealth::Live,
        };
        let status = MdEvent::Status {
            inst: INST,
            status: TradingStatus::Trading,
        };
        let window = MdEvent::Window {
            inst: INST,
            book: BOOK,
            lo: Ticks(90),
            hi: Ticks(110),
        };
        let prune = MdEvent::SidePrune {
            inst: INST,
            book: BOOK,
            side: BookSide::Ask,
            lo: Ticks(101),
            hi: Ticks(110),
        };
        let clear = MdEvent::SideClear {
            inst: INST,
            book: BOOK,
            side: BookSide::Bid,
        };
        let steps = [
            step(1, stating(900), Vec::new()),
            step(
                2,
                releasing(&[(900, None)]),
                vec![
                    (at(None), level(98)),
                    (at(Some(900)), level(99)),
                    (at(Some(900)), window),
                    (at(Some(900)), prune),
                    (at(Some(900)), clear),
                    (at(Some(900)), live),
                    (at(Some(900)), status),
                ],
            ),
        ];
        let (said, counts) = judged(&steps);
        let no_delta = |ev: &MdEvent| {
            (
                "VenueMeta.exch_ts".to_owned(),
                format!(
                    "f line 2: the HTTP answer carries no timestamp, yet {ev:?}, no book delta, \
                     carries exch_ts 900, line 1's: only a book delta is released from a held \
                     frame"
                ),
            )
        };
        assert_eq!(said, [no_delta(&live), no_delta(&status)]);
        assert_eq!((counts.judged, counts.released), (7, 4));
    }

    /// Codex r4489870341, the skeptic's second P3: `held=<ns>:<count>` bounds how many of the
    /// line's book deltas carry the held frame's timestamp, and a line past it breaches naming
    /// each of them, where the rule before let any number through, as `held=<ns>` without a
    /// count still does.
    #[test]
    fn a_count_bounds_the_book_deltas_a_line_releases_with_a_held_frames_timestamp() {
        let steps = |count| {
            [
                step(1, stating(900), Vec::new()),
                step(
                    2,
                    releasing(&[(900, count)]),
                    vec![(at(Some(900)), level(98)), (at(Some(900)), level(99))],
                ),
                // The unmarked event the case judges as its own.
                step(3, Tags::default(), vec![(at(None), level(97))]),
            ]
        };
        let (said, counts) = judged(&steps(Some(1)));
        assert_eq!(
            said,
            [(
                "VenueMeta.exch_ts".to_owned(),
                format!(
                    "f line 2: the HTTP answer carries no timestamp and releases line 1's held \
                     frame with `held=900:1`, yet 2 of its book deltas carry exch_ts 900: {:?}, \
                     {:?}",
                    level(98),
                    level(99)
                )
            )]
        );
        assert_eq!((counts.judged, counts.released), (3, 1));
        for count in [Some(2), Some(3), None] {
            let (said, counts) = judged(&steps(count));
            assert_eq!(said, [], "{count:?}");
            assert_eq!((counts.judged, counts.released), (3, 2));
        }
    }

    /// Codex r4489870341, the skeptic's third P3: a `held=` naming a frame whose line pushed an
    /// event carrying its timestamp at once, so held none, is the case's error, and so is one
    /// naming a frame released already, by an earlier line or the same one; the event carrying
    /// it breaches as one the line releases from no held frame. The rule before took all three.
    #[test]
    fn a_held_frame_is_one_that_held_and_is_released_once() {
        let released_no_frame = |line: usize| {
            (
                "VenueMeta.exch_ts".to_owned(),
                format!(
                    "f line {line}: the HTTP answer carries no timestamp and releases no held \
                     frame stamped 900, yet {:?} carries exch_ts 900",
                    level(99)
                ),
            )
        };
        let case = |what: &str| ("f".to_owned(), what.to_owned());
        let flushed = || vec![(at(None), level(98)), (at(Some(900)), level(99))];

        // Line 1 pushed its event carrying 900 at once.
        let steps = [
            step(1, stating(900), vec![(at(Some(900)), level(100))]),
            step(2, releasing(&[(900, None)]), flushed()),
        ];
        assert_eq!(
            judged(&steps).0,
            [
                case(
                    "line 2: `held=900` names line 1's frame, which pushed an event carrying \
                     exch_ts 900 at once: it held none stamped so"
                ),
                released_no_frame(2),
            ]
        );

        // Line 1's frame released by line 2, then again by line 3.
        let steps = [
            step(1, stating(900), Vec::new()),
            step(2, releasing(&[(900, None)]), flushed()),
            step(3, releasing(&[(900, None)]), flushed()),
        ];
        assert_eq!(
            judged(&steps).0,
            [
                case(
                    "line 3: `held=900` names line 1's frame, which line 2 released: a held \
                     frame is released once"
                ),
                released_no_frame(3),
            ]
        );

        // Released twice by one line: its events carrying 900 are still those of one release.
        let steps = [
            step(1, stating(900), Vec::new()),
            step(2, releasing(&[(900, Some(1)), (900, None)]), flushed()),
        ];
        assert_eq!(
            judged(&steps).0,
            [case(
                "line 2: `held=900` names line 1's frame, which line 2 released: a held frame \
                 is released once"
            )]
        );

        // Of two lines stating 900, one pushing it at once, the one that held is released; a
        // later line stating it again holds a frame a later release takes.
        let steps = [
            step(1, stating(900), vec![(at(Some(900)), level(100))]),
            step(2, stating(900), Vec::new()),
            step(3, releasing(&[(900, Some(1))]), flushed()),
            step(4, stating(900), Vec::new()),
            step(5, releasing(&[(900, Some(1))]), flushed()),
        ];
        let (said, counts) = judged(&steps);
        assert_eq!(said, []);
        assert_eq!((counts.judged, counts.released), (4, 2));
    }

    /// Codex r4489870341, the skeptic's fifth P3: a case whose only events from lines without a
    /// timestamp are those released from a held frame with its timestamp proves nothing, where
    /// the rule before counted them as judged.
    #[test]
    fn a_case_judging_only_released_events_proves_nothing() {
        let proves_nothing = (
            "f".to_owned(),
            "every event decoded from a line without a timestamp is released from a held frame \
             with its timestamp: a case that states none of its own proves nothing"
                .to_owned(),
        );
        let mut steps = vec![
            step(1, stating(900), Vec::new()),
            step(
                2,
                releasing(&[(900, None)]),
                vec![(at(Some(900)), level(99))],
            ),
        ];
        let (said, counts) = judged(&steps);
        assert_eq!(said, [proves_nothing]);
        assert_eq!((counts.judged, counts.released), (1, 1));
        // One event of its own is enough.
        steps.push(step(3, Tags::default(), vec![(at(None), level(98))]));
        assert_eq!(judged(&steps).0, []);
        // And a case judging nothing still proves nothing.
        let (said, counts) = judged(&steps[..1]);
        assert_eq!(
            said,
            [(
                "f".to_owned(),
                "no event is decoded from a line without a timestamp: a case that states none \
                 proves nothing"
                    .to_owned()
            )]
        );
        assert_eq!((counts.judged, counts.released), (0, 0));
    }
}
