//! FBC-aoh5's classifier of cancels not ours (`fbc_live::not_ours`, decision 0155 item 2): its
//! test moved from `testnet_quote`'s with the classifier, on the shard's monotonic clock
//! (`MonoNs`) where it read tokio's, and the consumer's bounds, each refused zero (decision 0155
//! item 8). Every instant is one the test chooses: the classifier reads no clock.

use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{AccountKey, CidMint, ExchNs, MonoNs, Namespace, NamespaceLease, Side, WallNs};
use fbc_live::not_ours::{CancelsNotOurs, NotOurs, NotOursConfig, NotOursConfigError};

/// The span `testnet_quote` gives its classifier (its `EVENT_SPAN`), which the moved test's
/// instants were chosen around.
const SPAN: Duration = Duration::from_secs(1);

/// The consumer's bounds: `SPAN` and `window`.
fn config(window: Duration) -> NotOursConfig {
    NotOursConfig::new(SPAN, window).unwrap()
}

/// A directory removed when it is dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

thread_local! {
    /// The lease directories the test on this thread made, removed when its thread ends.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of this test's own.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-live-not-ours-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
    dir
}

#[test]
fn a_zero_span_or_window_is_refused() {
    let s = Duration::from_secs;
    assert_eq!(
        NotOursConfig::new(Duration::ZERO, s(30)),
        Err(NotOursConfigError::ZeroSpan)
    );
    assert_eq!(
        NotOursConfig::new(s(1), Duration::ZERO),
        Err(NotOursConfigError::ZeroWindow)
    );
    assert_eq!(
        NotOursConfig::new(Duration::ZERO, Duration::ZERO),
        Err(NotOursConfigError::ZeroSpan)
    );
    assert_eq!(
        NotOursConfigError::ZeroSpan.to_string(),
        "the span of an event of cancels not ours is zero"
    );
    assert_eq!(
        NotOursConfigError::ZeroWindow.to_string(),
        "the window over cancels not ours is zero"
    );
    // The smallest of each is taken, and the window is read back as given.
    let tiny = Duration::from_nanos(1);
    let seen = CancelsNotOurs::new(NotOursConfig::new(tiny, tiny).unwrap());
    assert_eq!(seen.window(), tiny);
    assert!(!seen.seen());
    assert_eq!(seen.orders(), 0);
}

#[test]
fn a_venue_time_absent_on_either_side_judges_by_when_each_was_seen() {
    // The span is judged by the venue's times only when both cancels carry one: with the first's
    // absent, or the later one's, by when each was seen, within the span one event, past it
    // another.
    let t0 = MonoNs(1_000_000_000);
    let s = Duration::from_secs;
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let buy = Side::Buy;

    // The event's first stamped, the later ones not.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, b, c) = (cid(), cid(), cid());
    for order in [a, b, c] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(a, buy, at(0), t0 + s(1)), NotOurs::First);
    assert_eq!(seen.on_cancel(b, buy, None, t0 + s(2)), NotOurs::SameEvent);
    assert_eq!(seen.on_cancel(c, buy, None, t0 + s(3)), NotOurs::Repeat);

    // The event's first not stamped, the later ones stamped (a venue time 100 s on, which by
    // the venue's times alone would be another event).
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, b, c) = (cid(), cid(), cid());
    for order in [a, b, c] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(a, buy, None, t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(b, buy, at(100), t0 + Duration::from_millis(1_500)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.on_cancel(c, buy, at(100), t0 + s(3)), NotOurs::Repeat);
    assert_eq!(seen.orders(), 3);
    assert!(seen.seen());
    assert_eq!(seen.window(), s(30));
}

#[test]
fn a_requote_that_never_rested_leaves_its_side_to_the_next_place_and_an_ended_one_to_none() {
    // RB-aoh5-10: the side's requote is the first place after the event that rests. A requote
    // place that never rested, placed 5 s after the event, gives its side to the place 40 s on,
    // whose cancel 10 s later is the repeat; timed from the unrested place (45 s) it would be a
    // new first. Likewise when the requote's replace ended (a fill) before the requote's place
    // came back unrested: nothing is left of that requote.
    let t0 = MonoNs(1_000_000_000);
    let s = Duration::from_secs;
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let buy = Side::Buy;

    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (first, refused, rests) = (cid(), cid(), cid());
    seen.placed(first, buy, t0);
    assert_eq!(seen.on_cancel(first, buy, at(0), t0), NotOurs::First);
    seen.placed(refused, buy, t0 + s(5));
    seen.unrested(refused);
    seen.retain(|c| *c != refused);
    seen.placed(rests, buy, t0 + s(40));
    assert_eq!(
        seen.on_cancel(rests, buy, at(50), t0 + s(50)),
        NotOurs::Repeat
    );

    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (first, requote, replace, rests) = (cid(), cid(), cid(), cid());
    seen.placed(first, buy, t0);
    assert_eq!(seen.on_cancel(first, buy, at(0), t0), NotOurs::First);
    seen.placed(requote, buy, t0 + s(5));
    seen.placed(replace, buy, t0 + s(6));
    seen.retain(|c| *c != replace);
    seen.unrested(requote);
    seen.placed(rests, buy, t0 + s(40));
    assert_eq!(
        seen.on_cancel(rests, buy, at(50), t0 + s(50)),
        NotOurs::Repeat
    );
}

#[test]
fn cancels_not_ours_of_one_action_are_one_event_and_a_later_action_repeats_it() {
    let t0 = MonoNs(1_000_000_000);
    let s = Duration::from_secs;
    // The venue's time on a cancel, `secs` after the first action's.
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let (buy, sell) = (Side::Buy, Side::Sell);
    let mut seen = CancelsNotOurs::new(config(s(30)));
    // Both sides, placed before it was seen, of one reconcile: one event, even read 1.5 s apart
    // (split across a cycle); an earlier run's order (placed by no one this run) as well.
    let (b1, a1, a0, old) = (cid(), cid(), cid(), cid());
    for (order, side) in [(b1, buy), (a1, sell), (a0, sell)] {
        seen.placed(order, side, t0);
    }
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0 + s(1)), NotOurs::First);
    let read_later = t0 + Duration::from_millis(2_500);
    assert_eq!(
        seen.on_cancel(a1, sell, at(0), read_later),
        NotOurs::SameEvent
    );
    assert_eq!(
        seen.on_cancel(old, buy, at(0), t0 + s(2)),
        NotOurs::SameEvent
    );
    // RB-aoh5-1 / Codex r4308625185: an order resting since before the first, cancelled by a
    // later action (the venue's time 10 s on) within 30 s: the repeat.
    assert_eq!(
        seen.on_cancel(a0, sell, at(10), t0 + s(11)),
        NotOurs::Repeat
    );
    // A requote placed after it, cancelled within 30 s: the repeat again; its other side,
    // placed before and cancelled with it, the same event as it.
    let (b2, a2) = (cid(), cid());
    seen.placed(b2, buy, t0 + s(12));
    seen.placed(a2, sell, t0 + s(13));
    assert_eq!(seen.on_cancel(b2, buy, at(20), t0 + s(21)), NotOurs::Repeat);
    assert_eq!(
        seen.on_cancel(a2, sell, at(20), t0 + s(21)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.orders(), 6);

    // Codex r4309154208 / RB-aoh5-9: a cycle longer than the window. The loop's requotes of
    // both sides are placed 40 s after the first event and cancelled 3 s later by another
    // program's next reconcile: the repeat however long the cycle, the other side the same event.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (b1, a1) = (cid(), cid());
    seen.placed(b1, buy, t0);
    seen.placed(a1, sell, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    assert_eq!(seen.on_cancel(a1, sell, at(0), t0), NotOurs::SameEvent);
    let (b2, a2) = (cid(), cid());
    seen.placed(b2, buy, t0 + s(40));
    seen.placed(a2, sell, t0 + s(40));
    assert_eq!(seen.on_cancel(b2, buy, at(43), t0 + s(43)), NotOurs::Repeat);
    assert_eq!(
        seen.on_cancel(a2, sell, at(43), t0 + s(43)),
        NotOurs::SameEvent
    );

    // RB-aoh5-10: a requote place that never rested (refused for its rate, a post-only place
    // that would cross, or not sent) frees its side's slot, so the place after it, the one that
    // rests, is the requote: its cancel 3 s after its place is the repeat, 43 s from the first.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (b1, refused, b2) = (cid(), cid(), cid());
    seen.placed(b1, buy, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    seen.placed(refused, buy, t0 + s(25));
    seen.unrested(refused);
    seen.retain(|c| *c != refused);
    seen.placed(b2, buy, t0 + s(40));
    assert_eq!(seen.on_cancel(b2, buy, at(43), t0 + s(43)), NotOurs::Repeat);
    // A replace that ended unrested frees nothing, nor does the other side's: the requote
    // that rested stays timed from its place (40 s), so a replace cancelled 32 s after it is
    // a new first (from the unrested replace's place it would be the repeat).
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (b1, requote, refused, other, replace) = (cid(), cid(), cid(), cid(), cid());
    seen.placed(b1, buy, t0);
    assert_eq!(seen.on_cancel(b1, buy, at(0), t0), NotOurs::First);
    seen.placed(requote, buy, t0 + s(40));
    seen.placed(other, sell, t0 + s(40));
    seen.unrested(other);
    seen.retain(|c| *c != requote);
    seen.placed(refused, buy, t0 + s(55));
    seen.unrested(refused);
    seen.placed(replace, buy, t0 + s(60));
    assert_eq!(
        seen.on_cancel(replace, buy, at(72), t0 + s(72)),
        NotOurs::First
    );

    // Past 30 s from the window's first event, a new window: a requote cancelled more than
    // 30 s after its place; a later requote within 30 s of its place; a replace of a requote
    // (not the first placed on its side since the event) judged by the window's first only.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    assert_eq!(seen.on_cancel(old, buy, at(0), t0), NotOurs::First);
    let late = cid();
    seen.placed(late, buy, t0 + s(1));
    assert_eq!(
        seen.on_cancel(late, buy, at(32), t0 + s(32)),
        NotOurs::First
    );
    let requote = cid();
    seen.placed(requote, buy, t0 + s(33));
    assert_eq!(
        seen.on_cancel(requote, buy, at(40), t0 + s(40)),
        NotOurs::Repeat
    );
    // Codex r4310042604: a replace of the requote is timed from the requote's place: within
    // 30 s of it the repeat, past it a new first.
    let (requote, replace) = (cid(), cid());
    seen.placed(requote, buy, t0 + s(41));
    seen.placed(replace, buy, t0 + s(50));
    seen.retain(|c| *c != requote);
    assert_eq!(
        seen.on_cancel(replace, buy, at(63), t0 + s(63)),
        NotOurs::Repeat
    );
    let (requote, replace) = (cid(), cid());
    seen.placed(requote, buy, t0 + s(64));
    seen.placed(replace, buy, t0 + s(80));
    seen.retain(|c| *c != requote);
    assert_eq!(
        seen.on_cancel(replace, buy, at(95), t0 + s(95)),
        NotOurs::First
    );
    // A requote that filled: the next order on its side is its replace, timed from its place
    // (past 30 s from the window's first event, 95 s).
    let (requote, next) = (cid(), cid());
    seen.placed(requote, sell, t0 + s(126));
    seen.retain(|c| *c != requote);
    seen.placed(next, sell, t0 + s(140));
    assert_eq!(
        seen.on_cancel(next, sell, at(150), t0 + s(150)),
        NotOurs::Repeat
    );
    // RB-aoh5-2: an order resting unchanged since before the first, cancelled 10 minutes on.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (c1, c2) = (cid(), cid());
    seen.placed(c1, buy, t0);
    seen.placed(c2, sell, t0);
    assert_eq!(seen.on_cancel(c1, buy, at(1), t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(c2, sell, at(600), t0 + s(600)),
        NotOurs::First
    );

    // With no venue time, when each was seen: within the span one event, later another.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (d1, d2, d3) = (cid(), cid(), cid());
    for order in [d1, d2, d3] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(d1, buy, None, t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(d2, buy, None, t0 + Duration::from_millis(1_900)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.on_cancel(d3, buy, None, t0 + s(5)), NotOurs::Repeat);
}

#[test]
fn the_span_is_the_consumers_by_the_venues_times_and_by_when_each_was_seen() {
    // Decision 0155 item 8: the span is the consumer's. Under a span of 5 s a cancel 3 s after
    // the event's first is of the same event, by the venue's times and, with no venue time, by
    // when each was seen; under 1 s each would be the repeat; past 5 s it is.
    let t0 = MonoNs(1_000_000_000);
    let s = Duration::from_secs;
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let buy = Side::Buy;
    let five = NotOursConfig::new(s(5), s(30)).unwrap();

    let mut seen = CancelsNotOurs::new(five);
    let (a, b, c) = (cid(), cid(), cid());
    for order in [a, b, c] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(a, buy, at(0), t0 + s(1)), NotOurs::First);
    assert_eq!(seen.on_cancel(b, buy, at(3), t0 + s(2)), NotOurs::SameEvent);
    assert_eq!(seen.on_cancel(c, buy, at(6), t0 + s(2)), NotOurs::Repeat);

    let mut seen = CancelsNotOurs::new(five);
    let (a, b, c) = (cid(), cid(), cid());
    for order in [a, b, c] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(a, buy, None, t0 + s(1)), NotOurs::First);
    assert_eq!(seen.on_cancel(b, buy, None, t0 + s(4)), NotOurs::SameEvent);
    assert_eq!(seen.on_cancel(c, buy, None, t0 + s(7)), NotOurs::Repeat);
}

#[test]
fn the_span_and_the_window_hold_at_their_bounds_and_a_venue_time_before_counts_alike() {
    // The span and both windows are inclusive: a cancel the venue stamped exactly the span after
    // the event's first, or exactly the span before it, is of that event, and one two spans
    // before is not (the venue's times are compared either way round); a new event exactly the
    // window after the window's first repeats it; a requote cancelled exactly the window after
    // its place repeats; and a requote whose own place ended unrested while a replace of it is
    // still open stays the side's requote, timed from its place.
    let t0 = MonoNs(1_000_000_000);
    let s = Duration::from_secs;
    let at = |secs: i64| Some(ExchNs(1_759_500_000_000_000_000 + secs * 1_000_000_000));
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let (buy, sell) = (Side::Buy, Side::Sell);

    // The span by the venue's times, each cancel seen seconds after the event's first, so only
    // the venue's times can make it the same event.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, after, before, far) = (cid(), cid(), cid(), cid());
    for order in [a, after, before, far] {
        seen.placed(order, buy, t0);
    }
    assert_eq!(seen.on_cancel(a, buy, at(10), t0 + s(1)), NotOurs::First);
    assert_eq!(
        seen.on_cancel(after, buy, at(11), t0 + s(5)),
        NotOurs::SameEvent
    );
    assert_eq!(
        seen.on_cancel(before, buy, at(9), t0 + s(6)),
        NotOurs::SameEvent
    );
    assert_eq!(seen.on_cancel(far, buy, at(8), t0 + s(7)), NotOurs::Repeat);

    // A new event exactly the window after the window's first: placed before the first, so no
    // requote of its side, and stamped far past the span.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, b) = (cid(), cid());
    seen.placed(a, buy, t0);
    seen.placed(b, sell, t0);
    assert_eq!(seen.on_cancel(a, buy, at(0), t0), NotOurs::First);
    assert_eq!(seen.on_cancel(b, sell, at(30), t0 + s(30)), NotOurs::Repeat);
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, b) = (cid(), cid());
    seen.placed(a, buy, t0);
    seen.placed(b, sell, t0);
    assert_eq!(seen.on_cancel(a, buy, at(0), t0), NotOurs::First);
    let past = t0 + s(30) + Duration::from_nanos(1);
    assert_eq!(seen.on_cancel(b, sell, at(31), past), NotOurs::First);

    // A requote cancelled exactly the window after its place, 40 s after the window's first.
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, requote) = (cid(), cid());
    seen.placed(a, buy, t0);
    assert_eq!(seen.on_cancel(a, buy, at(0), t0), NotOurs::First);
    seen.placed(requote, buy, t0 + s(10));
    assert_eq!(
        seen.on_cancel(requote, buy, at(40), t0 + s(40)),
        NotOurs::Repeat
    );

    // The requote's own place ended unrested while its replace is open: the requote is kept,
    // so the replace's cancel 25 s after the requote's place, 35 s after the window's first, is
    // the repeat (were the requote freed, a new first).
    let mut seen = CancelsNotOurs::new(config(s(30)));
    let (a, requote, replace) = (cid(), cid(), cid());
    seen.placed(a, buy, t0);
    assert_eq!(seen.on_cancel(a, buy, at(0), t0), NotOurs::First);
    seen.placed(requote, buy, t0 + s(10));
    seen.placed(replace, buy, t0 + s(20));
    seen.unrested(requote);
    seen.retain(|c| *c != requote);
    assert_eq!(
        seen.on_cancel(replace, buy, at(35), t0 + s(35)),
        NotOurs::Repeat
    );
}
