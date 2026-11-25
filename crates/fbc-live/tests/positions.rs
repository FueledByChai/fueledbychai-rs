//! The positions comparison and its re-read rule (`fbc_live::positions`, FBC-hpg6, decision 0155
//! items 2, 7 and 8), moved from the samples with its behaviour: each verdict, an unread answer
//! first and on the re-read, the re-read asked at the first reading's time plus the consumer's
//! wait, a zero wait refused, what a reading shows, the notes' counts, and what the resyncs found
//! wrong. Every reading and every instant is one the test builds: the comparison reads no venue
//! and no clock.

use std::time::Duration;

use fbc_core::{FillKey, InstrumentId, MonoNs, SignedLots, WallNs, dispatch_market_data};
use fbc_live::link::Note;
use fbc_live::positions::{
    Next, PositionWatch, Reading, Verdict, ZeroReread, fill_seen, fills_in, resync_disagreements,
};
use fbc_oms::{PositionCheck, ResyncReport, ResyncSnapshot};

/// The consumer's wait before a re-read.
const WAIT: Duration = Duration::from_secs(2);

/// A reading at `at_ms` of the venue's size `venue` (`None`: unread) against the registry's
/// `registry`, with one fill of ours applied.
fn reading(at_ms: u64, venue: Option<i64>, registry: i64) -> Reading {
    Reading {
        at: MonoNs(at_ms * 1_000_000),
        venue: venue.map(SignedLots).ok_or_else(|| "status 503".to_owned()),
        registry: SignedLots(registry),
        fills: 1,
    }
}

/// A watch with the consumer's wait.
fn watch() -> PositionWatch {
    PositionWatch::new(WAIT).unwrap()
}

#[test]
fn a_first_reading_that_agrees_is_done_at_once() {
    let mut watch = watch();
    let first = reading(1_500, Some(12), 12);
    assert!(first.agrees());
    let next = watch.judge(first.clone());
    assert_eq!(next, Next::Done(Verdict::Agrees(first)));
    let Next::Done(verdict) = next else {
        unreachable!()
    };
    assert!(!verdict.persists());
}

#[test]
fn a_mismatch_is_read_again_at_the_first_readings_time_plus_the_wait_and_clears() {
    let mut watch = watch();
    let first = reading(1_500, Some(0), 12);
    assert!(!first.agrees());
    // The re-read is asked exactly the consumer's wait after the first reading's time.
    assert_eq!(
        watch.judge(first.clone()),
        Next::ReadAgainAt(MonoNs(3_500_000_000))
    );
    let second = reading(3_500, Some(12), 12);
    let next = watch.judge(second.clone());
    assert_eq!(next, Next::Done(Verdict::Cleared(first, second)));
    let Next::Done(verdict) = next else {
        unreachable!()
    };
    assert!(!verdict.persists());
}

#[test]
fn a_mismatch_the_re_read_still_shows_persists() {
    let mut watch = watch();
    let first = reading(10, Some(0), 12);
    assert_eq!(
        watch.judge(first.clone()),
        Next::ReadAgainAt(MonoNs(2_010_000_000))
    );
    let second = reading(2_011, Some(5), 12);
    let next = watch.judge(second.clone());
    assert_eq!(next, Next::Done(Verdict::Persists(first, second)));
    let Next::Done(verdict) = next else {
        unreachable!()
    };
    assert!(verdict.persists());
}

#[test]
fn an_unread_answer_is_read_again_and_clears_or_persists_as_the_re_read_says() {
    // Unread first, then read and agreeing: cleared.
    let mut watch = watch();
    let first = reading(100, None, 0);
    assert!(!first.agrees());
    assert_eq!(
        watch.judge(first.clone()),
        Next::ReadAgainAt(MonoNs(2_100_000_000))
    );
    let second = reading(2_100, Some(0), 0);
    assert_eq!(
        watch.judge(second.clone()),
        Next::Done(Verdict::Cleared(first, second))
    );

    // Unread on the re-read too: it persists, whatever the registry holds.
    let mut watch = PositionWatch::new(Duration::from_millis(1)).unwrap();
    let first = reading(100, None, 0);
    assert_eq!(
        watch.judge(first.clone()),
        Next::ReadAgainAt(MonoNs(101_000_000))
    );
    let second = reading(101, None, 0);
    assert_eq!(
        watch.judge(second.clone()),
        Next::Done(Verdict::Persists(first, second))
    );

    // A mismatch first and unread on the re-read: it persists.
    let mut watch = PositionWatch::new(Duration::from_millis(1)).unwrap();
    let first = reading(100, Some(1), 0);
    assert_eq!(
        watch.judge(first.clone()),
        Next::ReadAgainAt(MonoNs(101_000_000))
    );
    let second = reading(101, None, 0);
    assert_eq!(
        watch.judge(second.clone()),
        Next::Done(Verdict::Persists(first, second))
    );
}

#[test]
fn a_watch_that_came_to_a_verdict_judges_its_next_reading_as_a_first() {
    let mut watch = watch();
    assert!(matches!(
        watch.judge(reading(0, Some(1), 0)),
        Next::ReadAgainAt(_)
    ));
    assert!(matches!(
        watch.judge(reading(2_000, Some(1), 0)),
        Next::Done(Verdict::Persists(..))
    ));
    let again = reading(5_000, Some(0), 0);
    assert_eq!(
        watch.judge(again.clone()),
        Next::Done(Verdict::Agrees(again))
    );
}

#[test]
fn a_zero_wait_is_refused() {
    assert_eq!(PositionWatch::new(Duration::ZERO), Err(ZeroReread));
    assert_eq!(
        ZeroReread.to_string(),
        "the position re-read's wait is zero"
    );
    let error: &dyn std::error::Error = &ZeroReread;
    assert!(error.source().is_none());
    assert!(PositionWatch::new(Duration::from_nanos(1)).is_ok());
}

#[test]
fn a_reading_shows_its_time_in_whole_milliseconds_and_names_the_consumers_read() {
    let read = Reading {
        at: MonoNs(1_234_999_999),
        venue: Ok(SignedLots(-3)),
        registry: SignedLots(12),
        fills: 2,
    };
    assert_eq!(
        read.shown("the read"),
        "+1234ms the read -3 lots, the registry 12 lots (2 fills applied)"
    );
    let unread = Reading {
        at: MonoNs(999_999),
        venue: Err("status 503".to_owned()),
        registry: SignedLots(0),
        fills: 0,
    };
    assert_eq!(
        unread.shown("the read"),
        "+0ms the read unread (status 503), the registry 0 lots (0 fills applied)"
    );
}

/// A fill note.
fn fill(ours: bool, unexplained: bool) -> Note {
    Note::Fill {
        seen: "a fill".to_owned(),
        unexplained,
        ours,
    }
}

#[test]
fn the_notes_count_fills_of_ours_and_say_whether_any_fill_was_seen() {
    let none = [Note::Problem("not a fill".to_owned())];
    assert_eq!(fills_in(&none), 0);
    assert!(!fill_seen(&none));

    let foreign = [fill(false, true)];
    assert_eq!(fills_in(&foreign), 0);
    assert!(fill_seen(&foreign));

    let notes = [
        fill(true, false),
        Note::Problem("not a fill".to_owned()),
        fill(false, true),
        fill(true, false),
    ];
    assert_eq!(fills_in(&notes), 2);
    assert!(fill_seen(&notes));
}

/// A resync's note with `checks` compared and `unsettled` fills it could not place.
fn resynced(checks: Vec<PositionCheck>, unsettled: Vec<(InstrumentId, FillKey)>) -> Note {
    Note::Resynced {
        report: ResyncReport {
            checks,
            unsettled,
            ..ResyncReport::default()
        },
        snapshot: ResyncSnapshot {
            watermark: WallNs(0),
            requested_at: MonoNs(0),
            orders: Vec::new(),
            positions: Vec::new(),
        },
    }
}

#[test]
fn the_resyncs_report_every_position_they_could_not_agree_with() {
    let inst = InstrumentId::new(1);
    // The first resync (nothing compared), then a reconnect's that agrees, and a note that is
    // no resync: nothing is wrong.
    let agrees = PositionCheck::Agrees {
        inst,
        position: SignedLots(0),
    };
    let mut notes = vec![
        resynced(vec![], vec![]),
        Note::Problem("not a resync".to_owned()),
        resynced(vec![agrees], vec![]),
    ];
    assert!(resync_disagreements(&notes).is_empty());

    // A reconnect's resync showing the account long 5 lots where the registry holds 0, then one
    // compared too late and one whose market's fills could not be placed, then a fill the
    // resync could not place against its seed.
    notes.push(resynced(
        vec![PositionCheck::Desync {
            inst,
            venue: SignedLots(5),
            ledger: Some(SignedLots(0)),
        }],
        vec![],
    ));
    notes.push(resynced(
        vec![PositionCheck::Stale(inst), PositionCheck::Unsettled(inst)],
        vec![],
    ));
    let key = dispatch_market_data(&fbc_conformance::toy::caps(), |s| s.fill_id("f-1")).unwrap();
    notes.push(resynced(vec![], vec![(inst, FillKey::Venue(key))]));
    let found = resync_disagreements(&notes);
    assert_eq!(found.len(), 4, "{found:?}");
    assert!(found[0].starts_with("Desync"), "{found:?}");
    assert!(found[1].starts_with("Stale"), "{found:?}");
    assert!(found[2].starts_with("Unsettled("), "{found:?}");
    assert_eq!(found[3], "Unsettled fill on instrument 1", "{found:?}");
}
