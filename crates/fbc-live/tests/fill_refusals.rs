//! `Link::take_fill_refusals` (FBC-2dr2, decision 0155 items 2 and 8): it hands over every fill
//! the ledger refused for its time, in order, and `Link::fill_refusals` then holds none; a
//! refusal the ledger makes after a take is held for the next one, so none is lost before it is
//! read; and a link whose refusals are never taken keeps every one, as before. The fills go
//! through `Link::on_exec` as a session hands them over, on the conformance toy's caps, with no
//! session: a venue's own changes (decision 0120), which a fresh registry applies and the ledger
//! then records, and replays the ledger refuses `BeyondHorizon` or `Untimed`.

use std::time::Duration;

use fbc_conformance::toy::INST_A;
use fbc_core::{
    AssetSym, ConnKey, Envelope, ExchNs, ExchTsKind, ExecEvent, FillEvent, FillIdent, FillOrigin,
    Liquidity3, Lots, MonoNs, PxExact, Side, Stamp, VenueCaps, VenueFillKind, WallNs,
    dispatch_market_data,
};
use fbc_live::link::{FillRefusal, Link};
use fbc_oms::{FillLedger, LadderConfig, LedgerConfig, Registry};

/// The toy's capabilities.
fn toy() -> VenueCaps {
    fbc_conformance::toy::caps()
}

/// A link over a fresh registry under the toy's order caps, its ledger holding one fill at most,
/// so the second it records forgets the first and moves its horizon to that fill's time, and its
/// session-start watermark at 1,000 ns.
fn link() -> Link {
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1,
        },
        WallNs(1_000),
    )
    .unwrap();
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    Link::new(Registry::new(), ledger, toy().exec.unwrap().order, ladder)
}

/// A liquidation of one lot bought on `INST_A`, keyed by the venue's fill id `id`, replayed or
/// not.
fn fill(id: &str, replay: bool) -> FillEvent {
    let caps = toy();
    FillEvent {
        ident: FillIdent::Venue {
            fill: dispatch_market_data(&caps, |s| s.fill_id(id)).unwrap(),
            vid: None,
            cum_after: None,
        },
        origin: FillOrigin::Venue {
            kind: VenueFillKind::Liquidation,
            px: PxExact::new(100, 0),
        },
        inst: INST_A,
        side: Side::Buy,
        qty: Lots::new(1).unwrap(),
        liquidity: Liquidity3::Unknown,
        fee: dispatch_market_data(&caps, |s| s.fee(0, AssetSym::new("USDC").unwrap())).unwrap(),
        realized_pnl: None,
        realized_funding: None,
        replay,
    }
}

/// `fill`, the `seq`th input, received at `at`, executed at `exch` on the matching engine's
/// clock, or untimed.
fn arrives(link: &mut Link, seq: u64, at: u64, exch: Option<i64>, fill: FillEvent) {
    link.on_exec(Envelope {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(at),
            recv_wall: WallNs(i64::try_from(at).unwrap()),
            conn: ConnKey { conn: 0, epoch: 0 },
        },
        exch_ts: exch.map(ExchNs),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(seq),
        body: ExecEvent::Fill(fill),
    });
}

/// Two fills applied, the second forgetting the first, so the ledger's horizon is the first's
/// time, 2,000 ns; then a replay of a fill at that time, refused `BeyondHorizon`, and an untimed
/// replay, refused `Untimed`, received at 30 and 40.
fn two_refused(link: &mut Link) {
    arrives(link, 1, 10, Some(2_000), fill("a", false));
    arrives(link, 2, 20, Some(3_000), fill("b", false));
    assert!(link.fill_refusals().is_empty());
    arrives(link, 3, 30, Some(2_000), fill("c", true));
    arrives(link, 4, 40, None, fill("d", true));
}

fn refusal(admission: &'static str, at: u64) -> FillRefusal {
    FillRefusal {
        admission,
        at: MonoNs(at),
    }
}

#[test]
fn a_take_hands_over_every_refusal_in_order_and_leaves_none_held() {
    let mut link = link();
    two_refused(&mut link);
    let both = [refusal("BeyondHorizon", 30), refusal("Untimed", 40)];
    assert_eq!(link.fill_refusals(), both);
    assert_eq!(link.take_fill_refusals(), both);
    assert!(link.fill_refusals().is_empty());
    assert!(link.take_fill_refusals().is_empty());
}

#[test]
fn a_refusal_after_a_take_is_held_for_the_next_take() {
    let mut link = link();
    two_refused(&mut link);
    assert_eq!(link.take_fill_refusals().len(), 2);
    arrives(&mut link, 5, 50, None, fill("e", true));
    let after = [refusal("Untimed", 50)];
    assert_eq!(link.fill_refusals(), after);
    assert_eq!(link.take_fill_refusals(), after);
    assert!(link.fill_refusals().is_empty());
    // The ledger still counts every refusal, those taken included.
    let counts = link.ledger().replays();
    assert_eq!((counts.beyond_horizon, counts.untimed), (1, 2));
}

#[test]
fn a_link_whose_refusals_are_never_taken_keeps_every_one() {
    let mut link = link();
    two_refused(&mut link);
    arrives(&mut link, 5, 50, None, fill("e", true));
    assert_eq!(
        link.fill_refusals(),
        [
            refusal("BeyondHorizon", 30),
            refusal("Untimed", 40),
            refusal("Untimed", 50),
        ]
    );
}
