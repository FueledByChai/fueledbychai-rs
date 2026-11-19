//! FBC-4plj: stage three's pure parts ([`pager`]), at function level: its interleaving, how a
//! page's rows are named, what a page comes to and the stop it is, where the walk ends, the
//! order page 0 shows, and the report's and the evidence's section on it. Declared by path in
//! `tests/qfex_preprod_probe.rs`, so it is part of that test binary and Cargo builds no target of
//! its own for it. Every id is made up.

use super::*;

use compare::{Listed, PageAttempt};
use fbc_core::{CidMatch, Lots, Namespace, VenueOrderId, dispatch};
use pager::{Action, Came, EndedListed, Ends, PageRead, PagesRun, WalkEnd};

#[test]
fn the_plan_interleaves_places_pages_and_cancels_as_the_ticket_orders_them() {
    // Place P1 and P2; page 0; place P3; page 1; cancel P1; page 2; page 3; cancel P2 and P3;
    // then the pages from 0 until a short page.
    assert_eq!(
        pager::PLAN,
        [
            Action::Place(0),
            Action::Place(1),
            Action::Page(0),
            Action::Place(2),
            Action::Page(1),
            Action::Cancel(0),
            Action::Page(2),
            Action::Page(3),
            Action::Cancel(1),
            Action::Cancel(2),
            Action::Walk,
        ]
    );
    assert_eq!(pager::PAGE_SIZE, 1);
    // Every order is placed before it is cancelled, and each once.
    for n in 0..3 {
        let place = pager::PLAN.iter().position(|a| *a == Action::Place(n));
        let cancel = pager::PLAN.iter().position(|a| *a == Action::Cancel(n));
        assert!(place.unwrap() < cancel.unwrap());
    }
}

/// The caps the pager's pages decode under, built as the run builds them on origins nothing
/// listens on.
fn caps() -> fbc_core::VenueCaps {
    let argv = place_stages::argv_on(
        Path::new("/abs/probe"),
        "ws://127.0.0.1:1",
        "http://127.0.0.1:2",
        "ws://127.0.0.1:3",
    );
    let (opts, _) = place_stages::parsed(&argv).unwrap();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let base = stage::base_config(&opts);
    let (cfg, _) = stage::configured(&base, &lookup, Armed::Primary).unwrap();
    Qfex.caps(&cfg).unwrap()
}

/// The namespace our client ids are minted under.
const NS: Namespace = Namespace::new(7);

/// The made-up venue id ending `n`.
fn vid(n: u8) -> VenueOrderId {
    let id = format!("11111111-1111-4111-8111-0000000000{n:02x}");
    dispatch(&caps(), NS, |scope| scope.venue_order_id(&id)).unwrap()
}

/// Stage three's three orders as a page is matched to them: P1 and P2 with venue ids, P3 with
/// none yet.
fn ours() -> Vec<pager::Ours> {
    let (dir, _removed) = temp_dir("pages-lease");
    let lease = fbc_core::NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(1), NS).unwrap();
    let mut mint = fbc_core::CidMint::new(lease, 0, 0, WallNs(0));
    let venue = [Some(vid(1)), Some(vid(2)), None];
    (pager::NAMES.into_iter().zip(venue))
        .map(|(name, venue)| pager::Ours {
            name,
            cid: mint.mint().unwrap(),
            venue,
        })
        .collect()
}

fn listed(order: VenueOrderId, client: CidMatch) -> Listed {
    Listed { order, client }
}

#[test]
fn a_pages_rows_are_named_by_venue_id_or_by_client_id_while_no_venue_id_is_known() {
    let ours = ours();
    let rows = [
        // P2 by its venue id, whatever client id its row carries.
        listed(vid(2), CidMatch::Unparseable),
        // P3 by its client id, its venue id not known yet.
        listed(vid(3), CidMatch::Ours(ours[2].cid)),
        // P1's client id on a row whose venue id is not P1's: P1 has one, so it is not ours.
        listed(vid(4), CidMatch::Ours(ours[0].cid)),
        // Another's.
        listed(vid(5), CidMatch::Foreign(Namespace::new(3))),
    ];
    assert_eq!(pager::named(&rows, &ours), (vec!["P2", "P3"], 2));
    assert_eq!(pager::named(&[], &ours), (Vec::new(), 0));
}

#[test]
fn an_ended_order_a_page_lists_is_lag_until_a_comparison_after_its_end_ends_or_the_wait_passes() {
    // PR #284's second review, as compare::judge and compare::compare rule (decisions 0109 and
    // 0151 item 4): an order of ours a read lists that the streams showed ended is lag only until
    // a comparison begun after the streams showed it ended has ended, or --agree-wait-ms from
    // then has passed; past either, a page listing it is the run's stop.
    let wait = Duration::from_secs(5);
    let at = tokio::time::Instant::now();
    let mut ends = Ends::new(wait);
    assert!(ends.ended().is_empty() && ends.past(at).is_empty());
    ends.note(&["P1"], at);
    // Seen again later, it keeps the instant it was first seen ended.
    ends.note(&["P1"], at + Duration::from_secs(3));
    assert!(ends.past(at).is_empty());
    assert!(ends.past(at + wait - Duration::from_millis(1)).is_empty());
    assert_eq!(ends.past(at + wait), ["P1"]);
    // A comparison that began after P1 was seen ended has ended: P1 is past its lag at once, and
    // P2, seen ended only after that comparison began, is not.
    let covered = ends.ended();
    ends.note(&["P2"], at);
    ends.compared(&covered);
    assert_eq!(ends.ended(), ["P1", "P2"]);
    assert_eq!(ends.past(at), ["P1"]);
    assert_eq!(ends.past(at + wait), ["P1", "P2"]);
    // What a page lists of them: one past its lag stops the run, ReadDisagrees; one within its
    // lag, or one the streams did not show ended, stops nothing.
    let came = Came::Read {
        ours: vec!["P1", "P2", "P3"],
        others: 0,
        twaps: 0,
        full: true,
    };
    let (listed, stop) = pager::ended_listed(&came, 2, (&["P1", "P2"], &["P1"]));
    let past = EndedListed {
        page: 2,
        name: "P1",
        past: true,
    };
    let within = EndedListed {
        page: 2,
        name: "P2",
        past: false,
    };
    assert_eq!(listed, [past.clone(), within.clone()]);
    assert_eq!(stop, Some(places::Stop::ReadDisagrees));
    let (listed, stop) = pager::ended_listed(&came, 2, (&["P2"], &[]));
    assert_eq!((listed, stop), (vec![within.clone()], None));
    assert_eq!(
        pager::ended_listed(&came, 2, (&[], &[])),
        (Vec::new(), None)
    );
    let failed = pager::ended_listed(&Came::Failed, 2, (&["P1"], &["P1"]));
    assert_eq!(failed, (Vec::new(), None));
    // What the report says of each, with no id and no value.
    assert_eq!(
        pager::ended_said(&[&past, &within]),
        "; it listed P1, which the streams showed ended past its lag: the run's stop, \
         ReadDisagrees (decision 0148 item 8); and P2, which the streams showed ended within its \
         lag (decisions 0109 and 0151 item 4): no stop"
    );
    assert_eq!(pager::ended_said(&[]), "");
    // The last page recorded is the run's stop only when one of ours it listed was past its lag.
    let page = page_read(2, &["P2", "P3"], came);
    let mut run = PagesRun {
        pages: vec![page],
        ended_listed: vec![EndedListed { page: 0, ..within }],
        ..PagesRun::default()
    };
    assert!(!pager::last_disagreed(&run));
    run.ended_listed.push(EndedListed { page: 0, ..past });
    assert!(pager::last_disagreed(&run));
}

#[test]
fn a_page_comes_to_its_names_and_counts_and_a_refusal_is_the_runs_stop() {
    let ours = ours();
    let read = PageAttempt::Read {
        listed: vec![listed(vid(1), CidMatch::Unparseable)],
        twaps: 1,
        full: true,
        kept: "synthetic".to_owned(),
    };
    let (came, stop) = pager::came(&read, &ours);
    assert_eq!(
        came,
        Came::Read {
            ours: vec!["P1"],
            others: 0,
            twaps: 1,
            full: true
        }
    );
    // A TWAP on a page is what the streams did not show: the run's stop (decision 0148 item 8;
    // PR #284's review).
    assert_eq!(stop, Some(places::Stop::ReadDisagrees));
    assert_eq!(came.says(), "listed P1; 0 orders not ours; 1 TWAP; full");
    // A page listing ours alone stops nothing; one listing an order not ours stops the run.
    let clean = PageAttempt::Read {
        listed: vec![listed(vid(1), CidMatch::Unparseable)],
        twaps: 0,
        full: true,
        kept: "synthetic".to_owned(),
    };
    assert_eq!(pager::came(&clean, &ours).1, None);
    let other = PageAttempt::Read {
        listed: vec![listed(vid(5), CidMatch::Foreign(Namespace::new(3)))],
        twaps: 0,
        full: true,
        kept: "synthetic".to_owned(),
    };
    let (came, stop) = pager::came(&other, &ours);
    assert_eq!(stop, Some(places::Stop::ReadDisagrees));
    assert_eq!(
        came.says(),
        "listed none of ours; 1 order not ours; 0 TWAPs; full"
    );
    let unknown = PageAttempt::Read {
        listed: vec![listed(vid(6), CidMatch::Unparseable)],
        twaps: 0,
        full: false,
        kept: "synthetic".to_owned(),
    };
    assert_eq!(
        pager::came(&unknown, &ours).1,
        Some(places::Stop::ReadDisagrees)
    );
    let (came, stop) = pager::came(&PageAttempt::Refused, &ours);
    assert_eq!(came, Came::Refused);
    let pager = places::RefusedOn::Pager;
    assert_eq!(stop, Some(places::Stop::RateRefused { on: pager }));
    assert_eq!(stop.unwrap().name(), "RateRefused(Pager)");
    assert_eq!(pager::came(&PageAttempt::Skipped, &ours).0, Came::Skipped);
    let failed = PageAttempt::Failed("synthetic".to_owned());
    assert_eq!(pager::came(&failed, &ours), (Came::Failed, None));
    // What each says, with no id and no value.
    let none = Came::Read {
        ours: Vec::new(),
        others: 1,
        twaps: 0,
        full: false,
    };
    assert_eq!(
        none.says(),
        "listed none of ours; 1 order not ours; 0 TWAPs; short"
    );
    for (came, said) in [
        (Came::Skipped, "not sent: the shared limiter had no room"),
        (
            Came::Failed,
            "not read: its socket failed or its answer was no page",
        ),
        (
            Came::Refused,
            "a provider rate refusal on the pager: the run's stop",
        ),
    ] {
        assert_eq!(came.says(), said);
    }
}

#[test]
fn the_walk_ends_at_a_short_page_a_repeated_page_or_a_page_not_read() {
    let page = |listed: Vec<Listed>, full| PageAttempt::Read {
        listed,
        twaps: 0,
        full,
        kept: String::new(),
    };
    let one = || vec![listed(vid(1), CidMatch::Unparseable)];
    // A short page ends it; a full page with a row not seen before goes on.
    assert_eq!(
        pager::walk_end(&page(Vec::new(), false), &[]),
        Some(WalkEnd::Short)
    );
    assert_eq!(pager::walk_end(&page(one(), true), &[]), None);
    assert_eq!(pager::walk_end(&page(one(), true), &[vid(2)]), None);
    // A full page listing only rows the walk saw already, as a venue ignoring the offset answers.
    assert_eq!(
        pager::walk_end(&page(one(), true), &[vid(1)]),
        Some(WalkEnd::Repeated)
    );
    // A full page of a TWAP alone lists no order: the walk goes on, cut at its patience.
    let twap = PageAttempt::Read {
        listed: Vec::new(),
        twaps: 1,
        full: true,
        kept: String::new(),
    };
    assert_eq!(pager::walk_end(&twap, &[]), None);
    for attempt in [
        PageAttempt::Skipped,
        PageAttempt::Failed("synthetic".to_owned()),
        PageAttempt::Refused,
    ] {
        assert_eq!(pager::walk_end(&attempt, &[]), Some(WalkEnd::NotRead));
    }
    assert_eq!(pager::WALK_PATIENCE, stage::AFTER_PATIENCE);
    for (end, said) in [
        (WalkEnd::Short, "a short page, its end"),
        (
            WalkEnd::Cut,
            "cut 60 s after its first page (the reads after a session's patience)",
        ),
        (WalkEnd::NotRead, "a page that could not be made"),
    ] {
        assert_eq!(end.says(), said);
    }
    assert!(
        WalkEnd::Repeated
            .says()
            .starts_with("a full page listing an order")
    );
}

/// A page of stage three, asked at `offset` with `open` open, that came to `came`.
fn page_read(offset: u32, open: &[&'static str], came: Came) -> PageRead {
    PageRead {
        walk: false,
        size: 1,
        offset,
        open: open.to_vec(),
        came,
    }
}

/// A read page listing `ours` alone, full.
fn listing(ours: &[&'static str]) -> Came {
    Came::Read {
        ours: ours.to_vec(),
        others: 0,
        twaps: 0,
        full: true,
    }
}

#[test]
fn page_zero_shows_the_order_qfexs_pages_list_ours_in() {
    let oldest = [page_read(0, &["P1", "P2"], listing(&["P1"]))];
    assert_eq!(pager::order_shown(&oldest), Some("oldest first"));
    let newest = [page_read(0, &["P1", "P2"], listing(&["P2"]))];
    assert_eq!(pager::order_shown(&newest), Some("newest first"));
    // Not shown: no page 0, P1 and P2 not both open as it was asked, a page not read, or one
    // listing neither alone.
    assert_eq!(pager::order_shown(&[]), None);
    let walk = PageRead {
        walk: true,
        ..oldest[0].clone()
    };
    assert_eq!(pager::order_shown(&[walk]), None);
    let one_open = [page_read(0, &["P1"], listing(&["P1"]))];
    assert_eq!(pager::order_shown(&one_open), None);
    let skipped = [page_read(0, &["P1", "P2"], Came::Skipped)];
    assert_eq!(pager::order_shown(&skipped), None);
    let neither = [page_read(0, &["P1", "P2"], listing(&[]))];
    assert_eq!(pager::order_shown(&neither), None);
}

#[test]
fn the_report_and_the_evidence_give_stage_three_by_names_sizes_and_offsets_alone() {
    let run = |account| PagesRun {
        session: stage_two::CrossRun {
            account,
            authenticated: true,
            arm: Some("Accepted".to_owned()),
            resynced: true,
            started: true,
            ended_by: Some(stage::GRACEFUL),
            ..stage_two::CrossRun::default()
        },
        orders: vec![("P1", places::XRun::default())],
        cancels: vec![
            pager::CancelRun {
                name: "P1",
                sent: true,
                outcome: Some("Accepted".to_owned()),
                ended: true,
                ..pager::CancelRun::default()
            },
            pager::CancelRun {
                name: "P2",
                not_sent: Some(pager::AWAITING_ACK),
                ..pager::CancelRun::default()
            },
        ],
        pages: vec![
            page_read(0, &["P1", "P2"], listing(&["P1"])),
            PageRead {
                walk: true,
                ..page_read(0, &[], Came::Skipped)
            },
        ],
        walk: Some(WalkEnd::NotRead),
        frames: vec!["synthetic frame".to_owned()],
        ..PagesRun::default()
    };
    let runs = [run("primary"), run("subaccount")];
    let mut report = String::new();
    pager::markdown(&mut report, &runs, (&["primary", "subaccount"], true));
    for said in [
        "## Stage three (FBC-4plj, decision 0132 item 7)",
        "### Stage three on primary",
        "- The order QFEX's pages list ours in, as page 0 showed it: oldest first",
        "- The walk from offset 0: a page that could not be made",
        "- P1's cancel: handed to the session yes; its outcome Accepted; its order seen to end yes",
        &format!(
            "- P2's cancel: handed to the session no ({}); its outcome none reported",
            pager::AWAITING_ACK
        ),
        "- A page at size 1, offset 0 (open in the streams: P1, P2): listed P1; 0 orders not \
         ours; 0 TWAPs; full",
        "- The walk's page at size 1, offset 0 (open in the streams: none): not sent",
        &format!("- What followed: {}", pager::NEXT_ACCOUNT),
        &format!("- What followed: {}", pager::LAST_PAGES),
    ] {
        assert!(report.contains(said), "{said}: {report}");
    }
    // No frame the pager read reaches the report or the evidence.
    assert!(!report.contains("synthetic frame"), "{report}");
    let json = pager::to_json(&runs);
    assert!(!json.to_string().contains("synthetic frame"));
    assert_eq!(json[0]["page_order"], "oldest first");
    assert_eq!(json[0]["pages"][0]["offset"], 0);
    assert_eq!(json[0]["pages"][0]["size"], 1);
    assert_eq!(json[0]["pages"][1]["walk"], true);
    assert_eq!(json[1]["cancels"][1]["not_sent"], pager::AWAITING_ACK);
    // A session that stopped the run says so; one the run ended after, before the next
    // account's crossing session, says the run ended; one with a later account's crossing
    // session, the next account.
    let stopped = PagesRun {
        session: stage_two::CrossRun {
            stop: Some("RateRefused(Pager)"),
            ..run("primary").session
        },
        ..PagesRun::default()
    };
    let mut report = String::new();
    pager::markdown(&mut report, &[stopped], (&["primary"], false));
    assert!(
        report.contains("- What followed: the run's stop, RateRefused(Pager)"),
        "{report}"
    );
    let mut report = String::new();
    pager::markdown(&mut report, &[run("primary")], (&["primary"], false));
    assert!(
        report.contains(&format!("- What followed: {}", taker::RUN_ENDED)),
        "{report}"
    );
}

#[test]
fn an_order_resting_after_stage_three_is_said_as_stage_threes() {
    let run = |resting, counted| stage_two::CrossRun {
        resting,
        counted_resting: counted,
        read: true,
        ..stage_two::CrossRun::default()
    };
    let pages = stage_two::Session::Pages;
    assert_eq!(
        stage_two::hand_cancel(&run(true, false), pages),
        Some(pager::HAND_CANCEL_PAGES)
    );
    assert_eq!(
        stage_two::hand_cancel(&run(false, true), pages),
        Some(pager::HAND_CANCEL_PAGES_COUNTED)
    );
    assert_eq!(stage_two::hand_cancel(&run(false, false), pages), None);
    // The reads after it name stage three's session, and an order or a TWAP not ours is
    // ReadDisagrees.
    let (stop, when) = stage::stage_read_stop(pages, (None, true), (false, false, false)).unwrap();
    assert_eq!(
        (stop, when),
        (
            places::Stop::Interrupted,
            "during the reads after the stage-three session"
        )
    );
    let not_read = stage::stage_read_stop(pages, (None, false), (false, false, true));
    let said = "after the stage-three session, a named account not read after it";
    assert_eq!(not_read, Some((places::Stop::LeftResting, said)));
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::Rest,
    };
    let refused = stage::stage_read_stop(pages, (Some(rate), false), (true, true, true));
    assert_eq!(
        refused,
        Some((rate, "in the reads after the stage-three session"))
    );
    let resting = stage::stage_read_stop(pages, (None, false), (false, true, false));
    assert_eq!(
        resting,
        Some((places::Stop::LeftResting, refusals::AFTER_PAGES))
    );
    let not_flat = stage::stage_read_stop(pages, (None, false), (true, false, false));
    assert_eq!(
        not_flat,
        Some((places::Stop::NotFlat, refusals::AFTER_PAGES))
    );
    let others = stage::stage_after_stop(pages, (None, false), (false, true), (false, false));
    assert_eq!(
        others,
        Some((places::Stop::ReadDisagrees, pager::PAGES_OTHERS))
    );
    assert_eq!(
        refusals::read_when(refusals::AFTER_PAGES),
        refusals::AFTER_PAGES
    );
}

#[test]
fn stage_threes_list_holds_three_orders_and_refuses_a_fourth() {
    // The pages registry's own list: P1, P2 and P3 at the minimum size, and no fourth
    // (decision 0148 item 7).
    let lists = ledger::PlaceLists::new(Lots::new(1).unwrap(), 1).unwrap();
    let mut allowance = places::Allowance::new(&lists);
    for _ in 0..3 {
        assert_eq!(allowance.take(ledger::Step::Pages, ledger::PAGED), Ok(()));
    }
    let refused = allowance.take(ledger::Step::Pages, ledger::PAGED);
    assert_eq!(refused, Err(places::Stop::BeyondList));
}
