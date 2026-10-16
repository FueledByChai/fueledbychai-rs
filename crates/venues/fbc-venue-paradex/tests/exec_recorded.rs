//! FBC-8xr: the Paradex exec decoders on the frames recorded on Paradex testnet by the owner-run
//! acceptance program (`paradex_testnet_acceptance`, FBC-xji8), committed scrubbed under
//! `fixtures/paradex/exec-recorded/<run>/fixtures/` beside each run's `report.md`.
//!
//! Per run: every SBE `OrderEvent` decodes, in the order received, under one
//! [`ModifyRequests`]; every JSON-RPC reply the venue sent decodes as the answer to the request
//! the run sent under its id (the requests themselves are not recorded, as they carry order
//! signatures: [`RUNS`] restates them from the report's step table), and every subscription or
//! login answer is not the order tracker's; every `GET /orders` and `GET /positions` answer
//! decodes as a resync; and every `GET /orders-history?client_id=` answer decodes as the query
//! for the order it names. The client and venue ids are read from the report's order lines.

mod md;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use fbc_core::{
    AmendOrder, CancelOrder, CancelReason, CidMatch, ClientOrderId, ExecEvent, ExecSink,
    InstrumentId, Lots, Namespace, NewOrder, OrderKind, OrderRef, QueryOrder, RpcId, Side,
    SizeStep, SpecTable, SubmitOutcome, Ticks, Tif, VenueCommand, VenueMeta, VenueOrderId,
    VenueOrderState, WallNs, dispatch,
};
use fbc_core::{Channel, PriceGrid};
use fbc_venue_paradex::exec::{
    ModifyRequests, ParadexReplies, ReplyRead, decode_order_event, decode_order_query,
    decode_resync,
};
use fbc_venue_paradex::factory::caps;
use rust_decimal::Decimal;
use serde_json::Value;

/// The engine namespace the acceptance program minted its client ids in (`01109200-...`).
const OWN: Namespace = Namespace::new(0x1092);
const ETH: InstrumentId = InstrumentId::new(2);
const HYPE: InstrumentId = InstrumentId::new(3);
const EMPTY: &[u8] = br#"{"results":[]}"#;

/// What the run sent under one JSON-RPC id, naming its orders by the report's labels.
enum Sent {
    Place(&'static str),
    Batch(&'static [&'static str]),
    Amend(&'static str),
    Cancel(&'static str),
    CancelMany(&'static [&'static str]),
    ArmCancelOnDisconnect,
}

struct Run {
    id: &'static str,
    inst: InstrumentId,
    sent: &'static [(u64, Sent)],
}

/// The three recorded runs and, per run, the requests its report's step table lists (the
/// `cancel-on-disconnect` arms are the ids whose reply is `{"enabled":true}`: one per
/// connection, before its first order).
const RUNS: &[Run] = &[
    Run {
        id: "20261015T222347Z",
        inst: ETH,
        sent: &[
            (1, Sent::ArmCancelOnDisconnect),
            (2, Sent::Place("B1")),
            (3, Sent::Amend("B1")),
            (4, Sent::Cancel("B1")),
            (5, Sent::Place("IOC")),
            (6, Sent::Place("B3")),
            (7, Sent::ArmCancelOnDisconnect),
            (8, Sent::Place("B4")),
            (9, Sent::ArmCancelOnDisconnect),
        ],
    },
    Run {
        id: "20261016T115634Z",
        inst: HYPE,
        sent: &[
            (1, Sent::ArmCancelOnDisconnect),
            (2, Sent::Place("B1")),
            (3, Sent::Amend("B1")),
            (4, Sent::Cancel("B1")),
            (5, Sent::Batch(&["B2 buy", "S2 sell"])),
            (6, Sent::CancelMany(&["B2 buy"])),
            (7, Sent::ArmCancelOnDisconnect),
        ],
    },
    Run {
        id: "20261016T120633Z",
        inst: HYPE,
        sent: &[
            (1, Sent::ArmCancelOnDisconnect),
            (2, Sent::Place("B1")),
            (3, Sent::Amend("B1")),
            (4, Sent::Cancel("B1")),
            (5, Sent::Batch(&["B2 buy", "S2 sell"])),
            (6, Sent::CancelMany(&["B2 buy", "S2 sell"])),
            (7, Sent::Place("IOC")),
            (8, Sent::Place("T1 take")),
            (9, Sent::Place("B3")),
            (10, Sent::ArmCancelOnDisconnect),
            (11, Sent::Place("B4")),
            (12, Sent::ArmCancelOnDisconnect),
        ],
    },
];

fn run_dir(id: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/exec-recorded")
        .join(id)
}

/// The run's fixture files, by name, in the order received (their names start with it).
fn fixtures(id: &str) -> Vec<(String, Vec<u8>)> {
    let dir = run_dir(id).join("fixtures");
    let mut names: Vec<String> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n != "README.md")
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|n| {
            let bytes = fs::read(dir.join(&n)).unwrap();
            (n, bytes)
        })
        .collect()
}

/// A frame written as `fixtures/paradex/exec/` writes one: hex bytes, `#` to the end of a line
/// a comment.
fn hex(text: &[u8]) -> Vec<u8> {
    std::str::from_utf8(text)
        .unwrap()
        .lines()
        .flat_map(|l| l.split('#').next().unwrap().split_whitespace())
        .map(|b| u8::from_str_radix(b, 16).unwrap())
        .collect()
}

/// ETH-USD-PERP and HYPE-USD-PERP on testnet's grids: 0.01 x 0.0001 and 0.0001 x 0.01, the
/// grids every price and size the runs recorded lies on.
fn specs() -> SpecTable {
    let mut eth = md::spec(ETH, "ETH-USD-PERP");
    eth.price_grid = PriceGrid::fixed(Decimal::new(1, 2)).unwrap();
    eth.size_step = SizeStep::new(Decimal::new(1, 4)).unwrap();
    let mut hype = md::spec(HYPE, "HYPE-USD-PERP");
    hype.price_grid = PriceGrid::fixed(Decimal::new(1, 4)).unwrap();
    hype.size_step = SizeStep::new(Decimal::new(1, 2)).unwrap();
    let mut table = SpecTable::new();
    table.insert(eth);
    table.insert(hype);
    table
}

/// Every order the report names: label -> (client id, venue id), from its lines
/// `- order <label> (client id <cid>, venue id <vid>): ...`.
fn orders(id: &str) -> BTreeMap<String, (ClientOrderId, VenueOrderId)> {
    let report = fs::read_to_string(run_dir(id).join("report.md")).unwrap();
    let mut out = BTreeMap::new();
    for line in report.lines() {
        let Some(rest) = line.strip_prefix("- order ") else {
            continue;
        };
        let (label, rest) = rest.split_once(" (client id ").unwrap();
        let (cid, rest) = rest.split_once(", venue id ").unwrap();
        let (vid, _) = rest.split_once(')').unwrap();
        let ids = dispatch(&caps(), OWN, |scope| {
            let CidMatch::Ours(cid) = scope.client_order_id(cid) else {
                panic!("{id}: {cid} is not minted in the program's namespace");
            };
            (cid, scope.venue_order_id(vid).unwrap())
        });
        out.insert(label.to_owned(), ids);
    }
    out
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, ev: ExecEvent) {
        self.0.push(ev);
    }
}

fn new_order(cid: ClientOrderId, inst: InstrumentId) -> NewOrder {
    NewOrder {
        cid,
        inst,
        side: Side::Buy,
        qty: Lots::new(1).unwrap(),
        kind: OrderKind::Limit { px: Ticks(1) },
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

fn cancel(target: OrderRef, inst: InstrumentId) -> CancelOrder {
    CancelOrder {
        target,
        inst,
        side: Side::Buy,
        placement_nonce: None,
    }
}

/// The command `sent` restates, with its orders' ids.
fn command(
    sent: &Sent,
    inst: InstrumentId,
    ids: &BTreeMap<String, (ClientOrderId, VenueOrderId)>,
) -> VenueCommand {
    let both = |label: &str| {
        let (cid, vid) = ids.get(label).unwrap_or_else(|| panic!("no order {label}"));
        OrderRef::Both(*cid, vid.clone())
    };
    let cid = |label: &str| ids[label].0;
    match sent {
        Sent::Place(l) => VenueCommand::Place(new_order(cid(l), inst)),
        Sent::Batch(ls) => {
            VenueCommand::PlaceBatch(ls.iter().map(|l| new_order(cid(l), inst)).collect())
        }
        Sent::Amend(l) => VenueCommand::Amend(AmendOrder {
            target: both(l),
            inst,
            side: Side::Buy,
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(1),
            qty: Lots::new(1).unwrap(),
            cum_filled: Lots::ZERO,
        }),
        Sent::Cancel(l) => VenueCommand::Cancel(cancel(both(l), inst)),
        Sent::CancelMany(ls) => {
            VenueCommand::CancelMany(ls.iter().map(|l| cancel(both(l), inst)).collect())
        }
        Sent::ArmCancelOnDisconnect => VenueCommand::ArmCancelOnDisconnect(true),
    }
}

/// The JSON-RPC id of a reply frame.
fn rpc_id(bytes: &[u8]) -> Option<u64> {
    let v: Value = serde_json::from_slice(bytes).unwrap();
    v.get("id").and_then(Value::as_u64)
}

/// Ids of the read-only session's login and subscription requests (2^52 and up), which the
/// order tracker never holds.
const CONTROL_FLOOR: u64 = 1 << 52;

fn check_run(run: &Run) -> usize {
    let specs = specs();
    let ids = orders(run.id);
    assert!(!ids.is_empty(), "{}: the report names no order", run.id);
    let sent: BTreeMap<u64, &Sent> = run.sent.iter().map(|(n, s)| (*n, s)).collect();
    let mut seen = ModifyRequests::new();
    let mut replies = ParadexReplies::new();
    let mut checked = 0;
    for (name, bytes) in fixtures(run.id) {
        let at = |what: &str| format!("{}/{name}: {what}", run.id);
        if name.ends_with(".sbe.txt") {
            let frame = hex(&bytes);
            let mut sink = Sink::default();
            dispatch(&caps(), OWN, |scope| {
                decode_order_event(&frame, scope, &specs, &mut seen, &mut sink)
            })
            .unwrap_or_else(|e| panic!("{}: {e:?}", at("OrderEvent")));
            let update = sink
                .0
                .iter()
                .find_map(|ev| match ev {
                    ExecEvent::Order(u) => Some(u),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{}", at("no order update")));
            if matches!(update.state, VenueOrderState::Canceled(_)) {
                assert_eq!(update.state, closed_as(&frame), "{}", at("closed state"));
            }
        } else if name.ends_with("-frame.json") {
            let text = std::str::from_utf8(&bytes).unwrap();
            let id = rpc_id(&bytes);
            let mut sink = Sink::default();
            let read = match id {
                Some(n) if n < CONTROL_FLOOR => {
                    let cmd = sent.get(&n).unwrap_or_else(|| panic!("{}", at("not sent")));
                    replies.sent(RpcId(n), &command(cmd, run.inst, &ids));
                    dispatch(&caps(), OWN, |scope| {
                        replies.on_reply(text, scope, &mut sink)
                    })
                }
                _ => dispatch(&caps(), OWN, |scope| {
                    replies.on_reply(text, scope, &mut sink)
                }),
            };
            match id {
                Some(n) if n < CONTROL_FLOOR => {
                    assert_eq!(read, Ok(ReplyRead::Decoded), "{}", at("reply"));
                    assert!(!sink.0.is_empty(), "{}", at("reply pushed nothing"));
                    for ev in &sink.0 {
                        let ExecEvent::Outcome { rpc, outcome, .. } = ev else {
                            panic!("{}: {ev:?}", at("not an outcome"));
                        };
                        assert_eq!(*rpc, RpcId(n), "{}", at("outcome of another request"));
                        assert!(
                            matches!(outcome, SubmitOutcome::Accepted { .. }),
                            "{}: {outcome:?}",
                            at("not accepted")
                        );
                    }
                }
                _ => {
                    assert_eq!(read, Ok(ReplyRead::NotOurs), "{}", at("control answer"));
                    assert!(sink.0.is_empty(), "{}", at("control answer pushed"));
                }
            }
        } else if let Some(rest) = name.split_once("-rest-orders-history-").map(|(_, r)| r) {
            let label = rest.trim_end_matches(".json").replace('-', " ");
            let (cid, vid) = ids
                .get(&label)
                .unwrap_or_else(|| panic!("{}", at("no such order")));
            let query = QueryOrder {
                target: OrderRef::Client(*cid),
                inst: run.inst,
                placement_nonce: None,
            };
            let answer = dispatch(&caps(), OWN, |scope| {
                decode_order_query(RpcId(1), &query, &bytes, scope, &specs)
            })
            .unwrap_or_else(|e| panic!("{}: {e:?}", at("orders-history")));
            // Every history read the program kept found its order (a read that found none was
            // retried, and only the last read is recorded).
            let [(_, ExecEvent::QueryResult(answer))] = answer.events() else {
                panic!("{}", at("not one query result"));
            };
            let found = answer
                .found()
                .unwrap_or_else(|| panic!("{}", at("order not found")));
            assert_eq!(&found.vid, vid, "{}", at("another order"));
            assert_eq!(found.inst, run.inst, "{}", at("another market"));
            if matches!(found.state, VenueOrderState::Canceled(_)) {
                assert_eq!(found.state, closed_as(&bytes), "{}", at("closed state"));
            }
        } else if name.contains("-rest-orders-") {
            decode(&bytes, EMPTY, &specs).unwrap_or_else(|e| panic!("{}: {e}", at("orders")));
        } else if name.contains("-rest-positions-") {
            decode(EMPTY, &bytes, &specs).unwrap_or_else(|e| panic!("{}: {e}", at("positions")));
        } else if name.ends_with("-rest-resync.json") {
            // The session's own resync reads, GET /orders then GET /positions: each decodes as
            // either.
            decode(&bytes, EMPTY, &specs).unwrap_or_else(|e| panic!("{}: {e}", at("resync")));
            decode(EMPTY, &bytes, &specs).unwrap_or_else(|e| panic!("{}: {e}", at("resync")));
        } else {
            panic!("{}", at("a fixture no decoder reads"));
        }
        checked += 1;
    }
    checked
}

/// The cancel reasons the runs recorded, each with the state a CLOSED order with nothing filled
/// stating it is in (decision 0093): ours for our cancels, the venue's cancel-on-disconnect, an
/// IOC's unfilled rest, and the venue's refusals.
const REASONS: [(&str, VenueOrderState); 6] = [
    (
        "user_cancel_batch_ws",
        VenueOrderState::Canceled(CancelReason::Requested),
    ),
    (
        "user_cancel_ws",
        VenueOrderState::Canceled(CancelReason::Requested),
    ),
    (
        "cancel_on_disconnect",
        VenueOrderState::Canceled(CancelReason::Disconnect),
    ),
    (
        "REMAINING_IOC_CANCEL",
        VenueOrderState::Canceled(CancelReason::Unfilled),
    ),
    (
        "EXCEEDS_PRICE_BAND",
        VenueOrderState::Canceled(CancelReason::Venue),
    ),
    (
        "ORDER_SIZE_BELOW_MIN",
        VenueOrderState::Canceled(CancelReason::Venue),
    ),
];

/// The state of the closed order `bytes` states, by the first of [`REASONS`] it carries.
fn closed_as(bytes: &[u8]) -> VenueOrderState {
    REASONS
        .iter()
        .find(|(reason, _)| bytes.windows(reason.len()).any(|w| w == reason.as_bytes()))
        .map(|(_, state)| state.clone())
        .expect("a cancelled order states one of the recorded reasons")
}

fn decode(orders: &[u8], positions: &[u8], specs: &SpecTable) -> Result<(), String> {
    dispatch(&caps(), OWN, |scope| {
        decode_resync(WallNs(0), orders, positions, scope, specs)
    })
    .map(|_| ())
    .map_err(|e| format!("{e:?}"))
}

#[test]
fn eth_place_amend_cancel_ioc_and_cancel_on_disconnect_decode() {
    assert!(check_run(&RUNS[0]) > 0);
}

#[test]
fn hype_first_bare_array_batch_decodes() {
    assert!(check_run(&RUNS[1]) > 0);
}

#[test]
fn hype_batch_cancel_many_and_price_band_ioc_decode() {
    assert!(check_run(&RUNS[2]) > 0);
}

/// Every run directory is one of [`RUNS`], with its report beside its fixtures.
#[test]
fn every_recorded_run_is_checked() {
    let root = run_dir("");
    let mut dirs: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_dir())
        .map(|e| e.file_name().into_string().unwrap())
        .collect();
    dirs.sort();
    let listed: Vec<&str> = RUNS.iter().map(|r| r.id).collect();
    assert_eq!(dirs, listed);
    for id in listed {
        assert!(Path::new(&run_dir(id).join("report.md")).is_file(), "{id}");
    }
}
