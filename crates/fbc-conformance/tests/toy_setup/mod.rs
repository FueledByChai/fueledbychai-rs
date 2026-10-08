//! What the conformance toy's fixtures (`fixtures/conformance-toy`) assume, shared by the suite's
//! tests on the toy: its two instruments, no configuration, no credentials, the golden
//! commands whose encodings `signing_golden/` holds, its order-entry stream, which the case
//! files' frames are handed on, and how its order entry answers over the stub server
//! ([`order_entry`]).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use fbc_conformance::suite::{Answer, Golden, OrderEntryStub, Replier, Setup};
use fbc_conformance::toy::{self, INST_A, INST_B, OWN_NS, TOY_TOKEN};
use fbc_conformance::{Frame, HttpRouter, Responded, Responder, StubServer};
use fbc_core::{
    AccountKey, AmendOrder, CancelOrder, Channel, CidMint, ClientOrderId, EncodeCtx, Lots, MonoNs,
    NamespaceLease, NewOrder, NonceBlock, OrderKind, OrderRef, RpcId, Secrets, Side, Ticks, TifTag,
    VenueCommand, VenueConfig, VenueOrderId, WallNs,
};

/// The toy's fixture directory.
pub const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/conformance-toy"
);

/// The wall time the goldens are signed at.
const WALL: i64 = 1_759_363_200_000_000_000;

/// What the toy's fixtures assume.
pub fn assumed() -> Setup {
    Setup {
        specs: toy::specs(),
        cfg: VenueConfig::new(),
        creds: Secrets::new(),
        goldens: goldens(),
        exec_stream: toy::EXEC_STREAM,
        order_entry: Some(order_entry()),
    }
}

/// How the toy's order entry answers over the stub server: its order-entry URL pointed at the
/// stub's socket, nothing read over HTTP (it resyncs in frames), the opening answered (the
/// authentication acknowledged with the toy's token, the cancel-on-disconnect arm accepted, the
/// resync begun at its own instant and ended with nothing resting), and each request's items
/// answered as asked ([`replier`]).
pub fn order_entry() -> OrderEntryStub {
    OrderEntryStub {
        point: point_at,
        http: HttpRouter::new(),
        opening: vec![
            answer("auth", |_| Ok(vec![format!("auth|ok=1|token={TOY_TOKEN}")])),
            answer("cod", |r| {
                Ok(vec![format!("item|rpc={}|i=0|res=ok", field(r, "rpc")?)])
            }),
            answer("resync", |r| {
                Ok(vec![
                    format!("rsbegin|wm={}", field(r, "ts")?),
                    "rsend".into(),
                ])
            }),
        ],
        reply: replier(true),
    }
}

/// The toy's order-entry URL at the stub's socket.
pub fn point_at(cfg: &mut VenueConfig, stub: &StubServer) {
    cfg.insert(toy::EXEC_URL_KEY, &stub.ws_url("/exec"));
}

/// A responder answering a frame of `kind` with the records `with` computes from it.
fn answer(kind: &'static str, with: fn(&str) -> Result<Vec<String>, String>) -> Responder {
    Responder::new(move |frame| {
        let record = text(frame)?;
        if record.split('|').next() != Some(kind) {
            let read = record.split('|').next().unwrap_or_default();
            return Err(format!("expected {kind}, read {read}"));
        }
        Ok(with(record)?.into_iter().map(Frame::Text).collect())
    })
}

/// The text of a text frame.
fn text(frame: &Frame) -> Result<&str, String> {
    match frame {
        Frame::Text(text) => Ok(text),
        Frame::Binary(_) => Err("a binary frame".into()),
    }
}

/// The value of `key` in a `kind|key=value|...` record.
fn field<'a>(record: &'a str, key: &str) -> Result<&'a str, String> {
    let kv = record
        .split('|')
        .skip(1)
        .filter_map(|kv| kv.split_once('='));
    let value = kv.into_iter().find_map(|(k, v)| (k == key).then_some(v));
    // The record's kind only: a frame may carry a token, and the reason reaches the failure.
    let kind = record.split('|').next().unwrap_or_default();
    value.ok_or_else(|| format!("no {key} in the {kind} record"))
}

/// What the toy venue remembers between requests: the client id of each order it accepted, by
/// the venue id it gave it, and its last venue id and event sequence.
#[derive(Default)]
struct Book {
    cids: HashMap<String, String>,
    last_vid: u64,
    seq: u64,
}

impl Book {
    fn vid(&mut self) -> String {
        self.last_vid += 1;
        format!("toy-{}", self.last_vid)
    }
}

/// The toy venue's answer to a placement, a batch of placements or an amend, each item as
/// asked: accepted (a placement under a venue id it gives it; an amend with, where `events`, the
/// order event reporting the replaced order under a new venue id, as `AmendAck::ReplacedEvent`
/// has it), rejected with code 1002 (an invalid price), or left unanswered.
pub fn replier(events: bool) -> Replier {
    let book = Arc::new(Mutex::new(Book::default()));
    Replier::new(move |frame, answers| {
        let mut book = book.lock().unwrap();
        reply(&mut book, text(frame)?, answers, events)
    })
}

fn reply(book: &mut Book, request: &str, answers: &[Answer], events: bool) -> Responded {
    let mut lines = request.lines();
    let head = lines.next().unwrap_or_default();
    let rpc = field(head, "rpc")?;
    let items: Vec<&str> = match head.split('|').next() {
        Some("place" | "amend") => vec![head],
        Some("batch") => lines.collect(),
        _ => {
            let kind = head.split('|').next().unwrap_or_default();
            return Err(format!("not a request the toy answers: {kind}"));
        }
    };
    if items.len() != answers.len() {
        let n = answers.len();
        return Err(format!("{} items, {n} answers: {request}", items.len()));
    }
    let mut out = Vec::new();
    for (i, (item, answer)) in items.iter().zip(answers).enumerate() {
        let amend = item.starts_with("amend|");
        match answer {
            Answer::Accept if amend => {
                out.push(format!("item|rpc={rpc}|i={i}|res=ok"));
                if events {
                    out.push(amended(book, item)?);
                }
            }
            Answer::Accept => {
                let vid = book.vid();
                book.cids
                    .insert(vid.clone(), field(item, "cid")?.to_owned());
                out.push(format!("item|rpc={rpc}|i={i}|res=ok|vid={vid}"));
            }
            Answer::Reject => {
                out.push(format!(
                    "item|rpc={rpc}|i={i}|res=rej|code=1002|msg=refused"
                ));
            }
            Answer::Silent => {}
        }
    }
    Ok(out.into_iter().map(Frame::Text).collect())
}

/// The order event reporting the order `amend` names replaced, under a new venue id.
fn amended(book: &mut Book, amend: &str) -> Result<String, String> {
    let vid = field(amend, "vid")?;
    let cid = book.cids.get(vid).cloned();
    let cid = cid.ok_or_else(|| format!("no order {vid} to amend"))?;
    let nvid = book.vid();
    book.cids.insert(nvid.clone(), cid.clone());
    book.seq += 1;
    let f = |key| field(amend, key);
    Ok(format!(
        "order|cid={cid}|vid={vid}|sym={}|side={}|st=amended|nvid={nvid}|cum=0|px={}|qty={}|po={}|ro={}|seq={}",
        f("sym")?,
        f("side")?,
        f("px")?,
        f("qty")?,
        f("po")?,
        f("ro")?,
        book.seq
    ))
}

/// One golden command for each kind of request the toy signs: a placement, a batch
/// placement, an amend, a cancel and a batch cancel.
pub fn goldens() -> Vec<Golden> {
    vec![
        golden("place", 21, &[500], VenueCommand::Place(order(0, INST_A))),
        golden(
            "place-batch",
            22,
            &[501, 502],
            VenueCommand::PlaceBatch(vec![order(1, INST_A), order(2, INST_B)]),
        ),
        golden(
            "amend",
            23,
            &[503],
            VenueCommand::Amend(amend(OrderRef::Venue(vid("toy-7001")))),
        ),
        golden(
            "cancel",
            24,
            &[504],
            VenueCommand::Cancel(cancel(OrderRef::Client(cid(0)), None)),
        ),
        golden(
            "cancel-batch",
            25,
            &[505, 506],
            VenueCommand::CancelMany(vec![
                cancel(OrderRef::Venue(vid("toy-7002")), None),
                cancel(OrderRef::Client(cid(2)), Some(502)),
            ]),
        ),
    ]
}

fn golden(name: &'static str, rpc: u64, nonces: &[u64], cmd: VenueCommand) -> Golden {
    Golden {
        name,
        rpc: RpcId(rpc),
        ctx: EncodeCtx {
            wall: WallNs(WALL),
            mono: MonoNs(1),
            nonces: NonceBlock::new(nonces.to_vec()),
        },
        cmd,
    }
}

/// A post-only buy of 25 lots at 130 865 ticks (13 086.5 on the toy's grid of 0.5).
fn order(n: usize, inst: fbc_core::InstrumentId) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst,
        side: Side::Buy,
        qty: Lots::new(25).unwrap(),
        kind: OrderKind::Limit { px: Ticks(130_865) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// An amend to 10 lots at 130 860 ticks of an order 4 lots of which have filled.
fn amend(target: OrderRef) -> AmendOrder {
    AmendOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(130_860),
        qty: Lots::new(10).unwrap(),
        cum_filled: Lots::new(4).unwrap(),
    }
}

fn cancel(target: OrderRef, placement_nonce: Option<u64>) -> CancelOrder {
    CancelOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        placement_nonce,
    }
}

fn vid(wire: &str) -> VenueOrderId {
    toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory at a
/// fixed time, so every run mints the same ids.
fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("fbc-conformance-goldens-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..3).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[n]
}
