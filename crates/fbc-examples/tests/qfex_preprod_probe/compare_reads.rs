//! FBC-23ch: the place stages' comparison reads ([`compare::Socket`]) and the QFEX model's trade
//! sockets ([`super::model`]) they read, against the model on 127.0.0.1 under `production_probe`
//! with loopback origins. Declared by path in `tests/qfex_preprod_probe.rs`, so it is part of
//! that test binary and Cargo builds no target of its own for it. Nothing places on a venue, and
//! every symbol, key, id and value is made up.
//!
//! - every frame and request of a read is charged to the shared limiter first, and one it has no
//!   room for is not sent and skips that read, not waited for (decision 0135 item 1);
//! - a read pages `get_user_orders` from offset 0 until a page that is not full, decodes it under
//!   the run's namespace, and reads the REST position on the traded symbol, on one socket across
//!   the comparison's reads;
//! - a `RateLimited` page holds the shared budget and is a refusal on the read socket, a REST 429
//!   one on REST, and a failed REST read fails that read alone;
//! - a comparison against the model times its first agreeing read past the model's lag;
//! - the model answers the arm, a place, a cancel and the two pages as QFEX does, cancels an
//!   armed connection's account's orders as its knobs say once it ends, and sends each knob's
//!   frames.

use super::*;

use std::str::FromStr;

use compare::{Attempt, Cadence, Ended, Io, Ours, Read, Reader, Seen, Socket, Streams, Took};
use fbc_core::{
    CidMatch, ClientOrderId, EncodeCtx, LimitScope, Namespace, NonceSource, OpKind, RateLimit,
    TagSet, VenueCaps, VenueOrderId, dispatch,
};
use fbc_venue_qfex::account_reads::PAGE_WEIGHT;
use fbc_venue_qfex::auth::{AccountSelection, ReadAuth};
use fbc_venue_qfex::exec::CLIENT_ID_FORMAT;
use fbc_venue_qfex::sign::QfexNonces;
use futures_util::{SinkExt, StreamExt};
use model::{Limited, Order, QfexModel, SYMBOL, State, Venue};
use places::RefusedOn;
use rust_decimal::Decimal;

/// The namespace the reads decode ours by: the place-stage command line's `--namespace`.
const NS: Namespace = Namespace::new(7);

/// What a read needs from the run, built as the run builds it on the model's origins.
struct Bench {
    venue: Venue,
    cfg: VenueConfig,
    reader: ReadAuth,
    caps: VenueCaps,
    connector: fbc_runtime::Connector,
    _removed: Removed,
}

impl Bench {
    async fn start(model: QfexModel) -> Bench {
        let (dir, removed) = temp_dir("compare-reads");
        let venue = Venue::start(model).await;
        let (opts, _) = place_stages::parsed(&place_stages::argv(&dir, &venue)).unwrap();
        let env = environment();
        let lookup = |var: &str| env.get(var).cloned();
        let base = stage::base_config(&opts);
        let (cfg, _) = stage::configured(&base, &lookup, Armed::Primary).unwrap();
        let reader = auth_qfex::reader(&lookup, AccountSelection::Primary).unwrap();
        let caps = Qfex.caps(&cfg).unwrap();
        Bench {
            venue,
            cfg,
            reader,
            caps,
            connector: fbc_runtime::Connector::new(ProxyConfig::Direct),
            _removed: removed,
        }
    }

    /// A limiter for the probe's provider user, as the run builds its own, under a user of the
    /// test's own.
    fn limiter(&self) -> RateLimiter {
        let limits = self.caps.exec_limits.clone().unwrap_or_default();
        RateLimiter::for_user(user(), &limits, SafetyReserve::percent(10).unwrap()).unwrap()
    }

    fn io<'a>(&'a self, limiter: &'a RateLimiter, page_size: u16) -> Io<'a> {
        Io {
            connector: &self.connector,
            limiter,
            caps: &self.caps,
            ns: NS,
            page_size,
            connect_wait: Duration::from_secs(5),
            read_wait: Duration::from_secs(5),
            symbol: SYMBOL,
        }
    }

    fn model(&self) -> std::sync::MutexGuard<'_, QfexModel> {
        self.venue.model.lock().unwrap()
    }

    /// The frames the client sent on trade connection `conn`, as text.
    fn received(&self, conn: usize) -> Vec<String> {
        let conns = self.venue.trade.connections();
        let frames = conns
            .get(conn)
            .map(|c| c.received.clone())
            .unwrap_or_default();
        let text = frames.into_iter().map(|f| match f {
            Frame::Text(text) => text,
            Frame::Binary(_) => "(binary)".to_owned(),
        });
        text.collect()
    }

    /// Every `GET /user/positions` the model answered.
    fn positions_read(&self) -> usize {
        let log = self.venue.log();
        log.iter()
            .filter(|e| e.what.starts_with("GET /user/positions"))
            .count()
    }
}

/// The context of one signed frame or request, from `nonces`.
fn ctx_of(nonces: &mut QfexNonces) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(model::now_secs() * 1_000_000_000),
        mono: MonoNs(0),
        nonces: nonces.reserve(ReadAuth::NONCES),
    }
}

/// One limit of `units` an hour on `op`.
fn hourly(op: OpKind, units: u32) -> RateLimit {
    RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[op]),
        per: Duration::from_secs(3_600),
        units,
    }
}

/// An order of ours, resting on the primary account in the model, its client id `cid` as the
/// codec writes it, as a session's place would leave it.
fn resting(model: &mut QfexModel, id: &str, cid: ClientOrderId) -> Order {
    let order = Order {
        id: id.to_owned(),
        account: None,
        cid: fbc_core::encode_cid(&CLIENT_ID_FORMAT, cid)
            .unwrap()
            .as_str()
            .to_owned(),
        side: "BUY".to_owned(),
        kind: "ALO".to_owned(),
        tif: "GTC".to_owned(),
        price: json("99.5"),
        quantity: Decimal::from_str("0.01").unwrap(),
        filled: Decimal::ZERO,
        state: State::Open,
    };
    model.orders.push(order.clone());
    order
}

/// The venue id `id`, as the reads decode it.
fn vid(caps: &VenueCaps, id: &str) -> VenueOrderId {
    dispatch(caps, NS, |scope| scope.venue_order_id(id)).unwrap()
}

/// Client ids of the reads' namespace, minted under a lease of the test's own.
fn cids(n: usize) -> Vec<ClientOrderId> {
    let (dir, _removed) = temp_dir("compare-lease");
    let lease = fbc_core::NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(1), NS).unwrap();
    let mut mint = fbc_core::CidMint::new(lease, 0, 0, WallNs(0));
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

const ID_1: &str = "11111111-1111-4111-8111-0000000000a1";
const ID_2: &str = "11111111-1111-4111-8111-0000000000a2";

#[tokio::test]
async fn every_frame_and_request_of_a_read_is_charged_first_and_one_without_room_skips_it() {
    let bench = Bench::start(QfexModel::default()).await;
    // Room for one authentication, one page and one REST read an hour.
    let page = PAGE_WEIGHT.get();
    let limits = [
        hourly(OpKind::Control, 1),
        hourly(OpKind::Query, page),
        hourly(OpKind::Rest, 1),
    ];
    let limiter = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert_eq!(socket.read().await, Attempt::Read(Read::default()));
    assert_eq!(limiter.counts().refused.ip, 0);
    // The second read's page has no room: not sent, so not answered, and not waited for.
    let began = tokio::time::Instant::now();
    assert_eq!(socket.read().await, Attempt::Skipped);
    assert!(began.elapsed() < Duration::from_secs(1));
    assert_eq!(limiter.counts().refused.ip, 1);
    let sent = bench.received(0);
    let kinds: Vec<String> = sent.iter().map(|f| json(f)["type"].to_string()).collect();
    assert_eq!(kinds, [r#""auth""#, r#""get_user_orders""#]);
    socket.close().await;
    // A second socket's authentication has no room either, so nothing is written on it.
    let mut ctx = || ctx_of(&mut nonces);
    let mut second = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert_eq!(second.read().await, Attempt::Skipped);
    assert_eq!(limiter.counts().refused.ip, 2);
    second.close().await;
    assert!(bench.received(1).is_empty());
    // With room for pages but one REST read, the second read's REST read is the one refused.
    let limits = [hourly(OpKind::Rest, 1)];
    let limiter = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert!(matches!(socket.read().await, Attempt::Read(_)));
    let before = bench.positions_read();
    assert_eq!(socket.read().await, Attempt::Skipped);
    assert_eq!(bench.positions_read(), before);
    assert_eq!(bench.received(2).len(), 3);
    socket.close().await;
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_read_pages_until_a_short_page_and_reads_the_position_on_the_traded_symbol() {
    let bench = Bench::start(QfexModel::default()).await;
    let ours = cids(2);
    {
        let mut model = bench.model();
        resting(&mut model, ID_1, ours[0]);
        let filled = resting(&mut model, ID_2, ours[1]);
        model.orders[1].state = State::Filled;
        model.fills.push(model::Filled {
            account: None,
            trade_id: "55555555-5555-4555-8555-000000000001".to_owned(),
            quantity: filled.quantity,
            order: filled,
            at: 0,
        });
        resting(&mut model, "11111111-1111-4111-8111-0000000000a3", ours[1]);
    }
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    // A page of one: two full pages, then an empty one.
    let mut socket = Socket::new(bench.io(&limiter, 1), &bench.cfg, &bench.reader, &mut ctx);
    let Attempt::Read(read) = socket.read().await else {
        panic!("a read");
    };
    let listed: Vec<(VenueOrderId, CidMatch)> = read
        .listed
        .iter()
        .map(|l| (l.order.clone(), l.client))
        .collect();
    let id3 = vid(&bench.caps, "11111111-1111-4111-8111-0000000000a3");
    assert_eq!(
        listed,
        [
            (vid(&bench.caps, ID_1), CidMatch::Ours(ours[0])),
            (id3, CidMatch::Ours(ours[1]))
        ]
    );
    assert_eq!(read.position, Decimal::from_str("0.01").unwrap());
    // The second read is on the same socket: no second authentication.
    assert!(matches!(socket.read().await, Attempt::Read(_)));
    let heard = socket.close().await;
    assert_eq!(heard, Refusals::default());
    let sent = bench.received(0);
    let offsets: Vec<String> = (sent.iter())
        .map(|f| json(f))
        .filter(|v| v["type"] == "get_user_orders")
        .map(|v| v["params"]["offset"].to_string())
        .collect();
    assert_eq!(offsets, ["0", "1", "2", "0", "1", "2"]);
    assert_eq!(bench.venue.trade.connections().len(), 1);
    bench.venue.assert_served();
}

/// The model, its first place made, so the knobs that act after it act.
fn placed(model: &mut QfexModel) {
    let order = Frame::text(
        r#"{"type":"add_order","params":{"symbol":"SYN-USD","side":"BUY","order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"client_order_id":"x"}}"#,
    );
    model.trade(99, &order).unwrap();
}

#[tokio::test]
async fn a_rate_limited_page_holds_the_budget_and_refuses_on_the_read_socket() {
    let bench = Bench::start(QfexModel {
        rate_limit: Some(Limited::ReadSocket(0)),
        ..QfexModel::default()
    })
    .await;
    placed(&mut bench.model());
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    let refused = Attempt::Refused(RefusedOn::ReadSocket);
    assert_eq!(socket.read().await, refused);
    assert!(limiter.counts().rejected.user >= 1);
    let heard = socket.close().await;
    assert_eq!((heard.errs, heard.met() >= 1), (1, true));
    // No REST read follows a refused page.
    assert_eq!(bench.positions_read(), 0);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_rest_429_refuses_on_rest_and_a_failed_rest_read_fails_that_read_alone() {
    let bench = Bench::start(QfexModel {
        rate_limit: Some(Limited::Rest(1)),
        rest_fails: vec![0],
        ..QfexModel::default()
    })
    .await;
    placed(&mut bench.model());
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    let Attempt::Failed(why) = socket.read().await else {
        panic!("a failed read");
    };
    assert_eq!(why, "GET /user/positions: QFEX answered HTTP 500");
    assert_eq!(socket.read().await, Attempt::Refused(RefusedOn::Rest));
    assert!(matches!(socket.read().await, Attempt::Read(_)));
    socket.close().await;
    // A REST failure leaves the socket as it was: one connection, authenticated once.
    assert_eq!(bench.venue.trade.connections().len(), 1);
    let auths = bench
        .received(0)
        .iter()
        .filter(|f| f.contains(r#""auth""#))
        .count();
    assert_eq!(auths, 1);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_socket_that_fails_is_dropped_and_the_next_read_opens_another() {
    let bench = Bench::start(QfexModel {
        reads_silent: true,
        ..QfexModel::default()
    })
    .await;
    let limiter = bench.limiter();
    let mut io = bench.io(&limiter, 1000);
    io.read_wait = Duration::from_millis(200);
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(io, &bench.cfg, &bench.reader, &mut ctx);
    let Attempt::Failed(why) = socket.read().await else {
        panic!("a failed read");
    };
    assert_eq!(why, "no answer within 200 ms");
    bench.model().reads_silent = false;
    assert!(matches!(socket.read().await, Attempt::Read(_)));
    socket.close().await;
    assert_eq!(bench.venue.trade.connections().len(), 2);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_comparison_against_the_model_times_its_first_agreeing_read_past_the_lag() {
    let bench = Bench::start(QfexModel {
        rest_lag: 2,
        ..QfexModel::default()
    })
    .await;
    let ours = cids(1);
    // The place: the model rests the order, and its reads answer what was before it twice.
    let order = {
        let mut model = bench.model();
        let wire = fbc_core::encode_cid(&CLIENT_ID_FORMAT, ours[0]).unwrap();
        let add = format!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"BUY","order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"client_order_id":"{}"}}}}"#,
            wire.as_str()
        );
        model.trade(98, &Frame::text(add)).unwrap();
        model.orders[0].id.clone()
    };
    let streams = || Streams {
        orders: vec![Ours {
            cid: ours[0],
            venue: Some(vid(&bench.caps, &order)),
            seen: Seen::Open,
        }],
        position: Decimal::ZERO,
    };
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    let cadence = Cadence {
        interval: Duration::from_millis(200),
        wait: Duration::from_millis(1_000),
        cap: 10,
    };
    let since = tokio::time::Instant::now();
    let compared = compare::compare("place X", &mut socket, streams, cadence, since).await;
    socket.close().await;
    let took: Vec<Took> = compared.reads.iter().map(|(_, t)| *t).collect();
    assert_eq!(took, [Took::Lagged, Took::Lagged, Took::Agreed]);
    assert_eq!(compared.ended, Ended::Agreed);
    assert!(compared.first_agreed.unwrap() >= Duration::from_millis(400));
    assert_eq!(compared.stop, None);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_read_that_cannot_be_made_fails_and_one_whose_socket_failed_reopens_it() {
    let bench = Bench::start(QfexModel {
        auth_fails: true,
        ..QfexModel::default()
    })
    .await;
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    // A configuration the guard refuses builds no read; a page size of zero builds no page.
    let refused = VenueConfig::new();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(bench.io(&limiter, 1000), &refused, &bench.reader, &mut ctx);
    assert!(matches!(socket.read().await, Attempt::Failed(_)));
    socket.close().await;
    // An authentication QFEX refuses fails the read and drops the socket: each read opens one.
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    let failed = Attempt::Failed("QFEX did not authenticate the read socket".to_owned());
    assert_eq!(socket.read().await, failed);
    assert_eq!(socket.read().await, failed);
    socket.close().await;
    assert_eq!(bench.venue.trade.connections().len(), 2);
    bench.model().auth_fails = false;
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(bench.io(&limiter, 0), &bench.cfg, &bench.reader, &mut ctx);
    assert!(matches!(socket.read().await, Attempt::Failed(_)));
    socket.close().await;
    // A trade origin nothing listens on.
    let (dir, _removed) = temp_dir("compare-dead");
    let dead = place_stages::argv_on(
        &dir,
        "ws://127.0.0.1:1",
        "http://127.0.0.1:1",
        "ws://127.0.0.1:1",
    );
    let (opts, _) = place_stages::parsed(&dead).unwrap();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let base = stage::base_config(&opts);
    let (cfg, _) = stage::configured(&base, &lookup, Armed::Primary).unwrap();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(bench.io(&limiter, 1000), &cfg, &bench.reader, &mut ctx);
    assert!(matches!(socket.read().await, Attempt::Failed(_)));
    socket.close().await;
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_rate_limited_authentication_refuses_on_the_read_socket() {
    let bench = Bench::start(QfexModel {
        rate_limit: Some(Limited::ReadAuth(0)),
        ..QfexModel::default()
    })
    .await;
    placed(&mut bench.model());
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert_eq!(socket.read().await, Attempt::Refused(RefusedOn::ReadSocket));
    assert!(limiter.counts().rejected.user >= 1);
    assert_eq!(socket.close().await.errs, 1);
    // Nothing but the authentication was written.
    assert_eq!(bench.received(0).len(), 1);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_full_page_that_names_no_new_order_fails_the_read() {
    let bench = Bench::start(QfexModel {
        stuck_pages: true,
        ..QfexModel::default()
    })
    .await;
    let ours = cids(2);
    {
        let mut model = bench.model();
        resting(&mut model, ID_1, ours[0]);
        resting(&mut model, ID_2, ours[1]);
    }
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(bench.io(&limiter, 1), &bench.cfg, &bench.reader, &mut ctx);
    let why = "a full get_user_orders page named no new order".to_owned();
    assert_eq!(socket.read().await, Attempt::Failed(why));
    socket.close().await;
    bench.venue.assert_served();
}

#[tokio::test]
async fn the_runs_cadence_is_its_read_interval_and_agree_wait_and_ten_reads() {
    let (dir, _removed) = temp_dir("compare-cadence");
    let venue = Venue::start(QfexModel::default()).await;
    let (_, places) = place_stages::parsed(&place_stages::argv(&dir, &venue)).unwrap();
    let cadence = Cadence::of(&places);
    assert_eq!(cadence.interval, Duration::from_millis(1_000));
    assert_eq!(cadence.wait, Duration::from_millis(5_000));
    assert_eq!((cadence.cap, cadence.reads()), (10, 6));
}

/// The `get_user_orders` page `model` answers, as a read decodes it under [`NS`].
fn page_of(bench: &Bench, model: &mut QfexModel) -> fbc_venue_qfex::account_reads::OrdersPage {
    let ask = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let answered = model.trade(9, &Frame::text(ask)).unwrap();
    let Frame::Text(text) = &answered[0] else {
        panic!("a text page")
    };
    let ask = fbc_venue_qfex::account_reads::PageAsk::new(1000, 0).unwrap();
    dispatch(&bench.caps, NS, |scope| {
        fbc_venue_qfex::account_reads::orders_page(&bench.reader, RawFrame::Text(text), ask, scope)
    })
    .unwrap()
}

#[tokio::test]
async fn a_read_before_a_session_admits_one_order_of_ours_on_its_account_alone() {
    use capture::AllowedOrder;
    let bench = Bench::start(QfexModel::default()).await;
    let ours = cids(2);
    let page = {
        let mut model = bench.model();
        resting(&mut model, ID_1, ours[0]);
        resting(&mut model, ID_2, ours[1]);
        page_of(&bench, &mut model)
    };
    let x = AllowedOrder {
        account: Armed::Primary,
        cid: ours[0],
        venue: Some(vid(&bench.caps, ID_1)),
    };
    let primary = Armed::Primary;
    assert_eq!(AllowedOrder::count(Some(&x), primary, page.orders()), 1);
    assert_eq!(AllowedOrder::count(None, primary, page.orders()), 0);
    // On another account's read it admits nothing.
    let sub = Armed::Subaccount;
    assert_eq!(AllowedOrder::count(Some(&x), sub, page.orders()), 0);
    // Its client id alone, before its venue id is known; another venue id is not it.
    let unnamed = AllowedOrder {
        venue: None,
        ..x.clone()
    };
    assert_eq!(
        AllowedOrder::count(Some(&unnamed), primary, page.orders()),
        1
    );
    let other = AllowedOrder {
        venue: Some(vid(&bench.caps, ID_2)),
        ..x.clone()
    };
    assert_eq!(AllowedOrder::count(Some(&other), primary, page.orders()), 0);
    // A page listing its client id on two orders admits one of them: the other is held.
    let twice = {
        let mut model = bench.model();
        model.orders.clear();
        resting(&mut model, ID_1, ours[0]);
        resting(&mut model, ID_2, ours[0]);
        page_of(&bench, &mut model)
    };
    assert_eq!(twice.orders().len(), 2);
    let counted = AllowedOrder::count(Some(&unnamed), primary, twice.orders());
    assert_eq!(counted, 1);
    let held = capture::AccountRead {
        flat: true,
        page_orders: twice.orders().len(),
        page_allowed: counted,
        rest_open_orders: 2,
        ..capture::AccountRead::default()
    };
    let holds = "holds an open order, a TWAP or a position that is not flat";
    assert_eq!(held.refusal().as_deref(), Some(holds));
    // The read admits the one it counted, and nothing more.
    let read = capture::AccountRead {
        flat: true,
        page_orders: 1,
        page_allowed: 1,
        rest_open_orders: 1,
        ..capture::AccountRead::default()
    };
    assert_eq!(read.refusal(), None);
    let two = capture::AccountRead {
        page_orders: 2,
        ..read.clone()
    };
    assert_eq!(two.refusal().as_deref(), Some(holds));
    let counted = capture::AccountRead {
        rest_open_orders: 2,
        ..read.clone()
    };
    let said = counted.refusal().unwrap();
    assert!(said.starts_with(
        "shows no open order on get_user_orders but the one the read admits, but GET \
         /user/positions counts 2;"
    ));
    let none = capture::AccountRead {
        page_orders: 0,
        page_allowed: 0,
        ..read
    };
    let said = none.refusal().unwrap();
    assert!(said.starts_with("shows no open order on get_user_orders, but GET /user/positions"));
    bench.venue.assert_served();
}

// ----- The model's trade sockets -----

/// A frame of the model's answer, as JSON.
fn answer(frames: &[Frame], n: usize) -> Value {
    match &frames[n] {
        Frame::Text(text) => json(text),
        Frame::Binary(_) => panic!("a binary answer"),
    }
}

const ADD: &str = r#"{"type":"add_order","params":{"symbol":"SYN-USD","side":"BUY","order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"client_order_id":"cid-one"}}"#;

#[test]
fn the_model_answers_a_session_as_qfex_does() {
    let mut model = QfexModel::default();
    let auth = r#"{"type":"auth","params":{"hmac":{"public_key":"k"},"account_id":"sub-1"}}"#;
    assert_eq!(
        model.trade(0, &Frame::text(auth)).unwrap(),
        [Frame::text(AUTH_OK)]
    );
    assert_eq!(model.conns[&0].account.as_deref(), Some("sub-1"));
    let subscribe = r#"{"type":"subscribe","params":{"channels":["orders"]}}"#;
    model.trade(0, &Frame::text(subscribe)).unwrap();
    assert!(model.conns[&0].session);
    let arm = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;
    let armed = model.trade(0, &Frame::text(arm)).unwrap();
    assert_eq!(answer(&armed, 0)["ack"]["incoming_message"], json(arm));
    assert!(model.conns[&0].armed);
    // A place rests and is acknowledged with its client id, byte for byte.
    let acked = model.trade(0, &Frame::text(ADD)).unwrap();
    let ack = &answer(&acked, 0)["order_response"];
    assert_eq!(
        (ack["status"].as_str(), ack["client_order_id"].as_str()),
        (Some("ACK"), Some("cid-one"))
    );
    let Frame::Text(text) = &acked[0] else {
        panic!()
    };
    assert!(text.contains(r#""client_order_id":"cid-one""#));
    let id = ack["order_id"].as_str().unwrap().to_owned();
    assert_eq!(model.orders[0].account.as_deref(), Some("sub-1"));
    // Its account's page lists it; the primary account's does not.
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let listed = model.trade(0, &Frame::text(page)).unwrap();
    assert_eq!(
        answer(&listed, 0)["all_orders_response"]["orders"][0]["order_id"],
        id.as_str()
    );
    let listed = model.trade(5, &Frame::text(page)).unwrap();
    assert_eq!(
        answer(&listed, 0)["all_orders_response"]["orders"],
        json("[]")
    );
    // Positions follow: the account touched shows its open order; the other its body.
    let positions = json(&model.current(&Some("sub-1".to_owned())));
    assert_eq!(positions["positions"][0]["open_orders"], 1);
    assert_eq!(model.current(&None), model::FLAT);
    // A cancel ends it, and a second cancel of it finds no such order.
    let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":"{id}"}}}}"#);
    let cancelled = model.trade(0, &Frame::text(cancel.clone())).unwrap();
    assert_eq!(
        answer(&cancelled, 0)["order_response"]["status"],
        "CANCELLED"
    );
    let again = model.trade(0, &Frame::text(cancel)).unwrap();
    assert_eq!(
        answer(&again, 0)["order_response"]["status"],
        "NO_SUCH_ORDER"
    );
    // Ended, it is listed by GET /user/historic-orders with its client id.
    let get = fbc_conformance::HttpRequest {
        method: Method::GET,
        path: "/user/historic-orders".to_owned(),
        query: None,
        headers: vec![("x-qfex-requested-account-id".to_owned(), "sub-1".to_owned())],
        body: Vec::new(),
        params: Vec::new(),
    };
    let historic = model.rest(&get);
    let resp = fbc_core::HttpResponse {
        status: historic.status,
        headers: &[],
        body: &historic.body,
    };
    let decoded = fbc_venue_qfex::account_reads::decode_historic(&resp).unwrap();
    assert_eq!(decoded.rows()[0].client_order_id, "cid-one");
    assert_eq!(decoded.rows()[0].terminal_status, "CANCELLED");
    // The trades page lists the account's fills from its start time: none here.
    let trades = r#"{"type":"get_user_trades","params":{"start_ts":0,"limit":1000,"offset":0}}"#;
    let page = model.trade(0, &Frame::text(trades)).unwrap();
    assert_eq!(answer(&page, 0)["user_trades_response"], json("[]"));
}

#[test]
fn the_models_knobs_send_their_frames_after_the_first_ack() {
    let mut model = QfexModel {
        stray_order: true,
        cid_absent_update: true,
        maker_fill: true,
        ..QfexModel::default()
    };
    let frames = model.trade(0, &Frame::text(ADD)).unwrap();
    let kinds: Vec<(String, String)> = (0..frames.len())
        .map(|n| {
            let v = answer(&frames, n);
            let (name, body) = v.as_object().unwrap().iter().next().unwrap();
            let status = body["status"].as_str().unwrap_or("").to_owned();
            (name.clone(), status)
        })
        .collect();
    let want = [
        ("order_response", "ACK"),
        ("order_response", "ACK"),
        ("order_response", "ACK"),
        ("fill_response", ""),
        ("order_response", "FILLED"),
    ];
    let want: Vec<(String, String)> = want
        .iter()
        .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
        .collect();
    assert_eq!(kinds, want);
    let id = answer(&frames, 0)["order_response"]["order_id"].clone();
    // The stray names another order and a client id no run mints; the update names the order
    // with no client id; the maker fill names it with its own, and the position moves.
    assert_ne!(answer(&frames, 1)["order_response"]["order_id"], id);
    assert_eq!(
        answer(&frames, 1)["order_response"]["client_order_id"],
        model::STRAY_CID
    );
    assert_eq!(answer(&frames, 2)["order_response"]["order_id"], id);
    assert!(
        answer(&frames, 2)["order_response"]
            .get("client_order_id")
            .is_none()
    );
    assert_eq!(
        answer(&frames, 3)["fill_response"]["client_order_id"],
        "cid-one"
    );
    assert_eq!(model.orders[0].state, State::Filled);
    let positions = json(&model.current(&None));
    assert_eq!(positions["positions"][0]["position"], json("0.01"));
    // A fill naming no client id; the trades page lists the fill.
    let mut model = QfexModel {
        stray_fill: true,
        ..QfexModel::default()
    };
    let frames = model.trade(0, &Frame::text(ADD)).unwrap();
    assert_eq!(frames.len(), 2);
    assert!(
        answer(&frames, 1)["fill_response"]
            .get("client_order_id")
            .is_none()
    );
    let trades = r#"{"type":"get_user_trades","params":{"start_ts":0,"limit":1000,"offset":0}}"#;
    let page = model.trade(0, &Frame::text(trades)).unwrap();
    assert_eq!(
        answer(&page, 0)["user_trades_response"][0]["client_order_id"],
        "cid-one"
    );
    // The knobs act on the first place alone.
    let second = model.trade(0, &Frame::text(ADD)).unwrap();
    assert_eq!(second.len(), 1);
}

#[test]
fn the_models_rate_limits_answer_a_place_or_a_cancel_once() {
    for (limited, want) in [
        (Limited::PlaceErr, "RateLimited"),
        (Limited::PlaceOrder, "RATE_LIMITED"),
    ] {
        let mut model = QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        };
        let frames = model.trade(0, &Frame::text(ADD)).unwrap();
        let v = answer(&frames, 0);
        let said = v["error_code"]
            .as_str()
            .or(v["order_response"]["status"].as_str());
        assert_eq!(said, Some(want));
        assert!(model.orders.is_empty());
        if limited == Limited::PlaceErr {
            assert_eq!(v["incoming_message"], json(ADD));
        }
        let next = model.trade(0, &Frame::text(ADD)).unwrap();
        assert_eq!(answer(&next, 0)["order_response"]["status"], "ACK");
    }
    let mut model = QfexModel {
        rate_limit: Some(Limited::Cancel),
        ..QfexModel::default()
    };
    let acked = model.trade(0, &Frame::text(ADD)).unwrap();
    let id = answer(&acked, 0)["order_response"]["order_id"].clone();
    let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":{id}}}}}"#);
    let held = model.trade(0, &Frame::text(cancel.clone())).unwrap();
    assert_eq!(answer(&held, 0)["error_code"], "RateLimited");
    assert_eq!(model.orders[0].state, State::Open);
    let cancelled = model.trade(0, &Frame::text(cancel)).unwrap();
    assert_eq!(
        answer(&cancelled, 0)["order_response"]["status"],
        "CANCELLED"
    );
}

/// A WebSocket on the model's trade origin, as a session's would be.
async fn session_socket(venue: &Venue) -> fbc_runtime::ws::WebSocket {
    let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
    let url = fbc_core::WireUrl::plain(venue.trade.ws_url(""));
    auth_qfex::open(&connector, &url, Duration::from_secs(5))
        .await
        .unwrap()
}

/// Sends `text` on `ws` and reads the next data frame.
async fn send(ws: &mut fbc_runtime::ws::WebSocket, text: &str) -> Value {
    ws.send(Message::text(text.to_owned())).await.unwrap();
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return json(text.as_str()),
            _ => continue,
        }
    }
}

/// An armed session on `venue` with one order resting: its socket.
async fn armed_with_an_order(venue: &Venue) -> fbc_runtime::ws::WebSocket {
    let mut ws = session_socket(venue).await;
    send(&mut ws, r#"{"type":"auth","params":{}}"#).await;
    send(
        &mut ws,
        r#"{"type":"subscribe","params":{"channels":["orders"]}}"#,
    )
    .await;
    let arm = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;
    send(&mut ws, arm).await;
    send(&mut ws, ADD).await;
    ws
}

/// Waits until the model's next answer has seen trade connection `conn` end: its log names it.
async fn settled(venue: &Venue, conn: usize) -> String {
    let prefix = format!("cod {conn} ");
    for _ in 0..500 {
        let logged = {
            let mut model = venue.model.lock().unwrap();
            model.rest(&fbc_conformance::HttpRequest {
                method: Method::GET,
                path: "/user/historic-orders".to_owned(),
                query: None,
                headers: Vec::new(),
                body: Vec::new(),
                params: Vec::new(),
            });
            let log = model.log.iter().map(|e| e.what.clone());
            log.into_iter().find(|w| w.starts_with(&prefix))
        };
        if let Some(what) = logged {
            return what;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the model never saw connection {conn} end");
}

#[tokio::test]
async fn the_model_cancels_an_armed_accounts_orders_as_its_knobs_say_once_it_ends() {
    // A graceful close keeps them by default and cancels them with cod_on_close.
    for (on_close, want, state) in [
        (false, "cod 0 close keeps", State::Open),
        (true, "cod 0 close cancels", State::Cancelled),
    ] {
        let venue = Venue::start(QfexModel {
            cod_on_close: on_close,
            ..QfexModel::default()
        })
        .await;
        let mut ws = armed_with_an_order(&venue).await;
        ws.close(None).await.unwrap();
        while ws.next().await.is_some() {}
        assert_eq!(settled(&venue, 0).await, want);
        assert_eq!(venue.model.lock().unwrap().orders[0].state, state);
        venue.assert_served();
    }
    // A drop cancels them by default.
    let venue = Venue::start(QfexModel::default()).await;
    let ws = armed_with_an_order(&venue).await;
    drop(ws);
    assert_eq!(settled(&venue, 0).await, "cod 0 drop cancels");
    assert_eq!(
        venue.model.lock().unwrap().orders[0].state,
        State::Cancelled
    );
    // A second session subscribing while the first is open is noted.
    let venue = Venue::start(QfexModel::default()).await;
    let _first = armed_with_an_order(&venue).await;
    let mut second = session_socket(&venue).await;
    send(
        &mut second,
        r#"{"type":"subscribe","params":{"channels":["orders"]}}"#,
    )
    .await;
    assert!(venue.model.lock().unwrap().overlapped);
}

#[tokio::test]
async fn the_model_answers_a_place_late_and_closes_as_its_knobs_say() {
    let venue = Venue::start(QfexModel {
        ack_delay: Some(Duration::from_millis(300)),
        server_close: true,
        ..QfexModel::default()
    })
    .await;
    let mut ws = session_socket(&venue).await;
    send(&mut ws, r#"{"type":"auth","params":{}}"#).await;
    let sent = tokio::time::Instant::now();
    ws.send(Message::text(ADD.to_owned())).await.unwrap();
    let mut got = Vec::new();
    while let Some(Ok(msg)) = ws.next().await {
        match msg {
            Message::Text(text) => got.push((sent.elapsed(), json(text.as_str()))),
            Message::Close(_) => got.push((sent.elapsed(), json("\"close\""))),
            _ => {}
        }
    }
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(got[0].0 >= Duration::from_millis(300));
    assert_eq!(got[0].1["order_response"]["status"], "ACK");
    assert_eq!(got[1].1, json("\"close\""));
    // With no delay, the close follows the answer at once.
    let venue = Venue::start(QfexModel {
        server_close: true,
        ..QfexModel::default()
    })
    .await;
    let mut ws = session_socket(&venue).await;
    let acked = send(&mut ws, ADD).await;
    assert_eq!(acked["order_response"]["status"], "ACK");
    let closed = loop {
        match ws.next().await {
            Some(Ok(Message::Close(_))) | None => break true,
            Some(Ok(_)) => {}
            Some(Err(_)) => break false,
        }
    };
    assert!(closed);
    venue.assert_served();
}

#[tokio::test]
async fn an_authentication_refused_in_a_frame_kept_whole_refuses_on_the_read_socket() {
    // PR #272's thread 4605918671: a RateLimited err echoing the authentication, which the
    // journal keeps whole (decision 0134 item 2), holds no budget, since the adapter reads no
    // hold from it, but its refusal is one all the same (decision 0135 item 2).
    let bench = Bench::start(QfexModel {
        rate_limit: Some(Limited::ReadAuthEcho(0)),
        ..QfexModel::default()
    })
    .await;
    placed(&mut bench.model());
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert_eq!(socket.read().await, Attempt::Refused(RefusedOn::ReadSocket));
    let heard = socket.close().await;
    assert_eq!(
        (heard.errs, heard.unread, heard.whole),
        (0, 1, 1),
        "{heard:?}"
    );
    assert_eq!(heard.limiter, 0);
    // Nothing but the authentication was written.
    assert_eq!(bench.received(0).len(), 1);
    bench.venue.assert_served();
}

#[tokio::test]
async fn a_page_refused_in_a_frame_kept_whole_refuses_on_the_read_socket() {
    // The twin on the page: a RateLimited err echoing it whose message holds a JSON escape,
    // which the journal keeps whole (decision 0134 item 2), so the adapter reads neither a hold
    // nor a refusal from it, nor a page: its refusal is one all the same (decision 0135 item 2),
    // not a failed read that would reconnect, and no REST read follows it.
    let bench = Bench::start(QfexModel {
        rate_limit: Some(Limited::PageKeptWhole(0)),
        ..QfexModel::default()
    })
    .await;
    placed(&mut bench.model());
    let limiter = bench.limiter();
    let mut nonces = QfexNonces::os();
    let mut ctx = || ctx_of(&mut nonces);
    let mut socket = Socket::new(
        bench.io(&limiter, 1000),
        &bench.cfg,
        &bench.reader,
        &mut ctx,
    );
    assert_eq!(socket.read().await, Attempt::Refused(RefusedOn::ReadSocket));
    let heard = socket.close().await;
    assert_eq!(
        (heard.errs, heard.unread, heard.whole),
        (0, 1, 1),
        "{heard:?}"
    );
    assert_eq!(heard.limiter, 0);
    // The authentication and the one page, on the one connection; no REST read.
    assert_eq!(bench.received(0).len(), 2);
    assert_eq!(bench.venue.trade.connections().len(), 1);
    assert_eq!(bench.positions_read(), 0);
    bench.venue.assert_served();
}
