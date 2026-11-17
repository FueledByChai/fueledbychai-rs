//! FBC-77r8: what drop session two rests on, against the QFEX model ([`super::model`]) on no
//! socket, under `production_probe` on loopback origins nothing listens on. Declared by path in
//! `tests/qfex_preprod_probe.rs`, so it is part of that test binary and Cargo builds no target of
//! its own for it. Nothing places on a venue, and every symbol, key, id and value is made up.
//!
//! - the model's arm cancels its account's open orders as its knob says (decision 0132 items 2
//!   and 4): none, every one reported by a `CANCELLED` `order_response` before the ack, every one
//!   reported late, with the next place's answer, or every one with no frame; and the `n`th arm
//!   alone can be refused;
//! - the model's knob on one place's `ACK`: QFEX cancelling that order itself, or filling it
//!   whole, in the same answer, the fill holding the read sockets' pages until the placing
//!   connection's client writes again or ends;
//! - the model's cancel-on-disconnect, on a graceful close or a drop, reaches every open order of
//!   the account, only those placed on the connection that ended, or only the earlier ones, as
//!   its knobs say (decisions 0121 and 0132 item 4), and the foreign order it can leave once a
//!   connection ended; its later knobs: a maker fill at the `n`th arm or of a dropped
//!   connection's own orders, an ACK delayed on a later place, and historic-orders leaving out
//!   the last order placed;
//! - the read before drop session two, judged from the model's own page and positions as the
//!   probe's reads decode them: X alone admitted on its account, with REST counting one open
//!   order on its symbol and none on another; any other open order, X on another account, a
//!   REST count on another symbol or none at all refuses it (decision 0132 item 7, 0144 item 4).

use super::*;

use capture::{AccountRead, AllowedOrder};
use fbc_core::{ClientOrderId, HttpResponse, Namespace, VenueCaps, VenueOrderId, dispatch};
use fbc_venue_qfex::account_reads::{self, OrdersPage, PageAsk, Positions};
use fbc_venue_qfex::auth::{AccountSelection, ReadAuth};
use fbc_venue_qfex::exec::CLIENT_ID_FORMAT;
use model::{ArmCancels, QfexModel, Reach, STRAY_CID, State};

/// The namespace the reads decode ours by: the place-stage command line's `--namespace`.
const NS: Namespace = Namespace::new(7);

/// The subaccount the model's sockets authenticate as: a made-up id.
const SUB_1: &str = "sub-1";

/// A frame of the model's answer, as JSON.
fn answer(frames: &[Frame], n: usize) -> Value {
    match &frames[n] {
        Frame::Text(text) => json(text),
        Frame::Binary(_) => panic!("a binary answer"),
    }
}

/// Sends `text` to the model on trade connection `conn`: its answer.
fn send(model: &mut QfexModel, conn: usize, text: &str) -> Vec<Frame> {
    model.trade(conn, &Frame::text(text)).unwrap()
}

/// Opens trade connection `conn` on the model as a session's: its authentication, naming
/// `account` (the primary account when `None`), and a subscription.
fn session(model: &mut QfexModel, conn: usize, account: Option<&str>) {
    let auth = match account {
        Some(id) => format!(r#"{{"type":"auth","params":{{"account_id":"{id}"}}}}"#),
        None => r#"{"type":"auth","params":{}}"#.to_owned(),
    };
    send(model, conn, &auth);
    let subscribe = r#"{"type":"subscribe","params":{"channels":["orders"]}}"#;
    send(model, conn, subscribe);
}

const ARM_FRAME: &str = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;

/// Places a post-only order with the client id text `cid` on `conn`: its venue id.
fn place(model: &mut QfexModel, conn: usize, cid: &str) -> String {
    let add = format!(
        concat!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"BUY","#,
            r#""order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":99.5,"#,
            r#""client_order_id":"{cid}"}}}}"#,
        ),
        cid = cid
    );
    let acked = send(model, conn, &add);
    let id = answer(&acked, 0)["order_response"]["order_id"].clone();
    id.as_str().unwrap().to_owned()
}

/// Where the model's order `id` stands.
fn state(model: &QfexModel, id: &str) -> State {
    model.orders.iter().find(|o| o.id == id).unwrap().state
}

/// Whether the model logged `what`.
fn logged(model: &QfexModel, what: &str) -> bool {
    model.log.iter().any(|e| e.what == what)
}

#[test]
fn the_models_arm_cancels_its_accounts_open_orders_as_its_knob_says() {
    for knob in [
        ArmCancels::No,
        ArmCancels::Reported,
        ArmCancels::Late,
        ArmCancels::Silent,
    ] {
        let mut model = QfexModel {
            arm_cancels_open: knob,
            ..QfexModel::default()
        };
        // X rests on the primary account from a session that ended, and another order on the
        // subaccount; a later session arms on the primary account.
        session(&mut model, 0, None);
        let x = place(&mut model, 0, "cid-x");
        session(&mut model, 2, Some(SUB_1));
        let other = place(&mut model, 2, "cid-other");
        session(&mut model, 1, None);
        let armed = send(&mut model, 1, ARM_FRAME);
        let ack = answer(&armed, armed.len() - 1);
        assert_eq!(ack["ack"]["incoming_message"], json(ARM_FRAME), "{knob:?}");
        let (frames, x_state, logged_as) = match knob {
            ArmCancels::No => (1, State::Open, None),
            ArmCancels::Reported => (2, State::Cancelled, Some("arm 1 cancels 1 reported")),
            ArmCancels::Late => (1, State::Cancelled, Some("arm 1 cancels 1 reported late")),
            ArmCancels::Silent => (1, State::Cancelled, Some("arm 1 cancels 1 silently")),
        };
        assert_eq!(armed.len(), frames, "{knob:?}");
        assert_eq!(state(&model, &x), x_state, "{knob:?}");
        // The subaccount's order is not the armed account's.
        assert_eq!(state(&model, &other), State::Open);
        let arm_events: Vec<&str> = (model.log.iter())
            .map(|e| e.what.as_str())
            .filter(|w| w.starts_with("arm "))
            .collect();
        assert_eq!(arm_events, logged_as.into_iter().collect::<Vec<_>>());
        if knob == ArmCancels::Reported {
            // Its report names X by its venue id and client id, before the ack.
            let cancelled = &answer(&armed, 0)["order_response"];
            assert_eq!(cancelled["status"], "CANCELLED");
            assert_eq!(cancelled["order_id"], x.as_str());
            assert_eq!(cancelled["client_order_id"], "cid-x");
        }
        // An arm on an account with no open order cancels nothing and says nothing.
        session(&mut model, 3, Some("sub-empty"));
        assert_eq!(send(&mut model, 3, ARM_FRAME).len(), 1);
        assert!(model.log.iter().all(|e| !e.what.starts_with("arm 3 ")));
    }
}

#[test]
fn the_models_arm_reports_late_refuses_one_arm_and_ends_an_acked_order_as_its_knobs_say() {
    // Late: the arm cancels X at once and says nothing, until the next place's answer, whose
    // CANCELLED of X comes before that place's ACK.
    let mut model = QfexModel {
        arm_cancels_open: ArmCancels::Late,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    let x = place(&mut model, 0, "cid-x");
    session(&mut model, 1, None);
    let armed = send(&mut model, 1, ARM_FRAME);
    assert_eq!(armed.len(), 1);
    assert_eq!(
        answer(&armed, 0)["ack"]["incoming_message"],
        json(ARM_FRAME)
    );
    assert_eq!(state(&model, &x), State::Cancelled);
    assert!(logged(&model, "arm 1 cancels 1 reported late"));
    let add = r#"{"type":"add_order","params":{"client_order_id":"cid-y","quantity":0.01}}"#;
    let placed = send(&mut model, 1, add);
    assert_eq!(placed.len(), 2);
    let late = &answer(&placed, 0)["order_response"];
    assert_eq!(
        (late["status"].as_str(), late["order_id"].as_str()),
        (Some("CANCELLED"), Some(x.as_str()))
    );
    assert_eq!(late["client_order_id"], "cid-x");
    let acked = &answer(&placed, 1)["order_response"];
    assert_eq!(
        (acked["status"].as_str(), acked["client_order_id"].as_str()),
        (Some("ACK"), Some("cid-y"))
    );
    // Held once: the place after it answers its ACK alone.
    let again = r#"{"type":"add_order","params":{"client_order_id":"cid-z","quantity":0.01}}"#;
    assert_eq!(send(&mut model, 1, again).len(), 1);
    // The second arm alone refused: an err echoing it, the first and third acknowledged.
    let mut model = QfexModel {
        arm_err_on: Some(1),
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    let answered: Vec<Value> = (0..3)
        .map(|_| answer(&send(&mut model, 0, ARM_FRAME), 0))
        .collect();
    assert_eq!(answered[0]["ack"]["incoming_message"], json(ARM_FRAME));
    assert_eq!(answered[1]["error_code"], "InvalidRequest");
    assert_eq!(answered[1]["incoming_message"], json(ARM_FRAME));
    assert_eq!(answered[2]["ack"]["incoming_message"], json(ARM_FRAME));
    // On the second place's ACK: QFEX cancels it, or it fills whole, in the same answer; the
    // first place is answered its ACK alone.
    for (on, status, want) in [
        (model::OnAck::VenueCancel, "CANCELLED", State::Cancelled),
        (model::OnAck::MakerFill, "FILLED", State::Filled),
    ] {
        let mut model = QfexModel {
            on_ack: Some((1, on)),
            ..QfexModel::default()
        };
        session(&mut model, 0, None);
        let first = place(&mut model, 0, "cid-x");
        assert_eq!(state(&model, &first), State::Open);
        let add = r#"{"type":"add_order","params":{"client_order_id":"cid-y","quantity":0.01}}"#;
        let placed = send(&mut model, 0, add);
        let last = answer(&placed, placed.len() - 1);
        let id = answer(&placed, 0)["order_response"]["order_id"].clone();
        assert_eq!(
            answer(&placed, 0)["order_response"]["status"],
            "ACK",
            "{on:?}"
        );
        assert_eq!(last["order_response"]["status"], status, "{on:?}");
        assert_eq!(last["order_response"]["client_order_id"], "cid-y", "{on:?}");
        assert_eq!(state(&model, id.as_str().unwrap()), want, "{on:?}");
        let fills = usize::from(on == model::OnAck::MakerFill);
        assert_eq!(
            (placed.len(), model.fills.len()),
            (2 + fills, fills),
            "{on:?}"
        );
    }
}

#[test]
fn the_models_maker_fill_after_a_later_ack_holds_the_read_pages_until_its_connection_ends() {
    // FBC-77r8, as FBC-23ch's third review fix holds the pages under its stray-order knob: a
    // maker fill pushed after Y's ACK changes what a read shows at once, so a read socket's page
    // waits from that fill until the placing connection's client writes again or its end is
    // seen; the session's own pages, and every page before the fill, are answered at once. A
    // venue cancel after the ACK holds none: no client frame follows it to release a hold.
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let mut model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    place(&mut model, 0, "cid-x");
    assert!(!model.knob_frames_unread());
    assert_eq!(send(&mut model, 1, page).len(), 1);
    place(&mut model, 0, "cid-y");
    assert!(model.knob_frames_unread());
    assert!(send(&mut model, 1, page).is_empty());
    assert!(send(&mut model, 2, page).is_empty());
    assert_eq!(model.held_pages.len(), 2);
    assert_eq!(send(&mut model, 0, page).len(), 1);
    // The placing connection's end seen, a page is answered at once again.
    model.conns.get_mut(&0).unwrap().settled = true;
    assert!(!model.knob_frames_unread());
    assert_eq!(send(&mut model, 1, page).len(), 1);
    let mut model = QfexModel {
        on_ack: Some((1, model::OnAck::VenueCancel)),
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    place(&mut model, 0, "cid-x");
    place(&mut model, 0, "cid-y");
    assert!(!model.knob_frames_unread());
    assert_eq!(send(&mut model, 1, page).len(), 1);
}

#[test]
fn the_models_cancel_on_disconnect_reaches_as_its_knobs_say() {
    for (reach, graceful, on_drop) in [
        (Reach::Account, false, true),
        (Reach::Own, false, true),
        (Reach::Earlier, false, true),
        (Reach::Account, true, true),
        (Reach::Own, true, true),
        (Reach::Account, false, false),
    ] {
        let mut model = QfexModel {
            cod_on_close: true,
            cod_on_drop: on_drop,
            cod_reach: reach,
            ..QfexModel::default()
        };
        // X from a session that never armed: its end cancels nothing.
        session(&mut model, 0, None);
        let x = place(&mut model, 0, "cid-x");
        model.ended(0, true);
        assert_eq!(state(&model, &x), State::Open);
        // Y from an armed session, whose end cancels as the knobs say.
        session(&mut model, 1, None);
        send(&mut model, 1, ARM_FRAME);
        let y = place(&mut model, 1, "cid-y");
        model.ended(1, graceful);
        let how = if graceful { "close" } else { "drop" };
        let cancels = graceful || on_drop;
        let did = if cancels { "cancels" } else { "keeps" };
        assert!(logged(&model, &format!("cod 1 {how} {did}")), "{reach:?}");
        let (want_x, want_y) = match (cancels, reach) {
            (false, _) => (State::Open, State::Open),
            (true, Reach::Account) => (State::Cancelled, State::Cancelled),
            (true, Reach::Own) => (State::Open, State::Cancelled),
            (true, Reach::Earlier) => (State::Cancelled, State::Open),
        };
        assert_eq!((state(&model, &x), state(&model, &y)), (want_x, want_y));
        // Its end is applied once.
        let before = model.log.len();
        model.ended(1, graceful);
        assert_eq!(model.log.len(), before);
        assert!(model.conns[&1].settled);
        assert_eq!(model.conns[&1].dropped, !graceful);
    }
}

#[test]
fn the_models_later_knobs_fill_at_an_arm_or_a_drop_delay_a_later_place_and_hide_one_ended() {
    // fill_at_arm: the second arm fills its account's open order, X, as a maker, after its ack.
    let mut model = QfexModel {
        fill_at_arm: Some(1),
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    assert_eq!(send(&mut model, 0, ARM_FRAME).len(), 1);
    let x = place(&mut model, 0, "cid-x");
    session(&mut model, 1, None);
    let armed = send(&mut model, 1, ARM_FRAME);
    assert_eq!(armed.len(), 3);
    assert!(answer(&armed, 0)["ack"].is_object());
    assert_eq!(answer(&armed, 1)["fill_response"]["order_id"], x.as_str());
    assert_eq!(answer(&armed, 2)["order_response"]["status"], "FILLED");
    assert_eq!(state(&model, &x), State::Filled);
    // fill_on_drop: a drop fills the orders placed on the dropped connection, then cancels the
    // rest; a graceful close fills nothing.
    let mut model = QfexModel {
        fill_on_drop: true,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    let x = place(&mut model, 0, "cid-x");
    session(&mut model, 1, None);
    send(&mut model, 1, ARM_FRAME);
    let y = place(&mut model, 1, "cid-y");
    model.ended(1, false);
    assert_eq!(
        (state(&model, &x), state(&model, &y)),
        (State::Cancelled, State::Filled)
    );
    assert_eq!(model.fills.len(), 1);
    session(&mut model, 2, None);
    send(&mut model, 2, ARM_FRAME);
    let z = place(&mut model, 2, "cid-z");
    model.ended(2, true);
    assert_eq!(state(&model, &z), State::Open);
    // ack_delay_on: the delay is the second place's alone, the first answered at once.
    let mut model = QfexModel {
        ack_delay: Some(Duration::from_secs(3600)),
        ack_delay_on: 1,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    place(&mut model, 0, "cid-x");
    // historic_omits_last: the last-placed order is left out of GET /user/historic-orders,
    // whatever its state; the earlier ones are listed.
    let mut model = QfexModel {
        historic_omits_last: true,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    send(&mut model, 0, ARM_FRAME);
    place(&mut model, 0, "cid-x");
    place(&mut model, 0, "cid-y");
    model.ended(0, false);
    let historic = model.rest(&fbc_conformance::HttpRequest {
        method: Method::GET,
        path: "/user/historic-orders".to_owned(),
        query: None,
        headers: Vec::new(),
        body: Vec::new(),
        params: Vec::new(),
    });
    let listed = json(std::str::from_utf8(&historic.body).unwrap());
    let cids: Vec<&str> = (listed["data"].as_array().unwrap().iter())
        .map(|row| row["client_order_id"].as_str().unwrap())
        .collect();
    assert_eq!(cids, ["cid-x"]);
}

#[test]
fn the_model_can_leave_a_foreign_order_once_an_armed_connection_ended() {
    let mut model = QfexModel {
        foreign_after_close: true,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    send(&mut model, 0, ARM_FRAME);
    let x = place(&mut model, 0, "cid-x");
    model.ended(0, true);
    // A graceful close keeps X by default; another trader's order now rests beside it, once.
    assert_eq!(state(&model, &x), State::Open);
    let foreign: Vec<&model::Order> = (model.orders.iter())
        .filter(|o| o.cid == STRAY_CID)
        .collect();
    assert_eq!(foreign.len(), 1);
    assert_eq!(
        (foreign[0].state, foreign[0].account.clone()),
        (State::Open, None)
    );
    let page = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let listed = send(&mut model, 9, page);
    let rows = &answer(&listed, 0)["all_orders_response"]["orders"];
    assert_eq!(rows.as_array().unwrap().len(), 2);
    // A second end of another armed connection on the account adds none.
    session(&mut model, 1, None);
    send(&mut model, 1, ARM_FRAME);
    model.ended(1, false);
    assert_eq!(
        model.orders.iter().filter(|o| o.cid == STRAY_CID).count(),
        1
    );
}

/// What a read needs from the run, built as the run builds it on origins nothing listens on.
struct Reads {
    reader: ReadAuth,
    caps: VenueCaps,
}

impl Reads {
    fn new() -> Reads {
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
        Reads {
            reader: auth_qfex::reader(&lookup, AccountSelection::Primary).unwrap(),
            caps: Qfex.caps(&cfg).unwrap(),
        }
    }

    /// The `get_user_orders` page the model answers on connection `conn`, as a read decodes it.
    fn page(&self, model: &mut QfexModel, conn: usize) -> OrdersPage {
        let ask = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
        let answered = send(model, conn, ask);
        let Frame::Text(text) = &answered[0] else {
            panic!("a text page")
        };
        let ask = PageAsk::new(1000, 0).unwrap();
        dispatch(&self.caps, NS, |scope| {
            account_reads::orders_page(&self.reader, RawFrame::Text(text), ask, scope)
        })
        .unwrap()
    }

    /// `body`, a GET /user/positions answer, as a read decodes it.
    fn positions(&self, body: &str) -> Positions {
        let resp = HttpResponse {
            status: 200,
            headers: &[],
            body: body.as_bytes(),
        };
        let decoded = dispatch(&self.caps, NS, |scope| {
            account_reads::decode_positions(&resp, scope)
        });
        decoded.unwrap()
    }

    fn vid(&self, id: &str) -> VenueOrderId {
        dispatch(&self.caps, NS, |scope| scope.venue_order_id(id)).unwrap()
    }
}

/// Client ids of the reads' namespace, minted under a lease of the test's own.
fn cids(n: usize) -> Vec<ClientOrderId> {
    let (dir, _removed) = temp_dir("drop-two-lease");
    let lease = fbc_core::NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(1), NS).unwrap();
    let mut mint = fbc_core::CidMint::new(lease, 0, 0, WallNs(0));
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

/// `cid` as the codec writes it.
fn text(cid: ClientOrderId) -> String {
    fbc_core::encode_cid(&CLIENT_ID_FORMAT, cid)
        .unwrap()
        .to_string()
}

/// The positions of an account counting `x_symbol` open orders on the model's symbol and
/// `other` on another, flat.
fn counting(x_symbol: u64, other: u64) -> String {
    format!(
        concat!(
            r#"{{"balance":{{}},"positions":[{{"symbol":"SYN-USD","position":0,"#,
            r#""open_orders":{x_symbol}}},{{"symbol":"OTH-USD","position":0,"#,
            r#""open_orders":{other}}}]}}"#,
        ),
        x_symbol = x_symbol,
        other = other
    )
}

const HOLDS: &str = "holds an open order, a TWAP or a position that is not flat";

#[test]
fn the_read_before_session_two_admits_x_alone_on_its_symbol_at_function_level() {
    let reads = Reads::new();
    let ours = cids(2);
    let mut model = QfexModel::default();
    // X rests on the primary account, placed by drop session one's connection.
    session(&mut model, 0, None);
    let id = place(&mut model, 0, &text(ours[0]));
    let x = AllowedOrder {
        account: Armed::Primary,
        cid: ours[0],
        venue: Some(reads.vid(&id)),
    };
    let primary = reads.page(&mut model, 9);
    let positions = reads.positions(&model.current(&None));
    let read = AccountRead::of(&positions, &primary, Some(&x), Armed::Primary);
    assert_eq!((read.page_orders, read.page_allowed), (1, 1));
    assert_eq!((read.rest_open_orders, read.rest_open_elsewhere), (1, 0));
    assert!(read.flat);
    assert_eq!(read.refusal(), None);
    // Before its venue id is known, its client id alone admits it.
    let unnamed = AllowedOrder {
        venue: None,
        ..x.clone()
    };
    let read = AccountRead::of(&positions, &primary, Some(&unnamed), Armed::Primary);
    assert_eq!(read.refusal(), None);
    // The subaccount, empty and flat, is clean: X is admitted on its own account alone.
    session(&mut model, 8, Some(SUB_1));
    let sub = reads.page(&mut model, 8);
    let flat = reads.positions(model::FLAT);
    let read = AccountRead::of(&flat, &sub, Some(&x), Armed::Subaccount);
    assert_eq!((read.page_orders, read.page_allowed), (0, 0));
    assert_eq!(read.refusal(), None);
    // REST counting X's open order on another symbol, or none, refuses: one open order on its
    // symbol and none on another.
    for (on, other, elsewhere) in [(0, 1, 1), (0, 0, 0), (1, 1, 1)] {
        let counted = reads.positions(&counting(on, other));
        let read = AccountRead::of(&counted, &primary, Some(&x), Armed::Primary);
        assert_eq!(read.rest_open_elsewhere, elsewhere);
        let said = read.refusal().unwrap();
        assert!(
            said.starts_with(
                "shows no open order on get_user_orders but the one the read admits, but GET \
                 /user/positions counts"
            ),
            "{said}"
        );
    }
    let counted = reads.positions(&counting(1, 0));
    let read = AccountRead::of(&counted, &primary, Some(&x), Armed::Primary);
    assert_eq!(read.refusal(), None);
    // With X not listed (ended), no REST count is admitted, wherever it is.
    let none = AccountRead::of(&counted, &sub, Some(&x), Armed::Primary);
    assert_eq!((none.page_allowed, none.rest_open_elsewhere), (0, 0));
    assert!(
        none.refusal().unwrap().starts_with(
            "shows no open order on get_user_orders, but GET /user/positions counts 1;"
        )
    );
}

#[test]
fn any_other_open_order_refuses_the_read_before_session_two_at_function_level() {
    let reads = Reads::new();
    let ours = cids(2);
    let x_of = |id: &str| AllowedOrder {
        account: Armed::Primary,
        cid: ours[0],
        venue: Some(reads.vid(id)),
    };
    // Another order of ours beside X.
    let mut model = QfexModel::default();
    session(&mut model, 0, None);
    let id = place(&mut model, 0, &text(ours[0]));
    place(&mut model, 0, &text(ours[1]));
    let page = reads.page(&mut model, 9);
    let positions = reads.positions(&model.current(&None));
    let read = AccountRead::of(&positions, &page, Some(&x_of(&id)), Armed::Primary);
    assert_eq!((read.page_orders, read.page_allowed), (2, 1));
    assert_eq!(read.refusal().as_deref(), Some(HOLDS));
    // Another trader's order beside X.
    let mut model = QfexModel {
        foreign_after_close: true,
        ..QfexModel::default()
    };
    session(&mut model, 0, None);
    send(&mut model, 0, ARM_FRAME);
    let id = place(&mut model, 0, &text(ours[0]));
    model.ended(0, true);
    let page = reads.page(&mut model, 9);
    let positions = reads.positions(&model.current(&None));
    let read = AccountRead::of(&positions, &page, Some(&x_of(&id)), Armed::Primary);
    assert_eq!((read.page_orders, read.page_allowed), (2, 1));
    assert_eq!(read.refusal().as_deref(), Some(HOLDS));
    // X itself on another account than the one admitted: the subaccount's read admits nothing.
    let mut model = QfexModel::default();
    session(&mut model, 8, Some(SUB_1));
    let id = place(&mut model, 8, &text(ours[0]));
    let page = reads.page(&mut model, 8);
    let positions = reads.positions(&model.current(&Some(SUB_1.to_owned())));
    let read = AccountRead::of(&positions, &page, Some(&x_of(&id)), Armed::Subaccount);
    assert_eq!((read.page_orders, read.page_allowed), (1, 0));
    assert_eq!(read.refusal().as_deref(), Some(HOLDS));
    // X's client id under another venue id is not X.
    let mut model = QfexModel::default();
    session(&mut model, 0, None);
    place(&mut model, 0, &text(ours[0]));
    let page = reads.page(&mut model, 9);
    let positions = reads.positions(&model.current(&None));
    let elsewhere = x_of("22222222-2222-4222-8222-0000000000b2");
    let read = AccountRead::of(&positions, &page, Some(&elsewhere), Armed::Primary);
    assert_eq!(read.page_allowed, 0);
    assert_eq!(read.refusal().as_deref(), Some(HOLDS));
}
