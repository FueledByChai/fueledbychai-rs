//! FBC-t45e's done line: the place stages' capture scrub (decision 0148 item 10) and the arm
//! window a place's deadline does not close. Every frame here is hand-built: its ids, symbols,
//! sizes, prices and fees are made up.

use fbc_core::Inbound;
use fbc_venue_qfex::account_reads;
use fbc_venue_qfex::auth::AccountSelection;

use super::*;

/// A made-up market, in the shape QFEX writes one.
const MARKET: &str = "SYNTH-USD";

/// A made-up fill, its amounts written as strings and as numbers, holding a position whose `id`
/// is spanned nowhere else.
const FILL: &str = r#"{"fill_response":{"trade_id":"t-synthetic-0001","user_id":"u-synthetic-0001","symbol":"SYNTH-USD","price":"101.25","quantity":0.5,"side":"BUY","order_id":"o-synthetic-0001","fee":"-0.0125","client_order_id":"c-synthetic-0001","position":{"id":"p-synthetic-0001","position":"0.5"},"timestamp":1700000000.5}}"#;
/// [`FILL`] scrubbed: every id at its length, the symbol the placeholder, the fee its sign, every
/// other amount `0`.
const FILL_SCRUBBED: &str = r#"{"fill_response":{"trade_id":"2222222222222222","user_id":"2222222222222222","symbol":"22222222","price":"0","quantity":0,"side":"BUY","order_id":"2222222222222222","fee":"-1","client_order_id":"2222222222222222","position":{"id":"2222222222222222","position":"0"},"timestamp":0}}"#;
/// A made-up position.
const POSITION: &str = r#"{"position_response":{"id":"p-synthetic-0001","symbol":"SYNTH-USD","position":"-0.5","average_price":101.25,"margin_mode":"cross"}}"#;
const POSITION_SCRUBBED: &str = r#"{"position_response":{"id":"2222222222222222","symbol":"22222222","position":"0","average_price":0,"margin_mode":"cross"}}"#;

/// `text` received on the primary account's connection `n`, journaled with the spans the QFEX
/// adapter's `src/auth` names in it (decision 0028), through its account reads' own
/// `redact_inbound`, which a session's codec shares for a frame that echoes no request, read
/// under the test's synthetic credentials.
fn spanned(n: u16, text: &str) -> Offer {
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let reader = auth_qfex::reader(&lookup, AccountSelection::Primary).unwrap();
    let frame = RawFrame::Text(text);
    let spans: InboundSpans = account_reads::redact_inbound(&reader, Inbound::Frame(frame));
    Offer::Keep(Record::inbound_redacted(stamp(n), frame, &spans).unwrap())
}

/// `text` received on connection `n`, journaled with nothing blanked.
fn clear(n: u16, text: &str) -> Offer {
    Offer::Keep(Record::inbound(stamp(n), RawFrame::Text(text)))
}

/// The texts of `files`, the slice's, in order.
fn texts(files: &[(String, String)]) -> Vec<&str> {
    files.iter().map(|(_, text)| text.as_str()).collect()
}

#[test]
fn a_fill_responses_user_id_and_a_positions_id_are_blanked_whether_or_not_the_auth_spans_cover_them()
 {
    let (dir, _removed) = temp_dir("place-ids");
    // Each frame twice: once in clear, once as the journal keeps it with `src/auth`'s inbound
    // spans, which name the account's `user_id` and every `id` (decision 0028).
    let offers = session_records(
        0,
        vec![
            clear(0, FILL),
            spanned(0, FILL),
            clear(0, POSITION),
            spanned(0, POSITION),
        ],
    );
    let sink = journal(&dir, offers);
    let slice = |scrub| {
        let mut ev = run_of(reported(), [0, 0]);
        stage::journal_closed(&mut ev, Ok(()), &sink);
        let files = capture::slice_scrubbed(&dir.join("journal"), 0, &mut ev, scrub).unwrap();
        (ev, files)
    };
    let (ev, files) = slice(capture::Scrub::Places);
    // The authentication reply first, then each frame, blanked alike either way.
    assert_eq!(
        texts(&files)[1..],
        [
            FILL_SCRUBBED,
            FILL_SCRUBBED,
            POSITION_SCRUBBED,
            POSITION_SCRUBBED
        ]
    );
    // The spans did reach the journal: stage one's scrub, which blanks no member, reads the
    // spanned frames with exactly those members blanked and the ones in clear as written.
    let (_, arm) = slice(capture::Scrub::Arm);
    let arm = texts(&arm);
    assert_eq!(arm[2], SPANNED_FILL);
    assert!(arm[1].contains("u-synthetic-0001") && arm[1].contains("p-synthetic-0001"));
    assert!(!arm[4].contains("p-synthetic-0001") && arm[3].contains("p-synthetic-0001"));
    // The frames' kinds and places against the arm are the slice's as before.
    let kinds: Vec<(&str, When)> = (ev.sessions[0].frames.iter())
        .map(|f| (f.kind.as_str(), f.when))
        .collect();
    assert_eq!(
        kinds,
        [
            ("{result, type}", When::BeforeArm),
            ("{fill_response}", When::InWindow),
            ("{fill_response}", When::InWindow),
            ("{position_response}", When::InWindow),
            ("{position_response}", When::InWindow),
        ]
    );
}

/// What stage one's scrub makes of [`FILL`] as the journal keeps it spanned: its spans `2`s and
/// its numbers zeroed, but every other id, the symbol, the fee and the string amounts as written.
const SPANNED_FILL: &str = r#"{"fill_response":{"trade_id":"t-synthetic-0001","user_id":"2222222222222222","symbol":"SYNTH-USD","price":"101.25","quantity":0,"side":"BUY","order_id":"o-synthetic-0001","fee":"-0.0125","client_order_id":"c-synthetic-0001","position":{"id":"2222222222222222","position":"0.5"},"timestamp":0}}"#;

#[test]
fn a_price_given_as_a_string_is_zeroed_and_a_fee_given_as_a_string_or_a_number_keeps_only_its_sign()
{
    let text = r#"{"order_response":{"price":"101.25","quantity":"2","fee":"-0.0125","fills":[{"fee":"0.000"},{"fee":"3e-4"},{"fee":"-0"},{"fee":-0.3},{"fee":0},{"fee":2.5E-4},{"fee":-0.0},{"fee":"n/a"},{"fee":null}],"fees":0.002,"side":"BUY"}}"#;
    assert_eq!(
        capture::scrub_places(text),
        r#"{"order_response":{"price":"0","quantity":"0","fee":"-1","fills":[{"fee":"0"},{"fee":"1"},{"fee":"0"},{"fee":-1},{"fee":0},{"fee":1},{"fee":0},{"fee":"n/a"},{"fee":null}],"fees":0,"side":"BUY"}}"#
    );
    // Decimal-shaped: an optional sign, digits with an optional fraction (either side of the
    // point empty, not both) and an optional exponent. Anything else is kept, for the owner's
    // reading (`kept_strings`).
    let decimals = r#"{"a":["+5","-5",".5","5.","1e5","1E+05","2.5e-3","007"]}"#;
    assert_eq!(
        capture::scrub_places(decimals),
        r#"{"a":["0","0","0","0","0","0","0","0"]}"#
    );
    let others =
        r#"{"a":[".","e5","1e","1e+","1.2.3","-","","1,000","12%","0x1f","Infinity","BUY"]}"#;
    assert_eq!(capture::scrub_places(others), others);
    // A string is judged with its escapes decoded, so an amount spelled with them is zeroed and
    // a fee keeps its sign; an id is blanked at its length as written, as the codec's spans
    // blank one, and a string that is not a decimal is kept as written.
    let escaped = r#"{"price":"\u0031.5","quantity":"1\u002e5","fee":"\u002d0.5","id":"\u0031","side":"B\u0055Y"}"#;
    assert_eq!(
        capture::scrub_places(escaped),
        r#"{"price":"0","quantity":"0","fee":"-1","id":"222222","side":"B\u0055Y"}"#
    );
    // A top-level envelope echoing a request is kept as the codec's spans left it, as stage
    // one's scrub keeps one, but each `symbol` the spans blanked at its own length is the one
    // placeholder, whatever that length; one echoing nothing is scrubbed.
    let echo = r#"{"ack":{"incoming_message":{"type":"add_order","params":{"symbol":"222222222","price":"222222","quantity":22,"note":"BUY"}}}}"#;
    assert_eq!(
        capture::scrub_places(echo),
        r#"{"ack":{"incoming_message":{"type":"add_order","params":{"symbol":"22222222","price":"222222","quantity":22,"note":"BUY"}}}}"#
    );
    let err = r#"{"type":"Err","error_code":"InvalidOrder","message":"2222","incoming_message":{"type":"add_order","params":{"symbol":"2222222","price":"222"}}}"#;
    assert_eq!(
        capture::scrub_places(err),
        r#"{"type":"Err","error_code":"InvalidOrder","message":"2222","incoming_message":{"type":"add_order","params":{"symbol":"22222222","price":"222"}}}"#
    );
    let unspanned = r#"{"err":{"incoming_message":{"type":"cancel_order","params":{}}}}"#;
    assert_eq!(capture::scrub_places(unspanned), unspanned);
    assert_eq!(
        capture::scrub_places(r#"{"ack":{"order_id":"o-1","price":"9.5"}}"#),
        r#"{"ack":{"order_id":"222","price":"0"}}"#
    );
}

#[test]
fn every_id_member_at_any_depth_is_blanked_at_its_length_and_every_symbol_is_the_placeholder() {
    let text = r#"{"a":{"order_id":"o-1","b":[{"trade_id":"t-12"},{"id":123},{"id":-4.5e1}],"client_order_id":"cid-xyz","symbol":"SYNTH-USD"},"rows":[{"symbol":"OTHER-USD","user_id":"u"},{"symbol":null}],"id":["a","bb",7],"ids":["kept"],"symbol":"SYNTH-USD","_id":"z", "spaced" : { "symbol" : "SYNTH-USD" } , "twap_id":"","note":"say \"12\"","n":true}"#;
    assert_eq!(
        capture::scrub_places(text),
        r#"{"a":{"order_id":"222","b":[{"trade_id":"2222"},{"id":222},{"id":222222}],"client_order_id":"2222222","symbol":"22222222"},"rows":[{"symbol":"22222222","user_id":"2"},{"symbol":null}],"id":["2","22",2],"ids":["kept"],"symbol":"22222222","_id":"2", "spaced" : { "symbol" : "22222222" } , "twap_id":"","note":"say \"12\"","n":true}"#
    );
    // A member written twice is scrubbed twice, as written; an id's length is its bytes'.
    assert_eq!(
        capture::scrub_places(r#"{"order_id":"a","order_id":"bc","id":"é"}é"#),
        r#"{"order_id":"2","order_id":"22","id":"22"}é"#
    );
    // A name that does not decode (an unknown escape, or the text ending inside it) is read as
    // written; text cut inside a value or that is not JSON is scrubbed as far as it reads.
    assert_eq!(
        capture::scrub_places(r#"{"a\q":"1","b":2"#),
        r#"{"a\q":"0","b":0"#
    );
    assert_eq!(capture::scrub_places(r#"{"sym"#), r#"{"sym"#);
    assert_eq!(
        capture::scrub_places(r#"{"order_response":{"order_id":"o-12"#),
        r#"{"order_response":{"order_id":"2222"#
    );
    assert_eq!(capture::scrub_places(r#"{"price":"12.5"#), r#"{"price":"0"#);
    assert_eq!(
        capture::scrub_places(r#"{"symbol":""#),
        r#"{"symbol":"22222222"#
    );
    assert_eq!(capture::scrub_places("] x 12, -"), "] x 0, 0");
    assert_eq!(capture::scrub_places(r#""12""#), r#""0""#);
}

#[test]
fn a_place_deadline_does_not_close_the_arm_window() {
    let (dir, _removed) = temp_dir("place-deadline");
    // A place written under its own request id after the arm, its deadline firing before
    // anything answers the arm: the arm's ack after it is still in the arm's window, which only
    // the arm's own deadline ends.
    let place = Offer::Keep(Record::Outbound {
        at: MonoNs(0),
        conn: conn(0),
        rpc: Some(RpcId(2)),
        opcode: Opcode::Text,
        frame: WireSlice::plain(br#"{"type":"add_order","params":{}}"#.to_vec()),
    });
    let place_deadline = Offer::Keep(Record::RpcTimeout {
        stamp: stamp(0),
        rpc: RpcId(2),
    });
    // On the subaccount's connection too, the deadline of a request the arm was not written
    // under ends nothing; its arm's own does.
    let other = Offer::Keep(Record::RpcTimeout {
        stamp: stamp(1),
        rpc: RpcId(2),
    });
    let mut offers = session_records(0, vec![place, place_deadline, ack(0)]);
    let closed = offers.pop().unwrap();
    offers.extend([clear(0, POSITION), closed]);
    let mut sub = session_records(1, vec![other, ack(1)]);
    let closed = sub.pop().unwrap();
    sub.extend([ack(1), closed]);
    offers.extend(sub);
    let sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    let names: Vec<String> = files.unwrap().into_iter().map(|(file, _)| file).collect();
    assert_eq!(
        names,
        [
            "primary-001-before-arm.json",
            "primary-002-in-window.json",
            "primary-003-after-window.json",
            "subaccount-001-before-arm.json",
            "subaccount-002-in-window.json",
            "subaccount-003-after-window.json",
        ]
    );
    for run in &ev.sessions {
        assert!(
            run.answer().starts_with("An ack echoing the arm"),
            "{run:?}"
        );
    }
}

/// The made-up account's id, written as an EVM address, which the privacy check's patterns
/// refuse: built here, so this file holds none.
fn address() -> String {
    format!("0x{}", "5a".repeat(20))
}

#[test]
fn a_scrubbed_slice_of_hand_built_place_frames_passes_the_privacy_check() {
    let (dir, _removed) = temp_dir("place-privacy");
    let user = address();
    let order = format!(
        r#"{{"order_response":{{"order_id":"o-synthetic-0002","symbol":"{MARKET}","status":"ACK","quantity":"0.5","price":"101.25","side":"BUY","type":"LIMIT","time_in_force":"GTC","user_id":"{user}","client_order_id":"c-synthetic-0002","quantity_remaining":"0.5","update_time":1700000000.25,"trade_id":null}}}}"#
    );
    let fill = FILL
        .replace("u-synthetic-0001", &user)
        .replace("-0.0125", "0.0125");
    let page = format!(
        r#"{{"all_orders_response":{{"orders":[{{"order_id":"o-synthetic-0003","symbol":"{MARKET}","price":"99.5","quantity":1,"user_id":"{user}"}}],"twaps":[]}}}}"#
    );
    let balance = format!(
        r#"{{"balance_response":{{"id":"b-synthetic-0001","user_id":"{user}","deposit":"12.5","available_balance":12.25,"fees":"0.0125"}}}}"#
    );
    // A place's ack, as the codec's spans leave one (decision 0134): kept as it is, but its
    // `symbol` the placeholder.
    let echo = r#"{"ack":{"incoming_message":{"type":"add_order","params":{"symbol":"222222222","price":"222222","quantity":"222","client_order_id":"2222222222222222"}}}}"#;
    let frames = [
        order.as_str(),
        fill.as_str(),
        POSITION,
        page.as_str(),
        balance.as_str(),
        echo,
    ];
    // Four of them hold the account's address-shaped id, which the patterns refuse.
    let holding = [&order, &fill, &page, &balance];
    for frame in holding {
        assert!(!evidence::privacy_hits(frame).is_empty(), "{frame}");
    }
    let offers = session_records(0, frames.iter().map(|frame| clear(0, frame)).collect());
    let sink = journal(&dir, offers);
    let slice = |scrub, into: &str| {
        let mut ev = run_of(reported(), [0, 0]);
        stage::journal_closed(&mut ev, Ok(()), &sink);
        let files = capture::slice_scrubbed(&dir.join("journal"), 0, &mut ev, scrub).unwrap();
        capture::write_fixtures_scrubbed(&dir.join(into), files, &mut ev, scrub).unwrap();
        ev
    };

    // The place stages' scrub: every file written, and the privacy check passes them.
    let ev = slice(capture::Scrub::Places, "places");
    assert!(ev.withheld.is_empty(), "{:?}", ev.withheld);
    assert_eq!(ev.fixtures.len(), 1 + frames.len() + 1);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &dir.join("places")).unwrap();
    // No made-up id, account, symbol, size, price or fee is left, but each fee's sign.
    let written: Vec<String> = (ev.fixtures.iter())
        .filter(|file| file.ends_with(".json"))
        .map(|file| fs::read_to_string(dir.join("places").join(file)).unwrap())
        .collect();
    for text in &written {
        for made_up in [
            "synthetic",
            MARKET,
            "5a5a",
            "101.25",
            "99.5",
            "12.5",
            "0.0125",
            "0.5",
        ] {
            assert!(!text.contains(made_up), "{made_up} in {text}");
        }
    }
    assert!(written[2].contains(r#""fee":"1""#), "{}", written[2]);
    let placeheld = format!(r#""symbol":"{}""#, capture::SYMBOL);
    assert_eq!(
        written[6],
        echo.replace(r#""symbol":"222222222""#, &placeheld)
    );
    assert!(written[5].contains(r#""fees":"0""#), "{}", written[5]);
    // The members it lists for the owner's reading keep strings it did not write: no amount it
    // zeroed and no fee's sign.
    assert_eq!(ev.strings, PLACES_KEPT);

    // Stage one's scrub keeps the strings, so the address-shaped id would be published: every
    // file holding it is withheld.
    let ev = slice(capture::Scrub::Arm, "arm");
    assert_eq!(ev.withheld.len(), holding.len(), "{:?}", ev.withheld);
    // What it keeps of the rest is listed whole, the position's amount as received among it.
    assert!(
        ev.strings.iter().any(|m| m == "position"),
        "{:?}",
        ev.strings
    );
}

/// The members whose string values the place stages' slice of the privacy test's frames keeps
/// as received: the authentication reply's `result`, the order's and the fill's `side`, the
/// order's `status`, `time_in_force` and `type`, the echo's `type` and the position's
/// `margin_mode`.
const PLACES_KEPT: [&str; 6] = [
    "margin_mode",
    "result",
    "side",
    "status",
    "time_in_force",
    "type",
];

#[test]
fn the_place_scrub_leaves_stage_ones_production_capture_as_it_was_committed() {
    // FBC-291a's capture, which the owner committed after placing each `symbol` by hand: the
    // place stages' scrub leaves every file as it is, and makes the same of each with a symbol
    // in its place, so it scrubs as FBC-291a's capture was scrubbed (decision 0148 item 10).
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/qfex/exec-recorded/arm-production");
    let mut read = 0;
    for entry in fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|ext| ext != "json") {
            continue;
        }
        let committed = fs::read_to_string(&path).unwrap();
        assert_eq!(capture::scrub_places(&committed), committed, "{path:?}");
        assert_eq!(capture::scrub(&committed), committed, "{path:?}");
        let placeheld = format!(r#""symbol":"{}""#, capture::SYMBOL);
        let received = committed.replace(&placeheld, &format!(r#""symbol":"{MARKET}""#));
        assert_eq!(capture::scrub_places(&received), committed, "{path:?}");
        read += 1;
    }
    assert_eq!(read, 21);
}
