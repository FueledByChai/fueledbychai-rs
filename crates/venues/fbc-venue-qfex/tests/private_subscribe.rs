//! FBC-bsou: QFEX's trade codec reports the stream authenticated only once QFEX has confirmed
//! every private subscription the epoch sent (decision 0132 item 1), so the session's
//! cancel-on-disconnect arm and resync, which it sends on that report, never go out ahead of a
//! channel that would carry what they cause.
//!
//! - The auth reply, in its documented and its observed shape, sends one subscribe frame each
//!   for `order_responses`, `fills` and `positions` and pushes no connection event.
//! - `ConnState::Authenticated` is pushed once per epoch, on the last of the three
//!   `{"subscribed":…}` confirmations, whatever their order. A confirmation of a channel the
//!   codec never subscribes, a repeat and a generic ack confirm nothing.
//! - A frame between the auth reply and the last confirmation takes the path it takes once the
//!   stream is authenticated (a private state frame ends the epoch, a `RATE_LIMITED` order holds
//!   the budget) and confirms nothing.
//! - Before the last confirmation a non-rate err ends the epoch, and a RateLimited err holds the
//!   budget with the confirmation still pending (decision 0134 item 5). An `unsubscribed` frame
//!   for a subscribed channel ends the epoch, before and after the report.
//! - QFEX's 60-second authentication deadline runs from the opening until the last
//!   confirmation: with any confirmation missing it ends that epoch only.
//! - Every case runs twice: live, its frames journaled through fbc-journal with the codec's own
//!   `redact_inbound` spans, and in replay, a fresh codec handed the frames the journal gives
//!   back, as decoder replay hands them. Each step gives the same events and effects both ways,
//!   and the two codecs count the same. Decision 0134 names no echoing frame whose replay
//!   diverges, so there is no exception.
//!
//! QFEX's pages: the trade AsyncAPI (<https://docs.qfex.com/websocket/trade.yaml>) for the
//! subscribe requests and the `subscribed` and `unsubscribed` replies; the
//! [orders](https://docs.qfex.com/websocket/channels/trade/order),
//! [fills](https://docs.qfex.com/websocket/channels/trade/fills) and
//! [positions](https://docs.qfex.com/websocket/channels/trade/positions) channel pages, the last
//! of which queues one `position_response` per cached position after its acknowledgement; and
//! the [authenticate](https://docs.qfex.com/websocket/channels/trade/authenticate) and
//! [errors](https://docs.qfex.com/websocket/errors) pages. Every key, account and frame here is
//! synthetic: built from those shapes, or `fixtures/qfex/private` (its `SYNTHETIC` file).

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fbc_core::{
    ConnKey, ConnState, DecodeError, Effect, Effects, EncodeCtx, ExecCodec, ExecEvent, ExecSink,
    Inbound, MonoNs, Namespace, NonceBlock, OpKind, RateCharge, RawFrame, Secret, Secrets,
    SpecTable, Stamp, TimerTag, TrafficClass, VenueCaps, VenueConfig, VenueFactory, VenueMeta,
    WallNs, WireSlice, check_redactions, dispatch,
};
use fbc_journal::{JournalReader, JournalWriter, Record, RedactionKey};
use fbc_venue_qfex::auth::{self, TradeAuth};
use fbc_venue_qfex::exec::{AUTH_DEADLINE, CHANNELS, ErrCode, PrivateState, QfexExec, TRADE};
use fbc_venue_qfex::{KEY_MDS_URL, Qfex};
use rust_decimal::Decimal;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// The primary account and a subaccount selection, each case run for both.
const ACCOUNTS: [&str; 2] = ["primary", SUB];
/// The authenticate page's success reply, and the AsyncAPI's.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;
const AUTHENTICATED: &str = r#"{"authenticated":true}"#;
const REPLIES: [&str; 2] = [AUTH_OK, AUTHENTICATED];
/// The channels the codec subscribes, by name.
const ORDERS: &str = "order_responses";
const FILLS: &str = "fills";
const POSITIONS: &str = "positions";
/// What the codec answers a frame it does not recognise once past the auth reply.
const UNRECOGNISED: DecodeError = DecodeError::Malformed("unrecognised QFEX trade frame");

// ----- Building and driving a codec -----

fn cfg(account: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg.insert(auth::ACCOUNT_ID, account);
    cfg.insert(
        auth::ACCOUNT_KEY,
        if account == "primary" { "4" } else { "9" },
    );
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

fn fresh(account: &str) -> QfexExec {
    QfexExec::new(
        TradeAuth::new(&cfg(account), secrets()).unwrap(),
        Duration::from_secs(2),
    )
}

fn caps() -> VenueCaps {
    Qfex.caps(&cfg("primary")).unwrap()
}

/// The context of the `n`th opening: its wall time and its two nonces.
fn ctx(n: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_760_000_000_000_000_000 + n as i64 * 1_000_000_000),
        mono: MonoNs(n),
        nonces: NonceBlock::new(vec![n, u64::MAX - n]),
    }
}

#[derive(Default)]
struct Sink(Vec<ExecEvent>);

impl ExecSink for Sink {
    fn push(&mut self, _meta: VenueMeta, event: ExecEvent) {
        self.0.push(event);
    }
}

/// What one call gave.
#[derive(Debug, PartialEq)]
struct Out {
    result: Result<(), DecodeError>,
    events: Vec<ExecEvent>,
    effects: Vec<Effect>,
}

impl Out {
    fn quiet() -> Out {
        Out {
            result: Ok(()),
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    fn effects(effects: Vec<Effect>) -> Out {
        Out {
            effects,
            ..Out::quiet()
        }
    }

    /// Whether it ended its epoch, and only that.
    fn reconnects(&self) -> bool {
        self.events.is_empty()
            && matches!(
                self.effects.as_slice(),
                [Effect::Reconnect { stream: TRADE, .. }]
            )
    }

    /// Whether it reported the stream authenticated, and only that.
    fn authenticates(&self) -> bool {
        *self
            == Out {
                events: vec![authenticated()],
                ..Out::quiet()
            }
    }
}

fn authenticated() -> ExecEvent {
    ExecEvent::Conn {
        stream: TRADE,
        state: ConnState::Authenticated,
    }
}

fn held(op: Option<OpKind>, retry_after: Option<Duration>) -> Effect {
    Effect::RateLimited {
        stream: TRADE,
        charge: op.map(|op| RateCharge::one(op, None)),
        retry_after,
    }
}

/// The three subscribe frames, as the AsyncAPI's `subscribe_orders`, `subscribe_fills` and
/// `subscribe_positions` show them, each Safety traffic charged the rate page's 0.1.
fn subscriptions() -> Vec<Effect> {
    [ORDERS, FILLS, POSITIONS]
        .into_iter()
        .map(|channel| Effect::Send {
            stream: TRADE,
            frame: WireSlice::plain(
                format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#)
                    .into_bytes(),
            ),
            rpc: None,
            class: TrafficClass::Safety,
            charge: RateCharge::one(OpKind::Subscribe, None),
        })
        .collect()
}

fn on_frame(codec: &mut QfexExec, frame: &str) -> Out {
    let caps = caps();
    let mut sink = Sink::default();
    let mut fx = Effects::new();
    let result = dispatch(&caps, Namespace::new(1), |scope| {
        codec.on_frame(
            TRADE,
            RawFrame::Text(frame),
            scope,
            &SpecTable::new(),
            &mut sink,
            &mut fx,
        )
    });
    Out {
        result,
        events: sink.0,
        effects: fx.take(),
    }
}

/// The frame as the journal reads it back: each of the codec's spans blanked to `2`s.
fn redacted(codec: &QfexExec, frame: &str) -> String {
    let spans = codec.redact_inbound(Inbound::Frame(RawFrame::Text(frame)));
    check_redactions(frame.as_bytes(), spans.body()).unwrap();
    let mut bytes = frame.as_bytes().to_vec();
    for span in spans.body() {
        bytes[span.start as usize..span.end as usize].fill(b'2');
    }
    String::from_utf8(bytes).unwrap()
}

fn fixture(name: &str) -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../fixtures/qfex/private")
            .join(name),
    )
    .unwrap()
}

fn confirm(channel: &str) -> String {
    format!(r#"{{"subscribed":"{channel}"}}"#)
}

fn unsubscribed(channel: &str) -> String {
    format!(r#"{{"unsubscribed":"{channel}"}}"#)
}

/// QFEX's echo of the subscribe request for `channel`.
fn subscribe_echo(channel: &str) -> String {
    format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#)
}

/// A nested err, as QFEX's errors page and AsyncAPI write one, echoing `echo`.
fn nested(code: &str, message: &str, echo: &str) -> String {
    format!(r#"{{"err":{{"error_code":"{code}","message":{message},"incoming_message":{echo}}}}}"#)
}

/// A flat err, as QFEX's rate page writes one.
fn flat(code: &str, message: &str, echo: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"{code}","message":{message},"incoming_message":{echo}}}"#
    )
}

/// One input of a case.
enum Step {
    /// The `n`th opening of the trade stream.
    Open(u64),
    Frame(String),
    /// The deadline the `n`th opening asked for fires.
    Deadline(u64),
}

fn frame(text: &str) -> Step {
    Step::Frame(text.to_owned())
}

/// The steps that open epoch `n`, answer its authentication with `reply` and confirm
/// `channels`, in that order.
fn epoch(n: u64, reply: &str, channels: &[&str]) -> Vec<Step> {
    let mut steps = vec![Step::Open(n), frame(reply)];
    steps.extend(channels.iter().map(|channel| Step::Frame(confirm(channel))));
    steps
}

/// Runs `steps` through `codec`, reading each frame as `journal` hands it back.
fn drive(
    codec: &mut QfexExec,
    steps: &[Step],
    mut journal: impl FnMut(&QfexExec, &str) -> String,
) -> Vec<Out> {
    let mut deadlines = HashMap::new();
    let mut outs = Vec::new();
    for step in steps {
        let out = match step {
            Step::Open(n) => {
                let mut fx = Effects::new();
                codec.on_open(TRADE, &ctx(*n), &mut fx);
                let effects = fx.take();
                // The opening asks for the deadline, once: QFEX's minute.
                let tags: Vec<TimerTag> = effects
                    .iter()
                    .filter_map(|effect| match effect {
                        Effect::Timer { tag, after } => {
                            assert_eq!(*after, AUTH_DEADLINE);
                            Some(*tag)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(tags.len(), 1, "{effects:?}");
                deadlines.insert(*n, tags[0]);
                Out::effects(effects)
            }
            Step::Frame(text) => {
                let text = journal(codec, text);
                on_frame(codec, &text)
            }
            Step::Deadline(n) => {
                let mut fx = Effects::new();
                codec.on_timer(deadlines[n], &ctx(*n), &mut fx);
                Out::effects(fx.take())
            }
        };
        outs.push(out);
    }
    outs
}

fn stamp(seq: u64) -> Stamp {
    Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: MonoNs(seq),
        recv_wall: WallNs(1_760_000_000_000_000_000),
        conn: ConnKey { conn: 0, epoch: 1 },
    }
}

/// Runs `steps` for `account` live, journaling each frame through fbc-journal with the codec's
/// own spans as the runtime does, then hands a fresh codec the same steps with the frames the
/// journal reads back, as decoder replay does. Every step must give the same events and effects
/// both ways, and the two codecs must count and keep the same. Returns the live codec and what
/// each step gave it.
fn run(account: &str, steps: &[Step]) -> (QfexExec, Vec<Out>) {
    static CASE: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "fbc-qfex-bsou-{}-{}",
        std::process::id(),
        CASE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&root);
    let key = Arc::new(RedactionKey::new(&[7; 32]).unwrap());
    let mut writer = JournalWriter::create(&root, 0, key).unwrap();
    let mut seq = 0;
    let mut live = fresh(account);
    let live_outs = drive(&mut live, steps, |codec, text| {
        let raw = RawFrame::Text(text);
        let spans = codec.redact_inbound(Inbound::Frame(raw));
        check_redactions(text.as_bytes(), spans.body()).unwrap();
        let record = Record::inbound_redacted(stamp(seq), raw, &spans).unwrap();
        seq += 1;
        writer
            .append(WallNs(1_760_000_000_000_000_000), &record)
            .unwrap();
        text.to_owned()
    });
    writer.flush().unwrap();
    drop(writer);
    let back: Vec<String> = JournalReader::open(&root, 0)
        .unwrap()
        .map(|record| match record.unwrap() {
            Record::Inbound { bytes, .. } => String::from_utf8(bytes.0).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect();
    let _ = fs::remove_dir_all(&root);
    assert_eq!(back.len() as u64, seq);

    let mut replay = fresh(account);
    let mut journaled = back.into_iter();
    let replay_outs = drive(&mut replay, steps, |_, _| journaled.next().unwrap());
    assert_eq!(live_outs.len(), replay_outs.len());
    for (at, (live, replayed)) in live_outs.iter().zip(&replay_outs).enumerate() {
        assert_eq!(live, replayed, "{account}: step {at}");
    }
    assert_eq!(live.counts(), replay.counts(), "{account}");
    assert_eq!(live.last_private(), replay.last_private(), "{account}");
    (live, live_outs)
}

/// How many times `outs` reported the stream authenticated.
fn reports(outs: &[Out]) -> usize {
    outs.iter()
        .flat_map(|out| &out.events)
        .filter(|event| **event == authenticated())
        .count()
}

// ----- The auth reply -----

#[test]
fn the_auth_reply_in_either_shape_sends_the_three_subscriptions_and_pushes_no_conn_event() {
    assert_eq!(CHANNELS, [ORDERS, FILLS, POSITIONS]);
    for account in ACCOUNTS {
        for reply in REPLIES {
            let (_, outs) = run(account, &epoch(1, reply, &[]));
            let out = &outs[1];
            assert_eq!(out.result, Ok(()), "{account} {reply}");
            assert!(out.events.is_empty(), "{account} {reply}: {out:?}");
            assert_eq!(out.effects, subscriptions(), "{account} {reply}");
            // The opening asked only for the authentication frame and its deadline.
            assert!(outs[0].events.is_empty() && outs[0].effects.len() == 2);
            assert_eq!(reports(&outs), 0);
        }
    }
}

// ----- The confirmations -----

#[test]
fn authenticated_is_pushed_exactly_once_on_the_third_confirmation_in_every_order() {
    let orders = [
        [ORDERS, FILLS, POSITIONS],
        [ORDERS, POSITIONS, FILLS],
        [FILLS, ORDERS, POSITIONS],
        [FILLS, POSITIONS, ORDERS],
        [POSITIONS, ORDERS, FILLS],
        [POSITIONS, FILLS, ORDERS],
    ];
    for account in ACCOUNTS {
        for (at, order) in orders.iter().enumerate() {
            let reply = REPLIES[at % 2];
            let mut steps = epoch(1, reply, order);
            // Each confirmation again, once the stream is authenticated.
            steps.extend(order.iter().map(|channel| Step::Frame(confirm(channel))));
            // The next epoch confirms afresh, in the reverse order: nothing carries over.
            let mut reverse = *order;
            reverse.reverse();
            steps.extend(epoch(2, REPLIES[(at + 1) % 2], &reverse));
            let (codec, outs) = run(account, &steps);
            let case = format!("{account} {order:?}");
            assert_eq!(outs[1].effects, subscriptions(), "{case}");
            assert_eq!(outs[2], Out::quiet(), "{case}");
            assert_eq!(outs[3], Out::quiet(), "{case}");
            assert!(outs[4].authenticates(), "{case}: {:?}", outs[4]);
            for out in &outs[5..8] {
                assert_eq!(*out, Out::quiet(), "{case}: a repeat once authenticated");
            }
            assert!(outs[9].events.is_empty(), "{case}");
            assert_eq!(outs[10], Out::quiet(), "{case}");
            assert_eq!(outs[11], Out::quiet(), "{case}");
            assert!(outs[12].authenticates(), "{case}: {:?}", outs[12]);
            assert_eq!(reports(&outs), 2, "{case}: once per epoch");
            assert_eq!(codec.counts().subscribed, 9);
        }
    }
}

#[test]
fn a_balances_or_stop_orders_confirmation_a_repeat_and_an_ack_confirm_nothing() {
    for account in ACCOUNTS {
        // A repeat: only the first confirmation of a channel counts.
        let mut steps = epoch(1, AUTH_OK, &[ORDERS, ORDERS, FILLS, FILLS, ORDERS]);
        steps.push(Step::Frame(confirm(POSITIONS)));
        let (_, outs) = run(account, &steps);
        for out in &outs[2..7] {
            assert_eq!(*out, Out::quiet(), "{account}");
        }
        assert!(outs[7].authenticates(), "{account}: {:?}", outs[7]);
        assert_eq!(reports(&outs), 1);

        // An ack, even one echoing the subscribe of the missing channel: the journal keeps its
        // `subscribe` type but not the channel it names (decision 0134 items 3 and 4), so an ack
        // names no channel the codec may read.
        let ack = format!(
            r#"{{"ack":{{"incoming_message":{}}}}}"#,
            subscribe_echo(POSITIONS)
        );
        assert_eq!(
            redacted(&fresh(account), &ack),
            format!(
                r#"{{"ack":{{"incoming_message":{}}}}}"#,
                subscribe_echo(&"2".repeat(POSITIONS.len()))
            )
        );
        let mut steps = epoch(1, AUTHENTICATED, &[ORDERS, FILLS]);
        steps.extend([
            Step::Frame(ack.clone()),
            frame(r#"{"ack":{"incoming_message":{}}}"#),
            Step::Frame(confirm(POSITIONS)),
        ]);
        let (codec, outs) = run(account, &steps);
        assert_eq!(outs[4], Out::quiet(), "{account}");
        assert_eq!(outs[5], Out::quiet(), "{account}");
        assert!(outs[6].authenticates(), "{account}: {:?}", outs[6]);
        assert_eq!(codec.counts().acks, 2);

        // A confirmation of a channel the codec never subscribes confirms nothing, and takes the
        // path it takes once the stream is authenticated: it is not recognised, and ends the
        // epoch, whose last confirmation then reports nothing.
        for other in ["balances", "stop_orders"] {
            let mut steps = epoch(1, AUTH_OK, &[ORDERS, FILLS]);
            steps.extend([Step::Frame(confirm(other)), Step::Frame(confirm(POSITIONS))]);
            steps.extend(epoch(2, AUTH_OK, &[ORDERS, FILLS, POSITIONS]));
            steps.push(Step::Frame(confirm(other)));
            let (_, outs) = run(account, &steps);
            let case = format!("{account} {other}");
            assert_eq!(outs[4].result, Err(UNRECOGNISED), "{case}");
            assert!(outs[4].reconnects(), "{case}: {:?}", outs[4]);
            assert!(outs[5].reconnects() && outs[5].result.is_ok(), "{case}");
            assert!(
                outs[10].authenticates(),
                "{case}: the next epoch is untouched"
            );
            assert_eq!(outs[11], outs[4], "{case}: as once authenticated");
            assert_eq!(reports(&outs), 1, "{case}");
        }
    }
}

// ----- Frames between the auth reply and the last confirmation -----

#[test]
fn a_position_response_before_the_last_confirmation_takes_the_after_authentication_path() {
    let position = fixture("position_response.json");
    let rate_limited = r#"{"order_response":{"order_id":"11111111-1111-4111-8111-111111111111","symbol":"AAPL-USD","status":"RATE_LIMITED","client_order_id":"synthetic-cid-1"}}"#;
    for account in ACCOUNTS {
        // QFEX queues one position_response per cached position right after the positions
        // acknowledgement, so it can come before the other two.
        let mut steps = epoch(1, AUTHENTICATED, &[POSITIONS]);
        steps.extend([
            Step::Frame(position.clone()),
            Step::Frame(confirm(ORDERS)),
            Step::Frame(confirm(FILLS)),
        ]);
        let (codec, early) = run(account, &steps);
        // As once authenticated: a private state frame ends the epoch (FBC-zeb0).
        let mut steps = epoch(1, AUTHENTICATED, &[POSITIONS, ORDERS, FILLS]);
        steps.push(Step::Frame(position.clone()));
        let (_, late) = run(account, &steps);
        assert!(late[4].authenticates());
        assert_eq!(early[3], late[5], "{account}");
        assert!(
            early[3].reconnects() && early[3].result.is_ok(),
            "{account}"
        );
        assert_eq!(
            codec.last_private(),
            Some(&PrivateState::Position {
                position: Decimal::from_str_exact("1.0").unwrap(),
            })
        );
        assert_eq!(codec.counts().private_state, 1);
        // It confirmed nothing, and the epoch it ended reports nothing.
        for out in &early[4..] {
            assert!(out.reconnects() && out.result.is_ok(), "{account}: {out:?}");
        }
        assert_eq!(reports(&early), 0);

        // A RATE_LIMITED order holds the budget, as once authenticated, and the confirmations
        // go on.
        let mut steps = epoch(1, AUTH_OK, &[FILLS]);
        steps.extend([
            frame(rate_limited),
            Step::Frame(confirm(POSITIONS)),
            Step::Frame(confirm(ORDERS)),
            frame(rate_limited),
        ]);
        let (_, outs) = run(account, &steps);
        assert_eq!(outs[3], Out::effects(vec![held(None, None)]), "{account}");
        assert_eq!(outs[4], Out::quiet());
        assert!(outs[5].authenticates(), "{account}: {:?}", outs[5]);
        assert_eq!(outs[6], outs[3]);
    }
}

#[test]
fn before_the_last_confirmation_a_non_rate_err_ends_the_epoch_and_a_rate_refusal_holds() {
    let order = [ORDERS, FILLS, POSITIONS];
    for account in ACCOUNTS {
        for confirmed in 0..order.len() {
            // The channel whose confirmation the err stands in for.
            let refused = order[confirmed];
            let echo = subscribe_echo(refused);
            let mut errs: Vec<String> = ErrCode::ALL
                .into_iter()
                .filter(|code| *code != ErrCode::RateLimited)
                .flat_map(|code| {
                    [
                        nested(code.wire(), r#""refused""#, &echo),
                        flat(code.wire(), "null", &echo),
                    ]
                })
                .collect();
            errs.extend([
                nested("SomethingQfexAddedLater", "null", &echo),
                r#"{"err":{"error_code":"InvalidParameter","message":"no echo"}}"#.to_owned(),
                r#"{"err":{"message":"no code at all"}}"#.to_owned(),
            ]);
            for err in &errs {
                let mut steps = epoch(1, AUTH_OK, &order[..confirmed]);
                steps.push(Step::Frame(err.clone()));
                steps.extend(order[confirmed..].iter().map(|c| Step::Frame(confirm(c))));
                steps.extend(epoch(2, AUTHENTICATED, &order));
                let (_, outs) = run(account, &steps);
                let case = format!("{account}, {confirmed} confirmed, {err}");
                let at = 2 + confirmed;
                assert!(
                    outs[at].reconnects() && outs[at].result.is_ok(),
                    "{case}: {:?}",
                    outs[at]
                );
                // The rest of that epoch reports nothing; the next one is untouched.
                for out in &outs[at + 1..at + 1 + order.len() - confirmed] {
                    assert!(out.reconnects(), "{case}: {out:?}");
                }
                assert!(outs.last().unwrap().authenticates(), "{case}");
                assert_eq!(reports(&outs), 1, "{case}");
            }

            // A RateLimited err, in either envelope and either hint form, holds the subscribe
            // bucket for its hint; the confirmation stays pending, and its arrival reports.
            let rate = [
                (
                    nested(
                        "RateLimited",
                        r#""Too many requests. Please retry after 200ms.""#,
                        &echo,
                    ),
                    held(Some(OpKind::Subscribe), Some(Duration::from_millis(200))),
                ),
                (
                    flat(
                        "RateLimited",
                        r#""Rate limit exceeded, retry after 3 seconds""#,
                        &echo,
                    ),
                    held(Some(OpKind::Subscribe), Some(Duration::from_secs(3))),
                ),
                (nested("RateLimited", "null", "{}"), held(None, None)),
            ];
            for (err, hold) in rate {
                let mut steps = epoch(1, AUTHENTICATED, &order[..confirmed]);
                steps.push(Step::Frame(err.clone()));
                steps.extend(order[confirmed..].iter().map(|c| Step::Frame(confirm(c))));
                // Once reported, a request's err is counted and keeps the epoch open.
                steps.push(Step::Frame(nested("InvalidParameter", "null", &echo)));
                let (codec, outs) = run(account, &steps);
                let case = format!("{account}, {confirmed} confirmed, {err}");
                let at = 2 + confirmed;
                assert_eq!(outs[at], Out::effects(vec![hold]), "{case}");
                for out in &outs[at + 1..outs.len() - 2] {
                    assert_eq!(*out, Out::quiet(), "{case}");
                }
                assert!(outs[outs.len() - 2].authenticates(), "{case}");
                assert_eq!(*outs.last().unwrap(), Out::quiet(), "{case}");
                assert_eq!(codec.counts().errs(ErrCode::RateLimited), 1);
                assert_eq!(codec.counts().rate_holds, 1);
            }
        }
    }
}

#[test]
fn an_unsubscribed_frame_for_a_subscribed_channel_ends_the_epoch_before_and_after_authenticated() {
    let order = [FILLS, POSITIONS, ORDERS];
    for account in ACCOUNTS {
        for channel in CHANNELS {
            // Before any confirmation, between them, and once the stream is reported
            // authenticated.
            for confirmed in 0..=order.len() {
                let mut steps = epoch(1, AUTH_OK, &order[..confirmed]);
                steps.push(Step::Frame(unsubscribed(channel)));
                steps.extend(order[confirmed..].iter().map(|c| Step::Frame(confirm(c))));
                steps.extend(epoch(2, AUTH_OK, &order));
                let (codec, outs) = run(account, &steps);
                let case = format!("{account} {channel}, {confirmed} confirmed");
                let at = 2 + confirmed;
                assert!(
                    outs[at].reconnects() && outs[at].result.is_ok(),
                    "{case}: {:?}",
                    outs[at]
                );
                for out in &outs[at + 1..at + 1 + order.len() - confirmed] {
                    assert!(out.reconnects(), "{case}: {out:?}");
                }
                assert!(outs.last().unwrap().authenticates(), "{case}");
                // Reported once in the second epoch, and in the first only when every channel
                // was confirmed before QFEX unsubscribed one.
                let first = usize::from(confirmed == order.len());
                assert_eq!(reports(&outs), 1 + first, "{case}");
                assert_eq!(codec.counts().unsubscribed, 1, "{case}");
            }
        }
        // A channel the codec never subscribes is not recognised, and ends the epoch too.
        let mut steps = epoch(1, AUTH_OK, &[ORDERS]);
        steps.push(Step::Frame(unsubscribed("balances")));
        let (codec, outs) = run(account, &steps);
        assert_eq!(outs[3].result, Err(UNRECOGNISED));
        assert!(outs[3].reconnects());
        assert_eq!(codec.counts().unsubscribed, 0);
    }
}

// ----- The deadline -----

#[test]
fn with_only_two_confirmations_the_60_second_timer_ends_that_epoch_and_an_earlier_epochs_timer_does_nothing()
 {
    assert_eq!(AUTH_DEADLINE, Duration::from_secs(60));
    for account in ACCOUNTS {
        let mut steps = epoch(1, AUTH_OK, &[ORDERS, FILLS]);
        steps.extend([Step::Deadline(1), Step::Frame(confirm(POSITIONS))]);
        // Epoch 2: the first epoch's timer does nothing, the last confirmation reports, and
        // the epoch's own timer then does nothing either.
        steps.extend(epoch(2, AUTHENTICATED, &[FILLS, POSITIONS]));
        steps.extend([
            Step::Deadline(1),
            Step::Frame(confirm(ORDERS)),
            Step::Deadline(2),
        ]);
        // Epoch 3: the reply and no confirmation; epoch 4: two again, an earlier epoch's timer,
        // then its own, which ends it once.
        steps.extend(epoch(3, AUTH_OK, &[]));
        steps.push(Step::Deadline(3));
        steps.extend(epoch(4, AUTHENTICATED, &[POSITIONS, ORDERS]));
        steps.extend([Step::Deadline(3), Step::Deadline(4), Step::Deadline(4)]);
        // Epoch 5: no auth reply at all (FBC-zeb0 (e)).
        steps.extend([Step::Open(5), Step::Deadline(5)]);
        let (_, outs) = run(account, &steps);

        assert_eq!(outs.len(), 25);
        assert!(outs[4].reconnects() && outs[4].result.is_ok(), "{account}");
        assert!(outs[5].reconnects(), "late confirmation of an ended epoch");
        assert_eq!(outs[10], Out::quiet(), "{account}: the first epoch's timer");
        assert!(outs[11].authenticates(), "{account}: {:?}", outs[11]);
        assert_eq!(outs[12], Out::quiet(), "{account}: confirmed in time");
        assert!(outs[15].reconnects(), "{account}: no confirmation at all");
        assert_eq!(outs[20], Out::quiet(), "{account}: the third epoch's timer");
        assert!(outs[21].reconnects(), "{account}");
        assert_eq!(outs[22], Out::quiet(), "{account}: the epoch already ended");
        assert!(outs[24].reconnects(), "{account}: no auth reply");
        assert_eq!(reports(&outs), 1);
        // No auth reply arms a timer of its own, nor reports: the opening's deadline runs on.
        for reply in [1, 7, 14, 17] {
            assert_eq!(outs[reply], Out::effects(subscriptions()), "{account}");
        }
    }
}
