//! FBC-ja3's done line, the order-entry codec's part: the conformance toy resyncs over REST
//! (Codex r4172917335), its request keeping its base URL's credential spans, its response
//! decoded whole, envelope included, before anything is pushed, and pushed in one call; an
//! `HttpFailure`, a status other than 200 or a body it cannot decode whole pushes nothing and is
//! retried on a timer; a pinging codec arms its ping on open and again on each firing (Codex
//! r4172835747); and a fill frame missing any field its `FillCaps` declare is refused, nothing
//! pushed (Codex r4172917309).

use std::sync::OnceLock;

use fbc_conformance::toy::{
    self, EXEC_STREAM, FillIds, INST_A, INST_B, OWN_NS, PING_EVERY, PING_TAG, RESYNC_RETRY,
    RESYNC_RETRY_TAG, RESYNC_TIMEOUT, TOY_TOKEN, ToyExec, ToySigner,
};
use fbc_core::{
    AccountKey, CidMatch, CidMint, ClientOrderId, DecodeError, Effect, Effects, EncodeCtx,
    ExecCodec, ExecEvent, ExecSink, FillCaps, HttpFailure, HttpMethod, HttpRequest, HttpResponse,
    HttpTag, Lots, MonoNs, NamespaceLease, NonceBlock, OpKind, PxExact, RateCharge, RawFrame, Side,
    SignedLots, StreamId, Ticks, TimerTag, TrafficClass, VenueMeta, VenueOrderId,
    VenueOrderSnapshot, VenueOrderState, WallNs, WireSlice, WireUrl, encode_cid,
};

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const WALL: i64 = 1_759_363_200_000_000_000;
const WM: i64 = WALL + 5_000;

/// A synthetic credential in the REST base's path, of no account and no venue.
const SECRET: &str = "SYNTHETIC-REST-PATH-KEY";

fn ctx_at(wall: i64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(wall),
        mono: MonoNs(77),
        nonces: NonceBlock::new(vec![100, 101]),
    }
}

/// The REST base, its credential marked.
fn base() -> WireUrl {
    let head = "https://toy.invalid/v1/";
    let span = head.len() as u32..(head.len() + SECRET.len()) as u32;
    WireUrl::redacted(format!("{head}{SECRET}"), vec![span]).unwrap()
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory.
fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let name = format!("fbc-conformance-toy-rest-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..2).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[n]
}

fn wire_cid(cid: ClientOrderId) -> String {
    let caps = toy::caps().exec.unwrap().order;
    encode_cid(&caps.client_id, cid).unwrap().to_string()
}

fn vid(wire: &str) -> VenueOrderId {
    toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

#[derive(Default)]
struct Collect(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// One codec, and each call's result, what it pushed and what it asked for.
struct Rig {
    codec: ToyExec,
}

type Answered = (
    Result<(), DecodeError>,
    Vec<(VenueMeta, ExecEvent)>,
    Vec<Effect>,
);

impl Rig {
    fn rest() -> Rig {
        Rig {
            codec: ToyExec::new(Box::new(ToySigner)).rest_resync(base()),
        }
    }

    fn resync(&mut self, wall: i64) -> Vec<Effect> {
        let mut fx = Effects::new();
        self.codec.resync(&ctx_at(wall), &mut fx);
        fx.take()
    }

    fn open(&mut self, stream: StreamId) -> Vec<Effect> {
        let mut fx = Effects::new();
        self.codec.on_open(stream, &ctx_at(WALL), &mut fx);
        fx.take()
    }

    fn timer(&mut self, tag: TimerTag, wall: i64) -> Vec<Effect> {
        let mut fx = Effects::new();
        self.codec.on_timer(tag, &ctx_at(wall), &mut fx);
        fx.take()
    }

    fn http(&mut self, tag: HttpTag, resp: Result<HttpResponse<'_>, HttpFailure>) -> Answered {
        let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
        let result = toy::with_scope(|scope| {
            self.codec
                .on_http(tag, resp, scope, &specs, &mut sink, &mut fx)
        });
        (result, sink.0, fx.take())
    }

    /// `body` answers `tag` with 200.
    fn answer(&mut self, tag: HttpTag, body: &str) -> Answered {
        let resp = HttpResponse {
            status: 200,
            headers: &[],
            body: body.as_bytes(),
        };
        self.http(tag, Ok(resp))
    }
}

/// The resync over REST asked for at `wm` under `tag`.
fn asked(tag: u64, wm: i64) -> Effect {
    Effect::Http {
        tag: HttpTag(tag),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::redacted(
                format!("{}/resync?ts={wm}", base().as_str()),
                base().redactions().to_vec(),
            )
            .unwrap(),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: RESYNC_TIMEOUT,
        class: TrafficClass::Safety,
        charge: RateCharge::one(OpKind::Rest, None),
    }
}

fn retry() -> Vec<Effect> {
    vec![Effect::Timer {
        tag: RESYNC_RETRY_TAG,
        after: RESYNC_RETRY,
    }]
}

fn snapshot_fields(n: usize, wire_vid: &str) -> String {
    format!(
        "cid={}|vid={wire_vid}|sym=TOYA-PERP|side=B|st=open|px=130865|qty=25|cum=4|po=1|ro=0",
        wire_cid(cid(n))
    )
}

fn snapshot(n: usize, wire_vid: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid(n))),
        vid: vid(wire_vid),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(130_865)),
        qty: lots(25),
        cum_filled: lots(4),
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

/// The resync as of `wm`, one record per line.
fn body(wm: i64) -> String {
    [
        format!("rsbegin|wm={wm}"),
        format!("rsorder|{}", snapshot_fields(0, "V-1")),
        format!("rsorder|{}", snapshot_fields(1, "V-2")),
        "rspos|sym=TOYA-PERP|qty=-12|avg=65432.5".into(),
        "rspos|sym=TOYB-PERP|qty=0".into(),
        "rsend".into(),
    ]
    .join("\n")
}

fn resynced(wm: i64) -> Vec<(VenueMeta, ExecEvent)> {
    [
        ExecEvent::ResyncBegin {
            watermark: WallNs(wm),
        },
        ExecEvent::ResyncOrder(snapshot(0, "V-1")),
        ExecEvent::ResyncOrder(snapshot(1, "V-2")),
        ExecEvent::ResyncPosition {
            inst: INST_A,
            qty: SignedLots(-12),
            avg_entry: Some(PxExact::new(654_325, -1)),
        },
        ExecEvent::ResyncPosition {
            inst: INST_B,
            qty: SignedLots(0),
            avg_entry: None,
        },
        ExecEvent::ResyncEnd,
    ]
    .into_iter()
    .map(|ev| (VenueMeta::NONE, ev))
    .collect()
}

// ---------------------------------------------------------------------------------------------
// Resync over REST.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_resync_over_rest_keeps_the_bases_spans_and_is_pushed_whole_in_one_call() {
    let mut rig = Rig::rest();
    let sent = rig.resync(WM);
    assert_eq!(sent, [asked(1, WM)]);
    let Effect::Http { req, .. } = &sent[0] else {
        unreachable!();
    };
    assert!(!format!("{req:?}").contains(SECRET), "{req:?}");
    // A trailing newline is the end of the last line, not a record.
    let (result, pushed, fx) = rig.answer(HttpTag(1), &format!("{}\n", body(WM)));
    assert_eq!(result, Ok(()));
    assert_eq!(pushed, resynced(WM));
    assert!(fx.is_empty());
    // Answered, the resync waits for nothing: its response again, or its retry, does nothing.
    let (result, pushed, fx) = rig.answer(HttpTag(1), &body(WM));
    assert_eq!((result, pushed, fx), (Ok(()), Vec::new(), Vec::new()));
    assert!(rig.timer(RESYNC_RETRY_TAG, WM + 9).is_empty());
    // A record answering the frame resync is refused: none is being read in frames.
    let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
    let frame = RawFrame::Text("rsend");
    let read = toy::with_scope(|scope| {
        rig.codec
            .on_frame(EXEC_STREAM, frame, scope, &specs, &mut sink, &mut fx)
    });
    assert_eq!(read, Err(DecodeError::Malformed("no resync begun")));
    assert!(sink.0.is_empty());
}

#[test]
fn a_resync_over_rest_is_retried_on_an_http_failure_or_another_status_at_the_retrys_instant() {
    let mut rig = Rig::rest();
    let mut tag = 1;
    rig.resync(WM);
    let whole = body(WM);
    let refusals = [
        Err(HttpFailure::NotSent),
        Err(HttpFailure::TimedOut),
        Err(HttpFailure::Lost),
        Ok(HttpResponse {
            status: 503,
            headers: &[],
            body: whole.as_bytes(),
        }),
    ];
    for (n, resp) in (1..).zip(refusals) {
        let (result, pushed, fx) = rig.http(HttpTag(tag), resp);
        assert_eq!((result, pushed), (Ok(()), Vec::new()), "{resp:?}");
        assert_eq!(fx, retry(), "{resp:?}");
        // Waiting for its retry, a late response to the failed request is ignored.
        let (result, pushed, fx) = rig.answer(HttpTag(tag), &body(WM));
        assert_eq!((result, pushed, fx), (Ok(()), Vec::new(), Vec::new()));
        // The retry asks again, as of its own instant, under a fresh tag.
        tag += 1;
        let again = WM + n;
        assert_eq!(rig.timer(RESYNC_RETRY_TAG, again), [asked(tag, again)]);
        // A timer that is not the retry asks nothing more.
        assert!(rig.timer(RESYNC_RETRY_TAG, again).is_empty());
        assert!(rig.timer(TimerTag(99), again).is_empty());
    }
    // The last retry, answered, pushes the resync it asked for.
    let wm = WM + 4;
    let (result, pushed, _) = rig.answer(HttpTag(tag), &body(wm));
    assert_eq!((result, pushed), (Ok(()), resynced(wm)));
}

#[test]
fn a_resync_over_rest_pushes_nothing_from_a_body_it_cannot_decode_whole() {
    let lines: Vec<String> = body(WM).lines().map(str::to_owned).collect();
    let without = |skip: usize| {
        let kept = lines.iter().enumerate().filter(|(i, _)| *i != skip);
        kept.map(|(_, l)| l.as_str()).collect::<Vec<_>>().join("\n")
    };
    let cases: Vec<(String, &str)> = vec![
        (String::new(), "a resync begins with rsbegin"),
        (without(0), "a resync begins with rsbegin"),
        (without(lines.len() - 1), "a resync ends with rsend"),
        (format!("{}\nrsend", body(WM)), "a record after rsend"),
        (
            format!("{}\nrspos|sym=TOYA-PERP|qty=1", body(WM)),
            "a record after rsend",
        ),
        (
            body(WM).replace("rsend", "rsbegin|wm=1"),
            "resync begun twice",
        ),
        (body(WM).replace("rsend", "fill|fid=F-1"), "kind"),
        (body(WM + 1), "resync for another request"),
        (body(WM).replace("qty=-12", "qty=x"), "qty"),
        (
            body(WM).replace("rspos|sym=TOYB-PERP|qty=0", "rsorder|vid=V-3"),
            "qty",
        ),
        (
            body(WM).replace("\nrspos|sym=TOYB-PERP|qty=0", "\n"),
            "kind",
        ),
        (body(WM).replace("rsend", "rsend|seq=x"), "seq"),
        (body(WM).replace("rsbegin|wm", "rsbegin|ts=x|wm"), "ts"),
    ];
    for (bad, what) in cases {
        let mut rig = Rig::rest();
        rig.resync(WM);
        let (result, pushed, fx) = rig.answer(HttpTag(1), &bad);
        assert_eq!(result, Err(DecodeError::Malformed(what)), "{bad}");
        assert!(pushed.is_empty(), "{bad} pushed {pushed:?}");
        assert_eq!(fx, retry(), "{bad}");
        // Asked again, the resync can still complete.
        assert_eq!(rig.timer(RESYNC_RETRY_TAG, WM + 1), [asked(2, WM + 1)]);
        let (result, pushed, _) = rig.answer(HttpTag(2), &body(WM + 1));
        assert_eq!((result, pushed), (Ok(()), resynced(WM + 1)), "{bad}");
    }
    // Bytes that are not text are refused too.
    let mut rig = Rig::rest();
    rig.resync(WM);
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: &[0xff, 0xfe],
    };
    let (result, pushed, fx) = rig.http(HttpTag(1), Ok(resp));
    assert_eq!(result, Err(DecodeError::Malformed("body")));
    assert!(pushed.is_empty());
    assert_eq!(fx, retry());
}

#[test]
fn a_resync_over_rest_superseded_or_cut_short_by_a_reconnect_is_ignored() {
    let mut rig = Rig::rest();
    // A newer resync supersedes the first: the first's answer is ignored.
    rig.resync(WM);
    assert_eq!(rig.resync(WM + 1), [asked(2, WM + 1)]);
    let (result, pushed, fx) = rig.answer(HttpTag(1), &body(WM));
    assert_eq!((result, pushed, fx), (Ok(()), Vec::new(), Vec::new()));
    // A reconnect cuts the second short: its answer is ignored, and so is a failure of it.
    rig.open(EXEC_STREAM);
    let (result, pushed, fx) = rig.answer(HttpTag(2), &body(WM + 1));
    assert_eq!((result, pushed, fx), (Ok(()), Vec::new(), Vec::new()));
    let (_, _, fx) = rig.http(HttpTag(2), Err(HttpFailure::Lost));
    assert!(fx.is_empty());
    // A failure retried, then a reconnect: the retry's timer asks nothing.
    rig.resync(WM + 2);
    let (_, _, fx) = rig.http(HttpTag(3), Err(HttpFailure::TimedOut));
    assert_eq!(fx, retry());
    rig.open(EXEC_STREAM);
    assert!(rig.timer(RESYNC_RETRY_TAG, WM + 3).is_empty());
    // A codec resyncing in frames asks for no HTTP, and refuses a response.
    let mut frames = ToyExec::new(Box::new(ToySigner));
    let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
    frames.resync(&ctx_at(WM), &mut fx);
    assert!(matches!(fx.take().as_slice(), [Effect::Send { .. }]));
    let answered = toy::with_scope(|scope| {
        frames.on_http(
            HttpTag(1),
            Err(HttpFailure::Lost),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    });
    assert_eq!(
        answered,
        Err(DecodeError::Malformed("the toy asks for no HTTP"))
    );
    assert!(fx.is_empty() && sink.0.is_empty());
    frames.on_timer(RESYNC_RETRY_TAG, &ctx_at(WM), &mut fx);
    assert!(fx.is_empty());
}

// ---------------------------------------------------------------------------------------------
// The ping.
// ---------------------------------------------------------------------------------------------

fn ping_timer() -> Effect {
    Effect::Timer {
        tag: PING_TAG,
        after: PING_EVERY,
    }
}

fn ping(stream: StreamId, wall: i64) -> Effect {
    Effect::Send {
        stream,
        frame: WireSlice::plain(format!("ping|ts={wall}").into_bytes()),
        rpc: None,
        class: TrafficClass::Safety,
        charge: RateCharge::one(OpKind::Control, None),
    }
}

#[test]
fn a_pinging_codec_arms_its_ping_on_open_and_again_on_each_firing() {
    let mut rig = Rig {
        codec: ToyExec::new(Box::new(ToySigner)).pinging(),
    };
    // Before any open there is no stream to ping on.
    assert!(rig.timer(PING_TAG, WALL).is_empty());
    let opened = rig.open(EXEC_STREAM);
    let [Effect::Send { frame, .. }, timer] = opened.as_slice() else {
        panic!("{opened:?}");
    };
    assert!(frame.bytes().ends_with(TOY_TOKEN.as_bytes()));
    assert_eq!(timer, &ping_timer());
    for n in 1..=3 {
        let wall = WALL + n * PING_EVERY.as_nanos() as i64;
        assert_eq!(
            rig.timer(PING_TAG, wall),
            [ping(EXEC_STREAM, wall), ping_timer()]
        );
    }
    // A reconnect on another stream arms it again, and the ping goes out there.
    let other = StreamId(4);
    assert_eq!(rig.open(other).last(), Some(&ping_timer()));
    assert_eq!(rig.timer(PING_TAG, WALL), [ping(other, WALL), ping_timer()]);
    // Another tag is none of the ping's.
    assert!(rig.timer(TimerTag(99), WALL).is_empty());
    // The codec as built without it arms no ping and sends none.
    let mut quiet = Rig {
        codec: ToyExec::new(Box::new(ToySigner)),
    };
    assert_eq!(quiet.open(EXEC_STREAM).len(), 1);
    assert!(quiet.timer(PING_TAG, WALL).is_empty());
}

// ---------------------------------------------------------------------------------------------
// Fills.
// ---------------------------------------------------------------------------------------------

/// A whole fill frame with every field the toy can send, for the toy declared with `fill_ids`.
fn whole_fill(fill_ids: FillIds) -> String {
    let ident = match fill_ids {
        FillIds::Venue => "fid=F-1|vid=V-1|cum=7",
        FillIds::Derived => "vid=V-1|cum=7",
    };
    format!(
        "fill|{ident}|cid={}|sym=TOYB-PERP|side=S|px=130870|qty=3|liq=M|fee=-150000|fa=USDC\
         |pnl=-2500000000|fund=125000000|ts=7|seq=30",
        wire_cid(cid(1))
    )
}

/// `text` without its field `key`.
fn without_field(text: &str, key: &str) -> String {
    let prefix = format!("{key}=");
    let kept = text.split('|').filter(|f| !f.starts_with(&prefix));
    kept.collect::<Vec<_>>().join("|")
}

fn decode(fill_ids: FillIds, text: &str) -> (Result<(), DecodeError>, usize) {
    let mut codec = ToyExec::with_fill_ids(Box::new(ToySigner), fill_ids);
    let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
    let result = toy::with_scope_for(fill_ids, |scope| {
        codec.on_frame(
            EXEC_STREAM,
            RawFrame::Text(text),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    });
    (result, sink.0.len())
}

#[test]
fn a_fill_frame_missing_any_field_its_fill_caps_declare_is_refused() {
    for fill_ids in [FillIds::Venue, FillIds::Derived] {
        let fills = toy::caps_for(fill_ids).exec.unwrap().fills;
        // Every field of the declaration is named here, so one added to FillCaps fails to
        // compile until its wire field is listed.
        let FillCaps {
            source: _,
            liquidity_flag,
            realized_pnl,
            realized_funding,
            fee_sign: _,
            fee_asset_reported,
            fill_id,
            replays_fills_on_reconnect: _,
        } = fills;
        let declared = [
            (liquidity_flag, "liq"),
            (realized_pnl, "pnl"),
            (realized_funding, "fund"),
            (fee_asset_reported, "fa"),
            (fill_id, "fid"),
        ];
        let whole = whole_fill(fill_ids);
        assert_eq!(decode(fill_ids, &whole), (Ok(()), 1), "{whole}");
        let required: Vec<&str> = declared.iter().filter(|(on, _)| *on).map(|d| d.1).collect();
        let want: &[&str] = match fill_ids {
            FillIds::Venue => &["liq", "pnl", "fund", "fa", "fid"],
            FillIds::Derived => &["liq", "pnl", "fund", "fa"],
        };
        assert_eq!(required, want);
        for key in required {
            let text = without_field(&whole, key);
            assert_ne!(text, whole);
            assert_eq!(
                decode(fill_ids, &text),
                (Err(DecodeError::Malformed(key)), 0),
                "{text}"
            );
        }
    }
}
