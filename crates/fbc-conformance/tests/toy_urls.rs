//! FBC-ja3's done line, the configuration's part (Codex r4172917294): the conformance toy's
//! factory carries a configured base URL's credential spans, marked by the key named after it
//! with `.redact` appended, into the `WireUrl` it plans (the order-entry and market-data
//! connections) and into every URL it builds on it (an anchor, a resync over REST); and it
//! refuses a URL with user information, a query or a fragment, which it cannot mark, and spans
//! it cannot keep, rather than plan an address with an unmarked credential.

use std::collections::BTreeSet;
use std::ops::Range;

use fbc_conformance::toy::{
    self, ANCHOR_URL_KEY, ANCHOR_URL_REDACT_KEY, ANCHORED_BOOK, BOOK, EXEC_STREAM, EXEC_URL_KEY,
    EXEC_URL_REDACT_KEY, INST_A, INST_B, MD_STREAM, MD_URL_KEY, MD_URL_REDACT_KEY,
    MIN_CREDENTIAL_LEN, REST_URL_KEY, REST_URL_REDACT_KEY, ToyFactory, ToyMd,
};
use fbc_core::{
    ConfigError, ConfigScope, Effect, Effects, EncodeCtx, EndpointPlan, ExecEndpoint, Feed,
    FieldUnit, HeaderMark, HttpResponse, HttpTag, Inbound, InboundSpans, MdTransport, MonoNs,
    NonceBlock, RawFrame, Secrets, StreamId, Subscription, VenueConfig, VenueError, VenueFactory,
    WallNs, WireUrl,
};

/// A synthetic credential, of no account and no venue.
const SECRET: &str = "SYNTHETIC-URL-PATH-KEY";

/// `head` then the credential then `tail`, and the credential's span, as `.redact` spells it.
fn marked(head: &str, tail: &str) -> (String, String, Range<u32>) {
    let span = head.len() as u32..(head.len() + SECRET.len()) as u32;
    let text = format!("{head}{SECRET}{tail}");
    (text, format!("{}..{}", span.start, span.end), span)
}

/// Every URL configured, each with the credential in its path, marked.
fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, redact, head) in [
        (EXEC_URL_KEY, EXEC_URL_REDACT_KEY, "wss://toy.invalid/exec/"),
        (MD_URL_KEY, MD_URL_REDACT_KEY, "ws://127.0.0.1:9/md/"),
        (
            ANCHOR_URL_KEY,
            ANCHOR_URL_REDACT_KEY,
            "https://toy.invalid/rest/",
        ),
        (
            REST_URL_KEY,
            REST_URL_REDACT_KEY,
            "http://127.0.0.1:9/rest/",
        ),
    ] {
        let (text, spans, _) = marked(head, "");
        cfg.insert(key, &text);
        cfg.insert(redact, &spans);
    }
    cfg
}

fn url(head: &str, tail: &str) -> WireUrl {
    let (text, _, span) = marked(head, tail);
    WireUrl::redacted(text, vec![span]).unwrap()
}

fn sub(inst: fbc_core::InstrumentId, book: fbc_core::BookId) -> Subscription {
    Subscription {
        inst,
        feed: Feed::Book(book),
    }
}

fn ctx() -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(1_000),
        mono: MonoNs(1),
        nonces: NonceBlock::new(Vec::new()),
    }
}

/// The URL every HTTP request among `fx` asks for.
fn http_urls(fx: Vec<Effect>) -> Vec<WireUrl> {
    let urls = fx.into_iter().filter_map(|e| match e {
        Effect::Http { req, .. } => Some(req.url),
        _ => None,
    });
    urls.collect()
}

/// A URL whose path, from byte 18, is 46 characters long, with no credential of its own.
const LONG: &str = "wss://toy.invalid/0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJ";

/// A path with a percent escape at bytes 38..41, between two 20-byte runs.
const ESCAPED: &str = "wss://toy.invalid/0123456789abcdefghij%2D0123456789abcdefghij";

fn hidden(shown: String) {
    assert!(!shown.contains(SECRET), "{shown}");
}

#[test]
fn configured_urls_keep_the_spans_their_configuration_marks() {
    let cfg = cfg();
    let exec = ToyFactory.plan_exec(&cfg).unwrap();
    let want = ExecEndpoint {
        stream: EXEC_STREAM,
        url: url("wss://toy.invalid/exec/", ""),
    };
    assert_eq!(exec, [want]);
    hidden(format!("{exec:?}"));

    let specs = toy::specs();
    let subs = BTreeSet::from([sub(INST_A, BOOK), sub(INST_A, ANCHORED_BOOK)]);
    let md = ToyFactory.plan_md(&cfg, &specs, &subs).unwrap();
    let want = EndpointPlan {
        stream: MD_STREAM,
        transport: MdTransport::Socket {
            url: url("ws://127.0.0.1:9/md/", ""),
        },
        subs: subs.iter().copied().collect(),
    };
    assert_eq!(md, [want]);
    hidden(format!("{md:?}"));

    // The anchor is asked for under the configured base, the base's span kept.
    let mut codec = ToyFactory.md_codec(&cfg, &md[0]);
    let mut fx = Effects::new();
    codec
        .subscribe(&[sub(INST_B, ANCHORED_BOOK)], &[], &specs, &mut fx)
        .unwrap();
    let anchors = http_urls(fx.take());
    assert_eq!(
        anchors,
        [url("https://toy.invalid/rest/", "/book/TOYB-PERP")]
    );
    hidden(format!("{anchors:?}"));

    // So is a resync over REST, under the REST base.
    let mut exec = ToyFactory
        .exec_codec(&cfg, Secrets::new())
        .unwrap()
        .unwrap();
    let mut fx = Effects::new();
    exec.resync(&ctx(), &mut fx);
    let resyncs = http_urls(fx.take());
    assert_eq!(
        resyncs,
        [url("http://127.0.0.1:9/rest/", "/resync?ts=1000")]
    );
    hidden(format!("{resyncs:?}"));

    // A URL holding no credential needs no spans: absent or empty marks none.
    let mut plain = cfg.clone();
    plain.insert(EXEC_URL_KEY, "wss://toy.invalid/exec");
    plain.insert(EXEC_URL_REDACT_KEY, "");
    let exec = ToyFactory.plan_exec(&plain).unwrap();
    assert_eq!(exec[0].url, WireUrl::plain("wss://toy.invalid/exec"));
    let mut cfg = VenueConfig::new();
    cfg.insert(EXEC_URL_KEY, "ws://127.0.0.1:9");
    assert_eq!(
        ToyFactory.plan_exec(&cfg).unwrap()[0].url,
        WireUrl::plain("ws://127.0.0.1:9")
    );
    // Two spans, in order, each in the path, each the shortest a credential may be.
    let text = "wss://toy.invalid/a/SYNTHETIC-KEY-01/b/SYNTHETIC-KEY-02";
    cfg.insert(EXEC_URL_KEY, text);
    cfg.insert(EXEC_URL_REDACT_KEY, "20..36, 39..55");
    assert_eq!(&text[20..36], "SYNTHETIC-KEY-01");
    assert_eq!(36 - 20, MIN_CREDENTIAL_LEN);
    let want = WireUrl::redacted(text.into(), vec![20..36, 39..55]).unwrap();
    assert_eq!(ToyFactory.plan_exec(&cfg).unwrap()[0].url, want);
}

#[test]
fn a_url_with_user_information_a_query_or_spans_it_cannot_keep_is_refused() {
    let invalid = |key: &str, value: &str, redact: Option<&str>| {
        let mut cfg = cfg();
        cfg.insert(key, value);
        let redact_key = format!("{key}.redact");
        match redact {
            Some(spans) => cfg.insert(&redact_key, spans),
            None => cfg.insert(&redact_key, ""),
        };
        cfg
    };
    let refused = |cfg: &VenueConfig| match ToyFactory.plan_exec(cfg) {
        Err(VenueError::Config(ConfigError::Invalid { key, .. })) => key,
        other => panic!("planned {other:?}"),
    };
    for (value, redact, key) in [
        // User information, marked or not: the toy cannot mark it.
        ("wss://user:pw@toy.invalid/x", None, EXEC_URL_KEY),
        ("wss://user:pw@toy.invalid/x", Some("6..13"), EXEC_URL_KEY),
        ("wss://token@toy.invalid", None, EXEC_URL_KEY),
        // A query or a fragment.
        ("wss://toy.invalid/x?key=k", None, EXEC_URL_KEY),
        ("wss://toy.invalid/x#k", None, EXEC_URL_KEY),
        // Not a socket URL, or no host.
        ("https://toy.invalid/x", None, EXEC_URL_KEY),
        ("toy.invalid/x", None, EXEC_URL_KEY),
        ("wss:///x", None, EXEC_URL_KEY),
        // A host that cannot be used (Codex r4219602934): none before a port, a port that is
        // not one, an IPv6 literal left open or followed by anything but a port.
        ("wss://:443/x", None, EXEC_URL_KEY),
        ("wss://toy.invalid:/x", None, EXEC_URL_KEY),
        ("wss://toy.invalid:0/x", None, EXEC_URL_KEY),
        ("wss://toy.invalid:65536/x", None, EXEC_URL_KEY),
        ("wss://toy.invalid:44x/x", None, EXEC_URL_KEY),
        ("wss://[::1/x", None, EXEC_URL_KEY),
        ("wss://[::1]x/x", None, EXEC_URL_KEY),
        ("wss://[]/x", None, EXEC_URL_KEY),
        ("wss://toy invalid/x", None, EXEC_URL_KEY),
        // Literals that are not addresses (Codex r4219753503).
        ("wss://[::::]/x", None, EXEC_URL_KEY),
        ("wss://[1:2:3]/x", None, EXEC_URL_KEY),
        ("wss://999.1.1.1/x", None, EXEC_URL_KEY),
        ("wss://1.2.3/x", None, EXEC_URL_KEY),
        // A path the runtime's URI parser would refuse (Codex r4219929307): a space, a
        // character outside ASCII, a bare or short percent escape, a character URIs never hold.
        ("wss://toy.invalid/a b", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a%", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a%2", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a%zz", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a\"b", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a<b>", None, EXEC_URL_KEY),
        ("wss://toy.invalid/a\\b", None, EXEC_URL_KEY),
        // Spans the toy cannot keep: not ranges, outside the path, past the end, out of order,
        // overlapping, empty. The path is long enough that each span but the empty one is
        // long enough to name (Codex r4220116602).
        (LONG, Some("x"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..x"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..38,"), EXEC_URL_REDACT_KEY),
        (LONG, Some("6..30"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..99"), EXEC_URL_REDACT_KEY),
        (LONG, Some("40..60,18..38"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..38,30..50"), EXEC_URL_REDACT_KEY),
        (LONG, Some("20..20"), EXEC_URL_REDACT_KEY),
        // A span shorter than a credential may be (Codex r4220116602): a short value would be
        // named wherever a frame or response holds it, prices and sequence numbers included.
        (LONG, Some("18..19"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..33"), EXEC_URL_REDACT_KEY),
        (LONG, Some("18..38,40..55"), EXEC_URL_REDACT_KEY),
        // A character outside ASCII is refused in the URL, before any span is read.
        ("wss://toy.invalid/\u{e9}tat", Some("18..19"), EXEC_URL_KEY),
    ] {
        assert_eq!(
            refused(&invalid(EXEC_URL_KEY, value, redact)),
            key,
            "{value}"
        );
    }
    // The spans those refusals took, each on its own ground, are kept where they are long
    // enough: the shortest a credential may be is kept.
    let mut ok = cfg();
    for spans in ["18..34", "18..38,40..60", ""] {
        ok.insert(EXEC_URL_KEY, LONG);
        ok.insert(EXEC_URL_REDACT_KEY, spans);
        assert!(ToyFactory.plan_exec(&ok).is_ok(), "{spans}");
    }
    let short = invalid(EXEC_URL_KEY, LONG, Some("18..33"));
    assert_eq!(
        ToyFactory.plan_exec(&short),
        Err(VenueError::Config(ConfigError::Invalid {
            key: EXEC_URL_REDACT_KEY,
            reason: "a span shorter than 16 bytes, which the toy would name wherever a frame or \
                     response holds it, unrelated bytes included",
        }))
    );
    // A span holding a percent escape, or splitting one, is refused (Codex r4220333334): a
    // server that decodes the path could echo the credential's decoded spelling, which the toy,
    // looking for the configured one, would not name. Spans beside the escape are kept.
    for spans in [
        "30..50", "38..60", "39..60", "40..60", "18..39", "18..40", "18..41",
    ] {
        let escaped = invalid(EXEC_URL_KEY, ESCAPED, Some(spans));
        assert_eq!(
            ToyFactory.plan_exec(&escaped),
            Err(VenueError::Config(ConfigError::Invalid {
                key: EXEC_URL_REDACT_KEY,
                reason: "a span holding or splitting a %XX escape, whose decoded spelling a \
                         server could echo where the toy looks for the configured one",
            })),
            "{spans}"
        );
    }
    for spans in ["18..38", "41..61", "18..38,41..61"] {
        let beside = invalid(EXEC_URL_KEY, ESCAPED, Some(spans));
        assert!(ToyFactory.plan_exec(&beside).is_ok(), "{spans}");
    }
    // Nothing configured is missing.
    assert_eq!(
        ToyFactory.plan_exec(&VenueConfig::new()),
        Err(VenueError::Config(ConfigError::Missing(EXEC_URL_KEY)))
    );

    // The market-data URL, and the anchors' base when the anchored channel is subscribed.
    let specs = toy::specs();
    let plan = |cfg: &VenueConfig, book| {
        ToyFactory.plan_md(cfg, &specs, &BTreeSet::from([sub(INST_A, book)]))
    };
    let user = invalid(MD_URL_KEY, "ws://u@127.0.0.1/md", None);
    let refused = Err(VenueError::Config(ConfigError::Invalid {
        key: MD_URL_KEY,
        reason: "user information, which the toy cannot mark and the runtime never sends: put \
                 the credential in the path and mark it",
    }));
    assert_eq!(plan(&user, BOOK), refused);
    let query = invalid(ANCHOR_URL_KEY, "https://toy.invalid/rest?key=k", None);
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = plan(&query, ANCHORED_BOOK)
    else {
        panic!("planned");
    };
    assert_eq!(key, ANCHOR_URL_KEY);
    // The plain book needs no anchor base.
    assert!(plan(&query, BOOK).is_ok());
    // A base the toy appends its paths to cannot end in a slash, which would double it
    // (Codex r4219602924); a socket URL can.
    let slash = invalid(ANCHOR_URL_KEY, "https://toy.invalid/rest/", None);
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = plan(&slash, ANCHORED_BOOK)
    else {
        panic!("planned");
    };
    assert_eq!(key, ANCHOR_URL_KEY);
    let slash = invalid(REST_URL_KEY, "https://toy.invalid/", None);
    assert!(
        ToyFactory
            .exec_codec(&slash, Secrets::new())
            .unwrap()
            .is_err()
    );
    let mut ok = VenueConfig::new();
    for exec in [
        "wss://toy.invalid/x/",
        "wss://[::1]:9/x",
        "wss://[2001:db8::7]/x",
        "ws://127.0.0.1:65535",
        "wss://toy-1.example_x.invalid:443",
        "wss://toy.invalid/a-b._~!$&'()*+,;=:@/%2F%e9",
    ] {
        ok.insert(EXEC_URL_KEY, exec);
        assert_eq!(
            ToyFactory.plan_exec(&ok).unwrap()[0].url,
            WireUrl::plain(exec)
        );
    }
    let socket = invalid(ANCHOR_URL_KEY, "wss://toy.invalid/rest", None);
    assert!(plan(&socket, ANCHORED_BOOK).is_err());
    let mut none = cfg();
    none.insert(ANCHOR_URL_KEY, "");
    assert!(plan(&none, ANCHORED_BOOK).is_err());
    let mut missing = VenueConfig::new();
    missing.insert(MD_URL_KEY, "ws://127.0.0.1/md");
    assert_eq!(
        plan(&missing, ANCHORED_BOOK),
        Err(VenueError::Config(ConfigError::Missing(ANCHOR_URL_KEY)))
    );
    // A codec built under a refused anchor base anchors nothing: the channel is refused when
    // subscribed, nothing asked for.
    let ep = EndpointPlan {
        stream: StreamId(0),
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1/md"),
        },
        subs: Vec::new(),
    };
    let mut codec = ToyFactory.md_codec(&query, &ep);
    let mut fx = Effects::new();
    let subscribed = codec.subscribe(&[sub(INST_A, ANCHORED_BOOK)], &[], &specs, &mut fx);
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = subscribed else {
        panic!("{subscribed:?}");
    };
    assert_eq!(key, ANCHOR_URL_KEY);
    assert!(fx.is_empty());

    // A REST base configured and refused refuses the order-entry codec; none resyncs in frames.
    let rest = invalid(REST_URL_KEY, "https://u:p@toy.invalid/rest", None);
    let built = ToyFactory.exec_codec(&rest, Secrets::new()).unwrap();
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = built else {
        panic!("built");
    };
    assert_eq!(key, REST_URL_KEY);
    let mut frames = ToyFactory
        .exec_codec(&VenueConfig::new(), Secrets::new())
        .unwrap()
        .unwrap();
    let mut fx = Effects::new();
    frames.resync(&ctx(), &mut fx);
    assert!(matches!(fx.take().as_slice(), [Effect::Send { .. }]));
}

#[test]
fn the_factorys_schema_names_every_url_and_its_spans() {
    let schema = ToyFactory.config_schema();
    let keys: Vec<&str> = schema.iter().map(|f| f.key).collect();
    assert_eq!(
        keys,
        [
            EXEC_URL_KEY,
            EXEC_URL_REDACT_KEY,
            MD_URL_KEY,
            MD_URL_REDACT_KEY,
            ANCHOR_URL_KEY,
            ANCHOR_URL_REDACT_KEY,
            REST_URL_KEY,
            REST_URL_REDACT_KEY,
        ]
    );
    for field in schema {
        assert_eq!(field.scope, ConfigScope::Account);
        assert_eq!(field.unit, FieldUnit::Dimensionless);
        assert!(!field.doc.is_empty());
        if field.key.ends_with(".redact") {
            assert!(field.doc.contains("start..end"), "{}", field.doc);
        }
    }
}

#[test]
fn a_configured_credential_echoed_in_an_http_response_is_named_for_redaction() {
    // Codex r4219753519: a venue or a proxy echoing the path credential in a response's body or
    // headers; the codec that asked under that base names every occurrence, so the journal
    // keeps it only as a keyed hash.
    let cfg = cfg();
    let body = format!("err|path=/rest/{SECRET}|again={SECRET}{SECRET}|end");
    let at = |n: usize| body.match_indices(SECRET).nth(n).unwrap().0 as u32;
    let len = SECRET.len() as u32;
    let name = format!("x-{SECRET}");
    let headers = [
        ("content-type", "text/plain"),
        ("x-echo", SECRET),
        (name.as_str(), "1"),
        ("x-other", "SYNTHETIC-NOT-IT"),
    ];
    let resp = HttpResponse {
        status: 404,
        headers: &headers,
        body: body.as_bytes(),
    };
    let input = Inbound::Http(HttpTag(1), resp);
    // Two occurrences side by side are one span.
    let want = InboundSpans::response(
        vec![(1, HeaderMark::Value), (2, HeaderMark::NameAndValue)],
        vec![at(0)..at(0) + len, at(1)..at(1) + 2 * len],
    );
    let exec = ToyFactory
        .exec_codec(&cfg, Secrets::new())
        .unwrap()
        .unwrap();
    let ep = EndpointPlan {
        stream: MD_STREAM,
        transport: MdTransport::Socket {
            url: url("ws://127.0.0.1:9/md/", ""),
        },
        subs: Vec::new(),
    };
    let md = ToyFactory.md_codec(&cfg, &ep);
    for named in [exec.redact_inbound(input), md.redact_inbound(input)] {
        assert_eq!(named, want);
        assert_eq!(named.check(input), Ok(()));
    }
    // A response holding none names nothing, and a codec with no configured base names none.
    let clean = HttpResponse {
        status: 200,
        headers: &[],
        body: b"rsend",
    };
    let clean = Inbound::Http(HttpTag(1), clean);
    assert_eq!(exec.redact_inbound(clean), InboundSpans::NONE);
    let bare = VenueConfig::new();
    let exec = ToyFactory
        .exec_codec(&bare, Secrets::new())
        .unwrap()
        .unwrap();
    assert_eq!(exec.redact_inbound(input), InboundSpans::NONE);
    let plain = EndpointPlan {
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1:9/md"),
        },
        ..ep.clone()
    };
    let md = ToyFactory.md_codec(&bare, &plain);
    assert_eq!(md.redact_inbound(input), InboundSpans::NONE);
    // The endpoint's own URL holds the credential: an echo of it is named too.
    assert_eq!(ToyFactory.md_codec(&bare, &ep).redact_inbound(input), want);
}

#[test]
fn a_configured_socket_credential_echoed_in_a_frame_is_named_for_redaction() {
    // Codex r4219929319: the WebSocket peer echoing the order-entry or market-data URL's path
    // credential in a welcome or error frame; each codec names every occurrence, beside the
    // toy token the order-entry codec names already.
    let cfg = cfg();
    let text = format!("err|path={SECRET}|token=SYNTHETIC-TOKEN|again={SECRET}");
    let at = |n: usize| text.match_indices(SECRET).nth(n).unwrap().0 as u32;
    let len = SECRET.len() as u32;
    let token = text.find("SYNTHETIC-TOKEN").unwrap() as u32;
    let frame = Inbound::Frame(RawFrame::Text(&text));
    let exec = ToyFactory
        .exec_codec(&cfg, Secrets::new())
        .unwrap()
        .unwrap();
    let want = InboundSpans::frame(vec![
        at(0)..at(0) + len,
        token..token + "SYNTHETIC-TOKEN".len() as u32,
        at(1)..at(1) + len,
    ]);
    assert_eq!(exec.redact_inbound(frame), want);
    let plans = ToyFactory
        .plan_md(&cfg, &toy::specs(), &BTreeSet::from([sub(INST_A, BOOK)]))
        .unwrap();
    let md = ToyFactory.md_codec(&cfg, &plans[0]);
    let want = InboundSpans::frame(vec![at(0)..at(0) + len, at(1)..at(1) + len]);
    assert_eq!(md.redact_inbound(frame), want);
    assert_eq!(want.check(frame), Ok(()));
    // A binary frame is searched too.
    let binary = Inbound::Frame(RawFrame::Binary(text.as_bytes()));
    assert_eq!(md.redact_inbound(binary), want);
    // A codec whose URLs hold no credential names none in a frame without the token.
    let bare = VenueConfig::new();
    let exec = ToyFactory
        .exec_codec(&bare, Secrets::new())
        .unwrap()
        .unwrap();
    let plain = Inbound::Frame(RawFrame::Text("err|path=SYNTHETIC-URL-PATH-KEY"));
    assert_eq!(exec.redact_inbound(plain), InboundSpans::NONE);
    // An order-entry URL configured and refused refuses the codec, as plan_exec does.
    let mut bad = VenueConfig::new();
    bad.insert(EXEC_URL_KEY, "wss://u@toy.invalid/x");
    let built = ToyFactory.exec_codec(&bad, Secrets::new()).unwrap();
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) = built else {
        panic!("built");
    };
    assert_eq!(key, EXEC_URL_KEY);
}

#[test]
fn a_market_data_codec_holding_a_configured_credential_shows_none_in_its_debug() {
    // Codex r4220116591: the codec keeps the plaintext of its URLs' credentials to name them
    // when echoed; its Debug, which a panic or a log may print, shows none of them.
    let cfg = cfg();
    let plans = ToyFactory
        .plan_md(&cfg, &toy::specs(), &BTreeSet::from([sub(INST_A, BOOK)]))
        .unwrap();
    let MdTransport::Socket { url: socket } = &plans[0].transport else {
        panic!("{plans:?}");
    };
    let md =
        ToyMd::with_anchor_url(MD_STREAM, url("https://toy.invalid/rest/", "")).socket_url(socket);
    let shown = format!("{md:?}");
    hidden(shown.clone());
    hidden(format!("{md:#?}"));
    // The rest of it is still shown, the credentials only counted.
    assert!(shown.contains("ToyMd"), "{shown}");
    assert!(shown.contains("secrets: 2"), "{shown}");
    // A codec given only the socket URL hides its credential too.
    let socket_only = ToyMd::new(MD_STREAM).socket_url(socket);
    hidden(format!("{socket_only:?}"));
}
