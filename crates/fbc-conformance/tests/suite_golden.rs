//! FBC-onw's done line, its second half: `signing_golden` fails against a toy variant whose
//! signer changes one signed byte, naming every golden it breaks, and `legacy_symbols` fails
//! on a ticker the toy's hook cannot parse, naming it. Around them, every other way the two
//! checks fail or are skipped: no order entry, no golden command, a fixture directory missing
//! its goldens, its SYNTHETIC marker or a golden file, a stray golden, a bad or repeated
//! name, a refused command, a command written as two frames or as an HTTP request; and for
//! tickers, a venue without a rule, a missing, empty or unreadable ticker file.

mod toy_setup;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use fbc_conformance::suite::{self, Failure, Golden, Setup, Subject, Verdict};
use fbc_conformance::toy::{ToyExec, ToyFactory, ToySigner};
use fbc_core::{
    AccountSummary, AmendWire, AssetKey, CancelWire, ConfigError, CtxCall, DecodeError, Effect,
    Effects, EncodeCtx, EncodeReceipt, EndpointPlan, ExecCodec, ExecEndpoint, ExecSink, FieldSpec,
    HttpFailure, HttpPlan, HttpTag, Inbound, InboundSpans, InstrumentSpecDraft, MdCodec,
    NotSentReason, OrderKind, OrderSigner, PathStamps, PlaceWire, RawFrame, RpcId, Secrets, Sig,
    SignError, SpecTable, StreamId, Subscription, SymbolError, TimerTag, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory,
};
use toy_setup::{FIXTURES, assumed};

/// Every golden the toy's fixtures hold.
const GOLDENS: [&str; 5] = ["place", "place-batch", "amend", "cancel", "cancel-batch"];

// ---------------------------------------------------------------------------------------------
// The variants.
// ---------------------------------------------------------------------------------------------

/// How a variant's codec departs from the toy's.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Codec {
    /// The toy's codec, its signer changing the last byte of every signature.
    OneByte,
    /// The toy's codec, writing each frame twice.
    Twice,
    /// The toy's codec, writing each frame as the body of an HTTP request.
    OverHttp,
    /// No codec at all.
    Nothing,
}

/// The toy with its caps edited by `caps`, its order-entry codec departing as `codec` says,
/// and its legacy-symbol hook `symbols`.
struct Variant {
    caps: fn(&mut VenueCaps),
    codec: Option<Codec>,
    symbols: fn(&str) -> Result<AssetKey, SymbolError>,
}

impl Variant {
    /// The toy as it is.
    const TOY: Variant = Variant {
        caps: |_| {},
        codec: None,
        symbols: |s| ToyFactory.parse_fbc_common_symbol(s),
    };

    /// The toy, its codec departing as `codec` says.
    const fn twisted(codec: Codec) -> Variant {
        Variant {
            codec: Some(codec),
            ..Variant::TOY
        }
    }

    /// The toy, run against `fixtures` under `setup`.
    fn subject(&self, fixtures: &Path, setup: fn() -> Setup) -> Subject<'_> {
        Subject::new(self, fixtures, setup).unwrap()
    }
}

impl VenueFactory for Variant {
    fn id(&self) -> &'static str {
        "TOY-VARIANT"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        ToyFactory.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = ToyFactory.caps(cfg)?;
        (self.caps)(&mut caps);
        Ok(caps)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        (self.symbols)(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        ToyFactory.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        ToyFactory.plan_md(cfg, specs, subs)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        ToyFactory.md_codec(cfg, ep)
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        ToyFactory.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let Some(codec) = self.codec else {
            return ToyFactory.exec_codec(cfg, creds);
        };
        let codec: Box<dyn ExecCodec> = match codec {
            Codec::Nothing => return None,
            Codec::OneByte => Box::new(ToyExec::new(Box::new(OneByte))),
            Codec::Twice | Codec::OverHttp => Box::new(Rewritten {
                inner: ToyExec::new(Box::new(ToySigner)),
                how: codec,
            }),
        };
        Some(Ok(codec))
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ToyFactory.test_connection(cfg, creds)
    }
}

/// The toy's signer, the last byte of each signature changed: one signed byte of each frame.
struct OneByte;

impl OneByte {
    fn changed(sig: Sig) -> Sig {
        let mut bytes = sig.as_bytes().to_vec();
        let last = bytes.last_mut().expect("the toy signs 16 bytes");
        *last = if *last == b'0' { b'1' } else { b'0' };
        Sig::new(&bytes).unwrap()
    }
}

impl OrderSigner for OneByte {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        ToySigner.sign_place(w).map(OneByte::changed)
    }

    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        ToySigner.sign_amend(w).map(OneByte::changed)
    }

    fn sign_cancel(&mut self, w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        ToySigner
            .sign_cancel(w)
            .map(|sig| sig.map(OneByte::changed))
    }
}

/// The toy's codec, its frames rewritten as `how` says.
struct Rewritten {
    inner: ToyExec,
    how: Codec,
}

impl ExecCodec for Rewritten {
    fn nonces_for(&self, call: CtxCall) -> u16 {
        self.inner.nonces_for(call)
    }

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_open(stream, ctx, fx);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let receipt = self.inner.encode(cmd, rpc, specs, ctx, t, fx)?;
        for effect in fx.take() {
            let Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge,
            } = effect
            else {
                unreachable!("the toy writes one frame");
            };
            if self.how == Codec::OverHttp {
                fx.push(Effect::Http {
                    tag: HttpTag(1),
                    req: fbc_core::HttpRequest {
                        method: fbc_core::HttpMethod::Post,
                        url: fbc_core::WireUrl::plain("https://toy.invalid/orders"),
                        headers: Vec::new(),
                        body: frame,
                    },
                    rpc: rpc.map(|call| call.id),
                    timeout: core::time::Duration::from_secs(5),
                    class,
                    charge,
                });
                continue;
            }
            let send = Effect::Send {
                stream,
                frame,
                rpc,
                class,
                charge,
            };
            fx.push(send.clone());
            fx.push(send);
        }
        Ok(receipt)
    }

    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &fbc_core::DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.inner.on_frame(stream, f, scope, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<fbc_core::HttpResponse<'_>, HttpFailure>,
        scope: &fbc_core::DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.inner.on_http(tag, resp, scope, specs, sink, fx)
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.on_timer(tag, ctx, fx);
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        self.inner.on_rpc_timeout(rpc, sink);
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        self.inner.resync(ctx, fx);
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture directories made for a test.
// ---------------------------------------------------------------------------------------------

/// A fixture directory of the test's own, removed when dropped.
struct Scratch(PathBuf);

impl Scratch {
    /// An empty fixture directory named after `test`.
    fn empty(test: &str) -> Scratch {
        let dir = std::env::temp_dir().join(format!(
            "fbc-conformance-golden-{}-{test}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Scratch(dir)
    }

    /// A copy of the toy's fixture directory named after `test`.
    fn toy(test: &str) -> Scratch {
        let scratch = Scratch::empty(test);
        for sub in ["signing_golden", "legacy_symbols"] {
            let to = scratch.0.join(sub);
            fs::create_dir_all(&to).unwrap();
            for entry in fs::read_dir(Path::new(FIXTURES).join(sub)).unwrap() {
                let from = entry.unwrap().path();
                fs::copy(&from, to.join(from.file_name().unwrap())).unwrap();
            }
        }
        scratch
    }

    /// The path of `file` inside it.
    fn at(&self, file: &str) -> PathBuf {
        self.0.join(file)
    }

    /// Writes `text` to `file` inside it.
    fn write(&self, file: &str, text: &str) {
        fs::write(self.at(file), text).unwrap();
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The failure `outcome` must be.
fn failed(outcome: Result<Verdict, Failure>) -> Failure {
    match outcome {
        Err(failure) => failure,
        Ok(verdict) => panic!("expected a failure, got {verdict:?}"),
    }
}

/// What the breach naming `capability` says.
fn said<'a>(failure: &'a Failure, capability: &str) -> &'a str {
    let breach = failure.breaches.iter().find(|b| b.capability == capability);
    let breach = breach.unwrap_or_else(|| panic!("{capability} not named in {failure}"));
    &breach.what
}

/// The capabilities `failure` names, in order.
fn named(failure: &Failure) -> Vec<&str> {
    failure
        .breaches
        .iter()
        .map(|b| b.capability.as_str())
        .collect()
}

fn signing_golden(
    variant: &Variant,
    fixtures: &Path,
    setup: fn() -> Setup,
) -> Result<Verdict, Failure> {
    suite::signing_golden(&variant.subject(fixtures, setup))
}

fn legacy_symbols(variant: &Variant, fixtures: &Path) -> Result<Verdict, Failure> {
    suite::legacy_symbols(&variant.subject(fixtures, assumed))
}

// ---------------------------------------------------------------------------------------------
// signing_golden.
// ---------------------------------------------------------------------------------------------

#[test]
fn signing_golden_fails_a_toy_variant_that_changes_one_signed_byte_naming_every_golden() {
    let variant = Variant::twisted(Codec::OneByte);
    let failure = failed(signing_golden(&variant, Path::new(FIXTURES), assumed));
    assert_eq!(failure.check, "signing_golden");
    let goldens: Vec<String> = GOLDENS.iter().map(|g| format!("golden {g}")).collect();
    assert_eq!(named(&failure), goldens);
    // The place frame's last byte is its signature's last: the frames differ there alone,
    // and the breach says where, never what the bytes are.
    let place = fs::read(Path::new(FIXTURES).join("signing_golden/place.golden")).unwrap();
    let last = place.len() - 1;
    let want = format!(
        "encoded {} bytes that differ from the golden's {} at byte {last}",
        place.len(),
        place.len()
    );
    assert_eq!(said(&failure, "golden place"), want);
    assert!(!failure.to_string().contains("sig="), "{failure}");
}

#[test]
fn signing_golden_is_skipped_for_a_venue_without_order_entry() {
    let variant = Variant {
        caps: |caps| caps.exec = None,
        ..Variant::TOY
    };
    let verdict = signing_golden(&variant, Path::new(FIXTURES), assumed).unwrap();
    assert!(
        matches!(verdict, Verdict::Skipped { check: "signing_golden", why } if why.contains("no order entry")),
        "{verdict:?}"
    );
}

#[test]
fn signing_golden_fails_a_venue_with_order_entry_and_no_golden_command() {
    fn none() -> Setup {
        Setup {
            goldens: Vec::new(),
            ..assumed()
        }
    }
    let failure = failed(signing_golden(&Variant::TOY, Path::new(FIXTURES), none));
    assert!(said(&failure, "Setup.goldens").contains("lists no command"));
}

#[test]
fn signing_golden_fails_a_fixture_directory_without_goldens() {
    let scratch = Scratch::empty("no-goldens");
    let failure = failed(signing_golden(&Variant::TOY, &scratch.0, assumed));
    assert!(said(&failure, "signing_golden").starts_with("cannot read"));
}

#[test]
fn signing_golden_fails_goldens_outside_a_synthetic_directory() {
    let scratch = Scratch::toy("unmarked");
    fs::remove_file(scratch.at("signing_golden/SYNTHETIC")).unwrap();
    let failure = failed(signing_golden(&Variant::TOY, &scratch.0, assumed));
    assert_eq!(named(&failure), ["signing_golden/SYNTHETIC"]);
    assert!(said(&failure, "signing_golden/SYNTHETIC").contains("decision 0009"));
}

#[test]
fn signing_golden_fails_a_missing_golden_and_a_golden_no_command_names() {
    let scratch = Scratch::toy("stray");
    fs::remove_file(scratch.at("signing_golden/amend.golden")).unwrap();
    scratch.write("signing_golden/orphan.golden", "place|rpc=99");
    scratch.write("signing_golden/notes.txt", "not a golden: ignored");
    let failure = failed(signing_golden(&Variant::TOY, &scratch.0, assumed));
    assert_eq!(
        named(&failure),
        ["golden amend", "signing_golden/orphan.golden"]
    );
    assert!(said(&failure, "golden amend").starts_with("cannot read signing_golden/amend.golden"));
    assert!(said(&failure, "signing_golden/orphan.golden").contains("never checked"));
}

#[test]
fn signing_golden_fails_a_golden_named_badly_or_twice() {
    fn named_badly() -> Setup {
        let mut setup = assumed();
        let place = setup.goldens[0].clone();
        setup.goldens.push(Golden {
            name: "../place",
            ..place.clone()
        });
        setup.goldens.push(place);
        setup
    }
    let failure = failed(signing_golden(
        &Variant::TOY,
        Path::new(FIXTURES),
        named_badly,
    ));
    assert_eq!(named(&failure), ["golden ../place", "golden place"]);
    assert!(said(&failure, "golden ../place").contains("not a file name"));
    assert_eq!(
        said(&failure, "golden place"),
        "named twice in Setup.goldens"
    );
}

#[test]
fn signing_golden_fails_a_golden_command_the_venue_refuses() {
    fn market() -> Setup {
        let mut setup = assumed();
        if let VenueCommand::Place(order) = &mut setup.goldens[0].cmd {
            order.kind = OrderKind::Market;
        }
        setup
    }
    let failure = failed(signing_golden(&Variant::TOY, Path::new(FIXTURES), market));
    assert_eq!(named(&failure), ["golden place"]);
    assert_eq!(
        said(&failure, "golden place"),
        "refused as NotSent(Unsupported) by a freshly built codec"
    );
}

#[test]
fn signing_golden_fails_a_command_written_as_two_frames_or_over_http() {
    for codec in [Codec::Twice, Codec::OverHttp] {
        let variant = Variant::twisted(codec);
        let failure = failed(signing_golden(&variant, Path::new(FIXTURES), assumed));
        assert_eq!(failure.breaches.len(), GOLDENS.len(), "{codec:?}");
        assert!(
            said(&failure, "golden cancel").contains("not modelled by signing_golden"),
            "{codec:?}"
        );
    }
}

#[test]
fn signing_golden_fails_a_venue_that_declares_order_entry_and_builds_no_codec() {
    let variant = Variant::twisted(Codec::Nothing);
    let failure = failed(signing_golden(&variant, Path::new(FIXTURES), assumed));
    assert!(said(&failure, "VenueCaps.exec").contains("builds no codec"));
}

// ---------------------------------------------------------------------------------------------
// legacy_symbols.
// ---------------------------------------------------------------------------------------------

#[test]
fn legacy_symbols_fails_on_tickers_the_toys_hook_cannot_parse_naming_each() {
    let scratch = Scratch::toy("unparseable");
    scratch.write(
        "legacy_symbols/tickers.txt",
        "# the toy reads only X/USDT\nTOYA/USDT\n  TOYA/BTC  \n\nTOYA-PERP\n",
    );
    let failure = failed(legacy_symbols(&Variant::TOY, &scratch.0));
    assert_eq!(failure.check, "legacy_symbols");
    assert_eq!(
        named(&failure),
        [
            "VenueFactory::parse_fbc_common_symbol(TOYA/BTC)",
            "VenueFactory::parse_fbc_common_symbol(TOYA-PERP)",
        ]
    );
    let unmapped = said(&failure, "VenueFactory::parse_fbc_common_symbol(TOYA/BTC)");
    assert!(unmapped.ends_with("(Unmapped)"), "{unmapped}");
    let malformed = said(&failure, "VenueFactory::parse_fbc_common_symbol(TOYA-PERP)");
    assert!(malformed.ends_with("(NotCommonForm)"), "{malformed}");
}

#[test]
fn legacy_symbols_fails_a_hook_that_cannot_parse_a_ticker_in_the_toys_fixture() {
    let variant = Variant {
        symbols: |s| match s {
            "TOYB/USDT" => Err(SymbolError::Unmapped),
            s => ToyFactory.parse_fbc_common_symbol(s),
        },
        ..Variant::TOY
    };
    let failure = failed(legacy_symbols(&variant, Path::new(FIXTURES)));
    assert_eq!(
        named(&failure),
        ["VenueFactory::parse_fbc_common_symbol(TOYB/USDT)"]
    );
}

#[test]
fn legacy_symbols_is_skipped_only_for_a_venue_without_a_rule_and_without_tickers() {
    let no_rule = Variant {
        symbols: |_| Err(SymbolError::NoRule),
        ..Variant::TOY
    };
    let scratch = Scratch::empty("no-tickers");
    let verdict = legacy_symbols(&no_rule, &scratch.0).unwrap();
    assert!(
        matches!(verdict, Verdict::Skipped { check: "legacy_symbols", why } if why.contains("NoRule")),
        "{verdict:?}"
    );
    // A venue with a rule must list its tickers.
    let failure = failed(legacy_symbols(&Variant::TOY, &scratch.0));
    let file = "legacy_symbols/tickers.txt";
    assert!(said(&failure, file).starts_with("missing"));
    // A venue without a rule whose fixtures list tickers refuses each.
    let failure = failed(legacy_symbols(&no_rule, Path::new(FIXTURES)));
    assert_eq!(failure.breaches.len(), 2);
    assert!(failure.breaches[0].what.contains("(NoRule)"));
}

#[test]
fn legacy_symbols_fails_a_ticker_file_that_lists_none_or_cannot_be_read() {
    let scratch = Scratch::empty("empty-tickers");
    fs::create_dir_all(scratch.at("legacy_symbols")).unwrap();
    scratch.write("legacy_symbols/tickers.txt", "# nothing yet\n\n");
    let file = "legacy_symbols/tickers.txt";
    let failure = failed(legacy_symbols(&Variant::TOY, &scratch.0));
    assert_eq!(said(&failure, file), "lists no ticker");
    // A directory where the file should be cannot be read as one.
    fs::remove_file(scratch.at(file)).unwrap();
    fs::create_dir_all(scratch.at(file)).unwrap();
    let failure = failed(legacy_symbols(&Variant::TOY, &scratch.0));
    assert!(said(&failure, file).starts_with("cannot read"));
}

#[test]
fn the_variant_otherwise_delegates_to_the_toy() {
    let cfg = VenueConfig::new();
    let v = Variant::TOY;
    assert_eq!(v.id(), "TOY-VARIANT");
    assert_eq!(v.config_schema(), ToyFactory.config_schema());
    assert!(matches!(v.discover(&cfg), Err(VenueError::NoDiscovery)));
    assert!(matches!(v.plan_md(&cfg, &SpecTable::new(), &BTreeSet::new()), Ok(p) if p.is_empty()));
    assert!(v.plan_exec(&cfg).is_err());
    assert!(v.test_connection(&cfg, Secrets::new()).is_none());
}
