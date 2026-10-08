//! The named conformance suite (design §6, BT-502): the checks every venue adapter is held to,
//! run from the adapter crate's `tests/conformance.rs` through [`suite!`](crate::suite!), each
//! named check one test.
//!
//! ```ignore
//! fn assumed() -> fbc_conformance::suite::Setup { /* the spec table, configuration and
//!                                                    synthetic credentials the fixtures assume */ }
//!
//! fbc_conformance::suite! {
//!     factory: MyVenueFactory,
//!     fixtures: "../../fixtures/my-venue/conformance",
//!     setup: assumed,
//! }
//! ```
//!
//! Each check is also a public function taking a [`Subject`], so a test can run one check
//! against a venue built to break it and read the [`Failure`] it gives, which names every
//! [`Breach`] by the capability it breaks. A check passes ([`Verdict::Passed`], listing what it
//! probed) or fails; it is skipped ([`Verdict::Skipped`], saying why) only where the venue's
//! caps leave it nothing to check, and the test it becomes prints why.
//!
//! # The fixture directory
//!
//! `fixtures` is a directory, relative to the adapter crate's manifest, that must exist. A
//! check that reads recorded data reads it from a subdirectory named after the check
//! (`<fixtures>/<check>/`), which that check's documentation describes:
//!
//! - `signing_golden/`: one `<name>.golden` file per [`Golden`] command the [`Setup`] lists,
//!   holding the exact bytes of the frame it encodes to, and a `SYNTHETIC` file saying where
//!   the credentials and values come from (decision 0009: golden signing vectors live in a
//!   directory marked synthetic). The commands themselves are typed Rust values in the
//!   [`Setup`], as fault scripts are (decision 0025).
//! - `legacy_symbols/tickers.txt`: the Java-era ticker values, one per line; blank lines and
//!   lines starting with `#` are ignored.
//! - `fee_sign/`, `liquidity_reported/` and `position_signed/`: the case files their checks
//!   read, each a `<case>.frames` file of frames the venue sends (the format is
//!   [`frames`]'s): `fee_sign/rebate.frames` and `fee_sign/paid.frames`,
//!   `liquidity_reported/maker.frames` and `liquidity_reported/taker.frames`,
//!   `position_signed/long.frames` and `position_signed/short.frames`.
//! - `decoder_deterministic/`: optional, more `<case>.frames` files for
//!   [`decoder_deterministic`], which also decodes every case of the three directories above.
//! - `ids_roundtrip/java_era.txt`: client ids another system sent the venue (the Java
//!   brokers' ids, for a venue the Java stack traded), one per line; blank lines and lines
//!   starting with `#` are ignored.
//! - `restart_cid/resting.frames`: a case of what the venue shows a restarted process: a
//!   resync's answer with orders of ours resting, their client ids the suite's first
//!   [`MINTED_BEFORE`] ids in its namespace, the newest among them ([`restart_cid`]).
//! - `continuity/`, `no_exch_ts_synthesized/` and `book_channels/`: one market-data case per
//!   book channel the suite drives, `<channel>.frames`, its frames tagged with what the fixture
//!   knows of them (the format is [`book_cases`]'s); and, for a venue whose market data is
//!   binary, `continuity/longer_block/<channel>.frames`, frames whose binary block is longer
//!   than expected.
//!
//! [`caps_truthful`], [`commands_selfcontained`], [`encode_deterministic`], [`price_grid`] and
//! [`subscriptions_idempotent`] read no file: they need only the factory and the [`Setup`] the
//! `setup` function gives, called afresh wherever a check builds a codec.
//!
//! # The checks so far
//!
//! - [`caps_truthful`]: every operation, order kind, flag, time in force, channel and
//!   reference the caps declare absent is refused `NotSent(Unsupported)` with no effect, every
//!   declared flag conflict `NotSent(FlagConflict)`, and an instrument cancel-all is never
//!   widened to the account (decision 0003).
//! - [`commands_selfcontained`]: every amend, cancel and query encodes from the command and
//!   the spec table alone, with a freshly built codec (decisions 0005, 0014 item 5).
//! - [`signing_golden`]: every [`Golden`] command, encoded with its own [`EncodeCtx`] by a
//!   freshly built codec, gives exactly the committed golden bytes, signatures included.
//! - [`legacy_symbols`]: every Java-era ticker in the fixtures parses through the factory's
//!   [`parse_fbc_common_symbol`](VenueFactory::parse_fbc_common_symbol) (design §4.4).
//! - [`fee_sign`]: a rebate fill decodes to a fee below zero and a fill we paid for to one
//!   above zero (decision 0004).
//! - [`liquidity_reported`]: fills carry the liquidity their frames state.
//! - [`position_signed`]: positions are signed, positive long.
//! - [`decoder_deterministic`]: the same frames decode to identical events on two runs, each
//!   with a freshly built codec (decision 0006).
//! - [`encode_deterministic`]: the same command under the same [`EncodeCtx`] encodes to the
//!   same bytes at two real times, each with a freshly built codec (decision 0006).
//! - [`ids_roundtrip`]: our ids round-trip through the core's client-id codec under the
//!   venue's wire format, and the fixtures' Java-era ids read as `Unparseable` (decision 0004).
//! - [`restart_cid`]: a mint restarted while our orders rest, its high-water mark lost, issues
//!   no live or recent id again, held above them by the venue's resync answer (decision 0004).
//! - [`price_grid`]: every price quantized at the venue's grid boundaries is valid and
//!   maker-safe, and each is sent as a post-only order at the price it was given.
//! - [`continuity`]: the fixture's sequence breaks are detected as each book channel's
//!   `BookCaps.continuity` declares, and nothing else is taken for one; a binary venue's longer
//!   blocks are read, never taken for a break (decision 0022).
//! - [`subscriptions_idempotent`]: subscribing the same set twice through the runtime's
//!   reconciler sends nothing the second time, before and after a reconnect (decision 0002).
//! - [`no_exch_ts_synthesized`]: a market-data event carries the venue's timestamp only when
//!   its frame does.
//! - [`book_channels`]: a book's frames show the order channels its `BookCaps.includes_channels`
//!   declares, and decode onto that book.
//!
//! The four market-data checks skip by name a book channel anchored on REST (FBC-fhk4), and a
//! check's sub-case skipped (a text venue's longer blocks) is listed by name among what it
//! probed.

pub mod book_cases;
mod book_channels;
mod caps_truthful;
mod continuity;
mod decoded;
mod deterministic;
mod encode;
mod exch_ts;
pub mod frames;
mod golden;
mod grid;
mod harness;
mod idempotent;
mod ids;
mod legacy;
mod selfcontained;

use core::fmt;
use std::path::{Path, PathBuf};

use fbc_core::{
    EncodeCtx, RpcId, Secrets, SpecTable, StreamId, VenueCommand, VenueConfig, VenueFactory,
};

pub use book_channels::book_channels;
pub use caps_truthful::caps_truthful;
pub use continuity::continuity;
pub use decoded::{fee_sign, liquidity_reported, position_signed};
pub use deterministic::decoder_deterministic;
pub use encode::{APART, encode_deterministic};
pub use exch_ts::no_exch_ts_synthesized;
pub use golden::signing_golden;
pub use grid::price_grid;
pub use idempotent::subscriptions_idempotent;
pub use ids::{MINTED_BEFORE, ids_roundtrip, restart_cid};
pub use legacy::legacy_symbols;
pub use selfcontained::commands_selfcontained;

/// What the fixtures assume: the instruments, the configuration and the credentials the
/// factory is given. Credentials here are synthetic, never a real account's (decision 0009).
#[derive(Debug)]
pub struct Setup {
    /// The instruments; the checks place, amend, cancel and query on the first by id.
    pub specs: SpecTable,
    /// The venue's configuration.
    pub cfg: VenueConfig,
    /// Synthetic credentials for `exec_codec`; empty for a venue that takes none.
    pub creds: Secrets,
    /// The commands [`signing_golden`] encodes, each against its golden bytes; empty for a
    /// venue that declares no order entry.
    pub goldens: Vec<Golden>,
    /// The stream the fixture frames are handed to the order-entry codec on: the order-entry
    /// connection's, as the venue's [`plan_exec`](VenueFactory::plan_exec) numbers it.
    pub exec_stream: StreamId,
}

/// A command whose encoding is committed as golden bytes: [`signing_golden`] encodes it as
/// request `rpc` under `ctx` with a freshly built codec, and its one frame must equal
/// `<fixtures>/signing_golden/<name>.golden` byte for byte.
#[derive(Clone, Debug)]
pub struct Golden {
    /// The golden file's name without `.golden`: letters, digits, `-` and `_` only.
    pub name: &'static str,
    /// The request id it is encoded as.
    pub rpc: RpcId,
    /// The time and nonces it is encoded with.
    pub ctx: EncodeCtx,
    /// The command.
    pub cmd: VenueCommand,
}

/// The venue a check runs against: its factory, its fixture directory and the [`Setup`] its
/// fixtures assume.
pub struct Subject<'a> {
    factory: &'a dyn VenueFactory,
    fixtures: PathBuf,
    setup: fn() -> Setup,
}

impl<'a> Subject<'a> {
    /// The venue `factory` builds under what `setup` gives, its fixtures in `fixtures`;
    /// refused when `fixtures` is not a directory, so a mistyped path fails rather than
    /// leaving a later check nothing to read.
    pub fn new(
        factory: &'a dyn VenueFactory,
        fixtures: impl Into<PathBuf>,
        setup: fn() -> Setup,
    ) -> Result<Subject<'a>, Failure> {
        let fixtures = fixtures.into();
        if !fixtures.is_dir() {
            let what = format!("{} is not a directory", fixtures.display());
            return Err(Failure::one("suite", "fixture directory", what));
        }
        Ok(Subject {
            factory,
            fixtures,
            setup,
        })
    }

    /// The venue's factory.
    pub fn factory(&self) -> &'a dyn VenueFactory {
        self.factory
    }

    /// The fixture directory.
    pub fn fixtures(&self) -> &Path {
        &self.fixtures
    }

    /// What the fixtures assume, built afresh.
    pub fn setup(&self) -> Setup {
        (self.setup)()
    }
}

impl fmt::Debug for Subject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subject")
            .field("factory", &self.factory.id())
            .field("fixtures", &self.fixtures)
            .finish_non_exhaustive()
    }
}

/// A check that did not fail.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Verdict {
    /// It held for everything it probed, each named.
    Passed {
        check: &'static str,
        probed: Vec<String>,
    },
    /// The venue's caps leave it nothing to check, for the reason given.
    Skipped {
        check: &'static str,
        why: &'static str,
    },
}

/// One way a venue broke a check: the capability (or the setup step) and what happened.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Breach {
    /// The capability broken, as the caps spell it (`OrderCaps.post_only is false`), or the
    /// setup step that failed (`VenueFactory::caps`).
    pub capability: String,
    /// What the venue did.
    pub what: String,
}

/// A check that failed, with every breach it found.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Failure {
    /// The check.
    pub check: &'static str,
    /// What broke it, in the order found; never empty.
    pub breaches: Vec<Breach>,
}

impl Breach {
    /// A breach of `capability`.
    fn new(capability: &str, what: impl Into<String>) -> Breach {
        Breach {
            capability: capability.to_owned(),
            what: what.into(),
        }
    }
}

impl Failure {
    /// A failure with the one breach of `capability`.
    fn one(check: &'static str, capability: &str, what: impl Into<String>) -> Failure {
        Failure {
            check,
            breaches: vec![Breach::new(capability, what)],
        }
    }

    /// Whether a breach names `capability`.
    pub fn names(&self, capability: &str) -> bool {
        self.breaches.iter().any(|b| b.capability == capability)
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed:", self.check)?;
        for b in &self.breaches {
            write!(f, "\n  {}: {}", b.capability, b.what)?;
        }
        Ok(())
    }
}

impl std::error::Error for Failure {}

/// What a check gave, as a test: a failure panics with every breach, a skip prints its
/// reason, and a pass returns, printing any part of it skipped by name.
pub fn expect(outcome: Result<Verdict, Failure>) {
    match outcome {
        Ok(Verdict::Passed { check, probed }) => {
            for skipped in probed.iter().filter(|p| p.contains(": skipped: ")) {
                eprintln!("{check}: {skipped}");
            }
        }
        Ok(Verdict::Skipped { check, why }) => eprintln!("{check}: skipped: {why}"),
        Err(failure) => panic!("{failure}"),
    }
}

/// Runs `check` against the venue `factory` builds, its fixtures in `fixtures`, as a test
/// ([`expect`]). The body of each test [`suite!`](crate::suite!) writes.
pub fn run(
    check: fn(&Subject<'_>) -> Result<Verdict, Failure>,
    factory: &dyn VenueFactory,
    fixtures: &str,
    setup: fn() -> Setup,
) {
    expect(Subject::new(factory, fixtures, setup).and_then(|subject| check(&subject)));
}

/// The named suite, one test per check, for the venue `factory` builds: `fixtures` is the
/// fixture directory relative to the invoking crate's manifest (a string literal), and
/// `setup` a `fn() -> Setup` giving what the fixtures assume.
///
/// ```ignore
/// fbc_conformance::suite! {
///     factory: ToyFactory,
///     fixtures: "../../fixtures/conformance-toy",
///     setup: assumed,
/// }
/// ```
#[macro_export]
macro_rules! suite {
    (factory: $factory:expr, fixtures: $fixtures:literal, setup: $setup:expr $(,)?) => {
        #[test]
        fn caps_truthful() {
            $crate::suite::run(
                $crate::suite::caps_truthful,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn commands_selfcontained() {
            $crate::suite::run(
                $crate::suite::commands_selfcontained,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn signing_golden() {
            $crate::suite::run(
                $crate::suite::signing_golden,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn legacy_symbols() {
            $crate::suite::run(
                $crate::suite::legacy_symbols,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn fee_sign() {
            $crate::suite::run(
                $crate::suite::fee_sign,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn liquidity_reported() {
            $crate::suite::run(
                $crate::suite::liquidity_reported,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn position_signed() {
            $crate::suite::run(
                $crate::suite::position_signed,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn decoder_deterministic() {
            $crate::suite::run(
                $crate::suite::decoder_deterministic,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn encode_deterministic() {
            $crate::suite::run(
                $crate::suite::encode_deterministic,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn ids_roundtrip() {
            $crate::suite::run(
                $crate::suite::ids_roundtrip,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn restart_cid() {
            $crate::suite::run(
                $crate::suite::restart_cid,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn price_grid() {
            $crate::suite::run(
                $crate::suite::price_grid,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn continuity() {
            $crate::suite::run(
                $crate::suite::continuity,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn subscriptions_idempotent() {
            $crate::suite::run(
                $crate::suite::subscriptions_idempotent,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn no_exch_ts_synthesized() {
            $crate::suite::run(
                $crate::suite::no_exch_ts_synthesized,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }

        #[test]
        fn book_channels() {
            $crate::suite::run(
                $crate::suite::book_channels,
                &$factory,
                concat!(env!("CARGO_MANIFEST_DIR"), "/", $fixtures),
                $setup,
            );
        }
    };
}
