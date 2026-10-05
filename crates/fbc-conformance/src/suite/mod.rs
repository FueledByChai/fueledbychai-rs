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
//! (`<fixtures>/<check>/`), which that check's documentation describes. The checks so far
//! read none: [`caps_truthful`] and [`commands_selfcontained`] need only the factory and the
//! [`Setup`] the `setup` function gives, called afresh wherever a check builds a codec.
//!
//! # The checks so far
//!
//! - [`caps_truthful`]: every operation, order kind, flag, time in force, channel and
//!   reference the caps declare absent is refused `NotSent(Unsupported)` with no effect, every
//!   declared flag conflict `NotSent(FlagConflict)`, and an instrument cancel-all is never
//!   widened to the account (decision 0003).
//! - [`commands_selfcontained`]: every amend, cancel and query encodes from the command and
//!   the spec table alone, with a freshly built codec (decisions 0005, 0014 item 5).

mod caps_truthful;
mod harness;
mod selfcontained;

use core::fmt;
use std::path::{Path, PathBuf};

use fbc_core::{Secrets, SpecTable, VenueConfig, VenueFactory};

pub use caps_truthful::caps_truthful;
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

impl Failure {
    /// A failure with the one breach of `capability`.
    fn one(check: &'static str, capability: &str, what: impl Into<String>) -> Failure {
        Failure {
            check,
            breaches: vec![Breach {
                capability: capability.to_owned(),
                what: what.into(),
            }],
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
/// reason, and a pass returns.
pub fn expect(outcome: Result<Verdict, Failure>) {
    match outcome {
        Ok(Verdict::Passed { .. }) => {}
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
    };
}
