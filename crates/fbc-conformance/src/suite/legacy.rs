//! `legacy_symbols` (design §4.4 and §6, decisions 0003 and 0004): every Java-era ticker value
//! the consumer's configurations hold for this venue reads as an asset key through the
//! factory's [`parse_fbc_common_symbol`](fbc_core::VenueFactory::parse_fbc_common_symbol), the
//! hook the consumer's legacy key reader calls.
//!
//! The tickers are read from `<fixtures>/legacy_symbols/tickers.txt`, one per line, each
//! trimmed; blank lines and lines starting with `#` are ignored. Every one must parse; each
//! that does is probed with the key it gave. A file that lists no ticker fails: a pass that
//! probed nothing would prove nothing.
//!
//! Skipped only for a venue the Java library never traded, whose hook answers
//! [`SymbolError::NoRule`], and whose fixtures hold no ticker file. A venue with a rule and no
//! ticker file fails, and so does a ticker file for a venue without a rule: each of its
//! tickers is refused.

use std::fs;
use std::io::ErrorKind;

use fbc_core::SymbolError;

use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "legacy_symbols";

/// The ticker file, under the fixture directory.
const FILE: &str = "legacy_symbols/tickers.txt";

/// The ticker asked of a hook to learn whether the venue has a rule at all.
const PROBE: &str = "BTC/USDT";

/// Runs `legacy_symbols` against `subject`.
pub fn legacy_symbols(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let factory = subject.factory();
    let text = match fs::read_to_string(subject.fixtures().join(FILE)) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            return match factory.parse_fbc_common_symbol(PROBE) {
                Err(SymbolError::NoRule) => Ok(Verdict::Skipped {
                    check: CHECK,
                    why: "the venue has no FBC common-symbol rule (SymbolError::NoRule) and its \
                          fixtures list no Java-era ticker",
                }),
                _ => Err(Failure::one(
                    CHECK,
                    FILE,
                    "missing, though the venue reads Java-era tickers (its hook has a rule)",
                )),
            };
        }
        Err(e) => return Err(Failure::one(CHECK, FILE, format!("cannot read: {e}"))),
    };
    let tickers = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for ticker in tickers {
        match factory.parse_fbc_common_symbol(ticker) {
            Ok(key) => probed.push(format!(
                "{ticker}: {}/{} {:?}",
                key.base.as_str(),
                key.quote.as_str(),
                key.kind
            )),
            Err(e) => breaches.push(Breach {
                capability: format!("VenueFactory::parse_fbc_common_symbol({ticker})"),
                what: format!("refused a Java-era ticker in {FILE}: {e} ({e:?})"),
            }),
        }
    }
    if !breaches.is_empty() {
        return Err(Failure {
            check: CHECK,
            breaches,
        });
    }
    if probed.is_empty() {
        return Err(Failure::one(CHECK, FILE, "lists no ticker"));
    }
    Ok(Verdict::Passed {
        check: CHECK,
        probed,
    })
}
