//! `decoder_deterministic` (design §6, decision 0006): the same frames decode to identical
//! events on two runs, so a journal replayed through the venue's codec gives what the live run
//! gave.
//!
//! Every case file ([`frames`](super::frames)) in `fee_sign/`, `liquidity_reported/`,
//! `position_signed/` and `decoder_deterministic/` (the last for cases of its own: order
//! updates, rejects, anything the venue decodes) is handed, line by line, to two order-entry
//! codecs built fresh from the factory. For every line both runs must give the same result, push
//! the same events with the same [`VenueMeta`](fbc_core::VenueMeta) and ask for the same
//! effects; a frame refused is no breach, as long as both runs refuse it alike. A breach names
//! the case and line and which of the three differs, never the frame's bytes.
//!
//! Any of the four subdirectories may be absent, but together they must hold a case: a pass
//! that decoded nothing would prove nothing. Skipped for a venue whose caps declare no order
//! entry: there is no order-entry codec to run. Its market-data decoders are not held to this
//! check yet.

use super::frames::{self, EXT, Step};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "decoder_deterministic";

/// The subdirectories whose case files are decoded twice.
const DIRS: [&str; 4] = [
    "fee_sign",
    "liquidity_reported",
    "position_signed",
    "decoder_deterministic",
];

/// Runs `decoder_deterministic` against `subject`.
pub fn decoder_deterministic(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    if h.caps.exec.is_none() {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare no order entry (VenueCaps.exec is None): there is no \
                  order-entry codec to run",
        });
    }
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for dir in DIRS {
        let stems = match frames::cases_in(&subject.fixtures().join(dir)) {
            Ok(stems) => stems.unwrap_or_default(),
            Err(what) => {
                breaches.push(Breach::new(dir, what));
                continue;
            }
        };
        for stem in stems {
            let file = format!("{dir}/{stem}.{EXT}");
            let case = match frames::read(&subject.fixtures().join(&file)) {
                Ok(case) => case,
                Err(what) => {
                    breaches.push(Breach::new(&file, what));
                    continue;
                }
            };
            let (first, second) = (frames::decode(&h, &case)?, frames::decode(&h, &case)?);
            match first.iter().zip(&second).find(|(a, b)| a != b) {
                None if case.len() == 1 => probed.push(format!("{file}: 1 line")),
                None => probed.push(format!("{file}: {} lines", case.len())),
                Some((a, b)) => breaches.push(Breach::new(
                    a.call,
                    format!(
                        "{file} line {} decoded differently on two runs: {}",
                        a.line,
                        differs(a, b)
                    ),
                )),
            }
        }
    }
    if breaches.is_empty() && probed.is_empty() {
        let what = format!(
            "no .{EXT} case in {}: nothing was decoded",
            DIRS.map(|d| format!("{d}/")).join(", ")
        );
        return Err(Failure::one(CHECK, "fixtures", what));
    }
    if breaches.is_empty() {
        Ok(Verdict::Passed {
            check: CHECK,
            probed,
        })
    } else {
        Err(Failure {
            check: CHECK,
            breaches,
        })
    }
}

/// What differs between two runs' steps for the same line.
fn differs(a: &Step, b: &Step) -> String {
    let parts = [
        (a.result != b.result, "its result"),
        (a.events != b.events, "the events pushed"),
        (a.fx != b.fx, "the effects asked for"),
    ];
    let named: Vec<&str> = parts.iter().filter(|(d, _)| *d).map(|(_, n)| *n).collect();
    named.join(", ")
}
