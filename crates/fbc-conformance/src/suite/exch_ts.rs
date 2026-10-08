//! `no_exch_ts_synthesized` (design §6, decision 0006): an event carries the venue's
//! timestamp ([`VenueMeta::exch_ts`](fbc_core::VenueMeta)) only when its frame does. A codec
//! that fills a missing timestamp from a clock of its own would make the journal's latency
//! figures and a replay's ordering rest on a time the venue never sent.
//!
//! For every book channel the suite drives ([`book_cases`](super::book_cases)),
//! `no_exch_ts_synthesized/<channel>.frames` marks with the `ts` tag each frame that carries the
//! venue's timestamp. Every event pushed from a frame not marked must carry none. A case none of
//! whose unmarked frames pushes an event proves nothing and fails. Market data only: the
//! order-entry events are not held to this check yet.

use super::book_cases::{self, NO_BOOK, PerBook};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "no_exch_ts_synthesized";

/// Runs `no_exch_ts_synthesized` against `subject`.
pub fn no_exch_ts_synthesized(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let (books, skipped) = book_cases::books(&h, |_| None);
    let per_book = PerBook {
        check: CHECK,
        dir: CHECK,
        judge: &|_, file, steps, breaches| {
            let mut judged = 0usize;
            for step in steps.iter().filter(|s| !s.tags.ts) {
                for (meta, _) in &step.events {
                    judged += 1;
                    if let Some(ts) = meta.exch_ts {
                        let what = format!(
                            "{file} line {}: the frame carries no timestamp, yet an event \
                             carries exch_ts {}",
                            step.line, ts.0
                        );
                        breaches.push(Breach::new("VenueMeta.exch_ts", what));
                    }
                }
            }
            if judged == 0 {
                let what = "no event is decoded from a frame without a timestamp: a case that \
                            states none proves nothing";
                breaches.push(Breach::new(file, what));
            }
            format!("{file}: {judged} events from frames without a timestamp")
        },
    };
    let outcome = per_book.run(&h, subject, &books, skipped);
    book_cases::verdict(CHECK, &books, NO_BOOK, outcome)
}
