//! `book_channels` (design §6, decision 0003): a book's frames show the liquidity of the order
//! channels its caps declare ([`BookCaps::includes_channels`](fbc_core::BookCaps)), no more and
//! no fewer, and decode onto that book. A book declared public-only that shows RPI orders would
//! be quoted against as if every level could be hit; one declared to show RPI that never does
//! hides the levels a strategy thinks it sees.
//!
//! For every book channel the suite drives ([`book_cases`](super::book_cases)),
//! `book_channels/<channel>.frames` marks each frame with the order channels whose liquidity it
//! shows (`public`, `rpi`), as the venue documents or a recording shows. The check holds:
//!
//! - every channel a frame shows is one the book declares;
//! - every channel the book declares is shown by some frame of the case;
//! - every frame that pushes a level states its channels, and only such a frame states any;
//! - every book event a frame pushes (snapshot, level, window, and the book's health) is on the
//!   channel's own [`BookId`](fbc_core::BookId): the runtime routes each by its book (Codex
//!   r4218492678).

use std::collections::BTreeSet;

use fbc_core::{BookId, Channel, Feed, MdEvent};

use super::book_cases::{self, NO_BOOK, PerBook};
use super::harness::Harness;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "book_channels";

/// Runs `book_channels` against `subject`.
pub fn book_channels(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let (books, skipped) = book_cases::books(&h, |_| None);
    let per_book = PerBook {
        check: CHECK,
        dir: CHECK,
        judge: &|book, file, steps, _, breaches| {
            let declared = book.caps.includes_channels;
            let channel = book.caps.channel;
            let mut shown = BTreeSet::new();
            for step in steps {
                let line = step.line;
                let mut levels = false;
                for (_, ev) in &step.events {
                    let Some((on, level)) = book_of(ev) else {
                        continue;
                    };
                    levels |= level;
                    if on != book.id {
                        let what = format!(
                            "{file} line {line}, a frame of {channel} (book {}), decoded onto \
                             book {}",
                            book.id.0, on.0
                        );
                        breaches.push(Breach::new("BookCaps.channel", what));
                    }
                }
                if levels && step.tags.shows.is_empty() {
                    let what = format!(
                        "line {line} pushes levels, yet states no order channel (public, rpi)"
                    );
                    breaches.push(Breach::new(file, what));
                }
                // A frame shows liquidity only in its levels (Codex r4217682460).
                if !levels && !step.tags.shows.is_empty() {
                    let what = format!("line {line} states order channels, yet pushes no level");
                    breaches.push(Breach::new(file, what));
                    continue;
                }
                for &c in &step.tags.shows {
                    shown.insert(c);
                    if !declared.contains(c) {
                        let what = format!("{file} line {line} shows {c:?} liquidity");
                        let capability = format!("BookCaps.includes_channels lacks {c:?}");
                        breaches.push(Breach::new(&capability, what));
                    }
                }
            }
            for c in declared.iter().filter(|c| !shown.contains(c)) {
                let what = format!("no frame of {file} shows it");
                let capability = format!("BookCaps.includes_channels has {c:?}");
                breaches.push(Breach::new(&capability, what));
            }
            let shown: Vec<Channel> = shown.into_iter().collect();
            format!("{file}: shows {shown:?}")
        },
    };
    let outcome = per_book.run(&h, subject, &books, skipped);
    book_cases::verdict(CHECK, &books, NO_BOOK, outcome)
}

/// The book `ev` is on, and whether it is a level, if it is a book event.
fn book_of(ev: &MdEvent) -> Option<(BookId, bool)> {
    match ev {
        MdEvent::Level { book, .. } => Some((*book, true)),
        MdEvent::BookSnapshotBegin { book, .. }
        | MdEvent::BookSnapshotEnd { book, .. }
        | MdEvent::Window { book, .. }
        | MdEvent::Health {
            feed: Feed::Book(book),
            ..
        } => Some((*book, false)),
        _ => None,
    }
}
