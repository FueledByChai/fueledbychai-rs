//! `subscriptions_idempotent` (design §6, decision 0002): subscribing the same set twice
//! through the runtime's [`Reconciler`] sends nothing the second time, on the first connection
//! epoch and after a reconnect. The Java stack resubscribed on every reconnect without asking
//! what the connection already had, so duplicate subscriptions piled up, and the work behind
//! them with them.
//!
//! For every book channel the suite drives ([`book_cases`](super::book_cases)), the set is that
//! channel on the instruments one connection carries. In each of two epochs a codec built fresh
//! from the factory (one per epoch, as the runtime builds them) is opened, and the reconciler's
//! call for the set is handed to its [`subscribe`](fbc_core::MdCodec::subscribe), which must
//! take it. After the reconnect it must ask, opening and subscribing, for exactly the effects it
//! asked for on the first connection: an [`MdCodec`] is deterministic given its inputs and prior state, and a fresh one
//! has none, so anything else is state carried over (a codec that remembers the last
//! connection's subscriptions sends them twice). Within each epoch, opening and subscribing
//! together send each frame once: one sent from both would subscribe twice (Codex
//! r4218492650). Then the same set is desired again, and any call the reconciler yields is a
//! breach. It reads no fixture file.

use std::collections::BTreeSet;

use fbc_core::{ConnKey, Effect, Effects, MdCodec, SpecTable, VenueError};
use fbc_runtime::{Reconciler, SubscribeCall};

use super::book_cases::{self, Book, NO_BOOK};
use super::harness::{Harness, MD_STREAM};
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "subscriptions_idempotent";
/// The epochs each channel is subscribed in: the first connection, and one reconnect.
const EPOCHS: u32 = 2;

/// Runs `subscriptions_idempotent` against `subject`.
pub fn subscriptions_idempotent(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let (books, mut probed) = book_cases::books(&h, |_| None);
    let mut breaches = Vec::new();
    for book in &books {
        if let Some(sent) = book_twice(&h, book, &mut breaches) {
            probed.push(sent);
        }
    }
    book_cases::verdict(CHECK, &books, NO_BOOK, Ok((probed, breaches)))
}

/// `book` on every instrument, desired twice in each epoch: what was sent, or `None` when the
/// codec refused the set.
fn book_twice(h: &Harness<'_>, book: &Book, breaches: &mut Vec<Breach>) -> Option<String> {
    let subs = h.book_subs(book.id);
    let channel = book.caps.channel;
    let conn = MD_STREAM.0;
    let mut rec = Reconciler::new(ConnKey { conn, epoch: 0 });
    let mut sent = Vec::new();
    let mut opens: Vec<Vec<Effect>> = Vec::new();
    for epoch in 0..EPOCHS {
        let key = ConnKey { conn, epoch };
        if epoch > 0 {
            rec.begin_epoch(key).expect("each epoch is after the last");
        }
        let mut codec = h.md_codec(subs.clone());
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        let open = fx.take();
        // What opening asks for counts too: a codec subscribing there from its plan would send
        // the set twice with the subscribe (Codex r4217991947).
        if opens.first().is_some_and(|before| *before != open) {
            let what = format!(
                "{channel}: the reconnect's on_open asked for other effects than the first \
                 epoch's"
            );
            breaches.push(Breach::new("MdCodec::on_open", what));
        }
        opens.push(open);
        let opened = rec.opened(key).expect("the current epoch opened");
        let call = opened
            .or_else(|| rec.set_desired(subs.iter().copied()))
            .expect("a set not yet subscribed is a call");
        let first = match handed(&mut *codec, &call, h.specs()) {
            Ok(n) => n,
            Err(e) => {
                let what = format!("refused {channel} on every instrument in epoch {epoch}: {e}");
                breaches.push(Breach::new("MdCodec::subscribe", what));
                return None;
            }
        };
        let after = rec.sent(call).expect("the call of the current epoch");
        // A reconnect's codec is fresh: it asks for exactly what the first did, nothing piled up
        // in a payload or beside it (Codex r4217682449).
        if sent.first().is_some_and(|before| *before != first) {
            let what = format!(
                "{channel}: the reconnect's subscribe asked for other effects than the first \
                 epoch's"
            );
            breaches.push(Breach::new("MdCodec::subscribe", what));
        }
        // Within the epoch, a frame sent once (Codex r4218492650).
        if repeats(opens.last().into_iter().flatten().chain(&first)) {
            let what = format!(
                "{channel}: epoch {epoch} sends the same frame twice, opening and subscribing"
            );
            breaches.push(Breach::new("MdCodec::subscribe", what));
        }
        sent.push(first);
        // The same set again: no call, nor any after the first.
        let again: Vec<SubscribeCall> = after
            .into_iter()
            .chain(rec.set_desired(subs.iter().copied()))
            .collect();
        if !again.is_empty() {
            let what = format!(
                "{channel}: the same set desired again in epoch {epoch} made {} more calls",
                again.len()
            );
            breaches.push(Breach::new("Reconciler", what));
        }
    }
    Some(format!(
        "{channel}: {} subscriptions sent once in each of {EPOCHS} epochs ({:?} effects), \
         nothing for the same set again",
        subs.len(),
        sent.iter().map(Vec::len).collect::<Vec<_>>()
    ))
}

/// `call` handed to `codec`: the effects it asked for, or why it refused.
fn handed(
    codec: &mut dyn MdCodec,
    call: &SubscribeCall,
    specs: &SpecTable,
) -> Result<Vec<Effect>, VenueError> {
    let mut fx = Effects::new();
    codec.subscribe(call.add(), call.remove(), specs, &mut fx)?;
    Ok(fx.take())
}

/// Whether `effects` send the same frame on the same stream more than once.
fn repeats<'a>(effects: impl Iterator<Item = &'a Effect>) -> bool {
    let mut seen = BTreeSet::new();
    effects
        .filter_map(|e| match e {
            Effect::Send { stream, frame, .. } => Some((*stream, frame.bytes())),
            _ => None,
        })
        .any(|sent| !seen.insert(sent))
}
