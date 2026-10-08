//! `encode_deterministic` (design §6, decision 0006): the same command under the same
//! [`EncodeCtx`](fbc_core::EncodeCtx) encodes to identical bytes whenever it is encoded, so a
//! journal replayed through the venue's codec sends what the live run sent. Time and nonces
//! reach a codec only through its context; a codec that reads a clock, a counter of its own or
//! randomness breaks this.
//!
//! Every command is encoded twice, each time by a codec built fresh from the factory, at two
//! real times at least [`APART`] apart: every [`Golden`](super::Golden) command the
//! [`Setup`](super::Setup) lists, under its own context, and the commands the suite builds
//! from the caps under its fixed context: the plainest order they allow, and each amend,
//! cancel, batch cancel and query `commands_selfcontained` encodes. Both encodings must give
//! the same result and ask for the same effects, frames included. A breach names the command
//! and what differs, never the bytes: a frame can carry a credential.
//!
//! Skipped for a venue whose caps declare no order entry: it encodes nothing. A venue that
//! declares order entry with no golden command and no command the caps allow fails.

use core::time::Duration;

use fbc_core::{Effects, EncodeCtx, EncodeReceipt, NotSentReason, PathStamps, VenueCommand};

use super::harness::{Harness, Ids, RPC, Shape};
use super::{Breach, Failure, Subject, Verdict, selfcontained};

const CHECK: &str = "encode_deterministic";

/// How far apart in real time the two encodings are at least.
pub const APART: Duration = Duration::from_millis(5);

/// A command to encode: its label, request id, context and the command.
type Probe = (String, fbc_core::RpcId, EncodeCtx, VenueCommand);

/// What one encoding gave.
type Encoding = (Result<EncodeReceipt, NotSentReason>, Effects);

/// Runs `encode_deterministic` against `subject`.
pub fn encode_deterministic(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let Some(exec) = h.caps.exec.clone() else {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare no order entry (VenueCaps.exec is None): it encodes nothing",
        });
    };
    let mut probes: Vec<Probe> = subject
        .setup()
        .goldens
        .into_iter()
        .map(|g| (format!("golden {}", g.name), g.rpc, g.ctx, g.cmd))
        .collect();
    let ids = Ids::new(&h, 1)?;
    let built = Shape::plain(&exec.order)
        .map(|shape| {
            (
                "the plainest order".to_owned(),
                VenueCommand::Place(ids.order(&h, 0, shape)),
            )
        })
        .into_iter()
        .chain(selfcontained::commands(&h, &exec.order, &ids));
    for (label, cmd) in built {
        let ctx = h.ctx(cmd.items().unwrap_or(u16::MAX));
        probes.push((label, RPC, ctx, cmd));
    }
    if probes.is_empty() {
        let what = "lists no command, and the caps allow none to build: nothing to encode";
        return Err(h.fail("Setup.goldens", what));
    }
    let first = encode_all(&h, &probes)?;
    std::thread::sleep(APART);
    let second = encode_all(&h, &probes)?;
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for ((label, ..), (a, b)) in probes.iter().zip(first.iter().zip(&second)) {
        let parts = [
            (a.0 != b.0, "its result"),
            (a.1 != b.1, "the effects asked for"),
        ];
        let differs: Vec<&str> = parts.iter().filter(|(d, _)| *d).map(|(_, n)| *n).collect();
        if differs.is_empty() {
            probed.push(label.clone());
        } else {
            let what = format!(
                "encoded twice under one context by freshly built codecs, {} ms apart: {} \
                 differ",
                APART.as_millis(),
                differs.join(" and ")
            );
            breaches.push(Breach::new(label, what));
        }
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

/// Every probe encoded by a codec built fresh for it.
fn encode_all(h: &Harness<'_>, probes: &[Probe]) -> Result<Vec<Encoding>, Failure> {
    let mut out = Vec::with_capacity(probes.len());
    for (_, rpc, ctx, cmd) in probes {
        let mut codec = h.exec_codec()?;
        let mut fx = Effects::new();
        let stamps = &mut PathStamps::off();
        let result = codec.encode(cmd, *rpc, h.specs(), ctx, stamps, &mut fx);
        out.push((result, fx));
    }
    Ok(out)
}
