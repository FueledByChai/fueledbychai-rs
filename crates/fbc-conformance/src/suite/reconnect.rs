//! `resync_after_reconnect` (design §6, BT-502; decisions 0005, 0013, 0085): a reconnect leads
//! to a resync and to no order re-placed. It runs fbc-runtime's order-entry session against the
//! stub server as the other order-entry checks do (`live.rs`): a placement the stub accepts rests,
//! then the stub closes the connection. The session reconnects as its pacing says, on a clock
//! the check moves, and the stub answers the new connection's opening as it answered the first
//! (its authentication, the cancel-on-disconnect arm, a resync showing the account flat: the
//! close has cancelled what rested on the connection).
//!
//! It fails where the placement's acceptance is not reported once as `OrderCaps.ack` has it,
//! where no resync begins and then ends on a later connection epoch than the one the placement
//! was answered on (the runtime stamps each event with its epoch) and, where the session read
//! nothing over HTTP so its resync was answered in frames, once the stub has given the new
//! connection's last opening answer, its resync's (Codex r4226277849; a venue resyncing over
//! REST is tied by the epoch alone, FBC-6cvg), where the stub's script does not play to its
//! end (the new connection's opening never written or never answered), and where the order is
//! written more than once over both connections by [`SETTLE`] after the script ended, as the
//! other checks count a request written again: by the frames carrying its client id as the
//! venue's wire spells it, or, where the venue sends no such id, the frames equal to the
//! placement's.

use std::time::Duration;

use fbc_core::{ExecEvent, OpKind, SubmitOutcome};

use super::live::Live;
use super::orders::{accepted_once, own_item, verdict};
use super::stub::Answer::Accept;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "resync_after_reconnect";
/// The connections the session opens: the first, and the one after the stub closes it.
const CONNECTIONS: usize = 2;
/// How long the check moves the clock on once the stub's script has ended, before it counts
/// what was written (RB-y6y-1): a re-placement a codec arms on a timer as the reconnect's resync
/// ends is one too. Fifty times the harness's reconnect pacing ceiling (100 ms), and short of
/// the keepalives a longer wait would let fire unanswered.
const SETTLE: Duration = Duration::from_secs(5);

/// Runs `resync_after_reconnect` against `subject`.
pub fn resync_after_reconnect(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(CHECK, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let model = live.order.ack;
    // The placement accepted on the first connection; nothing asked on the second.
    let epochs = vec![vec![vec![Accept]], Vec::new()];
    let breaches = live.run_epochs(epochs, async |c| {
        c.ready().await?;
        let (cid, auth) = c.oms.place(c.h)?;
        let placed = c.send(auth, OpKind::Place).await?;
        // The stub accepts it, closes the connection, and answers the next one's opening.
        let played = c.played().await;
        c.advance(SETTLE).await;
        let mut breaches = Vec::new();
        let outcomes = c.outcomes(placed);
        let acks: Vec<&SubmitOutcome> = outcomes.iter().map(|(_, o)| o).collect();
        if !own_item(&outcomes, cid) || !accepted_once(&acks, model) {
            let what = format!(
                "the placement the stub accepted was reported {outcomes:?}, not once naming its \
                 one item"
            );
            breaches.push(Breach::new("ExecCodec::on_frame", what));
        }
        // A resync, begun and then ended, on a later connection epoch than the placement's
        // answer came on, as the runtime stamps each event: one the reconnect led to (Codex
        // r4225971621), a lone end not taken for one (Codex r4226060251). Where it was answered
        // in frames, heard once the stub gave the new connection's last opening answer, so a
        // pair decoded from the authentication's or the arm's answer is not taken for it
        // (Codex r4226277849).
        let events = c.events();
        let epochs = c.epochs();
        let opened = c.opened();
        let all = if c.over_http() {
            0
        } else {
            c.opening() * CONNECTIONS
        };
        let answered =
            |e: &ExecEvent| matches!(e, ExecEvent::Outcome { rpc, .. } if *rpc == placed);
        let first = events.iter().position(answered).map(|i| epochs[i]);
        let later = |i: &usize| first.is_some_and(|first| epochs[*i] > first) && opened[*i] >= all;
        let after: Vec<&ExecEvent> = (0..events.len())
            .filter(later)
            .map(|i| &events[i])
            .collect();
        let begun = after
            .iter()
            .position(|e| matches!(e, ExecEvent::ResyncBegin { .. }));
        let resynced =
            begun.is_some_and(|b| after[b..].iter().any(|e| matches!(e, ExecEvent::ResyncEnd)));
        if !resynced {
            let what = "no resync ended once the stub closed the connection and the session \
                        reconnected: a reconnect leads to a resync";
            breaches.push(Breach::new("ExecCodec::resync", what));
        }
        let times = c.written(cid);
        if times > 1 {
            let what = format!(
                "the placement the stub accepted was written {times} times over the \
                 {CONNECTIONS} connections; nothing is re-placed after a reconnect"
            );
            breaches.push(Breach::new("ExecCodec: nothing re-placed", what));
        }
        if let Err(failure) = played {
            breaches.extend(failure.breaches);
        }
        Ok(breaches)
    })?;
    verdict(CHECK, breaches, || {
        vec![
            "a reconnect after an accepted placement led to a resync, and nothing was re-placed"
                .into(),
        ]
    })
}
