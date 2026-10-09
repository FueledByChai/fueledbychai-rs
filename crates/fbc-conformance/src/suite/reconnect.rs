//! `resync_after_reconnect` (design §6, BT-502; decisions 0005, 0013, 0085): a reconnect leads
//! to a resync and to no order re-placed. It runs fbc-runtime's order-entry session against the
//! stub server as the other order-entry checks do (`live.rs`): a placement the stub accepts rests,
//! then the stub closes the connection. The session reconnects as its pacing says, on a clock
//! the check moves, and the stub answers the new connection's opening as it answered the first
//! (its authentication, the cancel-on-disconnect arm, a resync showing the account flat: the
//! close has cancelled what rested on the connection).
//!
//! It fails where the placement's acceptance is not reported once as `OrderCaps.ack` has it,
//! where no resync ends once the first connection's has (its end reaches the handler before the
//! epoch takes places, decision 0058), where the stub's script does not play to its end (the new
//! connection's opening never written or never answered), and where
//! the order is written more than once over both connections, as the other checks count a
//! request written again: by the frames carrying its client id as the venue's wire spells it,
//! or, where the venue sends no such id, the frames equal to the placement's.

use fbc_core::{ExecEvent, OpKind, SubmitOutcome};

use super::live::Live;
use super::orders::{accepted_once, verdict};
use super::stub::Answer::Accept;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "resync_after_reconnect";
/// The connections the session opens: the first, and the one after the stub closes it.
const CONNECTIONS: usize = 2;

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
        // The first connection's resync has ended: the session hands its end over before the
        // epoch takes places (decision 0058). A resync ending from here on is a later one's.
        let before = c.events().len();
        let (cid, auth) = c.oms.place(c.h)?;
        let placed = c.send(auth, OpKind::Place).await?;
        // The stub accepts it, closes the connection, and answers the next one's opening.
        let played = c.played().await;
        let mut breaches = Vec::new();
        let outcomes = c.outcomes(placed);
        let acks: Vec<&SubmitOutcome> = outcomes.iter().map(|(_, o)| o).collect();
        if !accepted_once(&acks, model) {
            let what =
                format!("the placement the stub accepted was reported {outcomes:?}, not once");
            breaches.push(Breach::new("ExecCodec::on_frame", what));
        }
        let ended = |e: &ExecEvent| matches!(e, ExecEvent::ResyncEnd);
        if !c.events()[before..].iter().any(ended) {
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
