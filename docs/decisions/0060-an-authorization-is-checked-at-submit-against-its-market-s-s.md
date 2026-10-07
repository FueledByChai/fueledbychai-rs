# 0060 — An authorization is checked at submit against its market's state generation at build, an instrument cancel-all against 0005's I7 inputs, and cancels always pass, augmenting 0045

Status: accepted
Date: 2026-10-06

## Context

0045 gives every order-affecting command an `Authorization` that carries its market's
`StateGeneration` and leaves the check at submit to FBC-afd. The shard host (FBC-ct0)
interleaves handler work between the commands of one decide pass, so the kill switch, a disarm
or another arming call can land between a command's build and its write: a quote ladder's
remaining places must not go out after the switch went on (0012, 0013 rule 2). The session that
writes runs on its own task and does not hold the registry (0057); its check belongs
immediately before encode (PR #87 Reviewer B B7). Two findings on PR #86 shape it: the
generation must be the one the command was built under, not the one at issue (RB86-3 / RA86-2),
and lease names given again can leave an armed market unleased without changing its state
(RB86-3).

## Decision

- **Issue.** `Registry::authorize(acct, PermittedCommand)` is the public way to an
  `Authorization`; it takes only what the registry built, so a command a cap or the market's
  state refused never receives one, and none is ever issued for an account cancel-all (also
  refused there, with an empty batch, a batch of several markets and a command that affects no
  order). It does not itself refuse a stale command: the check at submit does, so a stale
  command has one fate, not sent, which the consumer already resolves through `on_outcome`.
- **What it carries.** A place, a batch or an amend carries its market's generation as read when
  it was built, and a live handle to that market's counter. An instrument cancel-all carries
  0005's I7 inputs instead, each as a count at build with a live handle: how many times the
  registry's hold of the market's exclusive lease changed (it advances on arming a disarmed
  market, on disarming an armed one, and on lease names given again that cover its held leases
  differently), and how many events showed an order not ours open on the market. A cancel, a
  cancel-many and a cancel-all carry the generation at issue, for the record only.
- **The check.** `Authorization::check_at_submit()`, run by the gateway immediately before
  encoding, writing nothing when it refuses: a place, a batch or an amend passes only while its
  market's generation is unchanged (`StaleAuthorization::StateChanged`); an instrument
  cancel-all only while the registry's hold of its market's exclusive lease is unchanged
  (`StaleAuthorization::ExclusiveLeaseChanged`) and no order not ours was seen on its market
  (`StaleAuthorization::ForeignSeen`), since either moving may undo the I7 guard it was built
  under, and not for any other change of the market's state: the kill switch, its lift, and
  Flatten or Wind-down on the armed market leave I7 holding, and holding the cancel-all back
  would leave our Open orders resting on a Killed market (Reviewer B's RB94-1 on PR #94); a
  cancel and a cancel-many always pass, as 0012 builds cancels in every state and their
  targets are our orders by explicit reference (I4). A command of a guarded kind that carries
  no guard is refused (fail closed). The counters are shared atomics, so the check needs no
  registry and runs on any thread.
- **Lease names given again.** `Registry::with_lease_keys` advances the generation of every
  armed market whose held leases the new names cover differently, since what it admits changed;
  a command built before is then refused at submit.

## Alternatives

- Re-run the registry's `admits(market)` at submit: rejected. The session does not hold the
  registry (0057), and a shared lock across the decide pass and the write would serialise them.
- Refuse a stale command at issue as well: not taken. A refusal there would need a second path
  for the consumer to release what the build reserved (the PendingNew order, the amend in
  flight, PR #87 Reviewer B B6); one refusal point keeps one release path.
- Hold back cancels whose market changed state: rejected. The kill switch exists to stop
  orders, and holding back a cancel under it is the failure it guards against (0012).
- Guard the instrument cancel-all with the state generation as a place is: rejected (RB94-1).
  The kill switch and its lift change the generation but none of I7's inputs, so a cancel-all
  built just before the switch went on would be refused and our Open orders left resting
  until the next resync built it again.
- Advance the generation on every foreign order seen instead of a second counter: rejected. The
  generation counts state changes (0012), which no resync or fill makes, and a place does not
  reach orders not ours.

## Consequences

- FBC-j5bw calls `check_at_submit` in `ExecSession::send` at the point PR #87 marks, after the
  epoch and ready checks and before any nonce is reserved, and reports a refusal not sent.
- A cancel-all built and then refused at submit is built again by `Registry::cancel_everything`,
  which each resync asks for while the market is Killed; the explicit cancels go out meanwhile.
- `fbc-oms`'s authorization proof is `tests/authorization.rs`, through a recording gateway that
  runs the check as a live gateway does.

## What would show this was wrong

- A place, batch or amend built before a change of its market's state reaching a venue.
- A cancel or cancel-many held back by the kill switch or any other state.
- An instrument cancel-all held back by a change of its market's state that left the registry's
  hold of the exclusive lease and the orders not ours in view as they were.
- An instrument cancel-all reaching an order not ours that was in view before it was written,
  or written after the registry's hold of its market's exclusive lease changed since its
  build.
