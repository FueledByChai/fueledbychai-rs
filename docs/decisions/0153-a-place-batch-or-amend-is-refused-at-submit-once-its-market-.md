# 0153 — A place, batch or amend is refused at submit once its market's position moved since its authorization, in every state that builds one, augmenting 0060, 0066 and 0082

Status: accepted
Date: 2026-11-23

## Context

- **The window.** 0082 judges a place, a batch or an amend against the pre-trade caps (0052, 0005's
  I6) again when `Registry::authorize` issues its authorization, and not after. Fills can land
  between an authorization's issue and its write (0060), and the check at submit refuses a Quoting
  command only for a change of state or a trip to Cancel-only (0139). 0066 refuses an Exit command
  once its market's position revision moved since its build, and 0108 does the same for one built
  under an orphan cooldown. A Quoting place authorized and then held while a fill takes the position
  to the inventory cap is still written, breaching I6 by up to its own size (FBC-zux3). The fills
  that do it are those the side's counted orders did not hold: an own fill of an order the registry
  does not hold, a venue-initiated fill (0120), a late fill on an order the registry already ended.
  fbc-runtime sees the same window (FBC-madj, Codex r4216890742 on PR #114): a fill the session
  decodes after a submit and before its encode.
- **The question.** FBC-zux3 put three answers to the owner on 2026-11-17: (A) refuse at
  submit every place, batch or amend whose market's position revision moved since its
  authorization, 0066's watch in every state that builds them; (B) refuse only on a move the
  side's counted orders did not already hold, tracked by a second counter; (C) keep 0082's
  judgement at authorization and document the window.
- **The owner's answer.** On 2026-11-17, in the main session's chat, recorded on FBC-zux3, the
  owner chose "Refuse it, replan": any move of the market's position revision refuses a place,
  batch or amend not yet sent, as `StaleAuthorization::PositionMoved`, and the planner rebuilds
  on its next pass. That is A. As recorded on FBC-zux3, this record augments 0060, 0066 and
  0082 and states its cost: a fill during a pass refuses the rest of that pass's ladder.

## Decision

1. **Every place, batch and amend watches its market's position.** One built in Exit, or while
   an orphan cooldown suppresses its market's adding side, keeps the watch its build takes (0066,
   0108). Every other place, batch or amend, in Quoting, the one other state that builds them
   (0012), is watched from its authorization.
2. **The watch is taken in `Registry::authorize`.** It is read in the same call that judged the
   command against the caps again (0082), after that judgement, with nothing written to the
   inventory between. So a move before the authorization is judged there, with the registry in
   hand, and a move after it is refused at submit. The watch spans the authorization to the
   check at submit, which 0060 runs immediately before the encode.
3. **What refuses.** `Handed::check_at_submit` refuses the command once the market's position
   revision moved since the watch was taken: `StaleAuthorization::PositionMoved`, nothing
   written. The revision moves each time the inventory is written with a different value (0066):
   any fill of ours that moves it, tracked or not, a venue-initiated fill, a late fill, and a
   seed. A fill that moves no inventory of the market (another namespace's, one a seed already
   held, another market's) refuses nothing. The order of the check is unchanged: a trip to
   Cancel-only first (0139), then a change of state (0060), then the position.
4. **Unchanged.** A cancel and a cancel-many always pass, and the instrument cancel-all is
   checked against 0005's I7 guard alone (0060, 0013 rule 2). Exit's rule is 0066's as it was.
   The order-entry session reports the refusal `NotSent(StaleAuthorization)` with no nonce
   reserved and nothing written (0062); no new reason is added. The consumer settles it as any
   such outcome, and the execution planner places the level again on its next pass.

## Alternatives

- **(B) Refuse only a move the side's counted orders did not hold:** not taken; the owner chose
  A. It keeps a ladder going through our own quotes' fills, but needs each fill classified
  against the side's exposure and a second counter, and a misclassified fill lets the breach
  through.
- **(C) Document the window only:** not taken; the owner chose A. It leaves I6 breachable by a
  command's own size.
- **Take the watch at build, as 0066 does in Exit:** rejected. 0082 already judges a command
  built before a fill and authorized after it against the moved position, so a watch from the
  build would refuse commands the caps admit, such as a buy retained across a sell fill. Exit
  keeps its build watch: 0063 judges Exit's admission at build, and authorization judges again
  only the caps (0082 item 3) and an orphan cooldown's suppression (0108 item 2). A command
  built under an orphan cooldown is so judged again yet keeps 0108's build watch, which refuses
  more than this record needs; changing 0108's rule is outside this record. In a planner pass,
  which builds and authorizes each command with nothing between, the two places refuse the same
  commands.
- **Judge the caps again at submit:** rejected for 0060's reason. The session does not hold the
  registry, and the judgement needs every order on the side.

## Consequences

- **The cost the owner accepted.** A fill that moves a market's position refuses every place,
  batch and amend authorized on it and not yet checked at submit, on both sides, including one
  the fill made safer, and including after a fill of one of our own quotes. In the shard host's
  decide pass (0100), a fill a handler applies between two ladder commands refuses the rest of
  that pass's ladder; their cancels still go out. The planner builds them again on its next pass
  against the new position. Quoting trades the latency of that pass for never sending an order
  judged against a position a fill had already moved in the process.
- A fill the venue executes while our order is on its way to it is outside any check in the
  process, as 0066 says; once written, the order counts against the caps as it rests.
- `StaleAuthorization::PositionMoved` now also names a Quoting command; its text says the
  position moved since the command was judged against it.
- Proof:
  - `crates/fbc-oms/tests/quoting_submit.rs`, and the Quoting case of `tests/exit_submit.rs`;
  - `crates/fbc-runtime/tests/exec_position_moved.rs`, a fill decoded before the encode;
  - `crates/fbc-runtime/tests/shard_host.rs`, a fill between two ladder commands.

## What would show this was wrong

- A place, batch or amend authorized before a fill the registry had applied before the command
  was encoded reaching a venue.
- A command refused at submit for a move its authorization had already been judged against.
- A cancel held back by a move of the position.
- Quoting unable to keep its levels on a venue whose fills arrive so often that most of each
  pass's places and amends are refused before they are written; a new record would then weigh
  B again.
