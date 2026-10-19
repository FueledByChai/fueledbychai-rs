# 0107 — Orphans a resync finds are registered; the adding side is suppressed from the resync until the cooldown the consumer's cancel of them starts runs out; they are cancelled by explicit reference, escalating to an instrument cancel-all only under the kill switch's guard, augmenting 0005 and 0055

Status: accepted
Date: 2026-10-18

## Context

0005's I7 is the library side of the fix for the Hibachi orphan blowout: a reconnect storm
desynchronised the order map, modifies fell back to new orders, orphaned orders piled up and a
falling market swept the stack, six times past the position limit. I7 reads: when a trustworthy
resync finds own-namespace orders the OMS does not know, the adding side is suppressed for a
cooldown the consumer configures and the orphans are cancelled by explicit reference in a
cancel-many; above the level limit it escalates to an instrument cancel-all, only under an
exclusive account-and-instrument lease with no foreign-namespace orders visible, and otherwise
repeats explicit cancels on each resync; with a snapshot source that is not trustworthy it acts
only on orders two sources confirm. 0012 lets cancels through in every market state.

0055 already reports such orders (`ResyncReport::untracked`: open orders under our namespace on a
market a resync, or a hand seed, seeded before) and registers our open orders on a market not yet
seeded (an earlier run's, decision 0055), which are not I7's: the kill switch's cancel everything
reaches them (FBC-gzm). The fbc-oms OMS reads no clock, and its pre-trade path takes no time.
Paradex's snapshot is trustworthy (0094) from separate reads (0095), its `GET /orders` lagging the
stream by up to 3480 ms; a Java instance on a shared account cancels foreign orders every 3 s
(FBC-aoh5), so another namespace's order is never cancelled by us; fbc-oms never builds an
account-scope cancel-all.

Three things I7 leaves open are decided here: what the adding side is, when the cooldown runs,
and how an orphan is held while its cancel is out (FBC-840, BT-402).

## Decision

1. **Found and registered.** Every resync acts on each open order of `untracked` whose client id
   and venue id no order of ours holds: from a trustworthy snapshot source on every one; from any
   other only on one a live order event (not terminal) or a live fill under its client id showed
   while the registry did not hold it, and no terminal event withdrew since. The rest stay in
   `untracked`, still not held, and nothing is done with them. Acting registers the order as an
   order from the snapshot (as 0055 registers an earlier run's): never amended, counted against
   both pre-trade caps as it rests, and cancelled by the kill switch's cancel everything too. A
   later snapshot showing it open proves it was live after the seed read the account, so the
   seed holds a fill of it only when the fill arrived before the seed was requested or executed
   at or before the seed's watermark; every other fill of it, on either read mode (0095), moves
   the inventory (unless the seed showed it, under its venue id without our client id: what the
   seed showed then places its fills, as for an order the seed held, a fill naming only our
   client id included, so a fill the seeded position holds is not counted again and a later
   one is; Codex's r4316124604 and r4316366360) (Reviewer B's RB840-1 on PR #171: placed as the seed's own unshown order, its
   fills moved the record and not the inventory, or left a separate-reads market unsettled).
   The fills of it applied before it was registered, which a one-read seed held only because it
   did not show the order (0055's rule 3, not by their arrival or time), move the inventory as
   it is registered, once (Codex's r4310559340); a trustworthy resync not showing such an order
   open drops what was kept for it. A snapshot requested before a resync that already compared
   the market (its check is `PositionCheck::Stale`), or not after the seed's own request, may
   have been read before the seed, so it is no evidence on that market: nothing is registered
   there from it and nothing kept is dropped (Codex's r4310661297 on PR #171: a late answer
   showing an order the seed's position already held registered it and counted its fill
   twice). The orphans are acted on before each market's position is compared, so the resync
   that registers one compares the venue's position with the fills it counts (Codex's
   r4310661291). An order not ours
   (another namespace's, a non-canonical client id, or one nothing attributes) is never an orphan.
   A trustworthy resync forgets the live sightings. An order of ours the registry holds as
   ended Lost (the Unknown ladder ended it on snapshots that lagged it past the settle, 0094's
   up to 3480 ms) that a trustworthy, non-stale snapshot shows open, under a venue id no other
   order of ours holds, no amend of it replaced and the venue itself did not say ended (a
   terminal update naming it arrived after the ladder ended the order, or a cancel naming it was
   refused as already ended: the snapshot lags the venue, Reviewer B's RB840-11), is an orphan too: it is in `untracked` and
   registered again from the snapshot with the fills counted on it and the venue ids an amend
   replaced and the largest cumulative fill the venue reported (Codex's r4316124597), every
   other id the registry knows it by (its earlier current one, one a fill
   taught) now replaced by the one shown (Codex's r4315681141 and r4315910340: a delayed update
   naming only such an id would otherwise end it), so it counts on both caps, suppresses the
   adding side
   and is cancelled like any other (Reviewer B's RB840-8 on PR #171: held as ended, it counted
   on neither cap and nothing cancelled it). Its fills were routed to it while it was held, so
   none moves the inventory again. A source that is not trustworthy confirms none: a live event
   under its client id reaches the held record, not the sightings.
2. **The adding side.** A side adds when an order on it would grow the position's size: while a
   market's adding side is suppressed, a place, a batch item or an amend is built only on the side
   that reduces a known, non-zero position, and only when what the side may still move the
   position by, the order included, stays within the position's size (Exit's sizing in 0012,
   without its reduce-only requirement). A flat market, or one whose position is unknown, builds
   none. The refusal comes after the market's state and before Exit's admission and the caps
   (`OmsError::OrphanCooldown`, `AmendRefusal::OrphanCooldown`). Since the position sizes the
   build, it is judged again as the command is authorized (`IssueRefusal::OrphanCooldown`), and
   a command built while the cooldown lasts watches the position as an Exit command does
   (0066): it is refused at submit once a fill or a resync moved the inventory since its build
   (Reviewer B's RB840-2 on PR #171). Cancels are never suppressed.
3. **The cooldown.** A market is suppressed from the resync that finds orphans on it, and again
   from every resync while any orphan of it is not terminal (`ResyncReport::orphans` names
   those markets). The cooldown then waits (`Cooldown::Pending`) until the consumer's
   `Registry::cancel_orphans` at its `now` starts it, to `now` plus the consumer's cooldown or
   later if it already ran later (`Cooldown::Until`), and it ends only when the consumer's
   `Registry::end_orphan_cooldowns` is given a time at or past it. A suppression beginning, and
   any resync registering an orphan on the market, advances the market's state generation, so a
   place or amend authorized before it is refused at submit (0060; Codex's r4310559351 on PR
   #171: an orphan registered during a cooldown counts against what was authorized in it).
4. **The cancels.** `Registry::cancel_orphans` builds, in every market state, the cancels of the
   market's orphans that are not terminal by explicit reference (`Registry::cancel_many`), those
   with a cancel in flight included. When they number more than the consumer's level limit it
   builds an instrument cancel-all instead, only when the kill switch's cancel everything would
   (FBC-gzm): the venue has one, the registry holds the market's exclusive lease, a trustworthy
   resync has shown the account's open orders and no order not ours is in view on the market,
   which a snapshot's foreign order puts there; with it, the explicit cancels of our PendingNew
   and Unknown orders, which it can miss. Otherwise it says which condition failed and builds the
   explicit cancels. The consumer builds them again for each market a resync names, so the
   explicit cancels repeat on each resync until the orphans end. An order registered from a
   snapshot whose cancel the venue refuses as already ended goes on the Unknown ladder, since its
   terminal event may have come before it was registered: a trustworthy snapshot not showing it
   ends it Lost (Reviewer B's RB840-11 on PR #171; any other order's terminal event still settles
   such a refusal).
5. **The consumer's numbers.** `OrphanConfig` holds the cooldown and the level limit, with no
   defaults; zero is refused for either.

## Alternatives

- The adding side as the side the orphans rest on: rejected. An orphan on the side that reduces
  the position would then stop the orders that unwind it, and leave the side that grows the
  position free while the orphans are out. The repository already uses "adding side" for the
  side that grows the position (0052, 0065).
- Suppress only the side opposite the position's sign, letting a reducing order of any size
  through: rejected. A reducing order larger than the position crosses zero and adds on the
  other side.
- Start the cooldown at the resync's request time: rejected. The resync's answer can lag its
  request by seconds (3480 ms on Paradex), shortening the cooldown by that much, and the resync
  is applied without the consumer's time. A cooldown waiting for the consumer's call fails safe
  when the consumer never makes it.
- Cancel an orphan by the snapshot's references without registering it: rejected. It would
  count against no cap while it rests, which is the blowout's mechanism.
- Seed the registered orphan's fill count from the snapshot: rejected. A fill in flight would
  then count twice in the fill count and once in the inventory, and the order's exposure would
  read low. The fills before it was registered (which the inventory holds once it is registered)
  keep counting in its exposure instead: conservative.
- Act on an untrustworthy snapshot's orphan when a later snapshot shows it again: rejected. Two
  reads of the same stale source are not two sources.

## Consequences

- A consumer runs `cancel_orphans` for each market a resync's report names and
  `end_orphan_cooldowns` on its journaled timer; until it does, the adding side stays suppressed.
- An orphan with fills before it was registered counts those fills in its exposure for the
  inventory cap until the process restarts: the cap is tighter than it needs to be, never looser.
- A market whose orphans keep coming back (a lagging snapshot showing an order the venue already
  ended is held, never an orphan; an order the venue ended and a snapshot still shows open is
  cancelled again and resolved by the Unknown ladder) stays suppressed while they do.
- The evidence rule orders snapshots by their request, as the position check does: it relies on
  a session keeping one resync in flight (as the venue sessions do), so a snapshot requested
  after the seed's request was read after the seed's answer.
- With a Java instance on the account (FBC-aoh5), its orders are in view, so the escalation never
  happens there and the orphans go out as explicit cancels.

## What would show this was wrong

- A live session where orphans stay open past several resyncs while the explicit cancels go out,
  or one where suppression stops a market from unwinding a position the owner meant to reduce.
