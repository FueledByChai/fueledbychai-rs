# 0154 — A resync seeds no market on which its snapshot shows an open order without a client id that no order of ours had, and reports each, augmenting 0055 and 0108

Status: accepted
Date: 2026-11-23

## Context

- **The defect.** Reviewer B's RB80-1 on PR #80 (FBC-38r), deferred to FBC-5orj. A resync's
  snapshot can show an open order with no client id under a venue id no order the registry
  holds had: an earlier run's order the venue lists by venue id alone, or one placed in the
  venue's UI. 0055 registers only orders shown under our client id, and the pre-trade caps
  (0052, 0005's I6) count only orders the registry holds. Since FBC-gzm it enters the view of
  orders not ours, which only keeps an instrument cancel-all from being built (0005's I7); no
  resync report named it. The seed also placed a fill of ours naming its venue id by what the
  snapshot showed of it. With such a bid resting and the position flat, a bid of the whole
  inventory cap is admitted (Reviewer B's scratch test).
- **The exposure.** Paradex today, since 0094 made its snapshot trustworthy; QFEX once FBC-cnyh
  makes its snapshot trustworthy. The cancel-on-disconnect arm (0058, 0121) makes an earlier
  run's order rare. Whether Paradex lists an order placed in its UI with no client id is not
  confirmed: its decoder reads only an absent or empty client id as none.

## Decision

1. **Reported.** Every resync, from any source, names each open order its snapshot shows with
   no client id under a venue id no order the registry holds had, by instrument and venue id
   (`ResyncReport::without_cid`). One under a venue id the registry holds an order by is that
   order, and is not named.
2. **The seed refused.** A trustworthy resync seeds no market not seeded before it on which its
   snapshot shows such an order. The market's position stays unknown, so the owner's Start,
   Flatten and Wind-down are refused on it and nothing is built on it (0012, 0055). The other
   markets are seeded as 0055 says, and our orders it shows on the refused market are still
   registered, so a cancel reaches them.
3. **Seeded later.** A later trustworthy resync whose snapshot shows no such order on the
   market seeds it, placing every fill held since by 0055's rules.
4. **No fill placed against it.** No seed is taken from a snapshot showing such an order, so no
   fill of ours is placed against what it showed of that order. 0108's case of an orphan the
   seed showed under its venue id without our client id no longer arises from an open order
   with no client id: the later snapshot showing it under our client id finds the market
   unseeded and registers it as an earlier run's (0055). It still arises, as 0108 says, from an
   order the seed showed ended, or open under another namespace's or a non-canonical client
   id, whose market seeds as before.
5. **On a market seeded before, reported only.** A later resync of a seeded market that shows
   such an order names it and, as before, puts it in the view of orders not ours: the market
   stays seeded, its state and arming unchanged, and the caps still do not count the order.
6. **Never cancelled by the registry.** Such an order may be another system's (0005's I4), so
   fbc-oms builds no cancel of it. The owner ends it at the venue, as `without_cid` names it.

## Alternatives

- **Model it on the caps** (count it as resting on its side): not taken; FBC-5orj asks to refuse
  rather than model. The registry follows none of its order events and counts no fill of it,
  so the caps would count it at the snapshot's size until a later snapshot.
- **Ignore it**, as before: rejected. It leaves 0005's I6 breachable by the order's size on
  every market a resync seeds.
- **Register it as ours**: rejected. A venue id alone cannot tell an earlier run's order from
  another system's, and 0005's I4 forbids cancelling another system's order.
- **Make a seeded market's position unknown when such an order shows:** not taken. Nothing
  seeds an unsettled market again (0055), so one UI order would stop the market until the
  process ends.

## Consequences

- A market on which such an order rests is not seeded, so it does not trade, until the order
  ends and a trustworthy resync shows that. An owner who keeps orders resting from the venue's
  UI on a market the consumer quotes cannot start it.
- The residual: on a market seeded before, such an order (one placed in the UI mid-session,
  say) is counted by neither cap, so 0005's I6 can be breached by its size until it ends. The
  snapshot and its live order events put it only in the view of orders not ours, as they put
  an open order under another namespace's or a non-canonical client id on any market. A later
  ticket decides whether the registry refuses places and amends on a market while such an
  order is in that view, as this record refuses the seed.
- Proof: `crates/fbc-oms/tests/resync.rs` (the seed refused and reported, the other market
  seeded, a later resync seeding; an untrustworthy resync reporting; a fill naming such an
  order placed by the next seed; the residual; a fill placed by the venue id alone a seed
  showed) and `crates/fbc-oms/tests/orphans.rs` (0108's cases point 4 changes, and those it
  leaves, on a seed showing the order open under a non-canonical client id or ended).

## What would show this was wrong

- A market seeded from a snapshot that showed such an order open.
- An order of ours named in `without_cid`, or one under a venue id the registry holds.
- Markets kept unseeded so often by orders placed in the venue's UI that the owner cannot trade
  the markets the consumer quotes; a new record would then weigh modelling the order again.
