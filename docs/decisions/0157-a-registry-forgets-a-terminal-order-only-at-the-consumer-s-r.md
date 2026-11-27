# 0157 — A registry forgets a terminal order only at the consumer's retention pass, within the consumer's count, age and resync bounds, once neither it, its planner nor a command can still read the order and no answer can still name it, amending 0055, 0108, 0152 and 0154, augmenting 0084, 0109 and 0110

Status: accepted
Date: 2026-11-26

## Context

- **The growth (FBC-6pdo).** `Registry` keeps every order record and every entry of its
  venue-id index for its life. Nothing removes either, except 0108 item 1's re-registration,
  which replaces a Lost record. A quoting loop that requotes all day grows both. Every walk over
  a side's orders grows with them: the cap sums at each place, amend and authorization, the
  cancels due after each event, the ladder's pass and each resync.
- **What still reads a terminal record.**
  - The inventory cap, Exit's admission, the orphan suppression and the planner's room to reduce
    count a terminal order's fills that the venue reported and the ledger has not counted yet
    (0052's `OrderRecord::exposure`; 0063 rule 4, 0108 item 2, 0110 point 3).
  - A late fill under our client id is placed against the seed as a held order's fill is, when
    the registry holds the order. When it does not, 0055's rules for an earlier run's order
    place it: on separate reads the position becomes unknown, and on one read a fill of an order
    placed after the seed is held in the snapshot and never counted.
  - A fill or an update naming only a venue id needs the index. Without it, the event is
    unattributed and its id comes into view (0005's I7).
  - 0108 item 1 rebuilds a Lost order from its record.
  - The planner re-arms a crossing attempt only when it sees the attempt's order ended not sent
    (0110).
  - A seed's sets read every record on a market not yet seeded.
  - Read as bounding how long a trustworthy snapshot lags its stream in either direction, the
    venue's declared lag bound (0109) means that a snapshot requested at least that long after
    an order ended shows it ended; one requested earlier may show it open. It is applied only
    at its `ResyncEnd`, some time after its request. Applied while the record is held, it finds
    the ended record, so the order is not taken for an orphan.
  - A record removed while the index or the ladder's query index still names it causes a panic
    on the next event or pass.
- **What the registry cannot see.** No record says when the order ended. Ending a record discards
  its requests in flight and the ladder's unanswered ones, and a place carries no request id. So
  nothing says whether an answer may still name an ended order. A cancel or an amend built for
  an order that then ends holds nothing of it: 0152's holds cover only places and amend builds.
- **The other half.** FBC-mu68 already drops the glue's requests once they are answered.

## Decision

1. **One pass forgets.** `Registry::retain_ended(cfg, caps, now)` is the only call that
   forgets an order. Like every mutating call, it first applies the Cancel-only trips (0139) and
   releases what dropped commands and authorizations handed back (0084, 0152). No other call
   forgets:
   - not that release;
   - not the call that ends an order, because the glue reads the record right after it;
   - not a planner pass, because the planner reads its slots' records mid-pass.

   A registry whose consumer never calls the pass keeps every order, as before. `now` is the
   consumer's time on the shard's monotonic clock: fbc-oms reads no clock.
2. **The consumer's bound.** `RetentionConfig::new(max_ended, max_age, resync_within)` holds the
   consumer's count, age and resync bound. The resync bound is the longest time from a resync
   snapshot's `requested_at` to the `Registry::resync` call that applies it. The configuration
   has no `Default`, it refuses zero for each value, and it is passed to each pass as the
   ladder's configuration is.
   - Each pass stamps each terminal record it sees for the first time with its `now`. A
     record's age runs from that stamp, so an age is never longer than the time since the order
     ended.
   - Among the eligible records (item 3), each one at least `max_age` old is forgotten.
   - Then, while more than `max_ended` remain, the oldest by stamp, then by client id, is
     forgotten, whether or not it is younger than `max_age`.

   After a pass, the registry holds at most `max_ended` eligible records, and the pass counts
   the records the count forgot before `max_age`.
3. **What is eligible.** A terminal record is eligible when its stamp is at least the venue's
   snapshot lag bound (0109) plus the consumer's resync bound before `now`, and all of these
   hold:
   - **a.** It did not end Lost.
   - **b.** It did not end Canceled through an accepted tombstone (the Unknown ladder's cancel by
     client id). Such an end carries no final cumulative fill from the venue, so c cannot show
     that the order owes none, and a fill that raced the cancel (0080) may name only a venue id
     that its record keeps.
   - **c.** It owes no fill: the venue's cumulative fill on it is no more than its counted fills.
     Its exposure, its resting quantity and its uncounted fills are then all zero, so forgetting
     it changes no sum that the caps, Exit, the orphan suppression or the planner read.
   - **d.** No answer may still name it. Any of these pins it:
     - its place was authorized, was not released unsent, and no outcome of the place has come;
     - an amend, a cancel or a tombstone of it was reported sent, and no outcome names that
       request yet;
     - an order-affecting command fbc-oms built that names it (a place, a batch item, an
       amend, a cancel, a cancel-many item or a tombstone) still exists, as a permitted
       command, an authorization or a handed authorization.

     Since the order has ended, any outcome of a request counts as that request's answer here.
     An instrument cancel-all names no order and pins none. The Unknown ladder's queries pin
     none either, because an ended order has left the ladder, so the answer or the outcome of
     its query applies nothing. The handle covers an amend, a cancel or a tombstone until the
     gateway reports it, and a place is pinned from its authorization. The consumer therefore
     records each such report (`amend_sent`, `cancel_sent` or `tombstone_sent`, or the not-sent
     outcome) before its next pass. For an `ExecSession` it records it inside
     `ExecHandler::on_submitted`; with `OrderGateway::submit`, right after the call returns.
   - **e.** No slot of the execution planner holds it. Each slot carries a handle on its order
     from the slot's place until the planner frees the slot, which it does once it sees the
     order terminal, or until the planner is dropped. Only the planner reads its slots, and
     0110's re-arm reads the order only through its slot.
   - **f.** Its market is seeded (by a resync or by hand) or unsettled.
   - **g.** On a market a resync seeded, it was built here, or the seed holds it as held then or
     as an orphan found after. Any other order there, one `insert` registered after the seed,
     has its fills counted after the seed only while its record is held, and an inserted client
     id need not come from this registry's mint, so item 5's span cannot cover it.
   - **h.** No other record the registry holds knows a venue id that the index maps to this one.
     Nor does this one know a venue id that the index gives to another order the registry holds
     that has not ended, that ended Lost, or that is still among its market's orphans. An
     update's venue ids, an amend's new id included, are indexed only where no other order
     holds them, and an update routed by our client id is not checked against the index, so a
     record may know an id that the index gives to another order. Forgetting that other order
     would leave the id routed to no order. Forgetting the record that knows the id would route
     a later update or snapshot row under its client id naming that id to the other order,
     because the index is read whenever the client id is not held. That would end the other
     order while it rests, apply the row's total and fill to it, count a stale snapshot's fill
     on an orphan, or mark a Lost order's id ended so that 0108 item 1 never registers it
     again. Once that other order has ended otherwise, such an event changes nothing there.

   With the resync bound in the floor, every snapshot applied after a pass was requested no
   earlier than `now` less that bound. A trustworthy snapshot lagging within the venue's bound
   from its own request therefore shows the order ended, even when a pass falls between its
   request and its `ResyncEnd`. Where the snapshot source is not trustworthy, nothing bounds a
   snapshot's lag: 0109 reads the declared bound only from a trustworthy source (QFEX, which is
   untrustworthy, declares zero), so the floor there cannot make a snapshot show the order
   ended. Such a snapshot seeds nothing, and it registers an order it shows open as an orphan
   only after a live sighting: an open or amended update under the order's client id, or a
   fill of the order outside the built span (item 6). An orphan registered this way counts on
   both caps and is cancelled, which fails closed.

   A record that fails any of a to h is pinned. It is outside the bound and is counted by reason
   in the pass's report. A record within the floor is settling, and is also outside the bound. A
   record that is not terminal is never forgotten.
4. **Forgotten whole.** The record goes in one step, together with every index entry that names
   it: each venue id the index maps to it (ids that fills taught included), each ladder query
   entry naming it, and its orphan entry. No index ever names a record that is gone.
5. **What stays.** Nothing is kept per order. For each namespace, the registry keeps two spans,
   each the lowest and the highest sequence among a set of forgotten orders:
   - the built span, over the forgotten orders it built by `place` or `place_batch`;
   - the answer span, over every order it forgot.

   An order it built stays built here when 0108 item 1 registers it again from a snapshot. The
   replacement record keeps the old record's owed answers and the handle that its commands and
   its planner slot hold.
6. **A late event naming a forgotten order.**
   - **A fill under its client id** is placed against its market's seed as its held record's
     fill would be (0055):
     - For an order this registry built, the built span says that it was held. The order was
       placed after its market's seed, so the fill comes after it, unless a later resync
       registered the order as an orphan, whose fills the seed's rules for a later orphan then
       place.
     - On a market a resync seeded, any other forgotten order is among the seed's held orders
       or its later orphans (item 3g pins the one that is not), and there a fill is placed the
       same way whether the registry holds the order or not.
     - On a market seeded by hand or unsettled, a fill counts after the seed either way.

     Placed after the seed, the fill moves the inventory and is reported as
     `FillRouted::OursUntracked`. Otherwise it is reported as its placement says (`InSnapshot`
     or `Unsettled`). A fill within the built span records no live sighting; a fill of any
     other forgotten order records one, as for an order never held. The fill differs from a
     held record's in three ways:
     - **What it is checked against.** It is not checked against the order's instrument and
       side, which are gone, so a fill the held record would flag as `Disagrees` is counted.
       Nor, once the pass has also forgotten another order that the index gave one of its venue
       ids to (item 3h), is it checked against that order, so a fill naming that id that the
       held record would flag as `Conflicting` is counted too.
     - **Its cumulative fill.** The cumulative fill it reports is not kept. Pin c held at the
       forget, so a late fill reports a cumulative past the fills counted only when what ended
       the order stated fewer fills than it had. The held record would then owe the fills in
       between (`cum_venue` past `cum_fills`), which the inventory cap, Exit's admission and the
       planner's room to reduce count until they arrive. With the record gone, nothing counts
       them until they arrive.
     - **A venue id taught since.** Once an event of a held order has taught the registry one
       of its venue ids since the pass, a fill under its client id naming that id is
       `Conflicting` and not counted (item 11).
   - **An outcome or a sent report naming any client id between the answer span's ends, one the
     registry held or not,** gets a terminal record's answer (unchanged, false, or recorded as
     nothing). The one difference: a second send time or nonce that differs from one recorded
     before is not refused as `SentRecorded` or `NonceRecorded`, since nothing of the record is
     kept. It is not refused as an unknown client id. The span runs from the lowest seq the
     registry forgot to the highest, and the lowest may be an earlier run's order that a resync
     registered. So once a pass forgets such an order and one this run built, `UnknownCid` is
     left only for a client id outside the span. Only `on_outcome`, `amend_sent`,
     `cancel_sent`, `tombstone_sent`, `placement_sent`, `placement_nonce_used` and `query_sent`
     read the answer span. A fill and a snapshot row never do.
   - **Every other event** is handled as for an order the registry never held:
     - an update or a snapshot row under our client id naming a venue id that the index gave to
       another order when the pass forgot this one reaches that order by the id while the
       registry holds it. Item 3h forgets the order only once that other order has ended, not
       Lost and no orphan, so the event changes nothing there, and the row is not listed in
       `untracked`. Once a later pass forgets that other order too, the id leaves the index,
       and the event is handled as one under a client id the registry never held: an update is
       untracked, and an open or amended one records a live sighting; a row is handled as the
       snapshot bullets below say. Once an event of a held order teaches the registry the id,
       or a snapshot registers an orphan under it, the event reaches that order instead: a
       terminal update ends it while it may rest, and a row applies to it (item 11);
     - a fill naming a venue id alone is unattributed, and its id comes into view;
     - an update naming a venue id alone is untracked, and an open or amended one puts the order
       in view, so the market's cancel everything stays explicit;
     - either of these reaches a held order instead once an event of that order has taught the
       registry the id since the pass (item 11);
     - a snapshot row showing the order open on a seeded or unsettled market is listed in
       `untracked`, and from a trustworthy source not stale for its market it is an orphan
       (0108; from another source, only after a live sighting, item 3);
     - a snapshot row showing it ended is listed in `untracked` and not acted on;
     - a snapshot row with no client id showing it open is named in `without_cid` (0154) and
       put in view, and one showing it ended is not acted on.
7. **Client ids.** The registry refuses, as `OmsError::DuplicateCid`, a place, a batch item or
   an `insert` under a client id it holds, or under one within its namespace's built span. No
   order it built and forgot is therefore placed again under its id. A snapshot's registration
   of an orphan is not refused. The mint's sequence only rises, so no fresh id lies within the
   span. A client id the registry never registered that lies within the span is refused too:
   one whose place a cap, a state or a cooldown refused before registration, retried after
   later ids were built and forgotten, is refused here where it was admitted before, so a
   consumer mints afresh for each place, as the planner does. The built span is sound only while
   every client id of its namespace between its ends that reaches this registry's events was
   registered by this registry. A consumer whose mint serves several registries whose sessions
   see the same account's orders does not run the pass on them. Outside the span, a forgotten
   client id stays unique through the mint and the namespace lease alone (0013).
8. **Where the consumer runs it.**
   - **When.** The consumer runs the pass on a journaled timer, so that a closed-loop replay
     forgets the same orders at the same passes, and after reading the notes of the events
     before it. A decoder replay of an order-entry journal (`ExecReplay`) calls no
     `on_submitted`, issues no authorization and passes over the consumer's own records, so
     what a pass would forget there need not match live.
   - **fbc-live.** `Link::retain_ended` forwards the pass with the link's caps. The link's
     snapshots take the receive time of the socket's authentication as their `requested_at`,
     which is no later than the session's ask for the resync. Its consumer therefore sets the
     resync bound above the sum of three times:
     - the rest of the turn that took the authentication, up to that ask: the handler's work on
       that input; the codec's frames and the arm the session writes, and the frames the
       effects below add, each write bounded by the session's `WriteStall` window; the HTTP
       results and timer firings the session takes while one of those writes waits, which
       reach the codec and the handler; and, on a shard host, the host's other work while a
       write waits (its other members' turns and the consumer's decide pass), which can hold the
       session past that window;
     - the session's `ResyncDeadline` (FBC-nwpr);
     - how long the turn under way when the deadline passes may still run before its check,
       which follows its input, its effects and the commands waiting, with the HTTP results and
       timer firings it takes while one of those writes waits. A `ResyncEnd` it hands on before
       the check, from its input or from one of those results, ends the resync and keeps the
       epoch open, and only a turn that reaches its check without one ends the epoch. On a
       shard host, that turn can also wait on the host's other work.

     A bound set short may register a forgotten order as an orphan (the lagging-snapshot
     residual).
   - **What fbc-live notes.** The link notes a late event naming a forgotten order as it notes
     one of an order never held. An update under our client id is noted `Note::Orphan`, unless
     its venue id routes it to an order the registry holds (items 6 and 11), when it is noted
     `Note::Order(Routed::Ours(..))`. An update naming a venue id alone is noted
     `Note::Order(Routed::Untracked)`, unless that id routes it to an order the registry holds
     in the same way. A fill counted under the built span is noted as unexplained.
   - **The samples.** The owner-run samples do not run the pass.
9. **What this amends.**
   - **0055:** a fill of an order this registry built and forgot is placed as a held order's fill
     is (item 6), not by the rules for an order the registry never held.
   - **0108:** a snapshot showing open an order the registry forgot registers it as an orphan
     (item 6). 0108's consequence that a lagging snapshot showing an order the venue already
     ended is held, never an orphan, now holds only for an order the registry still holds.
   - **0152:** every command fbc-oms builds that names an order carries a handle on each order
     it names, and so do its authorization and its handed form. That includes a cancel, which
     holds nothing. While a handle lives, the order is pinned (item 3d), and an authorization
     grows by those handles beside its hold and its registry's instance.
   - **0154:** a forgotten order of ours that a snapshot shows open with no client id is named
     in `without_cid` (item 6). 0154's second falsifier, an order of ours named there, now
     excludes a forgotten order.
10. **What this augments.**
    - **0084:** a voided order is pinned only while its command lives (item 3d). After that it is
      forgotten like any other order that ended not sent. Once it is forgotten, a trustworthy
      snapshot showing it open registers it as an orphan (item 6).
    - **0109:** its bound is now also read as bounding how long a trustworthy snapshot may still
      show open an order its stream ended, so a trustworthy venue's declaration must cover that
      direction too. Paradex's does: FBC-8xr's comparisons waited until every order the stream
      showed ended was absent from its open-order snapshot. Under any snapshot source, the bound
      floors, together with the consumer's resync bound, the age at which an ended record may be
      forgotten (item 3).
    - **0110:** a planner slot pins its order through a handle on it, until the planner frees
      the slot or is dropped (item 3e).

    Unchanged: 0005's rule that an Unknown order is never resent, since an order that is not
    terminal is never forgotten, and its I2, since no record leaves its terminal state (a
    forgotten order that a snapshot shows open becomes a new record).
11. **Left out.**
    - A residue per forgotten order (instrument, side, venue ids) that would keep a late event
      naming a venue id alone ours.
    - The guard a forgotten order's venue ids gave the other orders (FBC-k2w9). Once they leave
      the index, a query answer, a snapshot row, a fill or an order update under a held order's
      client id that names one of them applies to the held order. The fill or the update then
      indexes that id to the held order, so a late event naming that id alone, or an update
      naming it under the client id of an order the registry forgot, reaches the held order,
      and a terminal one ends it while it may rest. A late fill under the forgotten order's own
      client id naming that id is then `Conflicting` and not counted, where its held record
      would have counted it, and a later resync's check reports the desync and leaves the
      inventory as it is. A residue per order would restore the guard.
    - The growth of the pinned kinds.
    - The other sets that grow: the seed's sets, the view of orders not ours, the orphan
      sightings, and the seed's fill log until a resync folds it.
    - Per-side indexes for the walks.
    - The fill ledger's retention.

## What the owner decides

Approving this text (0149) approves items 1 to 11, including five choices:

- **Late fill (item 6).** A late fill under the client id of an order this registry built and
  forgot is placed as its held record's fill would be, not by 0055's rules for an order never
  held, with item 6's three differences: it is not checked against the instrument, the side or
  a venue id another forgotten order shared, it keeps no cumulative fill it reports, and a venue
  id a held order has learnt since makes it `Conflicting`.
- **The bound's reach (items 2 and 3).** "At most the bound" holds over eligible records only,
  and each pass reports the pinned and settling records outside it by reason.
- **Count over age (item 2).** The count may forget a record younger than `max_age`, so the
  bound is hard.
- **The resync bound (item 2).** The settling floor takes a third consumer number, rather than
  every pass waiting while a resync is in flight.
- **No residue now (item 11).** A residue per forgotten order waits for a later ticket, opened
  if live sessions show late events naming one by venue id alone.

## Alternatives

- **Forget by age or count alone, with no pins:** rejected. It would free cap headroom that a
  terminal order still owes, lose 0108's carry-over and 0110's re-arm, break a seed's sets, and
  panic on a dangling index.
- **A compact residue per forgotten order**, answering late events as its record would: not
  taken now. It is a second structure with its own bound, and a dozen lookups would have to
  consult it. The consumer's age gives the same window at the cost of the full records.
- **The consumer passing the client ids to keep**, from the glue and the planner: rejected. A
  consumer that calls the registry directly would lose the pins, and fbc-oms can know them
  itself.
- **A tally of commands built, decremented as each is settled:** rejected. Every settle path
  would have to decrement exactly once, and a gateway that reports twice would free an order
  early. A handle's life is structural instead.
- **Keeping a request owed until its final answer:** rejected. Paradex answers a cancel only
  provisionally, so every order it cancels would be pinned forever.
- **Reading a client id in the span that a snapshot shows open as ended:** rejected. If the
  record was wrong, an order resting at the venue would count on no cap. Registering it as an
  orphan fails closed instead.
- **Forgetting only after a trustworthy resync requested past the lag bound:** rejected. A
  session on a steady connection may never resync again, and then nothing would ever be
  forgotten.
- **Re-pointing a shared venue id to its holder:** rejected. A late terminal update of the
  forgotten order would end the holder while it rests.
- **A late fill under a forgotten order's client id placed by 0055's rules for an order never
  held:** rejected. On separate reads it leaves the market's position unknown for the rest of
  the process, failing closed but stopping the market over an anomaly. On one read it holds the
  fill in the snapshot and never counts it, failing open until a later resync's check reports a
  desync.
- **`max_age` as a floor the count never crosses:** rejected. It would keep the whole late
  window on a venue whose events need not echo our client id (QFEX's caps declare so), but the
  count could then be exceeded.
- **No resync bound, every pass waiting while a resync is in flight:** rejected. fbc-live's link
  would run no pass from an epoch's authentication to its `ResyncEnd` or its end, and every
  consumer calling the registry directly would have to do the same. It would need no sizing in
  the link. The number needs item 8's sizing instead, and a consumer can set it short, which
  fails closed, as an orphan.

## Consequences

- **The bound on growth.** The records, and the walks over them, are bounded by the live
  orders, `max_ended`, the settling records and the pinned records.
- **The pinned kinds** grow only with rare events, and each pass reports them. Beside item 3's
  a, b, f, g and h, they are:
  - orphans whose fills from before their registration never come (0108);
  - orders whose fills the venue reported, on an order event or a snapshot, never count: a fill
    the ledger refused for its time (`Untimed`, `BeyondHorizon`), one routed `Disagrees` or
    `Conflicting`, or one lost in a connection gap that no replay recovers;
  - orders whose request a stopped session never answered;
  - orders whose amend, cancel or tombstone a running session reported sent and never wrote,
    because the connection's epoch ended before the session reached its frame, so that no
    answer and no deadline's `Unknown` ever names it;
  - the slots of a planner that is kept but no longer runs on their market;
  - under h, two ended orders that each know a venue id the index gives to the other, which
    stay held for the registry's life, and an ended order that knows a venue id the index gives
    to another order, for as long as that order is live, Lost or an orphan the registry holds.
- **A tombstone-ended record that a snapshot shows open** is still not acted on, as before this
  record: 0108 item 1 re-registers only a Lost one. A later ticket may extend it.
- **The residual: a late fill by venue id alone.** Once a record is gone, such a fill is
  unattributed. It is not counted, and a resync's check reports the desync and leaves the
  inventory as it is. Its id stays in view until an event ends it, so the kill switch's cancel
  everything stays explicit on that market. A venue whose events need not echo our client id
  meets this path; QFEX's caps declare so.
- **The residual: a lagging snapshot.** A snapshot that lags past the venue's bound, or is
  applied later than the resync bound after its request, may show a forgotten order open.
  Unless its venue id now routes to a held order, which then takes the row as its own (item
  11), the resync lists the order in `untracked`. Unless the snapshot is stale for that market,
  it then registers the order as an orphan, from a trustworthy source, or from an untrustworthy
  one only after a live sighting (item 3):
  - The orphan's record takes the snapshot's cumulative fill as reported and not counted. The
    order's fills, already on the inventory, then count again in the inventory cap's worst case.
  - A late fill is counted twice when its order is one a resync registered from an earlier
    snapshot before its market's seed, and that seed did not show it. On one read, the seed
    placed the fill in the snapshot. At the orphan's registration, that fill is placed again as
    after the seed and moves the inventory, so the position itself is wrong by it, which that
    resync's check reports as a desync.
  - The record, owing those fills, stays pinned (item 3c).
  - Its market's adding side is suppressed, and the orphan is cancelled, goes on the ladder and
    ends Lost.
- **What the consumer sizes.** `max_age` should cover the venue's late window, and the resync
  bound should cover every resync the consumer applies (item 8 sizes it for fbc-live's link).
  The count can forget a record earlier than `max_age`, which narrows that window, and the pass
  counts each such record.
- **Readers of ended records.**
  - fbc-live's stream check keeps working, since an order behind on its fills is pinned
    (item 3c).
  - A consumer that stops on a late event of an order never held, as testnet_quote does, now
    also stops on one the registry forgot whose venue id routes to no order it holds (items 6
    and 8).
  - fbc-live's `on_unprotected` queries each order the session says rests unprotected that the
    registry holds ended or not at all, with the order's placement nonce where the registry
    holds one. A forgotten order that the resync did not register as an orphan is queried with
    none. On a venue whose queries name orders by placement nonce alone (none does today), no
    query is sent, and the session keeps that market's places held until an order event or a
    query's answer shows the order ended, which fails closed.
  - A consumer that reads ended records later should call the pass after reading them, or not
    call it at all.
  - `Registry::get` gives none for a forgotten order, and the audit sink's verdict for a late
    update is the one an order never held gets (item 6).
- **The cost.**
  - Each command carries one handle per order it names, and each planner slot carries one.
  - A record grows by its stamp, its pins and its handle.
  - Each pass walks the records and the index once.
- **Proof:**
  - `crates/fbc-oms/tests/retention.rs`;
  - `crates/fbc-oms/tests/crossing.rs`;
  - `crates/fbc-oms/tests/ui_retention/`;
  - `crates/fbc-live/tests/retention.rs`;
  - `crates/fbc-live/tests/replay_clock.rs`.

## What would show this was wrong

- More than `max_ended` eligible records held after a pass, or a forgotten record that was not
  terminal, was pinned or was settling.
- A number that a cap, Exit, the orphan suppression or the planner reads differing between a
  registry that runs the pass and a twin that does not, compared after the same mutating calls,
  so both have applied the same releases and trips. Both are fed the same events, among them
  resyncs requested before a pass and applied after it within the resync bound. The only late
  events naming a forgotten order are snapshots that lag within the venue's bound from their
  own request.
- A panic on an event naming a forgotten order, or a late fill under a forgotten order's client
  id placed differently from how its held record's fill would be, other than in the three ways
  item 6 names.
- Unattributed fills or orphans naming forgotten orders seen in live sessions. A new record would
  then take up the residue per order.
