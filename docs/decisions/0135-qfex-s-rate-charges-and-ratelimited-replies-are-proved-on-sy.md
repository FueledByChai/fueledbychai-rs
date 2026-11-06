# 0135 — QFEX's rate charges and RateLimited replies are proved on synthetic evidence with no provoked limit, a RateLimited a later session meets is recorded as provider evidence, and the order-entry umbrella closes superseded by the production admission record, augmenting 0123, 0126 and 0128

Status: accepted
Date: 2026-11-04

## Context

FBC-lqa8 is the QFEX order-entry umbrella. Its done line asks for three things about rate
limits:

- the private codec integrates FBC-ywjb's `RateLimited` effect and echoed-frame redaction;
- its scaled general and cancel charges bind to the shared provider-user limiter from
  FBC-bg3p;
- "synthetic and provider fault evidence verify this end-to-end".

The records do not name that evidence:

- 0123:35-36 keeps the QFEX order-entry adapter at `Readiness::Record` "until its charges,
  `RateLimited` replies and acceptance are proved".
- 0128:49-51 says the private codec "still must charge actual wire messages in scaled units,
  handle `RateLimited` replies", and pass provider acceptance gates. 0126:35-36 also ties
  promotion to provider acceptance gates.
- 0132:16-17 says FBC-lqa8 needs provider evidence that only a session that places can gather.
- FBC-cnyh's record, which admits QFEX order entry on production, is to name evidence for the
  signer, outcomes and settlement, cancel-on-disconnect, the resync and the conformance suite.
  It names none for charges or `RateLimited`, yet it states the readiness ceiling QFEX
  declares, and any trading ceiling is a promotion that 0123:35-36 gates.

The provider half may never exist. The only ticket that places on QFEX, FBC-9sk2, provokes no
rate limit without QFEX's written consent. That is this project's own caution.
[QFEX's client agreement](https://docs.qfex.com/legal/client-agreement), section 10
(Prohibited Uses), bars message flooding and excessive order submissions and cancellations,
and says nothing of testing rate limits. The [rate page](https://docs.qfex.com/websocket/rate)
describes no test environment or procedure. No ticket asks QFEX for consent, and 0132's report
list has no rate-limit item.

The synthetic half is in pieces:

- Three landed tickets hold the limiter side. FBC-bg3p declares QFEX's two private buckets,
  FBC-e5bd shares them across a user's accounts, and FBC-ywjb classifies a refusal and holds
  the shared bucket.
- The codec's own tests are FBC-zeb0's and FBC-blcw's, both landed, and FBC-jxau's, still
  open.
- None of them runs the codec inside an `ExecSession`. FBC-ywjb's tests run its classifier,
  the limiter and the session core, and its done line says "End-to-end private codec wiring
  remains in FBC-lqa8". FBC-e5bd's and FBC-bg3p's run limiters alone. FBC-zeb0's and
  FBC-jxau's construct the codec and read its effects, and FBC-blcw's encode commands.
- Only FBC-blcw's tests charge commands: each place, `cancel_order` (the cancel bucket) and
  `get_order`. None charges the cancel-on-disconnect arm, an authentication or subscription
  frame, or a page.

lqa8 lands no commit of its own (item 4), so its "end-to-end" clause and these charges need a
carrier. Item 2 names one.

0134, landed on 2026-11-04, settles what an echoing err or ack keeps in clear. It narrows
0126's whole-frame redaction:

- an err keeps its envelope, a documented `error_code`, an echo `type` the codec maps to a
  bucket, and a `message` in one of the two documented hint forms (0134 item 3);
- an echo of authentication, a frame with a JSON escape or a repeated key, and a malformed
  frame stay redacted whole (0134 item 2), and such a frame holds no budget (0134 item 6).

FBC-lqa8 also cannot close as its done line is written:

- A ticket is done only when a commit whose subject starts with its id is on main. Each slice
  of lqa8's work lands under its own id. Otherwise a ticket leaves the queue only as dropped
  (kit decision 0030).
- Its done line ends "A new decision records any promotion beyond Readiness::Record". That
  record is FBC-cnyh's, and lqa8 does not wait for FBC-cnyh.
- Its description still promises modify outcomes and request/reply matching. 0132 narrowed
  both: 0132:24 refuses native modify until a capture settles its quantity semantics, and
  0132 item 8 settles a reply only against the request it names.
- Draft PR #196 must never merge. All twelve of its commits start "FBC-lqa8:", so merging it
  would mark lqa8 done, and it carries merge commit 69d51ce3, which this repository's
  rebase-only merge refuses.

### The owner's choice

FBC-sjht asks the owner three questions:

- (a) the evidence: either QFEX's written consent to provoke a rate limit on pre-production,
  with an owner-run ticket that gathers one, or the synthetic evidence above, with a follow-up
  ticket that records provider evidence when it occurs;
- (b) the close: (i) lqa8 lands its own non-empty commit, (ii) once FBC-cnyh lands, lqa8's
  holder or the owner runs `bd supersede FBC-lqa8 --with FBC-cnyh`, or (iii) a
  `loop:dropped` label with the owner's reason;
- (c) PR #196 and its branch.

On 2026-11-04, in chat, the sprint's coordinator proposed to "accept the synthetic rate-limit
tests as the evidence and log any real rate-limit error we hit later, rather than ask QFEX for
consent to trigger one; FBC-lqa8 would then close by being marked superseded by FBC-cnyh once
that lands". The owner answered: "Yes, that plan". That takes (a)'s second path and (b)'s
option (ii).

The owner had already decided (c) that day, as FBC-lqa8's comment of 2026-11-04 records: "draft
PR #196 closed as superseded (https://github.com/FueledByChai/fueledbychai-rs/pull/196), with
a comment naming the slices that carry its work: FBC-77aa (sign/auth), FBC-zeb0 (codec core),
FBC-blcw (order encoder). Branch ticket/FBC-lqa8 is kept at 39d4e09f as the source those
slices reuse and is released once FBC-blcw lands." That follows the owner's direction of
2026-10-29, in lqa8's comment of that date, that #196 closes as superseded once replaced.

The owner's words leave two points open, and the items that settle them say so:

- whether the rate-limit bar gates FBC-cnyh's production admission or only a readiness ceiling
  above `Readiness::Record`, which the owner confirmed on 2026-11-06 (item 3);
- whether the synthetic tests the slices plan cover lqa8's "end-to-end" and every charge, or
  need one more synthetic test, the carrier above (item 2).

## Decision

1. **No provoked limit.** QFEX's written consent to provoke a rate limit on pre-production is
   not sought. No ticket gathers a provoked `RateLimited`, so no follow-up ticket for a
   consented capture is filed. FBC-l931's and FBC-9sk2's runs provoke none, and 9sk2's rule
   that it provokes no rate limit without QFEX's written consent becomes a rule that it
   provokes none.
2. **Synthetic evidence proves the charges and the `RateLimited` replies.** For FBC-lqa8's done
   line, for 0123:35-36's "charges" and "`RateLimited` replies", and for 0128:49-51's charges
   and replies, the evidence is these tests. Each proves its part once its ticket's commit is
   on main.
   - **FBC-bg3p** (landed), `crates/venues/fbc-venue-qfex/tests/rate_topology.rs`,
     `private_buckets_share_across_accounts_but_not_provider_users`:
     - QFEX declares no public limit and two private user-scoped buckets, general and cancel,
       each 120,000 tenths per 60 seconds, with `Control` in both;
     - a limiter without a provider-user key is refused;
     - a primary and a subaccount limiter of one `RateUserKey` share the buckets, Safety
       traffic keeps its reserve, another user's budget stays apart, and a different reserve
       for the same key fails.
   - **FBC-e5bd** (landed), `crates/fbc-runtime/src/ratelimit.rs`,
     `one_provider_user_budget_covers_primary_and_subaccounts_across_threads`:
     - limiters built apart for a primary account and two subaccounts under one `RateUserKey`
       charge one general and one cancel budget, across threads;
     - the safety reserve still applies, and another user's budget stays apart;
     - a `RateLimited` reply reaches every account's counters.
   - **FBC-ywjb** (landed). Its classifier tests in
     `crates/venues/fbc-venue-qfex/src/rate.rs` were replaced when FBC-zeb0 rewrote that file:
     FBC-zeb0's bullet below cites what proves the classifier, the cross-account hold and the
     whole-frame redaction now. Its own tests that stand are fbc-runtime's:
     - in `crates/fbc-runtime/src/ratelimit.rs`,
       `qfex_provider_refusal_holds_the_shared_bucket_without_spending_cancel_reserve` and
       `unknown_provider_echo_holds_frames_even_without_a_matching_bucket`: a refusal holds
       the shared bucket for its hint and leaves the cancel bucket free, and an absent or
       unmatched echo holds every frame budget for at least the declared window;
     - in `crates/fbc-runtime/src/session_core.rs`,
       `provider_rate_effect_updates_the_shared_budget_without_writing_or_answering`: the
       session executes `Effect::RateLimited` as a hold alone, writing and answering nothing.
   - **FBC-zeb0** (landed), in `crates/venues/fbc-venue-qfex/src/rate.rs` as it rewrote it,
     `each_mapped_type_has_its_bucket_and_nothing_else_does`,
     `only_the_two_documented_forms_give_a_hint` and
     `an_echo_is_found_literally_or_behind_ascii_escapes` (only an exact echoed operation
     selects its bucket, a retry hint is read only in its two documented forms, and an echo
     is found literally or behind ASCII escapes), and in
     `crates/venues/fbc-venue-qfex/tests/private_codec.rs`:
     - `every_documented_rate_refusal_holds_the_budget_and_settles_or_reconnects_nothing`: a
       rate-limit frame in either documented envelope, and a `RATE_LIMITED` order status, each
       give `Effect::RateLimited` with the documented retry hint and no settlement or
       reconnect;
     - `a_hold_reaches_the_budget_the_primary_and_its_subaccounts_share`: a hold one account
       meets reaches every account of the provider user;
     - `a_frame_kept_whole_answers_live_as_its_blanked_bytes_answer_in_replay` and
       `an_auth_failure_a_missing_auth_reply_and_a_malformed_frame_each_end_their_own_epoch_only`:
       the frames 0134 item 2 keeps whole (an echo of authentication, a frame with a JSON
       escape, a malformed frame) answer live as their blanked bytes answer in replay;
     - its spans fall where 0134 items 3 and 4 place them, and replaying the redacted journal
       gives the same effects (0134's Consequences).
   - **FBC-blcw** (landed), `crates/venues/fbc-venue-qfex/tests/private_orders.rs`:
     `each_supported_place_encodes_the_documented_add_order_frame_for_the_selected_account_with_its_charge`
     (each supported place, GTC, ALO and IOC, with its charge),
     `cancel_and_get_order_by_venue_uuid_encode_exactly` (`cancel_order` charged to the cancel
     bucket, `get_order` as a query) and `the_charges_reach_the_provider_users_shared_buckets`.
   - **FBC-jxau**'s done line, `crates/venues/fbc-venue-qfex/tests/probe_resync.rs` and
     `crates/venues/fbc-venue-qfex/tests/probe_settle.rs`: an `err` echoing the arm gives
     `RateLimited` with no settlement, with its kept hint (0134 item 7).
   - **The carrier**, one new ticket, FBC-p7rn, titled "QFEX rate limits end to end: each
     written frame's charge, and RateLimited replies across a primary and a subaccount session
     on one provider-user limiter (0135)". It is filed with this record, blocked by FBC-jxau,
     and it blocks FBC-cnyh and FBC-lqa8. Its test, offline against the conformance stub:
     - every frame the codec writes under `preprod_probe` carries the charge its message
       type's documented weight gives, in tenths (0128). That covers authentication, each
       subscription, each place, `cancel_order` (the cancel bucket), `get_order`, the arm
       (`Control`, so both buckets), each `get_user_orders` page (5.0, 0132:77) and each
       `get_user_trades` page;
     - two `ExecSession`s of the codec under `preprod_probe`, a primary and a subaccount
       selection, each with its own `RateLimiter::for_user` from `caps.exec_limits` under one
       `RateUserKey`, beside a limiter of another user;
     - the stub refuses with a flat `RateLimited` err echoing `add_order` with a seconds hint,
       a nested one with a milliseconds hint, a `RATE_LIMITED` order status, a `RateLimited`
       err echoing `cancel_order`, and one whose echo is `{}`, which names no bucket;
     - after each refusal, the other account's next frame of each bucket the refusal holds is
       `NotSent(RateBudget)` with nothing written until the hint, or the declared window, has
       passed, while a frame of a bucket it does not hold goes out; the other user's limiter
       stays free, and `counts().rejected` shows each refusal;
     - no refusal settles, resends or reconnects. Each refused place ends `Unknown` at its
       deadline (0057, 0132 item 8), and none is resent (0005);
     - decoder replay of each session's journal gives the same events, effects and counts.

     This item goes further than the five tickets FBC-sjht lists. The carrier is synthetic,
     like the rest, and lqa8's own "end-to-end" asks for it.

   This evidence proves what the adapter charges and what it does with a refusal. It does not
   show that QFEX's EMA admits what the local budget admits. 0128:31-33 already makes the local
   reserve an admission guard, not a promise, and 0126's hold answers a refusal the local count
   did not foresee. Acceptance, 0123:36's third item, is not part of this record. It stays with
   the pre-production evidence that 0117 and 0132 name.

   In lqa8's done line, "synthetic and provider fault evidence" becomes this synthetic fault
   evidence.

   **Provider evidence when it occurs.** One follow-up ticket, FBC-s5ox, is filed with this
   record, titled "QFEX provider rate-limit evidence: record the first RateLimited a session
   meets (0135)". Its id goes into lqa8's done line.
   - It records the first `RateLimited` that a later owner-run session (FBC-l931, FBC-9sk2) or
     production session meets without provoking it:
     - the frame under `fixtures/qfex/exec-recorded/` as decoder replay reads it: the bytes
       0134 keeps in clear, each span blanked as the journal blanks it (0028), and no keyed
       hash of a span, from pre-production or production alike (0009);
     - a test that replays it to the effect the codec gave live;
     - whether the user's charged traffic in the declared window before it was within the
       declared budget and reserve, read from every journal of the user's sessions in that
       window (one owner process per user, 0123), or "not determinable" where one is missing.
       For a production session only that verdict is written, with no count (0009).
   - It then judges the frame against 0123:56-59, 0126:40-41 and this record's "What would
     show this was wrong", and closes.
   - A later `RateLimited` gets a ticket of its own only when its envelope, its echo's type,
     its hint's form or its within-budget verdict is one that no recorded or synthetic case
     covers yet. Only such a frame tells the project anything new.
   - It is filed deferred, since it waits on an event no ticket causes, and carries no
     `story:` label, so that no story waits on it.
   - **What makes one visible.** The owner-run probes' reports state how many provider
     refusals each session met, FBC-l931's qfex_preprod_probe once FBC-m9rj extends it and
     FBC-9sk2's from the start: its limiter's `counts().rejected`, and the journaled errs
     whose `error_code` is `RateLimited` and `order_response`s whose status is `RATE_LIMITED`.
     A count above zero un-defers the follow-up. In production the consumer's limiter's
     `counts().rejected` is where one shows. This asks nothing of QFEX and provokes nothing.
3. **What the bar gates for FBC-cnyh.** The owner's words of 2026-11-04 did not settle this;
   on 2026-11-06, in chat, the owner confirmed this reading ("Yes, option A") over gating a
   readiness ceiling above `Readiness::Record` on provider evidence. The bar 0123:35-36 sets
   for charges and `RateLimited` replies gates FBC-cnyh only through item 2's synthetic
   evidence:
   - FBC-cnyh's record cites item 2's tests for charges and `RateLimited` replies, and the
     carrier blocks FBC-cnyh. The citation covers both production admission (`exec: Some` on
     the production factory) and any readiness ceiling above `Readiness::Record` that the
     record states.
   - Neither waits on provider rate-limit evidence.
   - If by then the follow-up, or a ticket filed under its rule, has recorded a `RateLimited`
     that 0123:56-59 or 0126:40-41 says reopens the model, FBC-cnyh's record also cites the
     record that answers it. It admits nothing that record leaves unsettled.

   The provider acceptance gates of 0117, 0126:35-36 and 0128:49-51 are otherwise unchanged:
   they are the pre-production evidence that FBC-cnyh already names.
4. **FBC-lqa8 closes superseded by FBC-cnyh**, option (ii).
   - lqa8 lands no commit of its own, and nobody works it. No commit whose subject starts
     "FBC-lqa8:" lands on main: one would mark it done, and after the supersede it would fail
     `scripts/backlog-status.sh --reconcile`. An agent that holds or reclaims lqa8 only waits
     for FBC-cnyh's commit and then runs this item's close.
   - Once FBC-cnyh's commit is on main, lqa8's holder or the owner runs
     `bd supersede FBC-lqa8 --with FBC-cnyh`. This holds whichever way FBC-cnyh's record
     decides, since its done line covers both outcomes. Its record is the decision that lqa8's
     last sentence asks for, and the gap tickets it files when it admits nothing carry the
     rest.
   - FBC-cnyh does not block lqa8, and nobody makes it a blocker: `bd supersede` refuses to add
     its link over an existing edge (kit decision 0030). If an edge between the two appears,
     it is removed before the supersede.
   - Every open ticket that blocks lqa8 today also blocks FBC-cnyh, directly or through
     another ticket, so none is still open when FBC-cnyh lands. Item 2's carrier blocks both.
     A ticket added later as a blocker of lqa8 is added to FBC-cnyh's blockers too.
   - Before the supersede:
     - FBC-cnyh carries every `story:` label lqa8 carries (today both carry `story:BT-702`,
       QFEX order entry, from FBC-mjj7);
     - every ticket lqa8 blocks is re-pointed to FBC-cnyh (there is none today);
     - origin/ticket/FBC-lqa8 is released, as item 5 says.

     After the supersede, `scripts/backlog-status.sh --reconcile` passes.
   - lqa8 is then dropped, never done, and counts in no story's tally.
5. **PR #196 and ticket/FBC-lqa8.** This item quotes the owner's decision of 2026-11-04 (in
   the Context above) and makes no decision of its own:
   - #196 was closed as superseded on 2026-11-04. It is never reopened or merged.
   - ticket/FBC-lqa8 is kept at 39d4e09f, the source FBC-77aa and FBC-zeb0 reuse, until
     FBC-blcw's commit is on main. lqa8's holder or the owner then releases it with
     `scripts/open-ticket-pr.sh FBC-lqa8 --release`. FBC-blcw is among FBC-cnyh's transitive
     blockers (through FBC-99w5 and FBC-jdj7), so the release comes before item 4's supersede.
   - GitHub keeps refs/pull/196/head after the close and the release.

## Alternatives

- **Ask QFEX for written consent to provoke a rate limit on pre-production:** not taken, on the
  owner's choice. Its answer might never come, and lqa8 could not close while it waited.
- **Provoke a limit on pre-production without consent:** rejected. It would break FBC-9sk2's
  rule, and QFEX's client agreement bars message flooding.
- **Keep "provider fault evidence" in lqa8's done line:** rejected. That evidence may never
  exist, so lqa8 would never close.
- **Take the slices' tests alone as the end-to-end proof:** rejected. None runs the codec in a
  session on a shared limiter, FBC-ywjb's done line leaves that wiring to lqa8, and lqa8 lands
  nothing. Only the charges of places, `cancel_order` and `get_order` would be proved.
- **Widen FBC-jxau's or FBC-blcw's done line instead of filing the carrier:** not taken. The
  charges span four slices, and the sessions need the probe mode, the encoder and the resync
  together. One ticket after FBC-jxau holds them all, and FBC-cnyh waits on FBC-jxau anyway.
- **Record every `RateLimited` a session meets, each follow-up filing the next:** not taken.
  One ticket would always be open, and every one after the first that repeats a known shape
  tells the project nothing.
- **(i) lqa8 lands its own non-empty commit:** not taken. Every piece of its work lands under a
  slice's id, so the commit would carry nothing of its own. An empty one would be dropped by a
  rebase merge, as FBC-38r7's was. It could go out on ticket/FBC-lqa8 only once that branch
  is released, or on another ticket's branch, as FBC-j9xw's commit did through FBC-nknd's PR
  #191. The promotion sentence would also make FBC-cnyh its blocker.
- **(iii) a `loop:dropped` label:** rejected. Kit decision 0030 keeps that label for work
  nothing carries, and the slices and FBC-cnyh carry lqa8's.
- **Make FBC-cnyh a blocker of lqa8:** rejected, because `bd supersede` would refuse its link.
- **Gate FBC-cnyh's admission, or a ceiling above `Readiness::Record`, on provider rate-limit
  evidence:** not taken. With no limit provoked, that evidence comes only by chance, so the
  gate would wait on the consent the owner chose not to seek.
- **Merge #196:** rejected. It would mark lqa8 done, and the rebase-only merge refuses its
  merge commit.

## Consequences

- **Beads.** After this record lands:
  - FBC-lqa8's description and done line say what it decides, and that nobody works it;
  - FBC-cnyh's description names item 2's evidence as what its record cites for charges and
    `RateLimited` replies;
  - item 2's carrier is filed, blocking FBC-cnyh and FBC-lqa8;
  - item 2's follow-up is filed, deferred;
  - FBC-m9rj's and FBC-9sk2's done lines have the probes' reports and stub tests state the
    refusal count item 2 names, and
    FBC-9sk2's consent clause reads as item 1 says.

  lqa8's holder applies the edits to lqa8, the carrier's edge to it included, and any agent
  may file the carrier and the follow-up.
- **Code.** Nothing under `crates/` changes here. Production stays `exec: None` and
  `Readiness::Record` until FBC-cnyh's record (0117, 0122, 0132).
- **Known only synthetically.** Until a session meets a `RateLimited`, the adapter's handling
  of one is known only from the synthetic shapes, and QFEX's EMA and retry hint are taken as
  the rate page and the errors page document them.
- **Owner-run sessions.** A session's journal keeps every frame it receives (0006), a
  `RateLimited` included, so the follow-up records one from there. A run is asked only to
  report its refusal count.
- **Order.** FBC-sjht, this record's ticket, blocks FBC-lqa8 and FBC-cnyh. It does not block
  FBC-9sk2: with no consent sought, no request to QFEX has to go out before 9sk2's runs.

## What would show this was wrong

- A `RateLimited` that a session meets in a shape, echo or hint the synthetic fixtures do not
  cover, or that the codec decodes otherwise than item 2's tests show. The synthetic evidence
  then did not prove the replies, and a new record says what does.
- A refusal that QFEX sends in a frame 0134 keeps whole, which holds no budget (0134 item 6),
  met by a session. The synthetic evidence then did not cover the refusals QFEX sends.
- A `RateLimited` on admitted traffic whose user's complete traffic was within the declared
  budget (0123:56-59), or provider evidence that the echo or the hint means something else
  (0126:40-41).
- A frame the codec writes that QFEX charges otherwise than the carrier's charge test shows.
- QFEX publishing a test environment or procedure for rate limits, or offering consent. Then
  provoked evidence costs little, and a new record may ask for it.
- `bd supersede` refusing the link at the close, or `scripts/backlog-status.sh --reconcile`
  failing after it. Then the close needs another option, and a new record chooses it.
