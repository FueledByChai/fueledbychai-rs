# 0078 — An order-entry session journals what it receives under Safety, and each nonce it reserves and context it gives where the call takes it

Status: accepted
Date: 2026-10-07

## Context

0006 has the journal record everything that crosses the shard boundary, nonces and the wall
time of each encode included, with a reserve so safety records (cancels, reducing orders, acks
and fills) never wait on a full journal. The market-data session (0023) journals its inputs and
writes through the shared session core; the order-entry session (0053, 0056, 0057, 0058) ran on
the same core with no journal (FBC-2pr). Three things were not decided by the market-data path:

- Which class an order-entry input goes under. The session journals a frame or an HTTP result
  before the codec decodes it (0006, 0028), so it cannot tell an ack or a fill from a frame that
  carries neither.
- Where a nonce block and an encode context are written. `fbc-journal` has had `Nonce` and
  `EncodeCtx` records since format version 3 (FBC-ec9), with no producer.
- That the order-entry session journaled no `Opened` or `Closed` for its epochs, which replay
  needs to rebuild its epochs as the market-data session's journal lets it.

## Decision

This augments 0006, 0053, 0056 and 0057 (it supersedes nothing). With a journal set
(`ExecSession::set_journal`), an order-entry session journals as the market-data session does
(each epoch's `Opened` and `Closed`; every frame and HTTP result it receives at its stamp, a
stale one included, with the credentials its codec names in it as keyed hashes, 0028; every
ping, pong and close frame, 0041; every frame it writes with its kind and spans and the
write's result; every HTTP request with its request id; every codec timer that fires), and
also:

1. **Each nonce as it is reserved.** One `Nonce` record per value, in the order the
   consumer's `NonceSource` reserved them, for `on_open`, `on_timer`, the resync and every
   encode, written as the reservation returns. A reservation of another count than asked is
   written too (those values are spent) before the session ends with
   `ExecSessionError::Nonces`. The session's one source is journaled as the `NonceSourceId`
   of its account's number. An encode's time is read once its nonces are reserved, so a source
   that takes its time (persisting what it reserved) leaves the signed request no stale time.
2. **Each context just before its call.** The `EncodeCtx` the session hands `on_open`,
   `on_timer`, the resync or an `encode`, the encode's with its request id, `None` for the
   others (which replay matches by place). A call that is not made (a short reservation, an
   authorization refused at submit, a batch with no nonce block) has no context written.
3. **Classes.** What the order-entry stream brings, every data frame and HTTP result, is
   journaled under Safety, since the session cannot tell an ack or a fill apart before it is
   decoded. An encode's nonces and context go under its command's own class
   (`VenueCommand::traffic_class`), the class its frames are labelled and journaled with, so a
   cancel's, a reducing order's and the arm's are Safety; this is the journal's class, not the
   rate-limit class 0073 gives control commands. The ping, pong and close frames received, the
   connection changes, the codec's timer firings and the contexts and nonces of `on_open`,
   `on_timer` and the resync go under Normal. A market-data session's inputs stay Normal.
4. **A stop's inputs follow the close.** When the control drops as a frame, timer firing or
   HTTP result wakes the epoch, or a frame waits then, the epoch's `Closed` is written first
   and those inputs, which reach no codec, after it, as the market-data session does, so replay
   feeds them to no codec either. Spans a codec names that do not fit their input are counted
   (`ExecCounters::refused_redactions`) and that input is hashed whole, as on market data.

## Alternatives

- Journal inbound frames under Normal, as market data: an order-entry stream's frames are
  mostly acks and fills, the records 0006's reserve exists for; dropping them at the soft
  limit would leave the journal without the fills that moved the inventory.
- Journal an inbound frame after decoding, under the class its events imply: the record
  would follow the codec's events and effects, not precede them, so replay could not feed it
  where it was read, and a frame that fails to decode would have no class at all.
- One `Nonce` record per block rather than per value: the format already writes one per value
  (FBC-ec9), so a restarted source reads where the live one stood without a block layout.
- Write the context only for encodes: `on_open`'s and the resync's contexts carry the wall
  time the codec puts in its authentication and its resync watermark (the conformance toy
  writes both), which replay must hand back.

## Consequences

- A journal now holds every nonce and context an order-entry session used, so exact replay of
  its outbound bytes (0006) has what it needs from the session.
- An order-entry session's inputs can use the journal's safety reserve; a large resync answer
  read over HTTP is Safety too.
- A request's deadline firing (`on_rpc_timeout`) is stamped but not yet journaled, so the
  ingest sequence it takes is a gap no marker explains: FBC-0hfl adds its record. Resuming an
  account's `RpcIds` from the journal's request ids is FBC-gqyb.
- `crates/fbc-runtime/tests/exec_journal.rs` reads back sessions of the conformance toy and of
  fbc-core's `auth_toy` record by record.

## What would show this was wrong

- A recording whose order-entry inputs exhaust the safety reserve under load, so a cancel's
  own records are dropped.
- A replay of an order-entry journal that cannot reproduce an outbound frame because a nonce
  or a context it needs is not in the journal (outside a `Degraded` span).
- A nonce source restarted from a journal that reuses a value the live source had reserved.
