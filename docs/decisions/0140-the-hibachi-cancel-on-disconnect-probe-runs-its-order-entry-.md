# 0140 — The Hibachi cancel-on-disconnect probe runs its order-entry sessions in a declared probe mode that owes no arm and can drop, close or go silent, under caps declaring an arm whose effect is unproven, arms its registry on a flat hand seed, and places and cancels only through fbc-oms, while 0058 still refuses Hibachi order entry to every other caller, augmenting 0057, 0058, 0067 and 0080

Status: accepted
Date: 2026-11-05

## Context

BT-604 gates Hibachi order entry on a record that supersedes 0058's Hibachi sentence. That
record is FBC-mtck's, and it rests on the evidence of an owner-run probe. FBC-xvtr builds the
probe, `hibachi_cod_probe` in `crates/fbc-examples`.

- **What it is.** A supervised program that the owner runs, and no agent or test ever does.
- **What it does.** It places one minimum-size post-only order at a time, on the separate
  Rust-only Hibachi account the owner chose. It runs on mainnet unless the owner obtains testnet
  access. It records what `orders.enableCancelOnDisconnect` does.
- **Its scenarios.** A control: no arm, then a drop. Then the armed scenarios: a dropped
  socket, a graceful close, a silent loss, a reconnect without re-arming, an order placed
  before arming, and an order placed on another connection.

This record states how its sessions run. It is the narrow record two owner answers ask for:

- **2026-10-20 (FBC-xvtr's notes).** The question was: "The cancel-on-disconnect probe places
  real orders on a venue decision 0058 refuses. How should it run?" The owner chose "Through fbc-oms
  (Recommended)": "The probe uses fbc-oms and fbc-runtime like every other order: caps, a
  single-use authorization per command, Stop. A narrow record lets the probe's session start
  despite 0058, for this probe only."
- **2026-10-21 (FBC-jw1b's notes).** The question was how the probe's registry can be
  seeded on mainnet. The owner chose option B, a probe-only record:
  - Only the supervised probe, on the separate Rust-only account, arms its fbc-oms registry,
    and it arms on a seed of nothing.
  - It does so after REST shows no open order on any market and no position in the probe's
    market, with no other process trading that account.
  - It changes nothing for any other caller. Hibachi's open-order snapshot stays
    untrustworthy for everyone else until evidence, and 0067's testnet rule is unchanged.
- **2026-10-22.** The public Java library was found to configure a Hibachi testnet. The
  owner's answer stands. Nothing here depends on which network the probe runs on.

What stands in the way today:

- **0058.** `ExecSession::new` refuses a venue whose caps declare cancel-on-disconnect `None`
  or `DeadMan` (`ExecSessionError::CancelOnDisconnect`).
  - A session it builds sends its own arm on each epoch once authenticated.
  - It holds every place, batch of places and amend (`NotSent(Disconnected)`) until two things
    are done: the venue has finally accepted that arm, and the epoch's resync has ended
    (`Gate::admits` in `crates/fbc-runtime/src/exec_gate.rs`).
  - Three of the probe's scenarios place with no accepted arm: the control, an order placed
    before arming, and a reconnect without re-arming.
- **0080.** On an epoch that sends an arm, an order of ours that the resync shows resting from
  an earlier epoch is unprotected.
  - Its market is held, and the handler is told of it (`ExecHandler::on_unprotected`).
  - The glue in `crates/fbc-examples/src/link.rs` then cancels it at once.
  - The reconnect scenarios keep such an order resting by design, until the probe has read it.
- **The one path.** Every order command takes fbc-oms (0013 rule 2).
  - Each needs a single-use `Authorization`. It is issued only after the market's state and
    its kill switch (0012), the inventory cap and the resting cap (0052).
  - It is re-checked at submit (0060, 0062).
  - Turning cancel-on-disconnect on affects no order. It is a `ControlCommand` (0045).
- **The seed.**
  - Only a resync from a trustworthy open-order snapshot source seeds a market (0055).
  - A market seeded by hand is refused at Start, Flatten and Wind-down, outside a declared
    owner-assisted testnet run (0067).
  - FBC-yvmv plans Hibachi's source untrustworthy until evidence. No record or testnet run
    shows `GET /trade/orders` complete.
- **The silent loss.** Reviewer B's RB186F-1 on PR #186 added it. Every write on the armed
  connection stops, and the socket stays open. Only Hibachi's own idle close can then notice:
  about 60 s without application traffic, with WebSocket pings not counted (the public Java
  library's `HibachiTradeWebSocketClient.java`).
  - fbc-runtime's WebSocket is tokio-tungstenite over tungstenite 0.30. Its `WebSocket::read`
    writes two replies itself: the pong for a ping, and the reply to a close frame
    (`src/protocol/mod.rs` flushes what it queued before each read). Its configuration has no
    setting that turns this off.
  - An order-entry session runs no silence alarm and no rotation. 0033 gives those to
    market-data sessions only, and `ExecSessionConfig` carries none.
  - The session bounds each of its own writes by the write-stall window (0036).
- **The capability.** `CancelOnDisconnect` is `None`, `PerConnection`, `AccountWide` (0121) or
  `DeadMan`. None of them states what the probe's codec offers: an arm that is sent, with no
  disarm, whose effect no document or evidence shows.
  - `None` declares no arm. `caps_truthful` then expects the arm refused `NotSent(Unsupported)`
    with no byte sent (0003), and the probe's codec sends it.
  - The others declare protection that nothing has shown (0054).
  - A behaviour the model cannot express grows the model (0003). 0121 grew this one for
    `AccountWide`, with a construction condition of its own.
- **The codec.** Hibachi declares `exec: None` and builds no order-entry codec.
  - FBC-x7bi (the order-entry codec), FBC-btjh (the account stream) and FBC-yvmv (the
    `ExecCaps`) wait behind FBC-mtck's record.
  - That record waits on this probe. So the probe cannot use them without making a cycle.
- **The other connection.** An order-entry session carries every order on its first planned
  connection (0115). Order entry is WebSocket-only (0057), and HTTP order entry is FBC-4nfb's.
  The glue in `crates/fbc-examples/src/link.rs` serves one session: it collects one resync at a
  time and keeps one epoch's state.

## Decision

1. **Scope.** Everything below serves only `hibachi_cod_probe`. 0058 is unchanged for every
   session built without item 2's declaration:
   - Hibachi's default configuration still declares `exec: None` and builds no codec
     (`ExecSessionError::NoOrderEntry`).
   - The probe's caps (item 8) declare cancel-on-disconnect `Unproven` (item 2). A session for
     them built without the declaration is refused as 0058 refuses `None`.
   - Hibachi order entry for every other caller still waits for FBC-mtck's record (BT-604).
2. **A declared probe mode of the order-entry session (fbc-runtime), for an unproven arm
   (fbc-core).**
   - **The capability.** `CancelOnDisconnect::Unproven` is a new variant. It declares that the
     venue takes an arm, `ArmCancelOnDisconnect(true)`, and offers no disarm, and that no venue
     document or evidence shows what the arm protects. It protects nothing the library counts.
     - `ExecSession::new` refuses it with `ExecSessionError::CancelOnDisconnect`, as 0058
       refuses `None`, for every session built without item 2's declaration.
     - `caps_truthful` expects its arm sent, and its disarm and a dead-man refresh refused
       `NotSent(Unsupported)` with no effect (0003).
     - SimVenue cancels nothing on a disconnect for it, as for `None`.
     - In the venue crates, only the probe codec's mode (item 8) declares it.
     - It follows 0121's `AccountWide`: a variant whose session needs a declaration of its own.
   - **The declaration.** `fbc_runtime::CodProbe` is the consumer's declaration that a session
     belongs to the supervised cancel-on-disconnect probe. Only `CodProbe::owner_supervised()`
     makes one.
     - `ExecSessionConfig::cod_probe: Option<CodProbe>` carries it, `None` by default, as
       `account_wide_cod_authority` carries 0121's authority.
     - Only `hibachi_cod_probe` makes one in this repository. The type's documentation says
       so, and review holds it there, as it holds 0067's `TestnetRun`.
   - **Only for a venue that declares an unproven arm.** With the declaration, a session is
     built only for a venue whose caps declare `CancelOnDisconnect::Unproven`. A venue
     declaring `None`, `PerConnection`, `AccountWide` or `DeadMan` is refused before anything is
     planned. So the mode never weakens a session of a venue that declares protection.
   - **No arm of its own.** The session sends no arm on any epoch. It counts no arm failure,
     and ends no epoch for one.
   - **The resync still holds places.** The session holds every place, batch of places and
     amend on an epoch until that epoch's resync has ended, as 0058 does. That keeps 0013 rule
     1: the venue is re-read before anything is added. No arm holds them, so an epoch places
     with no arm accepted.
   - **No order counted unprotected.** 0080 counts as unprotected only the orders found resting
     by an epoch that sends an arm. Under this mode no epoch sends one. So no order is counted,
     no market is held, and `ExecHandler::on_unprotected` is never called.
   - **The controls.** The session gives the probe item 9's controls.
   - Nothing in the mode names Hibachi.
3. **The arm is a control command.**
   - **How it is sent.** The probe turns the protection on with
     `ControlCommand::ArmCancelOnDisconnect`, through `ExecOrders::submit_control`. It sends it
     at the step each scenario names, and never inside an observation window (item 10).
   - **Nothing holds it.** It carries no `Authorization`, so fbc-oms neither builds nor checks
     it. No resting cap, inventory cap, market state or kill switch holds it, whether the
     market is Quoting, Cancel-only or Killed. The session's place gate never holds a control
     command (0058).
   - **What can refuse it.** Only what refuses any control command the consumer submits (0057,
     0073, 0105), and item 9's silence:
     - an ended session (`SubmitRefusal::Ended`);
     - the outbound bound for its class (`SubmitRefusal::Backpressure`, 0105);
     - no authenticated epoch (`NotSent(Disconnected)`), and a silent connection (item 9);
     - the codec's refusal, whatever its reason, or effects the session cannot carry out for the
       request (0057, 0081);
     - the buckets, at their safety reserve (`RateBudget`, 0073);
     - the in-flight table (`RateBudget`, 0105);
     - the nonce source, where the arm is signed over its nonce: its refusal ends the session
       (`ExecSessionError::NonceRefused`, 0114, 0133).
   - **Its outcome.** It reaches the handler as any outcome does. Under item 2's mode it settles
     nothing in the session. The probe records it, with the form of the arm sent (item 8). A
     scenario that needs an arm and gets no Final acceptance is recorded inconclusive.
   - **No disarm.** fbc-oms has no command for one (0045). The codec refuses
     `ArmCancelOnDisconnect(false)` as `NotSent(Unsupported)`, since Hibachi documents no
     disable method.
4. **Every place and cancel takes fbc-oms's one path.**
   - **The registry.** The probe builds one `Registry` for the account.
     - It has the lease keys its market and account leases are taken under (0013 rule 3).
     - Its market's caps come from the operator's required `--resting-cap-usd` and
       `--inventory-cap-usd` (`MarketCapsConfig`, 0052). Each is refused when not positive, as
       `mainnet::mainnet_caps` refuses it, and converted to lots as `testnet::cap_lots` converts
       it (0113).
   - **Arming.** Start, with those leases, arms the market into Quoting.
   - **A place.** Each place goes through these steps, and its authorization is spent once:
     - it is built by `Registry::place`, which checks the market's state and its kill switch,
       then the inventory cap and the resting cap (0012, 0052);
     - it is authorized by `Registry::authorize`;
     - it is submitted through `ExecOrders::submit`;
     - `Authorization::check_at_submit` re-checks it just before its encode (0060, 0062).
   - **A cancel.** Each cancel names its order by venue order id, through the order's cancel
     permit (I4). It is authorized and submitted the same way.
     - No cap or kill switch blocks it (0012), and it always passes the re-check (0060).
     - The probe never builds a cancel-all. fbc-oms issues none for the account (0045).
   - **Interrupts and errors.** On either, the probe turns the market's kill switch on before
     its cancels, so nothing more is placed.
   - **No other write path.** The probe makes no REST place or cancel, and writes no frame of
     its own. Its REST traffic is reads, made through fbc-runtime (item 10).
5. **How the scenarios with no accepted arm pass 0058's gate.** Under item 2's mode the arm half
   of the gate is off for every scenario alike. No arm acceptance is forged.
   - **The control.** An epoch authenticates and its resync ends. The probe places, never
     arms, and drops the connection (item 9).
   - **An order placed before arming.** The epoch's resync ends. The probe places, then
     submits the arm (item 3). It drops the connection once the arm is finally accepted.
   - **A reconnect without re-arming.** The probe arms an epoch, ends it (item 9), and the
     session reconnects. The next epoch's resync ends, and the probe places with no arm on that
     epoch. Then it drops it.
   - **The armed scenarios.** These are a dropped socket, a graceful close, a silent loss, and
     item 12's scenario. Each arms first and places once the arm is finally accepted. The
     probe requires that, not the gate.
6. **The seed: a flat hand seed on one market, in a declared probe registry (fbc-oms).** This
   is the owner's option B of 2026-10-21, read as narrowly as it goes.
   - **The declaration.** `fbc_oms::CodProbeRun` names the probe's one market. Only
     `CodProbeRun::owner_supervised(market)` makes one, and `Registry::for_cod_probe(run)`
     gives it when the registry is built.
   - **What "a seed of nothing" means.** A hand seed of a flat position:
     `Registry::seed_position(market, SignedLots(0))`.
   - **What the declaration changes.** Start, Flatten and Wind-down no longer refuse that
     market as `ArmRefusal::SeededByHand`. Nothing else changes:
     - a hand seed of any other position is still refused;
     - a hand seed on any other market is still refused;
     - an unknown position, the kill switch and the leases refuse as before;
     - every other registry is unchanged.
   - **Not a testnet run.** The probe does not use `Registry::for_testnet_run`.
   - **When it seeds.** Only after all of these:
     - its own REST reads show no open order on any market of the account
       (`GET /trade/orders`);
     - they show no position in its market (`GET /trade/account/info`);
     - the operator's supervision flag asserts that no other process, Java or Rust, trades the
       account. This is the separate Rust-only account the owner chose.

     FBC-xvtr already refuses to start otherwise.
   - **Afterwards.** The seed's origin is fixed for the process (0067). Every later resync
     compares the market's position with the inventory (0055), whatever its source. A desync
     stops the probe before its next place.
7. **The snapshot source is untrustworthy, as planned.** The probe's codec (item 8) declares
   `SnapshotSource::Untrustworthy` and `SnapshotReads::Separate`, as FBC-yvmv plans.
   - No record and no testnet run shows Hibachi's open-order read complete, and positions are
     a separate read.
   - So no resync seeds a Hibachi market (0055). The probe's market is seeded only as item 6
     says.
   - Nothing here changes Hibachi's source for any other caller.
8. **The codec the probe's session runs.**
   - **The setting.** `hibachi.exec.mode` is absent, or `cod_probe`. Any other value is a
     configuration error.
   - **Absent.** The factory declares `exec: None`, plans no order-entry connection and builds
     no codec, as on main.
   - **`cod_probe`.** The factory declares `ExecCaps` and plans one order-entry connection,
     `/ws/trade` on a required origin. Its codec, a probe codec in `fbc-venue-hibachi`,
     encodes:
     - a place: a post-only limit order, signed by the signer `auth::order_signer` builds
       (FBC-8mzv), with its max fees from a required setting. Its absolute `creationDeadline`,
       in Unix microseconds, is the encode's wall time (`EncodeCtx::wall`) plus a required
       lifetime setting, since a codec takes time only from `EncodeCtx` (0002);
     - a cancel by venue order id, signed;
     - the arm, `orders.enableCancelOnDisconnect`, in the form a required setting names:
       unsigned as the SDK sends it, or signed over its nonce as FBC-8mzv's signer packs it;
     - an order query by placement nonce (`ControlCommand::Query` with `placement_nonce`), as
       one HTTP request, `GET /trade/order` (0081);
     - its keepalive, `orders.status`, on its own timer at a required interval (0056).
   - **Its resync** is REST: `GET /trade/orders`, `GET /trade/account/info`, and
     `GET /trade/account/trades` for the fills of orders it placed. They are decoded into
     `ResyncBegin` … `ResyncEnd`.
     - The same reads are exposed as requests and plans with their decoders, as Paradex's
       `src/exec/rest.rs` exposes its own, for the probe's reads between scenarios.
     - Its API-key headers come from `src/auth` (FBC-bn8j).
   - **What it refuses.** Every other command, as `NotSent(Unsupported)`: amends, batches,
     cancel-manys, cancel-alls and the disarm.
   - **Its caps.** Its `ExecCaps` cite a source for each value, and an undocumented value is
     conservative (0054).
     - They declare cancel-on-disconnect `Unproven` (item 2): the codec sends the arm, which is
       what the probe measures, and nothing shows what it protects. That keeps 0058's refusal
       in force for a session built without item 2's declaration.
     - `caps_truthful` holds the codec to these caps, the arm sent and the disarm refused.
     - These caps are a probe's, not Hibachi's order-entry declaration, which FBC-yvmv makes
       after FBC-mtck's record.
   - **Its ticket.** FBC-9ff3, filed with this record, builds it before the probe and blocks
     FBC-xvtr: "fbc-venue-hibachi: the cancel-on-disconnect probe's order-entry codec and
     ExecCaps under hibachi.exec.mode = cod_probe". It waits for FBC-bn8j (the API-key
     headers) and for item 2's ticket, FBC-2x2o (the capability and the probe mode its stub
     session is built with).
     - It is not FBC-x7bi or FBC-btjh. Both stay deferred behind FBC-mtck's record, which needs
       this probe's evidence, so naming either would make a cycle.
     - FBC-x7bi may build on this codec once FBC-mtck's record lands.
9. **The drop, the close and the silence.** Only a session in item 2's mode gives the probe
   these controls, for each WebSocket connection it holds. Each is taken on the session's next
   turn, after any write in progress has completed.
   - **Drop.** The socket is dropped with no WebSocket close frame. The epoch ends as a drop,
     and the session reconnects through its pacing.
   - **Close.** A WebSocket close frame is written, charged as the session's own close is, and
     the socket is closed. The epoch ends, and the session reconnects through its pacing. If
     the buckets do not admit the frame, nothing is closed and the probe is told.
   - **Silence.** From then on the session writes nothing on any WebSocket connection it holds:
     - **Its own writes.** It refuses and counts every frame for a silent connection, and
       queues none to send later. That covers the codec's `orders.status` keepalive, a
       command's frame and a reconnect the codec asks for. A command submitted meanwhile is
       `NotSent(Disconnected)`, except item 11's cancel, which first ends the silence.
     - **Below tungstenite.** A gate in the connection's TCP stream takes no byte while it is
       shut. The gate sits in `Tcp`, beneath TLS and WebSocket (0031). Three things wait in
       their buffers and never reach the socket: the pong tungstenite's read queues for a ping,
       its reply to a close frame, and anything the TLS layer would write.
     - **Reading.** The session keeps reading through tungstenite. A ping, a close frame and a
       data frame are each read, stamped and journaled as any input is (0041), and the codec
       decodes data frames. The session answers nothing it reads.
     - **No close.** It sends no close frame, FIN or reset. It touches no other system: no
       firewall, route or proxy change. The kernel's TCP acknowledgements of what arrives carry
       no data, and they still go.
     - **No epoch ended by the session.**
       - An order-entry session runs no silence alarm and no rotation (0033). If a later change
         gives it either, neither runs on a silent connection.
       - The write-stall bound (0036) bounds only a write the session began, and it begins none.
       - Request deadlines still fall due into `on_rpc_timeout` (0057). Nothing it asks for is
         written on a silent connection.
     - **The venue's close.** This is the first close frame read, or the end or reset of the
       stream, whichever comes first. The probe's window for the scenario starts there.
       - After a close frame, the connection is kept with the gate still shut, until the stream
         ends or the probe drops it.
       - When the stream ends, the epoch ends as a drop and the session reconnects through its
         pacing. The new epoch is not silent.
     - **The end of a silence.** Suppose no close has come `--silent-secs` after the probe's last
       write. The probe reads and records the order's REST state, and that no close came. Only
       then does it drop the connection, without opening the gate. It cancels the order through
       fbc-oms on the session's next epoch. A silence otherwise ends only by item 11.
   - **The journal.** The session journals each control at the instant it takes it, so the
     journal agrees with the wire (0006, 0078):
     - A drop or a close ends its epoch with the `Closed` record any end writes. A close's frame
       is journaled as any outbound frame is.
     - A silence's start and its end are each journaled as a connection change of their own: a
       `ControlEvent` that the journal's next format version adds. Its reader still reads the
       older versions.
     - A frame refused for a silent connection is never journaled as `Outbound`. Where it has a
       request id, its refusal is journaled as that request's write result,
       `NotSent(Disconnected)`, as 0105's refusal is. A frame with none, the keepalive, is only
       counted.
     - What the session reads meanwhile is journaled as any input is (0041): the venue's close
       frame as the `InboundControl` it is, and the stream's end as `Closed`.
10. **Nothing touches a scenario's order inside its window.** The window runs from the scenario's
    drop, close or silence until the probe has read and recorded the order's REST state after
    `--observe-secs`. In it, no command naming that order is written: no cancel, no order query
    through a session (`ControlCommand::Query`) and no amend. Item 11's three exceptions are the
    only ones.
    - **The session's own reconnect.** The session may reconnect inside the window: after a drop
      or a close, or after the venue's close of a silent connection.
      - A new epoch writes its authentication, the codec's REST resync reads and its keepalive.
        None of them names the order.
      - Its `ResyncEnd` reaches fbc-oms: `link.rs`'s resync handling calls `Registry::resync`.
        That applies what the resync shows as order updates and compares the position. It builds
        no command (0013 rule 1, 0055).
    - **Re-arming.** There is none. Item 2's mode sends no arm of its own, and the probe submits
      none inside a window.
    - **Unprotected-order handling.** There is none. Under item 2's mode no epoch sends an arm,
      so 0080 counts no order unprotected. `on_unprotected` is never called, and `link.rs`'s
      Safety cancel is never built.
    - **The probe's own passes.** The probe runs none of these: an Unknown-ladder pass (its query
      and its tombstone cancel are commands for the order), a planner pass, a kill-switch sweep,
      or a Stop. Its REST reads are reads made outside the session: `GET /trade/order` every
      `--poll-secs`, and `/market/exchange-info`. Each is an HTTP call through fbc-runtime with
      the consumer's `Connector`, so the SOCKS5 proxy the consumer configures applies (0002).
      The trade reads are built and decoded by item 8's exposed requests and decoders, and
      `/market/exchange-info` by the venue crate's discovery.
    - **Item 12's scenario.** The same holds for the order resting on the second session while
      the first session's window runs.
    - **How a breach shows.** FBC-xvtr counts any cancel the session or the glue sends for the
      order as the probe's. So a breach of this item shows in its run record as an inconclusive
      scenario.
11. **A fill, an operator interrupt or an error ends the hold at once.** Any of these ends the
    window: a fill a poll reads (by any of FBC-xvtr's three signals), an operator interrupt
    (SIGINT or SIGTERM), or an error. The probe does not wait for the venue's close or for
    `--silent-secs`.
    - **First.** The probe turns the market's kill switch on, then cancels the order through
      fbc-oms (item 4).
    - **On a silent connection with no close frame read and the stream open.** The probe ends
      the silence. The gate opens, what tungstenite held goes first, and the cancel follows on
      that connection at the session's next turn.
    - **Once a close frame has been read, or the stream has ended.** The WebSocket can no longer
      carry a request. The probe drops the connection, and the cancel goes on the session's next
      epoch as soon as it authenticates. A cancel refused meanwhile as `NotSent(Disconnected)`
      is built and authorized again.
    - **An error that ends the session.** It ends the silence too, since the socket drops with
      the session. The probe's cleanup (FBC-xvtr's clause 8) cancels through a new session in
      item 2's mode.
    - **The record.** The scenario is recorded inconclusive, with its cause.
12. **The order placed on another connection goes on a second session.**
    - **The second session.** It is a second `ExecSession` in item 2's mode, for the same account
      (`acct`), with its own `/ws/trade` connection and its own connection number
      (`ExecSessionConfig::conn`, distinct from the first's). It shares with the first:
      - the one registry, which holds the market and account leases, so every place and cancel
        of either session is authorized there;
      - the account's one `RpcIds` (0057), its rate buckets and its in-flight table (0105);
      - one nonce source: the account's one `WallClockNonces` over its one lease file (0133,
        which refuses a second holder). Both sessions reach it through a handle on the shard's
        thread, so no nonce repeats on the account. Both journal its values under one
        `NonceSourceId`, which 0078 already makes "the same for every session sharing that
        sequence".
    - **The glue.** Each session's events reach the one registry through `link.rs`, which keeps
      what it collects per session, by the connection number each envelope's stamp carries
      (`Stamp::conn`):
      - each session's resync collection, from its `ResyncBegin` to its `ResyncEnd`;
      - its epoch's authentication time, which is that resync's `requested_at`;
      - its epoch's state (authenticated, closed, an error), which that session's own
        `on_epoch_end` clears.

      So two sessions resyncing at once never mix. Each `ResyncEnd` applies its own snapshot
      through `Registry::resync`, with its own `requested_at`. Each snapshot reads the whole
      account, so it shows both sessions' orders. With one session, the glue behaves as it does
      today.
    - **How it runs.** The first session arms and the second never does.
      - The probe places the scenario's order on the second session while the first has no
        request in flight, so nonces reach the account in the order the source issued them.
      - Then it drops, closes or silences the first session's connection.
      - It watches the order on the second session, which stays open through the window.
    - **Its ticket.** FBC-g0mc, filed with this record, builds what the two sessions share, and
      blocks FBC-xvtr: "fbc-runtime and fbc-examples: two order-entry sessions of an account
      share one nonce source on the shard's thread and one registry through link.rs".
      It proves both against fbc-conformance's stub: two sessions resyncing at once into one
      registry, each resync applied apart, and no nonce issued twice. It is not FBC-x7bi or
      FBC-btjh.
    - **What it lets FBC-mtck say.** The scenario stays. So FBC-mtck's record can say from it
      whether an armed connection's loss cancels orders placed on another connection: whether
      Hibachi's protection is `AccountWide` (0121), or reaches only its own connection.

## Alternatives

- **The session's own arm, under caps declaring per-connection protection (0058 unchanged):**
  rejected.
  - Three scenarios place with no accepted arm: the control, an order placed before arming and
    a reconnect without re-arming. 0058's gate refuses each.
  - Declaring `PerConnection` would lift 0058's construction refusal for every caller of
    `cod_probe`.
  - It would claim protection no evidence shows (0054). BT-604's record declares no protection
    the probe did not show.
- **Caps declaring `None` while the codec sends the arm, with `caps_truthful` not run on the
  mode:** not taken. The caps would say the venue takes no arm while the codec sends one. That
  departs from 0003, whose `caps_truthful` refuses an absent capability with zero bytes sent,
  and from 0054's truthful caps, for this mode. A behaviour the model cannot express grows the
  model instead (0003), as 0121 grew it for `AccountWide`. The owner chose `Unproven` over this
  on 2026-11-05 (FBC-jw1b's notes).
- **A new `VenueFactory` method marking the probe's configuration probe-only, with
  per-connection caps:** not taken. It adds a factory hook. Caps declaring `Unproven` need
  none: 0058's refusal covers them without item 2's declaration, and item 2 accepts nothing
  else.
- **Forge the arm's acceptance in the gate for the unarmed scenarios:** rejected. Every counter
  and the journal would then show an arm that was never sent.
- **Place the unarmed scenarios' orders over REST, outside `ExecSession`:** rejected. Order
  entry is WebSocket-only (0057; HTTP order entry is FBC-4nfb's), and this would be a second
  write path.
- **A socket of the probe's own, outside fbc-runtime:** rejected. It breaks 0002, and the
  owner's 2026-10-20 answer, "Through fbc-oms".
- **Option A, a Hibachi testnet run under 0067:** the owner chose B. If the owner gets testnet
  trading access, the probe may run there under this record unchanged; 0067's declaration is not
  needed.
- **Option C, Hibachi's snapshot declared trustworthy on its documents:** the owner chose B. C
  would change every Hibachi caller.
- **The hand seed under `TestnetRun`:** rejected. That declares a testnet, and the probe runs on
  mainnet.
- **Any hand seed, not only a flat one, or one on any market:** not taken. Option B seeds an
  account REST shows empty, so a flat seed on the probe's one market is all it needs.
- **Read below tungstenite during a silence:** rejected. The session would consume raw bytes
  and leave tungstenite's framing behind. The connection could then neither read a close frame
  as one nor carry item 11's cancel.
- **Stop reading during a silence:** rejected. The venue's close would go unseen until the next
  write.
- **Shut down the socket's write half:** rejected. That sends a FIN: a close, not a silence.
- **A tungstenite setting:** none exists. tungstenite 0.30's configuration has no switch for its
  automatic replies.
- **The other connection as a second planned endpoint of one session (0115):** rejected.
  Commands go only on the first endpoint.
- **A REST place, `POST /trade/order`, through fbc-oms:** rejected, for the reason given for the
  unarmed scenarios.
- **Drop the other-connection scenario:** not taken. FBC-mtck's record could then not say
  whether an armed connection's loss reaches orders placed on other connections. It could
  neither declare `AccountWide` (0121) nor rule it out.
- **One glue per session, each with a registry of its own:** rejected. The account's leases
  and caps hold in one registry (0013 rules 2 and 3). The second session's order would escape
  the first's resting and inventory caps.

## Consequences

- **Tickets.** Four probe-scoped tickets, filed with this record (FBC-jw1b), carry the work.
  Each serves story BT-604 and blocks FBC-xvtr:
  - FBC-2x2o, "fbc-core and fbc-runtime: `CancelOnDisconnect::Unproven` and the order-entry
    session's probe mode": the variant, its refusal without the declaration, `caps_truthful`'s and
    SimVenue's reading of it, `CodProbe`, the gate that owes no arm, and the drop, close and
    silence controls with the write gate and their journal records (items 2, 9 and 11);
  - FBC-9ff3, "fbc-venue-hibachi: the cancel-on-disconnect probe's order-entry codec and
    ExecCaps under hibachi.exec.mode = cod_probe" (item 8), after FBC-bn8j and FBC-2x2o;
  - FBC-g0mc, "fbc-runtime and fbc-examples: two order-entry sessions of an account share one
    nonce source on the shard's thread and one registry through link.rs" (item 12);
  - FBC-80xo, "fbc-oms: `CodProbeRun`, a flat hand seed on the probe's one market" (item 6).

  Each proves its part offline, against fbc-conformance's stub on loopback, with hand-built
  SYNTHETIC fixtures (0009). No test or agent sends anything to Hibachi.
- **FBC-xvtr.** These change with this record:
  - Its clause (4) has the probe seed fbc-oms only from a snapshot source declared trustworthy,
    never by hand. Under item 6 it seeds as that item says instead.
  - Its stub case "a snapshot source not declared trustworthy (refused before anything is
    placed)" becomes a hand seed that is not flat, or is on another market, refused at Start.
  - Its explicit supervision flag also asserts that no other process, Java or Rust, trades the
    account (item 6).
  - Where its silent loss closes the connection after `--silent-secs`, that is item 9's drop,
    with the gate still shut.
  - Its notes point at this record.
- **Unchanged.** FBC-x7bi, FBC-btjh, FBC-yvmv and FBC-mtck are unchanged. Hibachi's default
  configuration keeps `exec: None`. 0058's Hibachi sentence stands until FBC-mtck's record
  supersedes it. That record may remove item 2's mode, or keep it for a later probe.
- **The risk the owner took with "Probe only".** The probe rests one minimum-size order at a
  time, far from the touch, with no protection it can count on through each window. It is
  supervised, and any fill ends the run (FBC-xvtr).
- **The held pong.** Hibachi may close a silent connection for a missing pong sooner than its
  idle close. A real silent loss answers no ping either. The run record gives the close's time,
  and FBC-mtck's record reads it.
- **The capability.** fbc-core's `CancelOnDisconnect` gains `Unproven`, so every match on it
  names the variant. No adapter's caps change: only the probe codec's mode declares it.
  `caps_truthful` holds that mode as it holds every adapter (item 8). 0141 (FBC-3ymj) has each
  variant that takes an arm state whether a disarm is offered, and a later variant either state
  the field or say why `offers_disarm()` is false for it. `Unproven` offers none by definition,
  since no document shows one: `offers_disarm()` is false for it, as for `None`, and
  `caps_truthful` probes its disarm as an absence.

## What would show this was wrong

- A place, batch or amend written on an epoch whose arm the venue had not finally accepted, by
  a session built without item 2's declaration.
- A session in item 2's mode built for a venue that declares protection.
- A session built for caps declaring `Unproven` without item 2's declaration, or a venue
  crate's caps other than the probe codec's mode declaring `Unproven`.
- A frame journaled as `Outbound` on a silent connection, or a write on one the journal does
  not show.
- One session's resync applied in the registry with another session's snapshot or
  `requested_at`.
- A byte of data reaching the venue on a silent connection before the probe's own drop, outside
  item 11.
- A silent epoch that the session ended itself.
- A command naming a scenario's order, written between its drop, close or silence and the
  probe's read after the window, other than for a fill, an interrupt or an error.
- A market armed under `CodProbeRun` on a hand seed that was not flat, or a market other than
  the one it names.
- A nonce issued twice on the account across the two sessions.
- A place or cancel of the probe that reached Hibachi without an fbc-oms authorization.
- A cancel of the probe that a cap or the kill switch held.
- Hibachi documenting what `orders.enableCancelOnDisconnect` does, or the owner getting Hibachi
  testnet trading access. Either may make a mainnet probe unnecessary.
