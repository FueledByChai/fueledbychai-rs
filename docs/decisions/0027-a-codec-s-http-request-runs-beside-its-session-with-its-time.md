# 0027 — A codec's HTTP request runs beside its session with its timeout and comes back only to the epoch that asked; a venue's plan is applied by stream

Status: accepted
Date: 2026-10-04

## Context

FBC-klr makes `fbc-runtime` the executor of the HTTP requests a market-data codec asks for
(0014 item 3), of connectionless poll endpoints (0014 item 9), and of `VenueFactory::plan_md`
(0014 item 4, design §4.7). The Binance diff-depth anchor and the Paradex REST snapshot depend on
it (BT-201). 0023's session refused `Effect::Http` as a codec defect until this ticket; that
refusal ends here, and 0023's other decisions stand. Three choices were not fixed by 0014: where
a request runs relative to the session's reads, how a failure is classified when the timeout
passes before anything was written, and how a changed plan reaches running connections.

## Decision

- **Beside the reads, in the same task.** A request runs as a future the session polls beside
  its socket, timers and control (no spawned task, 0023's one thread), on the session's
  `Connector`, so the consumer's proxy applies. Its result is stamped as an input of the epoch
  that asked: while that epoch is current it goes to that codec's `on_http`, with the
  `SpecTable`, inside the venue's decode scope, and the effects it asks for are executed; once
  the stream has moved to a newer epoch it is dropped and counted (`Input::Http`), so a
  snapshot asked for before a reconnect never anchors the new epoch's book. Requests keep
  going while a frame's write waits on the peer; a result that comes back meanwhile is
  stamped and goes to the codec as it comes, so the handler gets the shard's events in ingest
  order, and the effects it asks for are executed after the rest of that write's batch (Codex
  r4177481297, r4177547758, r4177698441). Requests still in flight when a session stops are
  dropped.
- **Classification.** The request's mandatory `timeout` runs from when the codec asked for it
  and bounds the whole call; a timeout past the end of the clock bounds nothing, so the
  request is not sent and comes back `NotSent` (Codex r4177481307). A failure before
  the connection is open (a request the runtime cannot make, the connect, the proxy, TLS, or
  the timeout passing meanwhile) wrote no byte of the request: `NotSent`. After that, the
  timeout passing is `TimedOut`, and any other failure is `Lost`, including a response body
  over the session's `http_max_body`, a number the consumer configures (0009): the request may
  have been written and acted on. A request's URL is absolute; one that is not is `NotSent`.
- **Poll endpoints** have one epoch, begun when the session runs: `on_open` is called as soon
  as the codec is built, `subscribe` takes the reconciler's difference as for a socket, and an
  `Effect::Send` or `Effect::Reconnect` naming the endpoint is refused and counted. A session
  whose control has already dropped builds no codec, so it asks for nothing (Codex
  r4177698436).
- **Plans.** `MdVenueControl::set_desired` calls `plan_md` with the spec table at once and
  returns a refusal (`UnknownInstrument`, `UnsupportedFeed`, configuration), a stream named
  twice, or a socket URL no attempt could open as a `PlanError`; nothing opens or closes for a
  refused plan and the last accepted plan stands. Dropping the control stops the venue; a plan
  published just before the drop is not applied, the venue sees the drop before it polls any
  session again, and a session sees its own control's drop before it starts an attempt that
  fell due at the same time (Codex r4177481301, r4177547754). `MdVenue::run` applies the latest accepted
  plan by `StreamId`: an endpoint planned again with the same transport keeps its connection
  and its reconciler sends the difference; a new endpoint, or one whose transport changed,
  opens a fresh session under the next connection number of the consumer's range for the
  venue; one no longer planned is closed. All of a venue's sessions run in the task that runs
  the venue and share the consumer's one handler.

## Alternatives

- Spawning a task per request: rejected; it puts a second thread of control beside the shard
  (0023) and a hand-off queue between the response and the codec.
- `TimedOut` for every timeout, wherever it fell: rejected. A timeout before the connection
  opened wrote nothing, and order entry (BT-402) must not treat a request the venue never saw
  as possibly acted on.
- Planning inside the running venue and reporting refusals through a counter: rejected; the
  caller that changed the set is the one that must learn the venue refused it.
- Moving a subscription by closing and reopening every endpoint: rejected; it drops data on
  endpoints the change did not touch and spends connection attempts against the shared IP.

## Consequences

- A codec can ask for any number of requests; nothing bounds them until the rate limits
  (FBC-bel). A dropped stale result is counted but not yet journaled (FBC-f3w).
- The consumer gives each venue a range of connection numbers disjoint from the shard's other
  connections; a venue whose range is spent stops with `SessionError::NoConnectionLeft`.
- A codec must not rely on a request it asked for outliving its epoch.

## What would show this was wrong

A venue whose snapshot must be requested before its stream reconnects and applied after it, or a
consumer that needs a venue's endpoints on different threads, which would want a hand-off of
its own in a new record.
