# 0083 — The order-entry conformance checks run fbc-runtime's session against replies the adapter states, on a paused clock, with fbc-oms authorizing, augmenting 0025 and 0047

Status: accepted
Date: 2026-10-08

## Context

BT-502's suite names three checks that need the venue answering over a connection (FBC-3il):
`amend_ack` (an accepted amend surfaces as an `OrderUpdate` in state `Amended`, synthesized from
the reply where `AmendCaps.ack` is `RpcReplyOnly`), `mixed_batch` (a batch with items accepted,
rejected and unanswered gives each item its outcome, the timeout marking the unanswered ones
`Unknown`) and `unknown_on_timeout` (an unanswered request becomes `Unknown` and is never
written a second time; 0005, 0014). The ticket has them run fbc-runtime's order-entry session
against the stub server answering from the requests it reads (0047), with every command
authorized through fbc-oms as a live consumer's is (0045). The suite knows no venue's protocol,
a request's deadline is the codec's own (five seconds for the toy), and the session takes its
venue for the program's life (`ExecSessionConfig::venue` is `&'static dyn VenueFactory`).

## Decision

- **The adapter states its replies.** `Setup` gains `order_entry: Option<OrderEntryStub>`: a
  function pointing the venue's configuration at the stub, the HTTP routes its order entry
  reads, one `Responder` per frame the session writes as an epoch opens (authentication, the
  cancel-on-disconnect arm, a resync showing the account flat with nothing resting), and a
  `Replier`, the venue's answer to a request whose items are each answered as an `Answer` says
  (`Accept`, `Reject`, `Silent`). An acceptance is answered as the venue reports it: the replaced
  order's event where `AmendCaps.ack` is `ReplacedEvent`, the reply alone where `RpcReplyOnly`.
  The setup is built afresh for each check, so a replier keeping state (the venue ids it gave)
  starts clean. A venue declaring order entry with no stub fails the three checks
  (`Setup.order_entry`); one declaring none is skipped by name.
- **A paused clock the check moves.** Each check builds a current-thread runtime with tokio's
  clock paused and a blocked thread keeping it from moving on its own while the sockets are
  idle, as fbc-runtime's own order-entry tests do; the check moves it in 100 ms steps (1 s
  steps after the first 30 s), up to 600 s, only once the stub has answered, so a deadline passes when the check says and never
  while an answer is on its way. fbc-conformance therefore takes tokio's `test-util` in its
  normal dependencies; it is only ever a dev-dependency of the crates it checks.
- **fbc-oms authorizes every command.** A registry under lease names of its own is resynced
  flat, started on the setup's first instrument with leases in a directory the check removes,
  and builds and authorizes each place, batch and amend through `Registry::authorize`; a
  placement's outcome is handed back to it before its amend. fbc-conformance depends on fbc-oms,
  and on fbc-journal for the session's nonce source id.
- **The factory lives for the program.** The three checks take `Subject<'static>` and run
  through `suite::run_live`; the macro's factory is a constant expression (a unit struct or a
  `static`), so `&$factory` is `'static`. The other checks keep `Subject<'_>`.
- Each check judges what the session reported (outcomes by request and item, order updates) and
  what the stub received (a request's frame written once).

## Alternatives

- The real clock, waiting out each deadline: rejected; a check would last at least the venue's
  deadline, and a keepalive or other timer could fire between the stub's steps and take a reply
  meant for a request.
- A scripted venue state in the kit answering every venue alike: rejected, as in 0047; the kit
  knows no venue's protocol.
- Authorizing with a test-only issuer instead of an armed registry: rejected; it would be a path
  around 0013 rule 2, which the owner keeps closed (RB-0ga-1).
- Make `ExecSession` borrow its factory: out of this ticket's scope; the runtime's session is
  run-once and owns everything it needs.

## Consequences

- An adapter crate's `tests/conformance.rs` setup states its order entry's replies in its own
  protocol, parsing its own request format there. Its fixtures stay synthetic (0009).
- A venue whose resync is untrustworthy (`SnapshotSource::Untrustworthy`) is armed by the
  check's registry only as fbc-oms allows; a refusal fails the check naming fbc-oms.
- The three checks cannot be called from inside a tokio runtime: they build their own.
- `test-util` is unified into any build that also builds fbc-conformance (the workspace's, the
  owner-run samples, which take it as a dev-dependency); it adds the clock's pause and advance
  and changes nothing that does not call them.

## What would show this was wrong

A venue whose order entry cannot be answered frame by frame in the order written (replies the
stub must send unprompted, or frames written in an order that depends on timing), or a
deadline longer than 600 s, or a build where `test-util` changes a non-test behaviour.
