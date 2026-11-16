# 0150 — A serving script is a second kind of WsScript that answers every frame of every connection through a responder made for it and pushes and closes on demand, for venue models whose frame count and order no timeline can know, augmenting 0025 and 0047

Status: accepted
Date: 2026-11-16

## Context

0025 made a `WsScript` one timeline of `Step`s, each naming the connection it acts on, and
0047 let a `Step::Respond` answer the frame it reads with what a `Responder` computes, a
function that needs state keeping it itself. A timeline states in advance how many frames each
connection sends and in what order the connections interleave.

The QFEX place stages' stub tests (FBC-c6kw and the tickets after it, story BT-702) need a
venue model that no timeline can state: several connections open at once (a trade session
beside read sockets that never arm, and a pager), each answered according to the frame it
receives, in counts and orders that depend on the code under test, and unsolicited frames (order
updates, fills) and a server close sent on an open connection when the model decides. 0025 named
what would show it wrong: a fault that a sequence of its steps cannot express. Concurrent
connections whose frame count and order the client decides are such a fault (FBC-0lng).

## Decision

- **A serving script is a second kind of `WsScript`.** `WsScript::serving(responder)` returns
  the script and its `Pusher`. The stub accepts every connection and answers each data frame of
  connection `n` (numbered in accept order, as its record is), in the order they arrive, with
  the frames `responder(n)` computes from it, every connection at once, until the stub is
  dropped. `responder(n)` is called once, when connection `n`'s upgrade completes. The stub
  records every connection as it does for a step script: its frames, whether it is open, and
  whether the client sent a close frame (`client_closed`, a close told from a drop).
- **The responder is 0047's.** It is a typed Rust function; a model that needs state keeps it
  itself, in what the responder or `responder` captures, shared between connections when the
  model needs that. The stub knows no venue protocol, and a venue's model lives in the tests
  that use it.
- **A refused answer fails itself alone.** A responder's refusal or a panic in it fails that
  answer; a panic in making a connection's responder fails each answer of that connection. The
  stub goes on serving that connection and every other. Each failed answer is reported as a
  script failure, `ScriptError::Answer` naming the connection and the frame's place in its
  record: `StubServer::failures` lists them all, and the first is the script's result, so a
  serving script's `finished` never returns `Ok`: it returns the first failure, while the stub
  goes on serving. An answer whose write does not go, the connection closing or the client
  gone, fails nothing.
- **A listener that fails is reported.** A serving script has no `Step::Accept` to fail, so an
  accept error on the stub's WebSocket listener, after which it accepts no connection, is the
  script's failure, `ScriptError::Listen`; the connections already accepted are still served.
- **The pusher acts on demand.** `Pusher::push` sends an unsolicited frame, and `Pusher::close`
  and `Pusher::close_with` a close frame, the latter with a code and a reason, as `Step::Close`
  and `Step::CloseWith` do, the stub recording until the client's close reply. Each goes on
  that connection in the order asked among its answers, and each is refused, nothing sent, on a
  connection closed or never upgraded.
- **A serving script takes no steps and is started on one stub only, ever,** so each push names
  one connection; either refused, the stub does not start. It counts as started once the stub's
  endpoints are bound. Its position (`StubServer::played`) is the number of frames it has
  answered, a frame counting once its answer was attempted or refused.
- **Step scripts are unchanged:** 0025's and 0047's decisions stand for them.

## Alternatives

- More steps in a timeline (one `Respond` per expected frame, per connection): rejected. The
  count and the interleaving are what the code under test decides, and a `Respond` waiting on
  one connection holds every later step while another connection's frame waits.
- A serving step inside a timeline: rejected. The timeline would still have to place each
  connection's accept, and nothing after the step could run while it serves; concurrency is
  the point.
- A scripted venue state in the kit: rejected again, as in 0047. The model and its state belong
  to the tests of the crate that knows the venue (FBC-c6kw puts QFEX's in fbc-examples' tests),
  and fbc-conformance stays venue-free.
- Unsolicited frames only as part of an answer: rejected. A model sends an order update or a
  fill when it decides (after another connection dropped, on a connection that sent nothing),
  not only in reply to a frame.
- A push that does not wait for its write: not taken. Waiting lets a push on a closed
  connection be refused rather than lost.

## Consequences

- A venue model whose frame count and order no timeline can know is a responder function in the
  test, its state its own, with a `Pusher` for what it sends unasked.
- A serving script's `finished` waits for a failure, so a test checks that `failures` is empty
  at its end instead of awaiting `finished`. What a serving script answers with nothing gives a
  test no frame to wait on, so the test waits on the record (the answers counted, the failures,
  the connections).
- The stub has two kinds of script to keep. A step-script control (a barrier, a silence) is not
  available on a served connection until a test needs one.

## What would show this was wrong

A venue model that needs a step-script control on a served connection, or timing within an
answer, which would want the two kinds merged or a new step; or models whose responders every
test rebuilds the same way, which would want a scripted venue state in the kit (0047).
