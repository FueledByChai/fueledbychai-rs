# 0045 — Order entry reaches a gateway only with an authorization fbc-oms issues, and the gateway traits live in fbc-oms, refining design 4.8

Status: accepted
Date: 2026-10-05

## Context

0013 rule 2 puts the inventory and resting caps and the per-market kill switch in the one path
every order command takes, `fbc-oms`, and 0012 says what the kill switch blocks. FBC-5 declared
`OrderGateway::submit(acct, cmd, ctx, t)` in `fbc-core` as design §4.8 has it, and
`VenueCommand` is a plain value anyone can build. Once a live gateway (runtime, exec codec and
signer) exists, a consumer holding it could submit a place, an amend, a batch, a cancel or an
account cancel-all around every check (Codex r4172231026 on PR #8; 0014 left it to FBC-ob2).

Sprint-3 planning (2026-10-04) settled the shape: the authorization type lives in `fbc-oms`,
`fbc-runtime` takes `fbc-oms` as a normal dependency (FBC-0ga builds the live gateway on this
record), `fbc-oms` never depends on `fbc-runtime`, and the authorization carries the market's
state generation, which FBC-afd's check at submit reads. `fbc-core` cannot name an `fbc-oms`
type, so this record also says where the gateway trait goes. It departs from design §4.8's
`submit(acct, cmd, ctx)`.

## Decision

- **An authorization.** `fbc_oms::Authorization` holds one order-affecting command for one
  account and one market, with that market's `StateGeneration` when it was issued. Only
  `fbc-oms` builds one (private fields, a crate-private constructor, no `Default` and no
  conversion from a command); it has no `Clone`, its command is read only through a shared
  reference, and submitting consumes it, so each one is spent once. `StateGeneration` cannot be
  made outside `fbc-oms` either; a market's generation advances on every change of its state
  (the market states are FBC-c4v's).
- **What it covers.** A place, an amend, a batch of places, a cancel, a cancel-many and an
  instrument cancel-all: every order-affecting command, cancels included, so 0005's I4 and I7
  (FBC-lrc, FBC-gzm) also sit in the one path. `fbc-oms` issues none for an account cancel-all
  (no record admits one; 0005's I7 admits only the instrument one), for an empty batch, or for a
  batch whose items name more than one market, since an authorization carries one market's
  generation. Whether a given command may be issued at all (caps, kill switch, market state,
  permits) is decided before issue by the tickets that build those checks (FBC-2e4, FBC-zf7,
  FBC-c4v, FBC-lrc, FBC-afd); until they land nothing outside `fbc-oms`'s own tests issues one.
- **The gateway traits move to `fbc-oms`.** `OrderGateway` and `ManagedGateway` leave
  `fbc-core` for `fbc-oms`, which already depends on `fbc-core`; a gateway's crate depends on
  `fbc-oms`. `SubmitHandle` stays in `fbc-core`. The trait has two methods:
  - `submit(auth: Authorization, ctx: &EncodeCtx, t: &mut PathStamps) -> SubmitHandle` for
    order-affecting commands. The account is the authorization's, so there is no separate
    `acct` argument to disagree with it.
  - `submit_control(acct, cmd: ControlCommand, ctx, t) -> SubmitHandle` for a command that
    affects no order. `ControlCommand` has a query (the Unknown ladder's), a fee query, a
    dead-man refresh and turning cancel-on-disconnect on, and no variant or conversion that
    carries an order-affecting command. Turning cancel-on-disconnect off has no path: it removes
    the protection every resting order relies on, and no record yet says who may ask for it.

## Alternatives

- Build the live gateway only inside `fbc-oms` and never hand it to the consumer: rejected in
  sprint-3 planning. `fbc-oms` would have to own sockets and the runtime, against design §3's
  dependency direction, and the simulated venue would need its own way in.
- Keep `OrderGateway` in `fbc-core` with a generic authorization parameter: rejected. Any
  implementation could pick its own parameter type, so the trait itself would prove nothing.
- One `submit` taking an authorization for every command, queries included: rejected. A
  query, a dead-man refresh and turning cancel-on-disconnect on must go out whatever the market's
  state, and tying them to a state generation would let a kill switch delay the Unknown ladder.
- An authorization per batch item: not taken. A batch goes out as one request with one
  outcome per item; the checks are per item (FBC-2e4), and one authorization covers the batch
  they admitted. Its items cannot be added to or replaced after issue.

## Consequences

- `fbc-core` no longer declares `OrderGateway` or `ManagedGateway`; its toy venue's gateway
  is a test struct with an inherent submit. 0014's "Alternatives" entry that deferred the
  authorization to FBC-ob2 is answered here.
- `fbc-runtime`'s live gateway (FBC-0ga) implements `fbc_oms::OrderGateway`, encodes
  `Authorization::command()`, and calls `fbc-oms`'s check at submit (FBC-afd) before encoding.
- The compile-fail cases in `crates/fbc-oms/tests/ui_authorization/` prove that code outside
  `fbc-oms` cannot build an authorization for any covered command, edit one, clone one or submit
  one twice, and that the control path carries no order-affecting command.

## What would show this was wrong

- A place, amend, batch item or cancel reaching a venue without an authorization `fbc-oms`
  issued, or one authorization reaching a venue twice.
- A venue whose batch must span several markets in one request, which one market's
  generation cannot cover.
- A need to turn cancel-on-disconnect off from the consumer, which then needs a record saying
  who may.
