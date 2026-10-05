# 0048 — A plan's round may name the next, built from an EncodeCtx when it is sent, and Paradex logs in that way, augmenting 0035 and 0043

Status: accepted
Date: 2026-10-05

## Context

0035 made `HttpPlan<T>` one round of requests, built when the plan is made, and a parser; 0043
made `test_connection` such a plan and recorded what it could not express: a request that
signs a timestamp, and a second request that carries the first one's answer (a login whose
token then reads the account). Paradex needs both (FBC-mz1, BT-402): its `POST /auth` is
signed at the time it is sent (`PARADEX-TIMESTAMP`, `PARADEX-SIGNATURE-EXPIRATION`, both in
seconds), and its account read carries the session token the login answers with. Codex
r4180237818 on FBC-b3b set the condition any extension must meet: nothing of a later step,
its nonces included, may be reserved before the answer that decides whether it is sent.

FBC-mz1 also builds Paradex authentication itself in `src/auth` (0009): the login from the
consumer's `Secrets`, the token's reading, journaling (0028) and use, and its refresh, which
the Java library keeps apart from the signature's expiry (`paradex.jwt.refresh.seconds`, 60 s,
against a one-hour signature).

## Decision

This augments 0035 and 0043 (it supersedes nothing there):

1. **A round may name the next.** `HttpPlan::parse` returns `PlanStep<T>`: `Done(T)`, or
   `Next(NextRound<T>)`. `HttpPlan::new` is unchanged (its parser's result is `Done`);
   `HttpPlan::then` takes a parser that returns a `PlanStep`. A `NextRound` states how many
   nonces it signs with and is built by `NextRound::build(ctx)` from the `EncodeCtx` the
   runtime fills when it is about to send that round: its wall time and exactly that many
   nonces. `HttpPlan::later(nonces, build)` is a plan whose first round needs a context: it
   asks for nothing, and its parse of no answers is that round. The runner's loop is: make
   the requests, parse the answers, and on `Next` fill a context and build the next round.
2. **Nothing is reserved ahead of the answer that decides it.** A `NextRound` exists only once
   its parser has read 2xx answers; a failed request or a non-2xx status ends the plan before
   the parser runs, so no later round is built and no nonce reserved for it. The parser still
   gets no `EncodeCtx` and no `Effects`.
3. **`PlanError::Sign(SignError)`** says a round's request could not be signed (a time before
   1970, an expiry past `u64` seconds); like every `SignError` it carries no key material.
4. **Paradex authentication lives in `crates/venues/fbc-venue-paradex/src/auth/`.**
   - `Login::new(cfg, creds)` moves the account address (`paradex.account.address`) and the
     Stark key (`paradex.private.key`, the Java library's names) out of `Secrets` into a
     `ParadexSigner`, and reads the chain id (`paradex.chain.id`: hex, decimal or the chain's
     name), the REST base (`paradex.rest.url`, a host and the path `/v1` alone, since the
     login is signed as `/v1/auth`, Codex r4186295475; its host parsed when configured as a
     DNS name, an IPv4 address or a bracketed IPv6 address with an optional port 1 to 65535,
     Codex r4186547040; `https://`, or `http://` only to a loopback host, `127.0.0.0/8`,
     `::1` or `localhost`, for a test stub, the owner's review), the signature lifetime (`paradex.auth.signature.lifetime`, whole seconds, at
     most the one week Paradex takes, Codex r4184897007),
     the refresh interval (`paradex.jwt.refresh`) and the request timeout
     (`paradex.rest.timeout`), all Account scope and all required: no number is defaulted in
     code. A missing or invalid key is refused by its name, never its value. The key is the
     account's own Stark key: a registered trading subkey logs in through
     `/auth/{public_key}`, whose signed path Paradex does not document and the Java library
     never used, and a subkey cannot be told from the main key by its bytes, so the field says
     so and subkey login is FBC-2vhg (Codex r4185155410).
   - `Login::request(ctx, tag)` is the login as an `Effect::Http`: `POST /auth` with the four
     documented headers in that order, timestamp `ctx.wall` in whole seconds, expiry that plus
     the lifetime, an empty body, safety traffic. The signature header is redacted (anyone
     holding it can mint a token until it expires), and so is the account header (an account
     address is private, 0009).
   - `SessionToken` holds the token as a `Secret` (not `Clone`, zeroed on drop); its `Debug`
     shows its length. `SessionToken::read` takes the answer's `jwt_token` and refuses one
     outside the JWT alphabet (letters, digits, `-`, `_`, `.`), so it stands verbatim in a
     JSON string and a header value. It goes out only as `ws_frame(id)`, the JSON-RPC `auth`
     frame with `params.bearer` inside its one redaction span, and `header()`, the
     `Authorization: Bearer` header marked redacted.
   - `token_spans(resp)` names the token for `redact_inbound`: every copy of the `jwt_token`
     string in the body, overlapping copies as one span, when the body mentions `jwt_token`
     once and holds no escape; nothing for a body that is empty, or JSON that neither mentions
     `jwt_token` nor escapes anything (a refusal); otherwise the whole body: not JSON, a token
     that is not a non-empty string, a repeated or nested key (a parsed object keeps only the
     last of repeated keys, Codex r4184896990), or an escape that could spell the key or
     token another way.
   - `LoginCycle` makes logins as effects for an order-entry codec: one on `start`, one on
     each firing of its refresh timer, which `on_answer` sets for the configured interval
     after every login answer, failed ones included, whatever the answer holds. It keeps the
     last token a login gave until another replaces it.
   - `test_connection` is `HttpPlan::later(0, ..)`: round one the login, built from the
     context it is sent under; round two, named once the login's answer gave a token,
     `GET /account` with the token's header. The summary is `account`, and `account_value`
     in `settlement_asset` truncated toward zero at a nanounit, or no equity when the answer
     lacks either, both optional in Paradex's documentation (Codex r4186547050). The signer is dropped, and its
     key zeroed, once the login is built.
   - The factory declares the REST limits these requests are charged against (Codex
     r4185685704), from Paradex's "API Rate Limits" table: the login (`Rest`) 600 per minute
     per IP; private GETs (`Query`) 120 per second and 600 per minute per account; and 1500
     per minute per IP across both.

## Alternatives

- A separate credentials plan type with per-step `EncodeCtx`s, FBC-b3b's first design:
  rejected again (0043); it is the parallel mechanism this record avoids, and it reserved a
  follow-up's nonces before the answer that decided it.
- Giving the parser an `EncodeCtx` or `Effects`: rejected. The parser would then sign while
  reading an answer, with time and nonces reserved before the answer arrived.
- Keeping `parse` returning `T` and adding a second method for rounds: not taken; one call
  with one result type keeps one runner loop, and its only callers were tests.
- A defaulted signature lifetime and refresh (Java's one hour and 60 s): rejected; a number a
  mechanism needs comes from the consumer's configuration (AGENTS.md).
- Scheduling the next login from the token's own expiry (its `exp` claim): rejected; the
  ticket and 0028 forbid reading the token's bytes, which replay blanks.
- Refreshing only after a successful login: not taken; a transient failure would stop the
  refresh for good, while the last token lapses.
- Leaving the account address header plain: not taken; the ticket requires only the signature
  redacted, but a plain account would reach every `Debug` of the request.

## Consequences

- A plan driver (still to be built beside 0027's HTTP effects, as 0043 says) loops over
  rounds and reserves each round's nonces only when it builds it.
- A plan has no `redact_inbound` yet: Paradex's Test Connection receives a token in its
  login answer, so the driver must not journal plan answers until a plan names their spans
  (FBC-f65). Nothing journals plan answers today.
- The Paradex order-entry codec (FBC-xzp) holds a `LoginCycle`, routes the login's answer and
  the refresh timer to it, answers `redact_inbound` for the login with `token_spans`, and
  sends `ws_frame` or `header()` with the current token. Its config schema already lists the
  auth keys.
- Discovery's parsers return `PlanStep` through `HttpPlan::new` unchanged; a test reads a
  one-round result with `PlanStep::done`.

## What would show this was wrong

- A venue whose next round must be decided by something other than the previous round's
  answers (a timer between rounds, a WebSocket message), which `NextRound` cannot wait for.
- A Paradex token outside the JWT alphabet, or a login answer whose token `token_spans`
  misses, found in a journal.
- Paradex refusing a login signed under a REST base that ends in `/v1`, or a token that lapses
  before the configured refresh while the cycle keeps it.
