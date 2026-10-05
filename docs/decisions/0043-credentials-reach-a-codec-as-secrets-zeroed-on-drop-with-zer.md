# 0043 — Credentials reach a codec as Secrets, zeroed on drop with zeroize 1.9.0 pinned, and test_connection is an HTTP plan, augmenting 0014

Status: accepted
Date: 2026-10-04

## Context

Design §4.7 gives `VenueFactory::exec_codec` a `creds: Secrets` argument and declares
`test_connection(cfg, creds) -> HttpPlan<AccountSummary>`, but defines neither type. FBC-5 left
both out (0014), because a type that holds a private key, JWT or API key must live in a crate's
`src/auth` (0009), a review path. FBC-b3b adds them; Paradex authentication (FBC-mz1) and every
order-entry codec built from credentials (FBC-xzp) wait on it. The design names the types only;
how a credential is kept, shown, copied and freed was open, and zeroing memory needs a
dependency, which the ticket allows only through a record. What a plan is, 0035 has since
fixed for discovery (FBC-ahf).

## Decision

- **`Secrets` and `Secret` live in `crates/fbc-core/src/auth.rs`.** `Secrets` maps the
  configuration keys a venue's schema names (`&'static str`) to `Secret` values; the consumer
  builds it from its own store for each call. A `Secret` holds one `String`, taken by move, so
  building one leaves no other copy; `Secret::expose` is the only way to read it, for a venue's
  `src/auth` or `src/sign` code.
- **Nothing shows a credential.** `Secret`'s `Debug` is `Secret(<redacted>)` and its `Display`
  `<redacted>`, without the length; `Secrets`' `Debug` lists the keys set and its `Display`
  their number. Neither type is `Clone` (a trybuild test proves it): a credential exists once,
  and a copy never drifts into a log.
- **Zeroed on drop with `zeroize` 1.9.0, pinned exactly.** A `Secret` holds its value in
  `Zeroizing<String>`, which overwrites the whole buffer, spare capacity included, when it is
  dropped, replaced in a `Secrets`, or dropped unread by a venue that takes no credentials.
  The workspace already pins `zeroize` `=1.9.0` with default features off (FBC-dm9, for the
  Paradex signing key); `fbc-core` gains it as a normal dependency and turns on its `alloc`
  feature, which the `String` impl needs. `zeroize` is Apache-2.0 OR MIT (inside 0017's
  allowlist) and has no dependencies with these features, so the lockfile gains no crate.
- **`exec_codec(cfg, creds: Secrets)`** takes the credentials by value, so the codec moves out
  what it keeps. A missing key is `VenueError::Config(ConfigError::Missing(key))`, naming the
  key, never a value. The design's `nonces` argument stays out, as 0014 item 2 decided.
- **`test_connection(cfg, creds) -> Option<Result<HttpPlan<AccountSummary>, VenueError>>`**,
  with `HttpPlan` as 0035 defines it for discovery: one round of `Effect::Http` requests built
  when the plan is made, and a parser that reads their 2xx answers inside a `DecodeScope`; a
  failed request or a non-2xx status (a refused key's 401) ends it with `PlanError::Http` or
  `PlanError::Status` before the parser runs. `None` for a venue that takes no credentials, as
  `exec_codec` is `None` for a market-data-only venue. The factory builds the requests from
  the credentials and drops them; the parser needs none of them. No `PlanError` variant is
  added: `Status`, `Http`, `Decode` and `Missing` say why a test failed, and none carries a byte
  the venue sent.
- **No nonce is reserved for a call that may not sign.** A plan asks for every request before
  it sees any answer, and its parser gets no `EncodeCtx` and no `Effects`, so it cannot sign or
  ask for more after an answer. Nothing is reserved for a follow-up that a 401 would cancel
  (Codex r4180237818 on the earlier step-by-step design).
- **`AccountSummary { account: String, equity: Option<Money> }`** is private account data
  (0009): its `Debug` shows the account by length and the equity only as present or not.

## Alternatives

- The `secrecy` crate: not taken. It wraps `zeroize` in the same way and adds a second pinned
  dependency and its own trait vocabulary for two small types.
- Hand-written zeroing with `ptr::write_volatile`: rejected; it needs `unsafe` and a compiler
  fence that `zeroize` already gets right, and the crate is already in the tree.
- `Secret` as bytes (`Vec<u8>`): not taken. Every credential the planned venues use is text
  (hex keys, API keys and secrets), and the consumer stores them as strings.
- A `Clone` on `Secrets`, so one set can serve `exec_codec` and `test_connection`: rejected by
  the ticket ("no Clone into logs"); the consumer reads its store again instead.
- A second plan type for credentials, a trait whose `start` and `on_http` steps each get an
  `EncodeCtx` with the nonces `nonces_for(call)` asks for: rejected, so the core keeps one
  `HttpPlan`, 0035's. That design also had to reserve a follow-up's nonces before it saw the
  response that decides whether the follow-up is sent at all.
- `test_connection` without `Option`, every venue proving something: not taken; a reference
  venue such as Binance USD-M has no account to prove.

## Consequences

- Every `VenueFactory` implements `test_connection`; the market-data-only venues return `None`
  and drop the credentials they are handed, which zeroes them.
- Bytes a codec or plan writes into a request it asks for (a redacted `Header` value, a
  `WireSlice` credential span, a signature input) are ordinary memory, not zeroed: `zeroize`
  covers only what a `Secret` holds. The journal still keeps them only as keyed hashes (0024,
  0028). Zeroing outbound request buffers once written is a ticket of its own (FBC-96h).
- The runtime has no driver for an `HttpPlan` yet; the consumer's Test Connection needs one,
  built beside the HTTP effects of 0027, the same driver discovery needs.
- A plan is one round, its requests built without a clock or nonces. A venue whose check needs
  a signed timestamp, or a second request that uses the first one's answer (a login whose token
  then reads the account), cannot express it yet. Paradex authentication (FBC-mz1) extends
  `HttpPlan`, through a decision record, when it builds the first such check.
- Changes to `src/auth.rs` wait for the owner's review (0009), as every credential change does.

## What would show this was wrong

- A credential found in a log, an error or a journal file that came through a `Secret`'s or a
  `Secrets`' formatting, or through a copy `Secret` made.
- A venue whose credential is not text, or whose test of its credentials needs more than HTTP
  requests (a WebSocket login), so `HttpPlan` cannot express it.
- Extending `HttpPlan` for a login and a read (FBC-mz1) needing a separate plan type for
  credentials after all.
- `zeroize` failing to install on the pinned toolchain, or a version move changing what it
  zeroes.
