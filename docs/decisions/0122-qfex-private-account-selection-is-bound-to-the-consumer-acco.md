# 0122 — QFEX private account selection is bound to the consumer account before planning

Status: accepted
Date: 2026-10-22

## Context

QFEX's [HMAC authentication](https://docs.qfex.com/websocket/channels/trade/authenticate)
uses an optional `params.account_id`: omitted means the primary account and a UUID selects a
subaccount. The [API key scope](https://docs.qfex.com/api-reference/api-keys) may cover one or
all accounts. `ExecSessionConfig.acct` is a consumer-local `AccountKey`; the venue factory's
configuration and credential hooks did not receive it. Decision 0121's account-wide COD
authority matched that local key but could not check whether QFEX authentication selected the
same provider account. FBC-lqa8 requires primary and subaccount support without a silent
primary fallback.

## Decision

1. A QFEX private configuration explicitly names `qfex.account_id` as `primary` or a canonical
   subaccount UUID and `qfex.account_key` as the consumer's canonical decimal `AccountKey` for
   that provider account. The consumer attests this mapping. Missing or invalid values refuse
   before private endpoint planning; there is no default to the primary account.
2. `VenueFactory::exec_account_key(cfg)` returns the configured local key for a venue that
   requires this binding, with `Ok(None)` as the default for existing venues. `ExecSession`
   checks any returned key against `ExecSessionConfig.acct` before caps or endpoint planning.
   Decision 0121 separately requires explicit account-wide COD authority for the same key.
3. QFEX's codec omits `params.account_id` only for an explicitly selected primary account and
   includes the selected UUID for a subaccount. It matches the configured public key against
   the `Secrets` credential before building an authenticated frame. The public key and
   subaccount UUID are account-identifying and redacted from wire debug/journal spans.
4. QFEX remains `Readiness::Record` with `caps.exec: None`. A factory can construct the private
   plan and codec for offline synthetic proof, but no `ExecSession` can open it until a later
   decision establishes complete order/fill/resync truth, private conformance, human review and
   owner-authorized provider acceptance. An undecoded private event or incomplete resync is
   never converted into an accepted command or a ready session.

## Alternatives

- Infer the local key from an API credential or nonce lease: neither identifies the session's
  selected provider account, and a key may have access to multiple accounts.
- Let a missing `account_id` select primary: that could trade the primary account when a
  subaccount setting was omitted or malformed.
- Declare QFEX execution capabilities now: the published examples do not establish command
  causality, fill fee sign, snapshot consistency or complete resync behavior.

## Consequences

The library catches an internal `AccountKey` mismatch before any socket opens and preserves
subaccount selection through authentication and reconnect. It cannot prove the caller's
provider-ID-to-key mapping or a key's server-side scope offline; owner-authorized acceptance
must verify those against QFEX. The generic hook adds one defaulted factory method; existing
venue behavior is unchanged.

## What would show this was wrong

Evidence that QFEX chooses a different account despite the authenticated `account_id`, or a
generic session mechanism that can verify provider identity without a caller-attested mapping.
