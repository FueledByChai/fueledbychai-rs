# 0087 — The order-entry checks' own registry takes the suite's flat snapshot as trustworthy whatever the venue declares, augmenting 0083

Status: accepted
Date: 2026-10-08

## Context

0083 has the order-entry checks build, bind and start an fbc-oms registry on the setup's first
instrument after a resync "showing the account flat with nothing resting", a snapshot the suite
makes itself and that the stub answers by construction, and its Consequences said a venue whose
resync is untrustworthy "is armed by the check's registry only as fbc-oms allows; a refusal
fails the check naming fbc-oms". Paradex declares `OrderCaps.snapshot_source` `Untrustworthy`
(0054), so under 0055 that resync seeds no position, the owner's Start refuses
`PositionUnknown`, and all six order-entry checks (`amend_ack`, `mixed_batch`,
`unknown_on_timeout`, `resync_after_reconnect`, `two_phase_ack`, `reject_coverage`) fail before
they exercise the venue's codec, on any venue declaring `Untrustworthy` or `None` (FBC-wyv2,
found running the suite on Paradex for FBC-6oj). The refusal says nothing about the adapter: it
is fbc-oms keeping the owner's rule that nothing is sent before the first trustworthy resync
(0013 rule 1), applied to a snapshot no venue gave.

## Decision

The checks' registry resyncs the suite's own flat snapshot under a copy of the venue's
`OrderCaps` whose `snapshot_source` is `Trustworthy`, whatever the venue declares; every command
is still built from and authorized under the venue's own caps through `Registry::authorize`
(0083). This replaces 0083's Consequence quoted above. Nothing in fbc-oms, fbc-runtime or any
venue crate changes: a live consumer's resync still seeds only from a venue declaring its
snapshot trustworthy (0055), and a venue's declared snapshot source is judged by the venue's own
evidence, never by this suite.

## Alternatives

- Seed by hand under `Registry::for_testnet_run(TestnetRun::owner_assisted())`: rejected; 0067
  reserves that declaration for the owner-assisted testnet run, and a conformance suite
  declaring it would make the declaration routine.
- Leave the checks failing on such venues, or skip them by name: rejected; Paradex is the venue
  the suite most needs to exercise before testnet (FBC-6oj), and a skip would prove nothing
  about its order entry.
- A test-only authorization path around the registry: rejected, as in 0083 (0013 rule 2,
  RB-0ga-1).

## Consequences

- The six order-entry checks start fbc-oms and exercise the adapter on venues declaring
  `Untrustworthy` or `None` as on one declaring `Trustworthy`; a refusal of the owner's Start
  still fails the check naming fbc-oms.
- A pass of these checks says nothing about whether the venue's snapshot can be trusted: that
  stays the subject of its own record on testnet evidence (the owner's answer C to RB-olg-3).

## What would show this was wrong

A check whose verdict depends on the registry's view of the venue's snapshot source (a resync
the venue answers during the check, judged by fbc-oms), so that taking the suite's snapshot as
trustworthy changes what the check proves about the adapter.
