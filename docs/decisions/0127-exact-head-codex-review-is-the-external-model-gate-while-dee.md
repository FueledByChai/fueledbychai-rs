# 0127 — Exact-head Codex review is the external model gate while DeepSeek is disabled

Status: accepted
Date: 2026-10-26

## Context

AGENTS.md still required a DeepSeek `github-actions[bot]` comment under the 2026-10-05 owner
policy, but that workflow was manually disabled. On FBC-ywjb PR #205, the active trusted Codex
connector completed a review on the exact head while the stale written rule blocked the final
gate. The owner explicitly chose the completed exact-head Codex review for #205 and asked for
the repository policy to be updated afterward (FBC-jqpk).

## Decision

For the selected candidate, a completed code review by `chatgpt-codex-connector[bot]` is the
external model gate. Its latest trusted summary must name the full current head SHA, report
`code_review.status=completed`, a known findings state and no unresolved findings; a newer
review request holds the gate until its new result appears. A verifier checks GitHub's bot
identity and these states, and independent reviewers still read the actual findings, replies,
threads, whole diff and done-line proof. This evidence precedes independent acceptance, final
head CI and the six-fact readiness gate. The #205 exception used this same evidence on head
`65eeba8ceb6d6aa6b053feb571954306545cb297` before the standing policy changed.

The DeepSeek workflow remains disabled. Its historical implementation and offline tests remain
in the check, but its comments no longer substitute for Codex. If Codex is unavailable or
incomplete, the candidate waits until it can complete, or the owner explicitly names an
alternate external review for that PR or adopts a new decision. No model result alone merges a
PR; the independent reviewer and exact-head CI remain separate gates.

## Alternatives

- Keep treating the disabled DeepSeek workflow as mandatory: no new review could complete, so
  the written rule would strand every otherwise reviewed candidate.
- Treat an in-progress marker or an independent review alone as enough: neither proves the
  external model finished examining the current head.
- Enable DeepSeek automatically: that changes the provider and secret-use policy without the
  owner's direction.

## Consequences

Reviewers use `scripts/codex_review_evidence.py` for the exact-head completion check. The
project check runs offline cases for stale, queued, running, unresolved, malformed and look-alike
summaries. Human inspection of findings and threads remains necessary. The historical DeepSeek
tests continue to guard dormant code without making its workflow a merge requirement.

## What would show this was wrong

A trusted Codex summary that incorrectly reports completion or resolution, a change in the
connector's authenticated identity or marker format, or a deliberate owner decision to restore
a different external reviewer would reopen this choice.
