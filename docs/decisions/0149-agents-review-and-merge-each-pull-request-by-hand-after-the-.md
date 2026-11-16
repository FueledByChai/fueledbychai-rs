# 0149 — Agents review and merge each pull request by hand after the gates, and the owner reviews only review-path pull requests, amending 0009

Status: accepted
Date: 2026-11-16

## Context

Decision 0009 says that almost every pull request here auto-merges when its checks are green,
and that only changes to signers and to JWT and API-key handling wait for the owner. Practice
moved away from both. Since FBC-nda8 (kit decision 0028), AGENTS.md's Merge profile keeps the
legacy handoff: each pull request opens as a draft, auto-merge stays disabled, CI runs on the
`ci:run` label, and "the owner merges each pull request by hand". That paragraph was to govern
over 0009's auto-merge clause until a record settled it (FBC-ftg0). Decision 0127 then made a
completed exact-head Codex review the external model gate. In practice, the owner has directed
the coordinating agent to merge each pull request once it has passed every gate. Every pull
request from #221 to #263, apart from Dependabot's #225, was merged that way.

On 2026-11-16 the owner asked: "can you please update this workflow so you can review and merge
the PRs?". This record writes that down. The owner's time then goes to what only the owner can
do: runs on a venue, reading recorded fixtures before they are committed, approving decision
records, reviewing signing and authentication code, and tagging releases.

## Decision

Agents review and merge each pull request by hand. Nothing merges by itself when green.

- **Review.** An independent reviewer agent accepts or refuses each pull request. It is never
  the pull request's author or a session that wrote its code. It follows
  `loop/prompts/review-prs.md` and the Merge profile in AGENTS.md. It reads the whole diff, the
  ticket's done line and its proof, and the Codex review's receipt, findings, replies and
  threads (0127). It posts the `Agent review` status on the head it judged, and records its
  selection, assessment, CI admission, CI result and final gate on the coordination epic. It
  adds `ci:run` only to the one candidate it accepted.
- **Merge.** The coordinating agent merges the selected candidate with
  `gh pr merge <number> --rebase --match-head-commit <head>`, and only once all of these hold on
  that exact head:
  - `scripts/codex_review_evidence.py` passes (0127);
  - the independent reviewer accepted it;
  - CI passed;
  - `scripts/pr-readiness.sh --pr <number>` passes all six facts.

  It never arms auto-merge, never runs `gh pr merge --auto`, and never bypasses a branch rule.
- **Review paths stay the owner's.** A pull request touching a review path (0009's signing and
  authentication paths, `review_paths` in `.loop.toml`) still waits for the owner's review. The
  coordinating agent merges it only after the owner has reviewed its current head and told the
  agent to merge it. Otherwise the owner merges it.
- **Decision records stay the owner's.** A pull request that adds a decision record merges only
  after the owner has approved that record's text.
- **What does not change:**
  - The legacy merge profile, and its draft-first, `ci:run` procedure.
  - One candidate at a time.
  - Codex as the external model gate (0127).
  - The rest of 0009: secrets, account data, live configuration, the privacy check and where
    credential code lives.

This replaces 0009's sentence "Everything else auto-merges when green", and its Context's
premise that most pull requests merge without a person reading them. Every other part of 0009
stands.

## Alternatives

- **Auto-merge when green, as 0009 wrote it.** Rejected. It would merge before the Codex review
  and the independent acceptance, which no check enforces. The legacy handoff requires
  auto-merge to stay disabled.
- **The owner merges each pull request by hand, as AGENTS.md has said since FBC-nda8.** Rejected
  by the owner on 2026-11-16. Every merge waited on the owner, for gates the agents already
  check.
- **Agents also merge review-path pull requests on their own review.** Not taken. 0009 keeps a
  person on signing and authentication code because a test that shares a wrong vector cannot
  see the mistake, and a leaked credential cannot be taken back.

## Consequences

- AGENTS.md's Merge profile paragraph says that the coordinating agent merges each pull request
  after the gates. Its Rules line says that every other pull request is reviewed and merged by
  the agents, in place of "auto-merges when green".
- A gate the agents skip has no branch rule to catch it, since the ruleset requires no approval
  and only the check status. The six readiness facts and the Codex evidence are therefore run
  again at the final gate, on the head being merged.
- The owner is asked only for:
  - review-path reviews;
  - decision-record approvals;
  - merge-gate waivers;
  - runs on a venue;
  - reading recorded fixtures before they are committed;
  - release tags.

## What would show this was wrong

- A pull request merges with a gate unmet: the Codex review incomplete on its head, no
  independent acceptance, red or missing CI, or a readiness fact failing. Then make the review
  status or CI admission a required branch rule, in a new record.
- A defect a person would plainly have caught reaches main through an agent's review and causes
  a live loss. Then widen the owner's review in a new record, as 0009 says for review paths.
