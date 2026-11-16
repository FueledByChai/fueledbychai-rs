# 0149 — Agents review and merge each pull request by hand after the gates, the owner keeping review-path and gate-rule pull requests, decision-record approval and gate waivers, amending 0009

Status: accepted
Date: 2026-11-16

## Context

Decision 0009 says that almost every pull request here auto-merges when its checks are green,
and that only changes to signers and to JWT and API-key handling wait for the owner. Practice
moved away from both:
- Since FBC-nda8 (kit decision 0028), AGENTS.md's Merge profile has kept the legacy handoff. Each
  pull request opens as a draft, auto-merge stays disabled, CI runs on the `ci:run` label, and
  "the owner merges each pull request by hand".
- That paragraph was to govern over 0009's auto-merge clause until a record settled it
  (FBC-ftg0).
- Decision 0127 had already made a completed exact-head Codex review the external model gate.

From #221 (2026-11-04), the session the owner works through merged each pull request by hand. It
did so on the owner's word in chat, after an independent reviewer's final gate on FBC-n2l5.
- The epic's entries for #236 to #256 also record that session's merges. For the rest, the epic
  records the reviewer's final gate.
- Every pull request from #221 to #263 merged that way, apart from Dependabot's #225, which is
  still open.
- GitHub shows the owner's account as the merger of each, because the agents use that account.

Six of them touched a review path: #224, #239, #243, #245, #253 and #254.
- Each waited for the owner's review of its head.
- Five merged once the owner had waived, for that pull request, one stale failed check run on
  its head: #224, #239, #243, #253 and #254. That run is the workflow's refusal of the
  `needs-review` label event, which `scripts/pr-readiness.sh` reads as a CI failure (FBC-vvn2).
- #245's stale run was on an earlier head, so the head that merged passed every gate.

That practice left two kit rules unmet:
- By that session's own account, it had written many of the pull requests it merged. Beads and
  GitHub cannot tell sessions apart. But `loop/prompts/respond-to-review.md` says the author
  "must never ... merge its own work".
- The reviewer may not merge either: `loop/prompts/review-prs.md` says "never push or merge" and
  "Independent acceptance is not merge authorization".

On 2026-11-16 the owner asked that session: "can you please update this workflow so you can
review and merge the PRs?". This record writes that down within the kit's rules. The owner's
time then goes to what only the owner can do:
- runs on a venue;
- reading recorded fixtures before they are committed;
- approving decision records;
- waiving a gate;
- reviewing signing and authentication code, and changes to the gates;
- tagging releases.

## Decision

Agents review and merge each pull request by hand. Nothing merges by itself when green.

### Three roles

No session holds two of these roles for one pull request. A subagent is a session of its own.

- **The author** writes the pull request and answers its review under
  `loop/prompts/next-ticket.md` and `respond-to-review.md`.
  - It names its session in the pull request's body, on a line `Author session: <name>`.
  - It never accepts or merges the pull request.
- **The reviewer** wrote no commit on the pull request. It is the named coordinator of kit
  decision 0028, working under `loop/prompts/review-prs.md`.
  - It claims the coordination epic (FBC-n2l5 today) and selects one candidate.
  - It reads the whole diff, the ticket's done line and its proof, and the external review's
    receipt, findings, replies and threads (0127).
  - It records its selection, assessment, CI admission, CI result and final gate on the epic. Its
    assessment and final gate each state that its session wrote no commit on the pull request.
  - It adds `ci:run` only to the selected candidate, after accepting it.
  - It posts the `Agent review` status on the head it judged.
  - It never merges.
  - A session that holds the epic and wrote a commit on a pull request hands the epic to another
    reviewer, in an epic entry, before that pull request is selected.
- **The merge agent** wrote no commit on the pull request and did not review it.
  - It takes only the pull request's number from whoever starts it. It reads the head, every gate
    and every record itself, and treats anything else in its task as a relay.
  - It changes nothing on the pull request and waives nothing.
  - After merging, it records on the epic the commit the rebase put on the default branch
    (`gh pr view <number> --json mergeCommit`).
  - The ticket's close and the clean-up stay with the session the owner works through.

### Epic entries

Every entry the merge agent reads is a comment on the epic (`bd comments add`), read with
`bd comments <epic> --json` after `bd dolt pull`.
- Its first line is `<KIND> #<number> <full 40-character head SHA> by <session>`.
- The kinds are:
  - from the reviewer: `SELECTION`, `ASSESSMENT`, `CI ADMISSION`, `CI RESULT`, `FINAL GATE`,
    `EPIC HANDOFF`;
  - from the session the owner spoke to: `OWNER REVIEW`, `OWNER APPROVAL`, `OWNER WAIVER`,
    `ALTERNATE REVIEW`;
  - from the merge agent: `MERGED`.
- The merge agent refuses an entry that does not have this first line.
- It refuses an `ASSESSMENT` or `FINAL GATE` whose session matches the `Author session` line, or
  that does not state its session wrote no commit.
- Every agent here shares one Beads actor and one GitHub account, so the merge agent checks naming
  and heads, not provenance.
- Entries written before this record keep their earlier form.

### The merge agent's checks

1. It fetches the default branch. From a checkout at that branch's tip, never the candidate's
   copies, it runs `scripts/pr-readiness.sh --pr <number>` and `scripts/codex_review_evidence.py`,
   and it reads `.loop.toml` there too.
2. It reads the head with `gh pr view <number> --json headRefOid`.
3. It lists the changed files with `gh api repos/<owner>/<repo>/pulls/<number>/files --paginate`.
   It reads each file's `filename` and, for a rename, `previous_filename`.
4. On that exact head, all of these must hold:
   - The external review completed. Either the Codex evidence passes, or the owner directed an
     alternate external review for this pull request (0127). In that case the `ALTERNATE REVIEW`
     entry names the reviewer and its trusted account, and the merge agent finds that review's
     own result for this head from that account.
   - An `ASSESSMENT` accepting the pull request and a `FINAL GATE` are on the epic for this head.
   - The required check, `Check (scripts/check.sh)`, has a successful run on this head
     (`gh api repos/<owner>/<repo>/commits/<head>/check-runs`).
   - Every readiness fact passes, apart from one the owner waived for this head.
   - Each owner entry the pull request needs is on the epic and holds for this head.
5. If anything fails, it merges nothing and reports.
6. Otherwise it merges with `gh pr merge <number> --rebase --match-head-commit <head>`. It never
   arms auto-merge, never runs `gh pr merge --auto`, and never bypasses a branch rule.

### What waits for the owner

The merge agent merges these pull requests only once an `OWNER REVIEW` says the owner reviewed
the current head and said to merge it. Otherwise the owner merges them.
- **A review path.** 0009's signing and authentication paths, and the rest of `review_paths` in
  `.loop.toml`.
- **A gate.** A change to any of these:
  - anything under `scripts/`, `loop/`, `ci/` or `.github/`;
  - `.loop.toml`, `loop.toml.example` or `deny.toml`;
  - `fixtures/licence-gate/` or `fixtures/dep-direction/`;
  - `coverage-floor.txt`, when the change lowers the floor;
  - a `SYNTHETIC` file anywhere, added, moved or removed;
  - AGENTS.md anywhere outside its Layout section;
  - `CLAUDE.md`, `.claude/`, `.codex/`, `.cursor/`, `.agents/` or `.beads/hooks/`.

  These decide what the check passes and what the agents read first. CI runs the candidate's own
  copy of the check, so the check cannot guard a change to itself, and only the owner can. Cargo
  manifests, `rustfmt.toml` and `rust-toolchain.toml` are left to the reviewer, since most tickets
  touch a manifest and moving the toolchain is a ticket of its own (0008).
- **A recorded fixture.** Anything under `fixtures/*/exec-recorded/`, which the owner reads before
  it is committed.
- **A decision record.** A pull request that adds or changes any record under `docs/decisions/`,
  the index aside, needs an `OWNER APPROVAL` instead. It lists each record file the owner
  approved, with its blob id at the head the owner read (`git rev-parse <head>:<path>`).
  - The merge agent requires every record file the pull request changes to be listed, with the
    same blob id at the head it merges.
  - Such an approval outlasts a refresh that leaves those files unchanged.

These owner-gated pull requests are not review paths, and `review_paths` is unchanged.

### The owner's word

Only the owner waives a gate, approves a decision record, clears a pull request that waits for
the owner, or directs an alternate external review.
- **Who records it.** The session the owner said it to records it on the epic. Each entry states
  that the owner's words came in that session's own chat, and gives the date. It quotes only what
  holds no secret, account data or live parameter (0009).
- **Relays.** A record written from another session's relay is not the owner's word.
- **How long one holds.**
  - An `OWNER REVIEW` or `OWNER WAIVER` holds for the head it names, and a new head ends it.
  - An `OWNER APPROVAL` holds while its blobs match.
  - An `ALTERNATE REVIEW` holds for the pull request, as 0127 has it.
  - No agent reads an entry as covering another pull request.
- **What a waiver may cover.** A waiver names one readiness fact, and one check run on that
  head. FBC-vvn2's stale run is one example. A waiver never covers any of these:
  - the external review: 0127 allows only an alternate review there;
  - the independent acceptance;
  - the need for a successful run of the required check on the head;
  - the separation of the roles;
  - an owner gate above.

  No agent waives anything.
- **An alternate external review is not a waiver.** It must complete on the exact head, and its
  findings are read and answered as Codex's are. Where `review-prs.md` asks for a completed Codex
  review before the pass status, AGENTS.md's External model review governs, as it already does.

### What does not change

- The legacy merge profile, with its draft-first, `ci:run` procedure.
- One candidate at a time.
- Codex as the external model gate (0127).
- The rest of 0009: secrets, account data, live configuration, the privacy check and where
  credential code lives.

This replaces three parts of 0009:
- its sentence "Everything else auto-merges when green";
- the words "instead of auto-merging" in its review-path bullet;
- the words "auto-merges when its checks are green" in its Context.

The reason 0009 draws from them still holds: most pull requests merge without the owner reading
them, so the privacy rules must be checked by a script. Every other part of 0009 stands.

## Alternatives

- **Auto-merge when green, as 0009 wrote it.** Rejected. It could merge as soon as a `ci:run` run
  passes, before the Codex review, the independent acceptance and the final gate. No check
  enforces any of those. The legacy handoff requires auto-merge to stay disabled.
- **The owner merges each pull request by hand, as AGENTS.md has said since FBC-nda8.** Rejected.
  Practice set it aside from #221 (2026-11-04), and the owner rejected it in words on
  2026-11-16. Under it, every merge waited on the owner for gates the agents already check.
- **The session that directs the work merges, as practised from #221 to #263.** Not taken. That
  session writes many pull requests, and `respond-to-review.md` forbids the author to merge its
  own work.
- **The reviewer merges after its own acceptance, as an App controller would.** Not taken.
  `review-prs.md` forbids the reviewer to merge. A separate merge agent also keeps the final
  check apart from the judgement it checks.
- **Agents also merge review-path, gate and recorded-fixture pull requests on their own
  review.** Not taken:
  - 0009 keeps a person on signing and authentication code. A test that shares a wrong vector
    cannot see a mistake there, and such a mistake can leak a credential.
  - A change to the gates could weaken every later review, and the check cannot guard itself.
  - Recorded fixtures are the owner's to read (0143, 0148).

## Consequences

- AGENTS.md's Merge profile paragraph summarises this record and cites it for the epic entries,
  the merge agent's checks and the owner's word.
- AGENTS.md's Rules line says that every other pull request is reviewed and merged by the agents,
  except as the Merge profile says, in place of "auto-merges when green".
- No branch rule catches a gate the agents skip: the ruleset requires no approval, only the check
  status.
  - Running the readiness and Codex scripts from the default branch protects those two scripts
    only.
  - Every other gate file is protected by the owner's review of a change to it.
- A review-path pull request whose merging head still carries the refused `needs-review` run
  needs the owner's waiver of that run, as well as the owner's review, until FBC-vvn2 is fixed.
- Each merge takes one more agent, and a short one, since its checks are mechanical.
- The owner is asked only for:
  - reviews of review-path, gate and recorded-fixture pull requests;
  - decision-record approvals;
  - gate waivers and alternate external reviews;
  - runs on a venue;
  - release tags.

## What would show this was wrong

- A pull request merges with a gate unmet that the owner did not waive for its head. That means
  one of these:
  - the external review incomplete;
  - no recorded acceptance;
  - the required check not green;
  - a readiness fact failing;
  - an owner entry missing or not holding for that head.

  Then make the review status or CI admission a required branch rule, in a new record.
- A session holds two roles on one pull request, an agent merges on a relayed owner entry, or a
  change to a gate merges without the owner. Then tighten how sessions and gate files are named,
  in a new record.
- A defect a person would plainly have caught reaches main through an agent's review and causes
  a live loss. Then widen the owner's review in a new record, as 0009 says for review paths.
