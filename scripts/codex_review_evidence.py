#!/usr/bin/env python3
"""Check the trusted Codex summary for one exact pull-request head.

This is only the external-model completion check. The independent reviewer still
reads findings, replies, review threads, acceptance proof, and final-head CI.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime


BOT = "chatgpt-codex-connector[bot]"
HEADING = "<!-- codex-pull-request-review-summary -->"
MARKER = re.compile(r"<!-- codex-review-state:v1 (\{.*\}) -->")
COMPLETION = re.compile(
    r'^\|[^\n]*\*\*Code Review\*\*[^\n]*\*\*Completed\*\*[^\n]*'
    r'<relative-time datetime="([^"]+)"', re.MULTILINE,
)
HEAD = re.compile(r"[0-9a-f]{40}\Z")
REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
NEW_REQUEST = re.compile(r"@codex\s+review\b", re.IGNORECASE)


def _time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _login(comment):
    user = comment.get("user") or {}
    return (user.get("login"), user.get("type")) if isinstance(user, dict) else (None, None)


def assess(comments, head):
    """Return (eligible, reason, summary URL); fail closed on uncertain evidence."""
    summaries = [
        comment for comment in comments
        if _login(comment) == (BOT, "Bot")
        and isinstance(comment.get("body"), str)
        and comment["body"].startswith(HEADING + "\n")
    ]
    if not summaries:
        return False, "no trusted Codex review summary", None
    try:
        summary = max(summaries, key=lambda comment: (_time(comment["updated_at"]), comment["id"]))
        lines = summary["body"].splitlines()
        marker = MARKER.fullmatch(lines[1])
        if marker is None:
            return False, "trusted summary has no valid state marker", summary.get("html_url")
        review = json.loads(marker.group(1))["code_review"]
    except (IndexError, KeyError, TypeError, ValueError):
        return False, "trusted summary is malformed", summary.get("html_url")
    if review.get("commit_sha") != head:
        return False, "trusted summary is for a different head", summary.get("html_url")
    if review.get("status") != "completed":
        return False, "Codex review is not completed", summary.get("html_url")
    if review.get("has_unresolved_findings") is not False:
        return False, "Codex findings are unresolved or unknown", summary.get("html_url")
    if not isinstance(review.get("has_any_findings"), bool):
        return False, "Codex findings state is unknown", summary.get("html_url")
    try:
        completion = COMPLETION.search(summary["body"])
        if completion is None:
            return False, "Codex completion time is missing", summary.get("html_url")
        completed_at = _time(completion.group(1))
        if any(
            _login(comment)[0] != BOT
            and NEW_REQUEST.search(comment.get("body") or "")
            and _time(comment["created_at"]) >= completed_at
            for comment in comments
        ):
            return False, "a newer Codex review request is pending", summary.get("html_url")
    except (KeyError, TypeError, ValueError):
        return False, "review request chronology is malformed", summary.get("html_url")
    findings = "findings to inspect" if review["has_any_findings"] else "no findings"
    return True, findings, summary.get("html_url")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--pr", required=True, type=int, help="pull request number")
    parser.add_argument("--head", required=True, help="full 40-character head SHA")
    args = parser.parse_args()
    if not REPO.fullmatch(args.repo) or args.pr <= 0 or not HEAD.fullmatch(args.head):
        parser.error("repo, PR number, or full head SHA is invalid")
    endpoint = f"repos/{args.repo}/issues/{args.pr}/comments?per_page=100"
    result = subprocess.run(
        ["gh", "api", endpoint, "--paginate", "--slurp"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        print("Codex evidence unavailable: GitHub API request failed", file=sys.stderr)
        return 1
    try:
        pages = json.loads(result.stdout)
        if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
            raise ValueError("expected paginated comment arrays")
        comments = [comment for page in pages for comment in page]
        if any(not isinstance(comment, dict) for comment in comments):
            raise ValueError("expected comment objects")
    except (TypeError, ValueError):
        print("Codex evidence unavailable: invalid GitHub API response", file=sys.stderr)
        return 1
    eligible, reason, url = assess(comments, args.head)
    print(f"Codex exact-head completion: {'pass' if eligible else 'hold'} — {reason}")
    if url:
        print(url)
    return 0 if eligible else 1


if __name__ == "__main__":
    raise SystemExit(main())
