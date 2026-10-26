#!/usr/bin/env python3
"""Offline proof of the exact-head Codex summary gate (FBC-jqpk)."""

import json
import unittest

from codex_review_evidence import BOT, HEADING, assess


HEAD = "a" * 40
OTHER = "b" * 40
BEFORE = "2026-10-26T05:07:27Z"
AFTER = "2026-10-26T05:08:54Z"


def summary(*, head=HEAD, status="completed", unresolved=False, findings=False,
            login=BOT, user_type="Bot", updated=AFTER, marker=True):
    state = {"code_review": {"commit_sha": head, "status": status,
             "has_unresolved_findings": unresolved, "has_any_findings": findings}}
    line = "<!-- codex-review-state:v1 " + json.dumps(state) + " -->" if marker else "no marker"
    return {"id": 10, "user": {"login": login, "type": user_type},
            "body": HEADING + "\n" + line, "updated_at": updated,
            "created_at": BEFORE, "html_url": "https://example.invalid/summary"}


def request(at):
    return {"id": 11, "user": {"login": "owner", "type": "User"},
            "body": "@codex review", "created_at": at}


class CodexEvidenceTests(unittest.TestCase):
    def test_completed_exact_head_is_eligible(self):
        self.assertEqual(assess([request(BEFORE), summary()], HEAD),
                         (True, "no findings", "https://example.invalid/summary"))

    def test_findings_need_independent_inspection(self):
        self.assertEqual(assess([summary(findings=True)], HEAD)[1], "findings to inspect")

    def test_rejects_stale_running_unresolved_and_unknown(self):
        for candidate in (summary(head=OTHER), summary(status="running"),
                          summary(status="queued"), summary(unresolved=True),
                          summary(unresolved=None), summary(findings=None)):
            with self.subTest(candidate=candidate):
                self.assertFalse(assess([candidate], HEAD)[0])

    def test_rejects_lookalikes_and_malformed_marker(self):
        for candidate in (summary(login="other[bot]"), summary(user_type="User"),
                          summary(marker=False)):
            with self.subTest(candidate=candidate):
                self.assertFalse(assess([candidate], HEAD)[0])

    def test_does_not_fall_back_to_older_completed_summary(self):
        old = summary(updated=BEFORE)
        old["id"] = 1
        self.assertFalse(assess([old, summary(status="running")], HEAD)[0])

    def test_newer_request_holds_even_when_summary_is_still_completed(self):
        self.assertEqual(assess([summary(), request("2026-10-26T05:09:00Z")], HEAD)[1],
                         "a newer Codex review request is pending")
        inline = request("2026-10-26T05:09:00Z")
        inline["body"] = "Could you run @codex review now?"
        self.assertFalse(assess([summary(), inline], HEAD)[0])


if __name__ == "__main__":
    unittest.main()
