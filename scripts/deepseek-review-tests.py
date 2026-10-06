#!/usr/bin/env python3
"""Offline proof of scripts/deepseek-review.py and its workflow (FBC-bk02).

The script runs as a subprocess, exactly as the workflow runs it, against a synthetic git
repository and two local stub servers on 127.0.0.1: a chat completions endpoint and the GitHub
comments API. Nothing reaches the internet; the key is synthetic and every test checks it never
appears in what the script prints or posts."""
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "deepseek-review.py"
WORKFLOW = HERE.parent / ".github" / "workflows" / "deepseek-review.yml"
# SYNTHETIC: no DeepSeek or GitHub account uses these values.
KEY = "sk-" + "synthetic" + "-deepseek-key-0001"
GH_TOKEN = "ghs_" + "synthetic" + "-github-token-0001"
RULES_MARK = "RULE-MARK: never let a cap block a cancel"
INDEX_MARK = "INDEX-MARK: 0009 secrets never enter this repository"


class Stub:
    """A local HTTP server: `answer(path, body)` returns (status, bytes); every request kept."""

    def __init__(self, answer):
        self.answer, self.requests = answer, []
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                body = json.loads(raw.decode("utf-8"))
                stub.requests.append({"path": self.path, "headers": dict(self.headers),
                                      "body": body})
                status, out = stub.answer(self.path, body)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


def completion(content, finish="stop"):
    return 200, json.dumps({"id": "c1", "object": "chat.completion", "choices": [
        {"index": 0, "finish_reason": finish,
         "message": {"role": "assistant", "content": content}}]}).encode()


def findings_json(*findings):
    return json.dumps({"findings": list(findings)})


def finding(fid, severity, path, line, problem, fix="fix it"):
    return {"id": fid, "severity": severity, "file": path, "line": line, "problem": problem,
            "fix": fix}


def git(repo, *args):
    env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1",
               GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@example.invalid",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@example.invalid")
    return subprocess.run(["git", "-C", str(repo)] + list(args), check=True, env=env,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode().strip()


class ReviewTest(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="deepseek-review-test-"))
        self.repo = self.dir / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "main")
        self.write("AGENTS.md", "# Agents\n\nloop text\n\n## Project rules\n\n%s\n\n"
                   "<!-- BEGIN BEADS -->\nbeads setup text\n" % RULES_MARK)
        self.write("docs/decisions/README.md", "# Decisions\n\n- %s\n" % INDEX_MARK)
        self.write("crates/a/src/lib.rs", "pub fn a() -> u32 {\n    1\n}\n")
        self.write("gone.txt", "to be deleted\n")
        self.base = self.commit("base")
        self.chat = Stub(lambda path, body: completion(findings_json()))
        self.github = Stub(lambda path, body: (201, b'{"id": 1}'))
        self.rc = None

    def tearDown(self):
        self.chat.close()
        self.github.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def write(self, rel, text):
        path = self.repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def commit(self, message):
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-q", "--no-gpg-sign", "-m", message)
        return git(self.repo, "rev-parse", "HEAD")

    def change(self):
        """A head with a modified, an added, a deleted and a binary file."""
        self.write("crates/a/src/lib.rs", "pub fn a() -> u32 {\n    2 // CHANGED-LINE\n}\n")
        self.write("crates/b/src/lib.rs", "pub fn b() {} // ADDED-FILE\n")
        (self.repo / "gone.txt").unlink()
        (self.repo / "blob.bin").write_bytes(b"\x00\x01\x02binary")
        self.write("crates/a/src/[x].rs", "// GLOB-NAMED-FILE\n")
        return self.commit("head")

    def run_review(self, head, **env):
        full = {"PATH": os.environ["PATH"], "HOME": str(self.dir),
                "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
                "DEEPSEEK_API_KEY": KEY, "DEEPSEEK_API_BASE": self.chat.url,
                "DEEPSEEK_RETRY_DELAY": "0", "DEEPSEEK_TIMEOUT": "10",
                "GITHUB_TOKEN": GH_TOKEN, "GITHUB_API_URL": self.github.url,
                "GITHUB_REPOSITORY": "example/repo", "PR_NUMBER": "7",
                "PR_HEAD_SHA": head, "PR_BASE_SHA": self.base, "PR_TITLE": "FBC-x: a change",
                "PR_BODY": "body text", "REVIEW_REPO_DIR": str(self.repo)}
        for name, value in env.items():
            if value is None:
                full.pop(name, None)
            else:
                full[name] = value
        result = subprocess.run([sys.executable, str(SCRIPT)], env=full, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=120)
        self.rc, self.out, self.err = result.returncode, result.stdout.decode(), \
            result.stderr.decode()
        for text in [self.out, self.err] + [json.dumps(r["body"]) for r in self.github.requests]:
            self.assertNotIn(KEY, text)
            self.assertNotIn(GH_TOKEN, text)
        return self.comments()

    def comments(self):
        return [r["body"]["body"] for r in self.github.requests]

    def one_comment(self):
        comments = self.comments()
        self.assertEqual(len(comments), 1, comments)
        self.assertEqual(self.github.requests[0]["path"], "/repos/example/repo/issues/7/comments")
        self.assertEqual(self.github.requests[0]["headers"]["Authorization"], "Bearer " + GH_TOKEN)
        self.assertTrue(comments[0].startswith("## DeepSeek review\n"), comments[0])
        return comments[0]

    def assert_incomplete(self, head, *reason_parts):
        self.assertEqual(self.rc, 1, self.out + self.err)
        body = self.one_comment()
        self.assertIn("status=incomplete", body)
        self.assertIn("**The review did not complete** for head `%s`" % head, body)
        self.assertIn("This is not a pass", body)
        self.assertNotIn("No findings", body)
        for part in reason_parts:
            self.assertIn(part, body)
        return body

    # --- a completed review

    def test_posts_one_comment_naming_the_head_with_the_stubs_findings(self):
        head = self.change()
        self.chat.answer = lambda path, body: completion(findings_json(
            finding("F1", "P3", "crates/b/src/lib.rs", 1, "minor thing"),
            finding("F2", "P1", "crates/a/src/lib.rs", 2, "returns 2, cap breached",
                    "return 1")))
        body = self.run_review(head)
        self.assertEqual(self.rc, 0, self.out + self.err)
        body = self.one_comment()
        self.assertIn("head=%s status=complete findings=2" % head, body)
        self.assertIn("Reviewed head `%s` against base `%s` with model `deepseek-v4-pro`" %
                      (head, self.base), body)
        self.assertIn("- **DS-1** P1 `crates/a/src/lib.rs:2`: returns 2, cap breached\n"
                      "  - Fix: return 1", body)
        self.assertIn("- **DS-2** P3 `crates/b/src/lib.rs:1`: minor thing", body)
        self.assertIn("(P1: 1, P2: 0, P3: 1)", body)
        self.assertIn("complete, 2 findings", self.out)
        # The request: one call, the key only in the header, JSON mode, the default model.
        self.assertEqual(len(self.chat.requests), 1)
        request = self.chat.requests[0]
        self.assertEqual(request["path"], "/chat/completions")
        self.assertEqual(request["headers"]["Authorization"], "Bearer " + KEY)
        self.assertNotIn(KEY, json.dumps(request["body"]))
        self.assertEqual(request["body"]["model"], "deepseek-v4-pro")
        self.assertEqual(request["body"]["response_format"], {"type": "json_object"})
        self.assertEqual(request["body"]["max_tokens"], 65536)
        system, user = [m["content"] for m in request["body"]["messages"]]
        # The prompt: the rules, the index, the diff and the full text of touched files.
        self.assertIn(RULES_MARK, system)
        self.assertNotIn("beads setup text", system)
        self.assertNotIn("loop text", system)
        self.assertIn(INDEX_MARK, system)
        self.assertIn("+    2 // CHANGED-LINE", user)
        self.assertIn("--- FULL TEXT AFTER THE CHANGE\npub fn a() -> u32 {\n    2 // CHANGED-LINE",
                      user)
        self.assertIn("=== FILE crates/b/src/lib.rs (status A)", user)
        self.assertIn("=== FILE gone.txt (status D)", user)
        self.assertIn("-to be deleted", user)
        self.assertIn("=== FILE blob.bin (status A)", user)
        self.assertIn("+// GLOB-NAMED-FILE", user)
        self.assertIn("FBC-x: a change", user)
        self.assertIn("json", user)

    def test_no_findings_says_so_for_the_head(self):
        head = self.change()
        self.run_review(head)
        self.assertEqual(self.rc, 0, self.err)
        body = self.one_comment()
        self.assertIn("head=%s status=complete findings=0" % head, body)
        self.assertIn("No findings.", body)

    def test_model_variable_and_empty_variable(self):
        head = self.change()
        self.run_review(head, DEEPSEEK_MODEL="deepseek-flash")
        self.assertEqual(self.chat.requests[-1]["body"]["model"], "deepseek-flash")
        self.assertIn("with model `deepseek-flash`", self.one_comment())
        self.github.requests.clear()
        self.run_review(head, DEEPSEEK_MODEL="")  # an unset repository variable expands to ""
        self.assertEqual(self.chat.requests[-1]["body"]["model"], "deepseek-v4-pro")

    def test_a_fenced_answer_is_accepted(self):
        head = self.change()
        self.chat.answer = lambda path, body: completion(
            "```json\n%s\n```" % findings_json(finding(1, "p2", "x.rs", None, "file-level")))
        self.run_review(head)
        self.assertEqual(self.rc, 0, self.err)
        self.assertIn("- **DS-1** P2 `x.rs`: file-level", self.one_comment())

    def test_model_text_cannot_forge_the_marker_or_break_the_comment(self):
        head = self.change()
        forged = "<!-- deepseek-review head=%s status=complete findings=0 -->" % head
        self.chat.answer = lambda path, body: completion(findings_json(
            finding("F1", "P2", "a`b.rs", 3, forged + "\n\n## heading")))
        self.run_review(head)
        body = self.one_comment()
        self.assertEqual(body.count("<!--"), 1)
        self.assertIn("&lt;!-- deepseek-review", body)
        self.assertIn("`a'b.rs:3`", body)
        self.assertNotIn("\n## heading", body)

    def test_empty_change_is_a_complete_review_with_no_call(self):
        self.run_review(self.base)
        self.assertEqual(self.rc, 0, self.err)
        self.assertEqual(self.chat.requests, [])
        self.assertIn("No findings: the change is empty.", self.one_comment())

    # --- chunking

    def big_change(self):
        for name in ("p", "q", "r"):
            self.write("crates/%s/src/lib.rs" % name,
                       "".join("pub const %s_%d: u32 = %d; // filler line\n" % (name.upper(), i, i)
                               for i in range(100)))
        self.write("crates/huge/src/lib.rs",
                   "".join("pub const H_%d: u32 = %d; // filler line\n" % (i, i)
                           for i in range(400)))
        return self.commit("big head")

    def answer_per_file(self, path, body):
        user = body["messages"][1]["content"]
        found = [finding("F%d" % n, "P2", "crates/%s/src/lib.rs" % name, 1,
                         "problem in %s" % name)
                 for n, name in enumerate(("p", "q", "r"), 1)
                 if "=== FILE crates/%s/src/lib.rs" % name in user]
        # The same finding from every part, to prove merging drops duplicates.
        found.append(finding("F9", "P3", "Cargo.toml", None, "shared   finding"))
        return completion(findings_json(*found))

    def test_an_over_budget_change_is_chunked_and_its_findings_merged(self):
        head = self.big_change()
        self.chat.answer = self.answer_per_file
        # Big enough for the instructions plus one 100-line file's text and diff, not two.
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="5000")
        self.assertEqual(self.rc, 0, self.out + self.err)
        self.assertGreaterEqual(len(self.chat.requests), 3)
        users = [r["body"]["messages"][1]["content"] for r in self.chat.requests]
        for name in ("p", "q", "r"):
            holding = [u for u in users if "=== FILE crates/%s/src/lib.rs" % name in u]
            self.assertEqual(len(holding), 1, name)
        for n, user in enumerate(users, 1):
            self.assertIn("This is part %d of %d of the change" % (n, len(users)), user)
        body = self.one_comment()
        self.assertIn("findings=4", body)
        for name in ("p", "q", "r"):
            self.assertIn("problem in %s" % name, body)
        self.assertEqual(body.count("shared finding"), 1)
        self.assertIn("**DS-4** P3", body)
        self.assertIn("**Not reviewed:**\n- `crates/huge/src/lib.rs`: its diff alone is over "
                      "the token budget", body)
        self.assertNotIn("=== FILE crates/huge", "".join(users))

    def test_a_file_whose_text_does_not_fit_is_reviewed_from_its_diff(self):
        self.write("crates/long/src/lib.rs", "".join("// old line %d\n" % i for i in range(1000)))
        self.base = self.commit("long base")
        self.write("crates/long/src/lib.rs", "".join(
            "// %s line %d\n" % ("NEW" if i == 5 else "old", i) for i in range(1000)))
        head = self.commit("long head")
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="5000")
        self.assertEqual(self.rc, 0, self.err)
        user = self.chat.requests[0]["body"]["messages"][1]["content"]
        self.assertIn("+// NEW line 5", user)
        self.assertNotIn("FULL TEXT", user)
        self.assertIn("Reviewed from the diff only (full text over the token budget): "
                      "`crates/long/src/lib.rs`", self.one_comment())

    def test_parts_past_the_request_limit_are_named_not_reviewed(self):
        head = self.big_change()
        self.chat.answer = self.answer_per_file
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="5000", DEEPSEEK_MAX_CHUNKS="1")
        self.assertEqual(self.rc, 0, self.err)
        self.assertEqual(len(self.chat.requests), 1)
        body = self.one_comment()
        self.assertIn("problem in p", body)
        self.assertIn("- `crates/q/src/lib.rs`: past the limit of 1 requests per review", body)
        self.assertIn("- `crates/r/src/lib.rs`: past the limit of 1 requests per review", body)

    def test_one_invalid_part_makes_the_whole_review_incomplete(self):
        head = self.big_change()
        calls = []

        def answer(path, body):
            calls.append(1)
            return completion("nope" if len(calls) == 2 else findings_json())
        self.chat.answer = answer
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="5000")
        self.assert_incomplete(head, "part 2 of ", "not valid JSON")

    def test_a_budget_smaller_than_the_instructions_is_incomplete(self):
        head = self.change()
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="100")
        self.assert_incomplete(head, "DEEPSEEK_TOKEN_BUDGET (100) is smaller")
        self.assertEqual(self.chat.requests, [])

    # --- an incomplete review is reported, never a pass

    def test_a_missing_key_is_an_incomplete_review(self):
        head = self.change()
        for value in (None, "", "   "):
            self.github.requests.clear()
            self.run_review(head, DEEPSEEK_API_KEY=value)
            self.assert_incomplete(head, "the DEEPSEEK_API_KEY secret is not set")
            self.assertEqual(self.chat.requests, [])

    def test_an_api_error_is_incomplete_retried_and_never_echoes_the_key(self):
        head = self.change()
        # A hostile or buggy endpoint echoing the key back must not leak it into the comment.
        self.chat.answer = lambda path, body: (500, ("upstream failed for key %s" % KEY).encode())
        self.run_review(head, DEEPSEEK_RETRIES="2")
        body = self.assert_incomplete(head, "the DeepSeek API answered HTTP 500",
                                      "upstream failed for key ***")
        self.assertEqual(len(self.chat.requests), 3)
        self.assertIn("did not complete", self.out)
        self.assertNotIn(KEY, body)

    def test_a_client_error_is_not_retried(self):
        head = self.change()
        self.chat.answer = lambda path, body: (401, b'{"error": "invalid key"}')
        self.run_review(head, DEEPSEEK_RETRIES="2")
        self.assert_incomplete(head, "HTTP 401", "invalid key")
        self.assertEqual(len(self.chat.requests), 1)

    def test_a_rate_limit_is_retried_until_it_succeeds(self):
        head = self.change()
        answers = [(429, b"slow down"), completion(findings_json())]
        self.chat.answer = lambda path, body: answers.pop(0)
        self.run_review(head, DEEPSEEK_RETRIES="2")
        self.assertEqual(self.rc, 0, self.err)
        self.assertIn("No findings.", self.one_comment())

    def test_an_unreachable_endpoint_is_incomplete(self):
        head = self.change()
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        self.run_review(head, DEEPSEEK_API_BASE="http://127.0.0.1:%d" % port,
                        DEEPSEEK_RETRIES="0")
        self.assert_incomplete(head, "the DeepSeek API could not be reached")

    def test_invalid_json_is_an_incomplete_review(self):
        head = self.change()
        self.chat.answer = lambda path, body: completion("I found no issues!")
        self.run_review(head)
        self.assert_incomplete(head, "the model's answer is not valid JSON")
        self.github.requests.clear()
        self.chat.answer = lambda path, body: completion('{"findings": [', finish="length")
        self.run_review(head)
        self.assert_incomplete(head, "cut off at max_tokens")

    def test_findings_that_break_the_schema_are_an_incomplete_review(self):
        head = self.change()
        cases = [
            ('{"issues": []}', "not an object with a findings list"),
            ('[]', "not an object with a findings list"),
            (findings_json("x"), "finding 1 is not an object"),
            (findings_json(finding("F1", "P0", "a.rs", 1, "p")), "finding 1 has an invalid severity"),
            (findings_json(finding("F1", "P1", "", 1, "p")), "invalid file"),
            (findings_json(finding("F1", "P1", "a.rs", "12", "p")), "invalid line"),
            (findings_json(finding("F1", "P1", "a.rs", True, "p")), "invalid line"),
            (findings_json(finding("F1", "P1", "a.rs", 1, " ")), "invalid problem"),
            (findings_json(finding("", "P1", "a.rs", 1, "p")), "invalid id"),
            (findings_json(finding("F1", "P1", "a.rs", 1, "p", None)), "invalid fix"),
        ]
        for content, reason in cases:
            self.github.requests.clear()
            self.chat.answer = lambda path, body, c=content: completion(c)
            self.run_review(head)
            self.assert_incomplete(head, reason)

    def test_a_response_that_is_not_a_chat_completion_is_incomplete(self):
        head = self.change()
        self.chat.answer = lambda path, body: (200, b"<html>gateway</html>")
        self.run_review(head)
        self.assert_incomplete(head, "not a chat completion")

    def test_a_bad_base_sha_is_incomplete(self):
        head = self.change()
        self.run_review(head, PR_BASE_SHA="main")
        self.assert_incomplete(head, "PR_BASE_SHA is not a full commit SHA")

    def test_a_bad_setting_is_incomplete(self):
        head = self.change()
        self.run_review(head, DEEPSEEK_TOKEN_BUDGET="lots")
        self.assert_incomplete(head, "DEEPSEEK_TOKEN_BUDGET is not a whole number")

    def test_a_failed_post_exits_non_zero_without_leaking(self):
        head = self.change()
        self.github.answer = lambda path, body: (403, ("denied %s" % GH_TOKEN).encode())
        self.run_review(head)
        self.assertEqual(self.rc, 1)
        self.assertIn("posting the comment failed: HTTP 403 denied ***", self.err)

    def test_without_the_pull_request_context_nothing_is_posted(self):
        head = self.change()
        for name in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "PR_NUMBER", "PR_HEAD_SHA"):
            self.run_review(head, **{name: None})
            self.assertEqual(self.rc, 2, name)
            self.assertEqual(self.comments(), [])
            self.assertEqual(self.chat.requests, [])


class WorkflowTest(unittest.TestCase):
    """The workflow file, read as text (no YAML parser in the standard library)."""

    def setUp(self):
        self.text = WORKFLOW.read_text()
        self.lines = [l.rstrip() for l in self.text.splitlines() if not l.lstrip().startswith("#")]

    def block(self, key):
        """The lines under a top-level key, up to the next top-level key."""
        start = self.lines.index(key + ":")
        out = []
        for line in self.lines[start + 1:]:
            if line and not line.startswith(" "):
                break
            if line:
                out.append(line.strip())
        return out

    def test_triggers_only_on_pull_requests_of_this_repository_that_are_ready(self):
        self.assertEqual(self.block("on"),
                         ["pull_request:", "types: [opened, ready_for_review, synchronize, reopened]"])
        code = "\n".join(self.lines)
        self.assertNotIn("pull_request_target", code)
        self.assertNotIn("workflow_run", code)
        conditions = [l.strip() for l in self.lines if l.strip().startswith("if:")]
        self.assertEqual(conditions, [
            "if: github.event.pull_request.draft == false && "
            "github.event.pull_request.head.repo.full_name == github.repository"])

    def test_permissions_are_read_contents_and_write_pull_requests_only(self):
        self.assertEqual(self.block("permissions"), ["contents: read", "pull-requests: write"])
        self.assertEqual(sum(l.strip().startswith("permissions:") for l in self.lines), 1)

    def test_a_new_head_cancels_the_old_review(self):
        self.assertEqual(self.block("concurrency"), [
            "group: deepseek-review-${{ github.event.pull_request.number }}",
            "cancel-in-progress: true"])

    def test_the_secret_reaches_only_the_review_step(self):
        secrets = [l.strip() for l in self.lines if "secrets." in l]
        self.assertEqual(secrets, ["DEEPSEEK_API_KEY: ${{ secrets.DEEPSEEK_API_KEY }}"])
        self.assertIn("DEEPSEEK_MODEL: ${{ vars.DEEPSEEK_MODEL }}", self.text)
        self.assertIn("persist-credentials: false", self.text)
        self.assertIn("run: python3 scripts/deepseek-review.py", self.text)
        # Untrusted PR text reaches the script through the environment, never the run line.
        runs = [l for l in self.lines if l.strip().startswith("run:")]
        self.assertTrue(all("${{" not in l for l in runs), runs)


if __name__ == "__main__":
    unittest.main(verbosity=1)
