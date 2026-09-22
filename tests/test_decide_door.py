"""The awdecide door rung: ask before the local engine, teach after a finish,
and refuse the untaught shape by name.

Measured against the live door 2026-09-19: an untaught fork answers
``{"answer": "yes", "confidence": 0.0, "source": "none", "learned_from": 0}``.
A caller that reads ``answer`` alone acts on nothing, so the rung treats that
as no-answer and falls through to awpredict exactly as if the door were off.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from awrise import cli


class _Door(BaseHTTPRequestHandler):
    reply: dict = {}
    seen: list = []

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's own spelling
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        type(self).seen.append((self.path, body))
        payload = json.dumps(type(self).reply).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_a):  # keep the test output clean
        return


@pytest.fixture
def door(monkeypatch):
    _Door.seen = []
    server = HTTPServer(("127.0.0.1", 0), _Door)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv(cli.DECIDE_URL_ENV, f"http://127.0.0.1:{server.server_address[1]}")
    yield _Door
    server.shutdown()


JOB = {
    "interval_s": 900.0,
    "timeout_s": 300,
    "last_state": "timeout",
    "consecutive_failures": 2,
    "detach": False,
}


def test_the_door_answers_when_it_has_evidence(door):
    door.reply = {
        "source": "engine",
        "confidence": 0.25,
        "learned_from": 3,
        "p_yes": 1.0,
        "answer": "yes",
    }
    verdict = cli._decide_verdict("j", JOB)
    assert verdict["verdict"] == "bad"
    assert verdict["mode"] == "door:engine" and verdict["rows"] == 3
    assert "p(fail)=1.00" in verdict["reason"]


def test_a_confident_no_reads_as_good(door):
    door.reply = {
        "source": "engine",
        "confidence": 0.9,
        "learned_from": 12,
        "p_yes": 0.1,
        "answer": "no",
    }
    assert cli._decide_verdict("j", JOB)["verdict"] == "good"


def test_the_untaught_shape_is_no_answer(door):
    """The exact live reply from a fork nobody has taught."""
    door.reply = {
        "source": "none",
        "confidence": 0.0,
        "learned_from": 0,
        "answer": "yes",
        "p_yes": None,
    }
    assert cli._decide_verdict("j", JOB) is None


def test_a_door_that_errors_is_no_answer(door):
    door.reply = {"error": "decide door unreachable (ConnectError)"}
    assert cli._decide_verdict("j", JOB) is None


def test_an_absent_door_is_no_answer(monkeypatch):
    monkeypatch.delenv(cli.DECIDE_URL_ENV, raising=False)
    assert cli._decide_post("/decide", {}, 0.5) is None


def test_teach_sends_the_outcome_and_never_the_command(door):
    door.reply = {"ok": True}
    outcome = cli.Outcome(state="timeout", reason="killed after 3300s", exit_code=None)
    cli._decide_teach_async(
        cli.Path("."), "fleet-gates", dict(JOB, run="secret --token x"), outcome
    )
    for _ in range(50):
        if door.seen:
            break
        import time

        time.sleep(0.1)
    assert door.seen, "teach never reached the door"
    path, body = door.seen[-1]
    assert path == "/decide/teach"
    assert body["answer"] == "yes" and body["reward"] == 1.0
    assert body["fork"] == cli.DECIDE_FORK
    assert "run" not in body["state"] and "secret" not in json.dumps(body)
