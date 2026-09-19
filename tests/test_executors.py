"""The optional executors: python, http, awrun, agent, session.

None of them is reached unless a job names it, none of their imports happens
at module import time, and every failure they can have is a closed-set state
with a reason the code measured. The fake daemon here exists to PIN the
"turn complete" signal: the session executor polls until it sees that one
event kind, so a test that never asserts which kind it is would let a rename
upstream turn every session wake into a silent timeout.
"""

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest
from awrise import executors, store

HAS_AWRUN = False
try:  # optional by contract -- the whole point of this slice
    import awrun.store as _awrun_store  # noqa: F401

    HAS_AWRUN = True
except ImportError:  # pragma: no cover - depends on the host
    _awrun_store = None


# ------------------------------------------------------------------- helpers


def write_fake_tool(bindir: Path, name: str, body: str) -> Path:
    """A real executable on PATH that runs ``body`` as Python.

    A `.cmd` shim on Windows and a shebang script elsewhere, because that is
    what a pip console script actually looks like on each OS -- and the
    Windows shim is exactly the case a bare-name spawn cannot start.
    """
    bindir.mkdir(parents=True, exist_ok=True)
    impl = bindir / f"{name}_impl.py"
    with open(impl, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(body)
    if sys.platform == "win32":
        launcher = bindir / f"{name}.cmd"
        with open(launcher, "w", encoding="utf-8", newline="") as fh:
            fh.write(f'@echo off\r\n"{sys.executable}" "{impl}" %*\r\n')
    else:
        launcher = bindir / name
        with open(launcher, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f'#!/bin/sh\nexec {sys.executable!s} {str(impl)!r} "$@"\n')
        launcher.chmod(0o755)
    return launcher


@pytest.fixture
def bindir(tmp_path, monkeypatch) -> Path:
    path = tmp_path / "bin"
    path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PATH", str(path) + os.pathsep + os.environ.get("PATH", ""))
    return path


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    return store.home()


def job(**spec) -> dict:
    base = dict(store.SPEC_DEFAULTS)
    base.update({"every": "15m", "interval_s": 900.0, "timeout_s": 30})
    base.update(spec)
    return base


def wake(**extra) -> dict:
    envelope = {"wake_id": "w-test0001", "pass_id": "p-test0001", "job": "j"}
    envelope.update(extra)
    return envelope


# ------------------------------------------------------- the guarded imports

GUARD_PROBE = r"""
import sys


class Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("awrun", "adk"):
            raise ImportError("blocked for the test: " + name)
        return None


sys.meta_path.insert(0, Blocker())
import awrise            # noqa: E402
import awrise.cli        # noqa: E402
import awrise.executors as ex   # noqa: E402
import awrise.store      # noqa: E402
import awrise.selftest   # noqa: E402

spec = dict(awrise.store.SPEC_DEFAULTS)
spec.update({"executor": "awrun", "run": "render {}", "timeout_s": 5})
out = ex.run(spec, {"wake_id": "w-x"})
print(out.state, out.reason)
"""


def test_every_optional_import_is_guarded():
    """awrise imports, and RUNS, on a machine with neither awrun nor adk."""
    done = subprocess.run(
        [sys.executable, "-c", GUARD_PROBE], capture_output=True, timeout=120, encoding="utf-8"
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "error awrun_not_installed", done.stdout


def test_no_optional_dependency_is_imported_at_module_level():
    """The negative twin: a top-level import would make the probe above fail,
    so assert the module list directly too -- a guarded import that moved to
    the top would otherwise only be caught on a host without the package."""
    probe = (
        "import sys, awrise, awrise.cli, awrise.executors, awrise.store;"
        "print([m for m in sys.modules if m.split('.')[0] in ('awrun', 'adk')])"
    )
    done = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, timeout=120, encoding="utf-8"
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "[]", done.stdout


# ------------------------------------------------------------------- dispatch


def test_run_dispatches_on_the_executor_key():
    out = executors.run(job(executor="shell", run='python -c "pass"'), wake())
    assert out.state in ("success", "failure")


def test_an_unknown_executor_is_an_error_not_a_shell_command(tmp_path):
    marker = tmp_path / "ran"
    spec = job(executor="sneaky", run=f"python -c \"open(r'{marker}','w').close()\"")
    out = executors.run(spec, wake())
    assert out.state == "error" and out.reason == "unknown_executor:sneaky"
    assert not marker.exists(), "an unknown kind fell through to the shell"


# --------------------------------------------------------------------- python


def test_python_executor_runs_in_this_interpreter():
    out = executors.run_python(job(run="import sys; print(sys.version_info[:2])"), wake())
    assert out.state == "success" and out.exit_code == 0
    assert str(sys.version_info[0]) in out.stdout_tail


def test_python_executor_failure_is_a_failure_not_an_error():
    out = executors.run_python(job(run="raise SystemExit(7)"), wake())
    assert out.state == "failure" and out.exit_code == 7


def test_a_hung_python_callable_still_hits_the_timeout():
    """The reason this is a subprocess: an in-process callable that blocks
    cannot be killed, and would take every other job in the pass with it."""
    out = executors.run_python(job(run="import time; time.sleep(120)", timeout_s=1), wake())
    assert out.state == "timeout", out.reason
    assert out.reason.startswith("killed after 1s"), out.reason


def test_an_empty_python_body_is_skipped_not_run():
    assert executors.run_python(job(run="   "), wake()).state == "skipped_empty"


# ----------------------------------------------------------------------- http


class _Handler(BaseHTTPRequestHandler):
    seen: list = []

    def log_message(self, *args):  # noqa: D102 - silence the test server
        return

    def _reply(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        # The REQUEST body, read and kept: a fake that throws it away lets a
        # test assert a method and call the body covered, and the executor
        # could then send no body at all -- or half of one -- with every test
        # still green.
        raw = self.rfile.read(length) if length else b""
        type(self).seen.append(
            {
                "path": parsed.path,
                "method": self.command,
                "auth": self.headers.get("Authorization"),
                "content_type": self.headers.get("Content-Type"),
                "body": raw,
            }
        )
        body = b'{"ok": true}'
        if parsed.path == "/401":
            self.send_response(401)
        elif parsed.path == "/500":
            self.send_response(500)
            # Distinctive on purpose: what the server said about the failure
            # is the only thing an operator reading the row has to go on.
            body = b'{"error": "the widget exploded"}'
        elif parsed.path == "/moved":
            self.send_response(302)
            self.send_header("Location", "http://elsewhere.invalid/steal")
        else:
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 - the name BaseHTTPRequestHandler dispatches on
        self._reply()

    def do_POST(self):  # noqa: N802 - the name BaseHTTPRequestHandler dispatches on
        self._reply()


@pytest.fixture
def http_server():
    _Handler.seen = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def test_http_success_and_the_body_tail(http_server):
    out = executors.run_http(job(run=f"{http_server}/ok"), wake())
    assert out.state == "success" and out.reason == "http_200"
    assert "ok" in out.stdout_tail


def test_http_401_is_an_error_and_500_is_a_failure(http_server):
    assert executors.run_http(job(run=f"{http_server}/401"), wake()).reason == "http_401"
    assert executors.run_http(job(run=f"{http_server}/401"), wake()).state == "error"
    assert executors.run_http(job(run=f"{http_server}/500"), wake()).state == "failure"


def test_http_5xx_carries_the_body_tail(http_server):
    """A 500 with no tail is a row that says only that something went wrong.
    What the server said is the whole content of the wake's record."""
    out = executors.run_http(job(run=f"{http_server}/500"), wake())
    assert out.state == "failure" and out.reason == "http_500"
    assert out.exit_code == 500
    assert "the widget exploded" in out.stderr_tail, out.stderr_tail
    # negative twin: a 200's body is the STDOUT tail, and the error tail is empty
    good = executors.run_http(job(run=f"{http_server}/ok"), wake())
    assert "ok" in good.stdout_tail and good.stderr_tail == ""


def test_http_method_and_body_are_read_from_the_command(http_server):
    out = executors.run_http(job(run=f'POST {http_server}/thing {{"a": 1}}'), wake())
    assert out.state == "success"
    assert _Handler.seen[-1]["method"] == "POST"
    assert _Handler.seen[-1]["body"] == b'{"a": 1}', _Handler.seen[-1]
    assert _Handler.seen[-1]["content_type"] == "application/json"


def test_the_body_survives_the_method_being_omitted(http_server):
    """`URL BODY` is in the grammar, and splitting it on the first space
    posts `{"a":` -- malformed JSON, with nothing in the row saying it was
    cut, and only for the shape that leaves the method out."""
    out = executors.run_http(job(run=f'{http_server}/thing {{"a": 1, "b": [2, 3]}}'), wake())
    assert out.state == "success"
    sent = _Handler.seen[-1]
    assert sent["method"] == "GET", "the method is only defaulted, never invented"
    assert json.loads(sent["body"].decode("utf-8")) == {"a": 1, "b": [2, 3]}, sent


def test_a_url_with_no_body_sends_none(http_server):
    """The negative twin: nothing after the URL is no body and no content type."""
    out = executors.run_http(job(run=f"{http_server}/ok"), wake())
    assert out.state == "success"
    assert _Handler.seen[-1]["body"] == b""
    assert _Handler.seen[-1]["content_type"] is None


def test_a_name_that_does_not_resolve_is_skipped_unresolvable():
    """A laptop off the network has not run the job badly; it has not run it."""
    out = executors.run_http(job(run="http://awrise.invalid.invalid/health", timeout_s=5), wake())
    assert out.state == "skipped_unresolvable", out.reason
    assert out.reason.startswith("dns:")


def test_a_non_http_scheme_is_refused_before_any_request():
    out = executors.run_http(job(run="file:///etc/passwd"), wake())
    assert out.state == "error" and out.reason.startswith("unsupported_scheme:")


def test_the_bearer_comes_from_a_confined_file_and_is_sent(http_server, home):
    token = home / "token"
    with open(token, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("s3cret\n")
    out = executors.run_http(job(run=f"{http_server}/ok", bearer_file=str(token)), wake())
    assert out.state == "success"
    assert _Handler.seen[-1]["auth"] == "Bearer s3cret"


def test_a_bearer_path_outside_the_confinement_is_refused(tmp_path, home, http_server):
    stray = tmp_path / "elsewhere" / "id_rsa"
    stray.parent.mkdir(parents=True, exist_ok=True)
    with open(stray, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("not a bearer")
    assert executors.bearer_path_problem(str(stray)) is not None
    out = executors.run_http(job(run=f"{http_server}/ok", bearer_file=str(stray)), wake())
    assert out.state == "error" and out.reason.startswith("bearer_file_refused:")
    assert not _Handler.seen, "the request was made before the path was judged"


def test_a_missing_bearer_file_inside_the_confinement_is_an_error(home, http_server):
    out = executors.run_http(job(run=f"{http_server}/ok", bearer_file=str(home / "absent")), wake())
    assert out.state == "error" and out.reason.startswith("bearer_file_unreadable:")


def test_a_redirect_is_refused_rather_than_followed_with_the_bearer(http_server, home):
    """urllib re-sends the headers it was given, so a 302 is otherwise enough
    to make a job hand its token to whatever host the answer names."""
    token = home / "token"
    token.write_text("s3cret", encoding="utf-8")
    out = executors.run_http(job(run=f"{http_server}/moved", bearer_file=str(token)), wake())
    assert out.state == "failure" and out.reason == "http_302", out.reason
    assert [c["path"] for c in _Handler.seen] == ["/moved"]


@pytest.mark.skipif(
    sys.platform == "win32", reason="a symlink needs developer mode or admin on Windows"
)
def test_a_symlink_out_of_the_home_does_not_escape_the_confinement(tmp_path, home):
    """The confinement compares REAL paths: comparing spellings would be
    passed by one `ln -s` inside a directory the operator already writes."""
    secret = tmp_path / "outside" / "id_rsa"
    secret.parent.mkdir(parents=True, exist_ok=True)
    secret.write_text("not yours", encoding="utf-8")
    link = home / "looks-local"
    link.symlink_to(secret)
    assert executors.bearer_path_problem(str(link)) is not None
    # negative twin: a real file in the same directory is still accepted
    real = home / "token"
    real.write_text("mine", encoding="utf-8")
    assert executors.bearer_path_problem(str(real)) is None


# ---------------------------------------------------------------------- awrun


def test_awrun_refuses_the_two_kinds_by_name():
    for kind in ("ci", "comet-deploy"):
        out = executors.run_awrun(job(run=f"{kind} {{}}"), wake())
        assert out.state == "error" and out.reason == f"awrun_kind_refused:{kind}"


def test_awrun_spec_must_be_a_json_object():
    assert executors.run_awrun(job(run="render [1,2]"), wake()).reason.startswith(
        "spec_not_an_object:"
    )
    assert executors.run_awrun(job(run="render {nope}"), wake()).reason.startswith("spec_not_json:")


#: The one kind used here. `agent` is the only non-GPU kind awrise will
#: submit, and it HAS a real run handler -- draining it would start a coding
#: agent from a test. `render` is host-registered with no handler in this
#: process, so a drain claims it, finishes it `failed` and executes nothing.
AWRUN_SPEC = '{"what": "a-thing", "priority": 3, "gpu": {"class": "arc", "vram_mb": 1024}}'


@pytest.fixture
def awrun_queue(tmp_path, monkeypatch) -> Path:
    queue = tmp_path / "queue"
    monkeypatch.setenv("AITHER_AWRUN_DIR", str(queue))
    # No relay credential anywhere: the dispatcher broadcasts best-effort, and
    # a test must not post to a server that happens to be up on this host.
    monkeypatch.delenv("AITHER_RELAY_TOKEN", raising=False)
    monkeypatch.delenv("AITHER_SESSION_BEARER", raising=False)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path / "nohome"))
    return queue


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_awrun_submit_returns_queued_with_a_handoff_id(awrun_queue):
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake())
    assert out.state == "queued", out.reason
    assert out.reason == "awrun_render_queued"
    assert out.handoff_id and out.handoff_id.startswith("r-")
    written = list((awrun_queue / "queued").glob("*.json"))
    assert [p.stem for p in written] == [out.handoff_id]
    item = json.loads(written[0].read_text(encoding="utf-8"))
    assert item["kind"] == "render" and item["priority"] == 3
    assert "priority" not in item["spec"], "priority leaked into the spec"


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_drain_claims_the_item_through_dispatch_once(awrun_queue):
    """`render` has no run handler registered here, so the item is claimed,
    started and finished `failed` WITHOUT executing anything -- which is
    exactly the evidence that dispatch_once ran."""
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake(drain=True))
    assert out.state == "failure" and out.reason == "awrun_drained:failed", out.reason
    assert out.handoff_id and out.handoff_id.startswith("r-")
    assert not list((awrun_queue / "queued").glob("*.json"))
    assert len(list((awrun_queue / "failed").glob("*.json"))) == 1


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_the_drain_never_runs_another_actors_item(awrun_queue, monkeypatch):
    """The refused kinds are refused on BOTH halves.

    The queue's dispatcher claims the highest-priority queued item of any
    kind, so a drain handed the bare queue runs whatever another process
    posted -- including the two kinds this module refuses by name, whose
    handlers spend the host's own credentials, with a cron entry in front of
    them. The drain must claim the item this wake submitted and nothing else.
    """
    from awrun import dispatcher
    from awrun.store import RunStore

    ran = []
    monkeypatch.setitem(
        dispatcher._RUN_FNS, "ci", lambda item: (ran.append(item.id), (0, "ci ran"))[1]
    )
    foreign = RunStore(str(awrun_queue)).submit("ci", {"workflow": "deploy.yml"}, priority=99)
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake(drain=True))
    assert ran == [], "the drain ran another actor's ci item"
    assert out.handoff_id and out.handoff_id != foreign.id
    # the stranger's item is untouched: still queued, still unclaimed
    still = json.loads((awrun_queue / "queued" / f"{foreign.id}.json").read_text(encoding="utf-8"))
    assert still["status"] == "queued" and not still.get("claimed_by"), still


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_the_drained_outcome_is_this_wakes_own_run(awrun_queue):
    """A stranger's result is never this wake's verdict, and the id in the
    row is the id of the run this wake queued -- that link is the whole
    reason `queued` carries a handoff id at all."""
    from awrun.store import RunStore

    foreign = RunStore(str(awrun_queue)).submit("ci", {}, priority=99)
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake(drain=True))
    assert out.handoff_id != foreign.id
    finished = [
        json.loads(p.read_text(encoding="utf-8")) for p in (awrun_queue / "failed").glob("*.json")
    ]
    assert [item["id"] for item in finished] == [out.handoff_id], finished
    queued = [
        json.loads(p.read_text(encoding="utf-8")) for p in (awrun_queue / "queued").glob("*.json")
    ]
    assert [item["id"] for item in queued] == [foreign.id], queued


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_a_requeued_item_is_queued_not_a_failure(awrun_queue, monkeypatch):
    """The queue puts an item BACK when a lease says "not now". That is
    contention it handled, not a wake that failed, and paging someone for it
    teaches them to ignore the pager."""
    from awrun import dispatcher

    submitted = {}

    def fake_dispatch(store_view, *, worker_id, **kwargs):
        claimed = store_view.claim_next(worker_id=worker_id)
        submitted["id"] = claimed.id
        return store_view.requeue(claimed.id, 0.0, wait={"reason": "gpu busy"})

    monkeypatch.setattr(dispatcher, "dispatch_once", fake_dispatch)
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake(drain=True))
    assert out.state == "queued" and out.reason == "awrun_render_requeued", out.reason
    assert out.handoff_id == submitted["id"]


@pytest.mark.skipif(not HAS_AWRUN, reason="awrun is not importable on this host")
def test_without_drain_nothing_is_claimed(awrun_queue):
    out = executors.run_awrun(job(run=f"render {AWRUN_SPEC}"), wake())
    assert out.state == "queued"
    assert len(list((awrun_queue / "queued").glob("*.json"))) == 1
    assert not list((awrun_queue / "failed").glob("*.json"))


# ---------------------------------------------------------------------- agent

FAKE_ADK = """
import sys
print("agent says hello: " + " ".join(sys.argv[1:]))
"""


def test_agent_falls_back_to_adk_chat(bindir):
    write_fake_tool(bindir, "adk", FAKE_ADK)
    out = executors.run_agent(job(run="hydra review the diff"), wake())
    assert out.state == "success", out.reason
    assert out.reason.startswith("fallback_adk_chat:"), out.reason
    assert "chat hydra review the diff" in out.stdout_tail


def test_without_adk_the_agent_executor_says_so(bindir, monkeypatch):
    monkeypatch.setattr(executors, "which", lambda name: None)
    out = executors.run_agent(job(run="hydra review"), wake())
    assert out.state == "error" and out.reason == "no_agent_executor:adk"


def test_an_agent_job_with_no_task_is_refused(bindir):
    write_fake_tool(bindir, "adk", FAKE_ADK)
    out = executors.run_agent(job(run="hydra"), wake())
    assert out.state == "error" and out.reason == "agent_task_missing:hydra"


def test_a_public_run_agent_is_used_inline_when_one_exists(monkeypatch):
    calls = []

    def fake_public():
        def run_agent(item):
            calls.append(item)
            return 0, "inline done"

        return run_agent

    monkeypatch.setattr(executors, "_public_run_agent", fake_public)
    out = executors.run_agent(job(run="hydra review"), wake())
    if not HAS_AWRUN:  # RunItem comes from the same optional package
        assert out.state == "error" and out.reason.startswith("agent_inline_failed:")
        return
    assert out.state == "success" and out.reason == "agent_inline:exit_0"
    assert "inline done" in out.stdout_tail
    assert calls and calls[0].spec == {"agent": "hydra", "task": "review"}


# -------------------------------------------------------------------- session


class _Daemon(BaseHTTPRequestHandler):
    """The harness daemon, reduced to the four calls this executor makes."""

    seen: list = []
    #: When False the session never reports a completed turn, so the executor
    #: must end on its own deadline -- and still delete the session. It keeps
    #: STREAMING meanwhile, which is what a live turn does: every page comes
    #: back non-empty, so a poll loop that only pauses on an empty page never
    #: pauses at all.
    complete = True
    seq = 0

    def log_message(self, *args):
        return

    def _json(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _record(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        type(self).seen.append(
            {
                "method": self.command,
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "body": json.loads(raw) if raw else None,
            }
        )

    def do_POST(self):
        self._record()
        if self.path == "/sessions":
            self._json(200, {"id": "s-42", "state": "idle"})
        else:
            self._json(200, {"ok": True, "turn": 1, "seq": 0})

    def do_GET(self):
        self._record()
        if type(self).complete:
            events = [
                {"seq": 1, "kind": "text.delta", "text": "working"},
                {"seq": 2, "kind": "turn.completed", "text": ""},
            ]
        else:
            type(self).seq += 1
            events = [{"seq": type(self).seq, "kind": "text.delta", "text": "working"}]
        self._json(200, {"events": events, "last_seq": events[-1]["seq"], "state": "working"})

    def do_DELETE(self):
        self._record()
        self._json(200, {"stopped": True})


@pytest.fixture
def daemon(monkeypatch):
    _Daemon.seen = []
    _Daemon.complete = True
    _Daemon.seq = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Daemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv(executors.SESSION_URL_ENV, f"http://127.0.0.1:{server.server_address[1]}")
    try:
        yield _Daemon
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def _token(home: Path) -> Path:
    path = home / "harness_token"
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("t0ken\n")
    return path


def test_a_session_wake_creates_sends_polls_and_deletes(daemon, home):
    out = executors.run_session(job(run="say hi", bearer_file=str(_token(home))), wake())
    assert out.state == "success" and out.reason == "turn_completed", out.reason
    assert out.handoff_id == "s-42"
    assert "working" in out.stdout_tail
    calls = [(c["method"], c["path"].split("?")[0]) for c in daemon.seen]
    assert calls[0] == ("POST", "/sessions")
    assert calls[1] == ("POST", "/sessions/s-42/input")
    assert ("GET", "/sessions/s-42/events") in calls
    assert calls[-1] == ("DELETE", "/sessions/s-42"), calls
    assert all(c["auth"] == "Bearer t0ken" for c in daemon.seen)


def test_the_turn_complete_signal_is_the_one_the_daemon_emits(daemon, home):
    """Pin the literal. Rename it upstream and every session wake becomes a
    timeout with no error anywhere -- the quietest failure this brick has."""
    assert executors.TURN_COMPLETE == "turn.completed"
    daemon.complete = False
    out = executors.run_session(
        job(run="say hi", timeout_s=2, bearer_file=str(_token(home))), wake()
    )
    assert out.state == "timeout", out.reason
    assert daemon.seen[-1]["method"] == "DELETE", "a timed-out session was leaked"


def test_a_streaming_turn_is_polled_at_the_pace_the_constant_sets(daemon, home):
    """A live turn streams deltas, so every page comes back non-empty. A
    pause that only runs on an empty page therefore never runs, and the
    executor asks the local daemon for events as fast as the socket allows,
    for the whole turn, on every wake -- measured at ~112 requests a second
    against the one service this executor exists to talk to.
    """
    daemon.complete = False
    timeout_s = 2
    out = executors.run_session(
        job(run="say hi", timeout_s=timeout_s, bearer_file=str(_token(home))), wake()
    )
    assert out.state == "timeout", out.reason
    polls = [c for c in daemon.seen if c["method"] == "GET" and "/events" in c["path"]]
    ceiling = int(timeout_s / executors.SESSION_POLL_S) + 4
    assert len(polls) <= ceiling, f"{len(polls)} polls in {timeout_s}s (ceiling {ceiling})"
    assert len(polls) >= 2, "the loop must still poll more than once"
    assert daemon.seen[-1]["method"] == "DELETE", "a timed-out session was leaked"


def test_a_session_is_deleted_even_when_the_input_call_fails(daemon, home, monkeypatch):
    real = executors._session_call

    def fail_on_input(url, token, *, method, payload, timeout):
        if url.endswith("/input"):
            raise OSError("the socket went away")
        return real(url, token, method=method, payload=payload, timeout=timeout)

    monkeypatch.setattr(executors, "_session_call", fail_on_input)
    out = executors.run_session(job(run="say hi", bearer_file=str(_token(home))), wake())
    assert out.state == "error" and out.reason.startswith("daemon_unreachable:")
    assert daemon.seen[-1]["method"] == "DELETE", "a session with no turn was leaked"


def test_a_permission_mode_outside_the_allowlist_never_reaches_the_daemon(daemon, home):
    out = executors.run_session(
        job(run="say hi", permission_mode="bypassPermissions", bearer_file=str(_token(home))),
        wake(),
    )
    assert out.state == "error" and out.reason == "permission_mode_refused:bypassPermissions"
    assert not daemon.seen, "the session was created before the mode was judged"


def test_an_allowlisted_permission_mode_is_passed_through(daemon, home):
    out = executors.run_session(
        job(run="say hi", permission_mode="plan", bearer_file=str(_token(home))), wake()
    )
    assert out.state == "success"
    assert daemon.seen[0]["body"]["permission_mode"] == "plan"


def test_without_a_token_the_session_executor_refuses(daemon, home, monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path / "nowhere"))
    out = executors.run_session(job(run="say hi"), wake())
    assert out.state == "error" and out.reason == "daemon_token_missing"
    assert not daemon.seen


def test_an_unreachable_daemon_is_an_error_not_a_failure(home, monkeypatch):
    monkeypatch.setenv(executors.SESSION_URL_ENV, "http://127.0.0.1:1")
    out = executors.run_session(job(run="hi", timeout_s=5, bearer_file=str(_token(home))), wake())
    assert out.state == "error" and out.reason.startswith("daemon_unreachable:"), out.reason
