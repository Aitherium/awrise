"""``report.memory: true`` -- the optional awm-backed memory sink.

Every property here reduces to one of two halves: the payload round-trips
through a REAL awm store scoped to this job, or a broken/absent ``awm`` never
touches the wake's own verdict. Every case runs against a TEMP ``AWRISE_HOME``
(the ``home`` fixture below); none of this ever opens
``~/.aither/awrise/awm``.
"""

from __future__ import annotations

import argparse
import json
import socket
import time
from pathlib import Path

import pytest
from awrise import cli, ledger, store

#: A python-executor job body that echoes exactly what it finds in its own
#: env -- the one observable a subprocess can hand back to the test.
ECHO_ENV = "import os, sys; sys.stdout.write(os.environ.get('AWRISE_MEMORY_JSON', 'MISSING'))"


def _close_memory_engines() -> None:
    """Every cached MemoryStore lives on `cli`'s one worker thread (sqlite3
    connections are thread-affine), so closing them must run THERE too."""

    def _work() -> None:
        for engine in cli._MEMORY_ENGINES.values():
            engine.close()
        cli._MEMORY_ENGINES.clear()

    try:
        cli._call_with_timeout(_work, 5.0)
    except Exception:
        cli._MEMORY_ENGINES.clear()


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv(cli.MEMORY_ENV, raising=False)
    _close_memory_engines()
    try:
        yield store.home()
    finally:
        _close_memory_engines()


def _add(name: str = "m", every: str = "1h", run: str = "pass", **extra) -> int:
    args = argparse.Namespace(
        name=name,
        every=every,
        run=run,
        timeout=None,
        cwd=None,
        at=None,
        allow_overrun=False,
        disabled=False,
        detach=False,
        missed=None,
        executor="python",
        bearer_file=None,
        permission_mode=None,
        report_relay=None,
        card_after=0,
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _set(name: str, key: str, raw: str) -> int:
    return cli._dispatch(
        cli.cmd_set,
        argparse.Namespace(name=name, assignments=[f"{key}={raw}"], allow_overrun=False),
    )


def _run_due() -> int:
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=None), argparse.Namespace(quiet=True, invoker="test")
    )


def _later(monkeypatch, seconds: int) -> None:
    monkeypatch.setenv("AWRISE_NOW", f"+{seconds}")


def _finished(base: Path, name: str = "m") -> list:
    return [
        r for r in ledger.read(base) if r.get("event") == "finished" and r.get("job") == name
    ]


def _report_errors(base: Path) -> list:
    return [r for r in ledger.read(base) if r.get("event") == "report_error"]


# --------------------------------------------------------------- off by default


def test_memory_off_by_default_sets_nothing_and_creates_no_store(home):
    assert _add(run=ECHO_ENV) == 0
    assert _run_due() == 0
    rows = _finished(home)
    assert rows and rows[0]["stdout_tail"] == "MISSING"
    assert not (home / "awm").exists()
    assert not _report_errors(home)


def test_report_memory_must_be_a_boolean(home):
    assert _add() == 0
    assert _set("m", "report.memory", "sideways") == 1


# ------------------------------------------------------------- happy path


def test_memory_on_exports_empty_array_on_first_wake(home):
    assert _add(run=ECHO_ENV) == 0
    assert _set("m", "report.memory", "true") == 0
    assert _run_due() == 0
    rows = _finished(home)
    assert rows[0]["stdout_tail"] == "[]"
    assert not _report_errors(home)
    assert (home / "awm" / "memory.db").is_file()


def test_memory_round_trips_two_wakes_most_recent_first(home, monkeypatch):
    assert _add(run=ECHO_ENV) == 0
    assert _set("m", "report.memory", "true") == 0
    assert _run_due() == 0  # wake 1: recalls [], remembers wake-1
    _later(monkeypatch, 3700)
    assert _run_due() == 0  # wake 2: recalls [wake-1], remembers wake-2
    payload_2 = json.loads(_finished(home)[-1]["stdout_tail"])
    assert len(payload_2) == 1
    _later(monkeypatch, 7400)
    assert _run_due() == 0  # wake 3: recalls [wake-2, wake-1]
    payload_3 = json.loads(_finished(home)[-1]["stdout_tail"])
    assert len(payload_3) == 2, payload_3
    # remember() stamps `updated` from wall-clock time.time(), not the
    # simulated AWRISE_NOW the job spec runs on, so real write order is what
    # `recall` sorts by -- and wake 2 was written after wake 1 in real time.
    assert payload_3[0]["ts"] >= payload_3[1]["ts"]
    assert not _report_errors(home)


def test_memory_excludes_ancestor_scope_facts(home):
    import awm

    assert _add(run=ECHO_ENV) == 0
    assert _set("m", "report.memory", "true") == 0
    ancestor_db = awm.MemoryStore(home / "awm" / "memory.db")
    ancestor_db.remember(
        awm.Scope("awrise", socket.gethostname(), "*"),
        key="wake-poison",
        value=json.dumps(
            {"state": "success", "reason": "poison", "duration_s": 1.0, "exit_code": 0, "ts": "x"}
        ),
        kind="wake",
    )
    ancestor_db.close()
    assert _run_due() == 0
    payload = json.loads(_finished(home)[-1]["stdout_tail"])
    # An unfiltered recall() WOULD have returned the ancestor fact too (that
    # is awm's own decay, working as designed) -- proving the sink's exact-
    # scope filter is what keeps it out.
    scope = awm.Scope("awrise", socket.gethostname(), "m")
    raw = awm.MemoryStore(home / "awm" / "memory.db").recall(scope, kind="wake", limit=20)
    assert any(m.value.find("poison") != -1 for m in raw), "fixture did not set up ancestor decay"
    assert all("poison" not in json.dumps(fact) for fact in payload)


# ---------------------------------------------------------------- failure


def test_memory_failure_is_a_report_error_row_never_a_failed_wake(home, monkeypatch):
    assert _add(run="pass") == 0
    assert _set("m", "report.memory", "true") == 0

    def _boom(base):
        raise RuntimeError("store exploded")

    monkeypatch.setattr(cli, "_memory_store", _boom)
    assert _run_due() == 0
    rows = _finished(home)
    assert rows and rows[0]["state"] == "success"
    reasons = " ".join(r.get("reason") or "" for r in _report_errors(home))
    assert "memory_recall_failed" in reasons
    assert "memory_remember_failed" in reasons


# ----------------------------------------------------------- tick-path regression
#
# cli.py's own comments claimed report.memory's recall/remember
# calls were "off the tick path in every way that matters" -- measured
# 2026-09-19, false: both ran INLINE in `_run_due_pass`'s sequential
# `for name in sorted(jobs)` loop, so a degraded store cost every OTHER due
# job in the same pass up to 2 x MEMORY_TIMEOUT_S before it even got a turn.
# These two cases are the regression test for that fix, and the coverage
# gap the defect named: the old suite never put a SECOND due job behind a
# memory-enabled one, and never made the store hang rather than raise.


def test_memory_remember_never_blocks_its_own_caller(home, monkeypatch):
    """`_record`'s call into the memory sink must return immediately even
    when the underlying store call hangs -- this is the direct unit-level
    proof that `remember` cannot cost the per-pass loop any wait at all,
    independent of subprocess-spawn noise from an actual job run."""
    assert _add(run="pass") == 0
    assert _set("m", "report.memory", "true") == 0

    def _hang(base):
        time.sleep(2.0)  # bounded -- long enough that a synchronous
        # `remember` would clearly fail this assertion; short enough the
        # shared worker frees itself again well inside this test.
        raise RuntimeError("store exploded after hanging")

    monkeypatch.setattr(cli, "_memory_store", _hang)
    jobs = store.load(home)
    outcome = cli.Outcome("success", "ok")
    started = time.monotonic()
    cli._record(
        home,
        jobs,
        "m",
        cli.ledger.new_id("w-"),
        "p-test",
        "test",
        outcome,
        cli.clock.iso(cli.clock.now_utc()),
        executed=True,
    )
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, elapsed


def test_memory_hang_does_not_delay_an_unrelated_due_job(home, monkeypatch):
    """job 'b' (report.memory OFF) must get its own turn in the pass even
    while job 'a' (report.memory ON, due in the SAME pass) is still stuck
    inside a hung store call -- proven by LEDGER ORDER rather than
    wall-clock timing, which a subprocess-spawning job makes noisy: b's own
    ``started`` row can only appear before a's ``memory_remember_failed``
    row if b was dispatched WITHOUT waiting for a's `remember` to resolve,
    since that row is written by the background worker only once the hung
    call actually finishes.

    The store call is bounded, not literally infinite: a REAL "sleeps
    forever" mock would wedge awrise's one persistent memory-worker thread
    for the rest of this process, poisoning every later test in this suite
    that touches `report.memory` -- so this uses a short, finite hang
    instead.
    """
    assert _add(name="a", run="pass") == 0
    assert _set("a", "report.memory", "true") == 0
    assert _add(name="b", run="pass") == 0

    def _hang(base):
        time.sleep(1.5)
        raise RuntimeError("store exploded after hanging")

    monkeypatch.setattr(cli, "_memory_store", _hang)
    monkeypatch.setattr(cli, "MEMORY_TIMEOUT_S", 0.2)

    assert _run_due() == 0
    events = [(r.get("job"), r.get("event"), r.get("reason") or "") for r in ledger.read(home)]
    b_started_idx = next(
        (i for i, e in enumerate(events) if e[0] == "b" and e[1] == "started"), None
    )
    assert b_started_idx is not None, events
    remember_failed_idx = next(
        i
        for i, e in enumerate(events)
        if e[0] == "a" and e[1] == "report_error" and "memory_remember_failed" in e[2]
    )
    assert b_started_idx < remember_failed_idx, events
    assert _finished(home, "a") and _finished(home, "b")
    reasons = " ".join(r.get("reason") or "" for r in _report_errors(home))
    assert "memory_recall_failed" in reasons, reasons


def test_memory_recall_budget_is_shared_across_a_pass_not_per_job(home, monkeypatch):
    """A SECOND memory-enabled job due in the same pass must not buy a
    second full timeout once the pass's shared recall budget is already
    spent -- it skips the call outright rather than queuing behind the
    first hang."""
    assert _add(name="a", run="pass") == 0
    assert _set("a", "report.memory", "true") == 0
    assert _add(name="c", run="pass") == 0
    assert _set("c", "report.memory", "true") == 0

    calls = {"n": 0}

    def _hang(base):
        calls["n"] += 1
        if calls["n"] == 1:
            time.sleep(1.0)
        raise RuntimeError("store exploded after hanging")

    monkeypatch.setattr(cli, "_memory_store", _hang)
    monkeypatch.setattr(cli, "MEMORY_TIMEOUT_S", 0.25)

    assert _run_due() == 0
    reasons = [r.get("reason") or "" for r in _report_errors(home)]
    joined = " ".join(reasons)
    # a pays for the one real (bounded) attempt; c must find the pass's
    # budget already exhausted and skip the call rather than hang a second
    # time -- proving the budget is PER PASS, not per job.
    assert "memory_recall_failed" in joined, reasons
    assert any("memory_recall_skipped_pass_budget_exhausted" in r for r in reasons), reasons


# ----------------------------------------------------------------- explain


def test_explain_shows_recalled_memory_facts(home, capsys):
    assert _add(run=ECHO_ENV) == 0
    assert _set("m", "report.memory", "true") == 0
    assert _run_due() == 0
    capsys.readouterr()
    rc = cli._dispatch(cli.cmd_explain, argparse.Namespace(name="m", since=None))
    assert rc == 0
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if ln.startswith("memory")]
    assert lines, out
    assert lines[0].startswith("memory       ["), lines[0]


def test_explain_says_nothing_about_memory_when_off(home, capsys):
    assert _add(run=ECHO_ENV) == 0
    assert _run_due() == 0
    capsys.readouterr()
    rc = cli._dispatch(cli.cmd_explain, argparse.Namespace(name="m", since=None))
    assert rc == 0
    out = capsys.readouterr().out
    assert not [ln for ln in out.splitlines() if ln.startswith("memory")]
