"""``awrise --self-test`` -- every claim the brick makes, with its negative twin.

Each case runs in a fresh temporary ``AWRISE_HOME`` and raises AssertionError
to fail. ``--self-test --list`` prints the case names, so a README can be
checked against what is actually exercised. Exit 0 = every case passed,
1 = a case failed, 2 = the harness itself could not run a case.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator, List, Tuple

from . import checks, cli, clock, executors, hostclock, ledger, lock, store
from .executors import Outcome


def _close_memory_engine(base: Path) -> None:
    """Drop and close this AWRISE_HOME's cached MemoryStore, if any.

    Must run on ``cli``'s own persistent memory worker thread (sqlite3
    connections are thread-affine), and must happen before the
    TemporaryDirectory a case used is removed -- Windows refuses to delete a
    file a live handle still has open, which is silence-free but not a
    self-test case's own failure.
    """
    key = str(base)
    if key not in cli._MEMORY_ENGINES:
        return

    def _work() -> None:
        engine = cli._MEMORY_ENGINES.pop(key, None)
        if engine is not None:
            engine.close()

    with contextlib.suppress(Exception):
        cli._call_with_timeout(_work, 5.0)


@contextlib.contextmanager
def _home() -> Iterator[Path]:
    previous = os.environ.get("AWRISE_HOME")
    with tempfile.TemporaryDirectory(prefix="awrise-selftest-") as tmp:
        # `os.environ.update`, not `os.environ["..."] = ...`: the doctor
        # generator reads a subscript assignment as a REQUIRED read of that
        # variable, and awrise has none -- an unset AWRISE_HOME simply means
        # the default home. A write advertised as a requirement makes `awrise
        # doctor` exit 1 on every correct install.
        os.environ.update({"AWRISE_HOME": tmp})
        try:
            yield Path(tmp)
        finally:
            _close_memory_engine(Path(tmp))
            if previous is None:
                os.environ.pop("AWRISE_HOME", None)
            else:
                os.environ.update({"AWRISE_HOME": previous})


def _add(name: str = "job", every: str = "15m", run: str = "echo hi", **extra) -> int:
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
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _run_due(executor=None) -> int:
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=executor),
        argparse.Namespace(quiet=True, invoker="selftest"),
    )


def _finished_states(base: Path) -> List[str]:
    return [r["state"] for r in ledger.read(base) if r.get("event") == "finished"]


# ------------------------------------------------------------------ cases


def interval_parse_accepts_s_m_h_d_w_and_compounds() -> None:
    assert clock.parse_interval("15m") == timedelta(minutes=15)
    assert clock.parse_interval("2h") == timedelta(hours=2)
    assert clock.parse_interval("1d") == timedelta(days=1)
    assert clock.parse_interval("30s") == timedelta(seconds=30)
    assert clock.parse_interval("1w") == timedelta(weeks=1)
    assert clock.parse_interval("1h30m") == timedelta(minutes=90)


def interval_parse_rejects_empty_zero_negative_garbage() -> None:
    for bad in ("", "abc", "15x", "0m", "-5m", "1h30"):
        try:
            clock.parse_interval(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} should be rejected")


def store_crash_mid_save_keeps_jobs() -> None:
    with _home() as base:
        assert _add("keep") == 0
        before = (base / "jobs.json").read_bytes()
        jobs = store.load(base)
        jobs["keep"]["run"] = "echo changed"
        real_replace = store._replace

        def boom(src, dst):
            raise OSError("simulated crash between tmp and replace")

        store._replace = boom
        refused = False
        try:
            try:
                store.save(jobs, base)
            except store.StoreError:
                refused = True
        finally:
            store._replace = real_replace
        assert refused, "save must raise when the replace fails"
        assert (base / "jobs.json").read_bytes() == before, "the old store must survive"
        assert not list(base.glob("jobs.json.tmp-*")), "no tmp litter"
        assert store.load(base)["keep"]["run"] == "echo hi"


def store_corrupt_exits_2_and_restore_returns_bak() -> None:
    with _home() as base:
        assert _add("a") == 0
        assert _add("b") == 0  # the second save makes a .bak holding job a
        (base / "jobs.json").write_text("{not json", encoding="utf-8")
        rc = cli._dispatch(cli.cmd_list, argparse.Namespace(json=False))
        assert rc == 2, f"corrupt store must exit 2, got {rc}"
        assert list(base.glob("jobs.json.corrupt-*")), "corrupt copy kept aside"
        rc = cli._dispatch(cli.cmd_list, argparse.Namespace(json=False))
        assert rc == 2, "still refused until restored (never silently {})"
        rc = cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=True))
        assert rc == 0, "restore from .bak"
        jobs = store.load(base)
        assert "a" in jobs, "the .bak held job a"
        assert not list(base.glob("jobs.json.corrupt-*")), "corrupt copies parked"
        # negative twin: a valid store is not refused
        assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 0
    with _home() as base:
        # ...and the SAME recovery works after the very first save, which has
        # no previous version to keep. A store corrupted in that window used
        # to leave every verb exiting 2 forever, naming a restore that exited
        # 2 as well -- an advertised recovery that could not run.
        assert _add("only") == 0
        (base / "jobs.json").write_text('{"schema": 2, "jobs": {"only": ', encoding="utf-8")
        assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
        assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=True, reset=False)) == 0
        assert "only" in store.load(base)
        # and the last resort refuses to touch a store it can read
        (base / f"{store.STORE_NAME}.bak").unlink()
        assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False, reset=True)) == 2
        assert "only" in store.load(base)


def store_v1_migrates_with_tz_aware_stamps_and_keeps_v1_bak() -> None:
    with _home() as base:
        v1 = {
            "old": {
                "interval": "900.0",
                "command": "echo v1",
                "last_run": "2026-01-02T03:04:05.000001",
                "last_status": "success",
            }
        }
        (base / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
        jobs = store.load(base)
        job = jobs["old"]
        assert job["run"] == "echo v1" and job["every"] == "15m" and job["interval_s"] == 900.0
        assert job["last_started_at"].endswith("+00:00"), job["last_started_at"]
        assert job["last_state"] == "success"
        assert (base / "jobs.json.v1.bak").exists()
        written = json.loads((base / "jobs.json").read_text(encoding="utf-8"))
        assert written["schema"] == store.SCHEMA
        # the migrated stamp keeps the job out of its window
        assert not clock.is_due(job, clock.parse_ts("2026-01-02T03:10:00+00:00"))
        assert clock.is_due(job, clock.parse_ts("2026-01-02T03:19:06+00:00"))


def ledger_refuses_unknown_event_state_and_empty_reason() -> None:
    with _home() as base:
        for row in (
            {"event": "exploded", "reason": "x", "job": "j"},
            {"event": "finished", "state": "kinda_ok", "reason": "x", "job": "j"},
            {"event": "finished", "state": "success", "reason": "", "job": "j"},
            {"event": "finished", "reason": "no state", "job": "j"},
            {"event": "started", "reason": "no job"},
        ):
            try:
                ledger.append(base, row)
            except ledger.LedgerRefusedError:
                continue
            raise AssertionError(f"writer accepted {row}")
        assert not ledger.read(base), "a refused row leaves no trace"


def ledger_accepts_the_closed_vocabulary() -> None:
    with _home() as base:
        for state in sorted(ledger.STATES):
            ledger.append(base, {"event": "finished", "state": state, "reason": "ok", "job": "j"})
        rows = ledger.read(base)
        assert sorted(r["state"] for r in rows) == sorted(ledger.STATES)
        assert all(r["ts"].endswith("+00:00") and r["pid"] and r["host"] for r in rows)


def ledger_redacts_output_tails() -> None:
    samples = {
        "token sk-abcdefghijklmnop end": "sk-",
        "ghp_ABCDEFGHIJKLMNOP123": "ghp_",
        "ghs_ABCDEFGHIJKLMNOP123": "ghs_",
        "AKIAIOSFODNN7EXAMPLE": "AKIA",
        "xoxb-123456789-abcdef": "xoxb-",
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.x.y": "eyJ",
        "aither_sk_live_abcdef123456": "aither_sk_live",
    }
    for text, needle in samples.items():
        out = ledger.redact(text)
        assert needle not in out, f"{needle} survived redaction: {out}"
        assert "<redacted:" in out
    assert ledger.redact("plain output, nothing secret") == "plain output, nothing secret"
    with _home() as base:
        ledger.append(
            base,
            {
                "event": "finished",
                "state": "failure",
                "reason": "exit_1",
                "job": "j",
                "stderr_tail": "leak sk-abcdefghijklmnop here",
            },
        )
        raw = next(ledger.ledger_dir(base).glob("*.jsonl")).read_text(encoding="utf-8")
        assert "sk-abcdefghijklmnop" not in raw


def run_due_failure_exit_code_propagates() -> None:
    with _home():
        assert _add("bad") == 0
        assert _run_due(lambda job, wake: Outcome("failure", "exit_3", exit_code=3)) == 1
    with _home():
        assert _add("good") == 0
        assert _run_due(lambda job, wake: Outcome("success", "exit_0", exit_code=0)) == 0


def every_terminal_state_recorded_exactly_once() -> None:
    from_executor = sorted(ledger.STATES - {"skipped_disabled", "skipped_empty", "orphaned"})
    with _home() as base:
        for state in from_executor:
            assert _add(f"j_{state}", every="1h") == 0
            rc = _run_due(lambda job, wake, s=state: Outcome(s, f"fake_{s}"))
            assert rc == (1 if state in ledger.BAD_STATES else 0), (state, rc)
            jobs = store.load(base)
            # skipped_overlap / would_fire are retried next pass: not stamped
            assert jobs[f"j_{state}"]["last_state"] == (
                None if state in ledger.UNSTAMPED_STATES else state
            ), state
            assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name=f"j_{state}")) == 0
        assert _add("off", every="1h") == 0
        assert cli._dispatch(cli.cmd_disable, argparse.Namespace(name="off")) == 0
        assert _run_due(lambda job, wake: Outcome("success", "unreachable")) == 0
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="off")) == 0
        jobs = store.load(base)
        jobs["empty"] = store.new_job(every="1h", run="")
        store.save(jobs, base)
        assert _run_due(lambda job, wake: Outcome("success", "unreachable")) == 0
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="empty")) == 0
        # an orphan: a started row an hour old whose pass never wrote finished
        assert _add("orphan", every="1d") == 0
        old = clock.iso(clock.now_utc() - timedelta(hours=1))
        row = {
            "schema": 1,
            "wake_id": "w-orphan01",
            "pass_id": "p-dead0001",
            "ts": old,
            "invoker": "selftest",
            "host": "h",
            "pid": 1,
            "interpreter": "x",
            "job": "orphan",
            "event": "started",
            "state": None,
            "reason": "due",
            "timeout_s": 300,
        }
        day = ledger.day_file(base, clock.parse_ts(old))
        day.parent.mkdir(exist_ok=True)
        with open(day, "a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(row) + "\n")
        assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 1, "orphaned -> exit 1"
        states = _finished_states(base)
        for state in sorted(ledger.STATES):
            assert states.count(state) == 1, f"{state} recorded {states.count(state)}x"


def reconcile_closes_orphan_and_does_not_refire() -> None:
    with _home() as base:
        assert _add("once", every="1h") == 0
        ledger.ledger_dir(base).mkdir(exist_ok=True)
        recent = clock.iso(clock.now_utc() - timedelta(minutes=30))
        row = {
            "schema": 1,
            "wake_id": "w-orphan02",
            "pass_id": "p-dead0002",
            "ts": recent,
            "invoker": "selftest",
            "host": "h",
            "pid": 1,
            "interpreter": "x",
            "job": "once",
            "event": "started",
            "state": None,
            "reason": "due",
            "timeout_s": 60,
        }
        with open(
            ledger.day_file(base, clock.now_utc()), "a", encoding="utf-8", newline="\n"
        ) as fh:
            fh.write(json.dumps(row) + "\n")
        fired = []
        rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
        assert rc == 1 and not fired, "the orphan is closed, the job is NOT re-fired"
        job = store.load(base)["once"]
        assert job["last_state"] == "orphaned" and job["last_started_at"] == recent
        assert job["consecutive_failures"] == 1
        rows = [r for r in ledger.read(base) if r["wake_id"] == "w-orphan02"]
        assert [r["event"] for r in rows] == ["started", "finished"]
        assert rows[-1]["state"] == "orphaned"
        assert rows[-1]["reason"] in ("pid_gone", "age_exceeded_120s")
        # negative twin: a live wake is left alone
        live = dict(row, wake_id="w-live0003", ts=clock.iso(clock.now_utc()), pid=os.getpid())
        with open(
            ledger.day_file(base, clock.now_utc()), "a", encoding="utf-8", newline="\n"
        ) as fh:
            fh.write(json.dumps(live) + "\n")
        _jobs, closed, in_progress = cli.reconcile(base, store.load(base), "p-self0001", "selftest")
        assert closed == [] and in_progress == ["w-live0003"]


def spec_keys_accepted_equal_keys_read() -> None:
    declared = set(store.SPEC_DEFAULTS)
    assert declared == set(executors.READS) | set(cli.READS), declared ^ (
        set(executors.READS) | set(cli.READS)
    )
    # A dotted settable key (``report.relay``) addresses one field inside a
    # declared block, so the comparison is against the block it names.
    settable_roots = {key.split(".", 1)[0] for key in store.SETTABLE}
    assert settable_roots | {"interval_s"} == declared
    pattern = re.compile(r"""job(?:s\[[^\]]+\])?(?:\.get\(|\[)["'](\w+)["']""")
    read: set = set()
    # `checks` is a real reader: WL006 judges a detached wake from the
    # `receipt` path the job declares. Leaving it out of this scan would make
    # a knob that IS read look like dead config.
    for module in (cli, executors, clock, checks):
        read |= set(pattern.findall(Path(module.__file__).read_text(encoding="utf-8")))
    allowed = declared | set(store.STATE_DEFAULTS)
    assert read <= allowed, f"code reads keys the spec does not declare: {read - allowed}"
    assert declared <= read, f"declared knobs nobody reads: {declared - read}"


def add_refuses_empty_command_duplicate_and_bad_timeout() -> None:
    with _home() as base:
        assert _add("e", run="   ") == 1
        assert store.load(base) == {}
        assert _add("d") == 0
        assert _add("d") == 1
        assert _add("t", every="1m", timeout=60) == 1, "timeout >= interval refused"
        assert _add("t", every="1m", timeout=60, allow_overrun=True) == 0
        assert _add("x", every="15x") == 1
        rc = cli._dispatch(
            cli.cmd_set,
            argparse.Namespace(name="d", assignments=["colour=red"], allow_overrun=False),
        )
        assert rc == 1, "unknown spec key refused at set time"
        rc = cli._dispatch(
            cli.cmd_set, argparse.Namespace(name="d", assignments=["every=2h"], allow_overrun=False)
        )
        assert rc == 0 and store.load(base)["d"]["interval_s"] == 7200.0


def run_due_is_idempotent_inside_the_window() -> None:
    with _home() as base:
        assert _add("once", every="1h") == 0
        fired = []
        exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
        assert _run_due(exe) == 0
        assert _run_due(exe) == 0
        assert len(fired) == 1, "second pass inside the window must not fire"
        stamp = store.load(base)["once"]["last_started_at"]
        assert stamp and stamp.endswith("+00:00")
        assert _run_due(exe) == 0 and len(fired) == 1
        assert store.load(base)["once"]["last_started_at"] == stamp, "the stamp drifted"
        assert not [r for r in ledger.read(base) if r["event"] == "reconciled"]
        # negative twin: run --name ignores the window
        rc = cli._dispatch(
            lambda a: cli.cmd_run(a, executor=exe), argparse.Namespace(name="once", force=False)
        )
        assert rc == 0 and len(fired) == 2


def python_m_awrise_entry_exists() -> None:
    assert importlib.util.find_spec("awrise.__main__") is not None


def started_row_precedes_exec_and_stamp_follows_finished() -> None:
    """The pinned write order: started row -> exec -> finished row -> jobs.json.
    The executor is the witness; it reads the ledger while it runs."""
    with _home() as base:
        assert _add("order", every="1h") == 0
        seen = {}

        def peek(job, wake):
            rows = [r for r in ledger.read(base) if r.get("wake_id") == wake["wake_id"]]
            seen["events"] = [r["event"] for r in rows]
            seen["stamp"] = store.load(base)["order"]["last_started_at"]
            return Outcome("success", "exit_0", exit_code=0)

        assert _run_due(peek) == 0
        assert seen["events"] == ["started"], f"exec saw {seen['events']}, not the started row"
        assert seen["stamp"] is None, "the store was stamped before the finished row"
        assert store.load(base)["order"]["last_state"] == "success"
        # negative twin: an executor that raises still closes its wake
        assert _add("crash", every="1h") == 0

        def boom(job, wake):
            raise RuntimeError("boom")

        assert _run_due(boom) == 1
        rows = [r for r in ledger.read(base) if r.get("job") == "crash"]
        assert [r["event"] for r in rows] == ["started", "finished"]
        assert rows[-1]["state"] == "error" and rows[-1]["reason"].startswith("executor_crashed:")


def finished_wake_whose_stamp_was_lost_is_not_refired() -> None:
    """A save that fails AFTER the finished row (disk full, a foreign reader
    on Windows) must not turn into a second fire: the ledger is the memory."""
    with _home() as base:
        assert _add("once", every="1h") == 0
        real_save = store.save
        fail = {"on": True}

        def flaky(jobs, b=None):
            if fail["on"] and jobs.get("once", {}).get("last_state"):
                raise store.StoreError("simulated sharing violation")
            return real_save(jobs, b)

        store.save = flaky
        fired = []
        try:
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
            assert rc == 2 and fired == [1]
            assert store.load(base)["once"]["last_started_at"] is None
            fail["on"] = False
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
        finally:
            store.save = real_save
        assert rc == 0 and fired == [1], f"fired {len(fired)}x: the stamp was not recovered"
        job = store.load(base)["once"]
        assert job["last_state"] == "success" and job["last_started_at"]
        assert any(
            r["event"] == "reconciled" and r["reason"] == "recovered_1_stamps"
            for r in ledger.read(base)
        )
        # negative twin: once recovered, a healthy store recovers nothing again
        snapshot = (base / "jobs.json").read_bytes()
        assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
        assert fired == [1] and (base / "jobs.json").read_bytes() == snapshot
        assert sum(r["event"] == "reconciled" for r in ledger.read(base)) == 1


def ledger_concurrent_appends_lose_nothing() -> None:
    """Two writers at the same instant: every row lands, none interleave.
    (The C runtime's O_APPEND on Windows lost ~10% of rows here.)"""
    with _home() as base:
        threads, per_thread = 8, 40
        errors: List[BaseException] = []

        def writer(tag: str) -> None:
            try:
                for i in range(per_thread):
                    ledger.append(
                        base,
                        {
                            "event": "finished",
                            "state": "success",
                            "job": "j",
                            "reason": f"{tag}-{i}",
                        },
                    )
            except Exception as exc:  # noqa: BLE001 - reported by the assertion
                errors.append(exc)

        pool = [threading.Thread(target=writer, args=(f"t{i}",)) for i in range(threads)]
        for t in pool:
            t.start()
        for t in pool:
            t.join()
        assert not errors, errors
        rows = ledger.read(base)
        assert len(rows) == threads * per_thread, f"{threads * per_thread - len(rows)} rows lost"
        assert len({r["reason"] for r in rows}) == threads * per_thread
        assert not [r for r in rows if r["event"] == "unreadable"]


def status_is_unjudged_for_a_job_that_never_woke() -> None:
    with _home():
        assert _add("a", every="1h") == 0
        assert _add("b", every="1h") == 0
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="b")) == 0
        rc = cli._dispatch(cli.cmd_status, argparse.Namespace())
        assert rc == 2, f"a job with no wake is UNJUDGED (2), got {rc}"
        assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
        assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0
        # negative twin: a failing job is 1, whatever else is pending
        assert _add("c", every="1h") == 0
        assert _run_due(lambda job, wake: Outcome("failure", "exit_1")) == 1
        assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1


def store_refuses_malformed_records_with_exit_2() -> None:
    shapes = (
        {"x": 5},
        {"x": {"every": "1h", "interval_s": 3600.0, "run": "echo", "last_started_at": "garbage"}},
        {"x": {"every": "1h", "interval_s": 3600.0, "run": "echo", "timeout_s": "abc"}},
        {"x": {"every": "1h", "interval_s": "abc", "run": "echo"}},
        {"x": {"every": "1h", "interval_s": 3600.0, "run": "echo", "colour": "red"}},
        {"x": {"every": "1h", "interval_s": 3600.0, "run": "echo", "last_state": "kinda"}},
        {"..": {"every": "1h", "interval_s": 3600.0, "run": "echo"}},
    )
    for jobs in shapes:
        with _home() as base:
            (base / "jobs.json").write_text(
                json.dumps({"schema": 2, "jobs": jobs}), encoding="utf-8"
            )
            rc = cli._dispatch(cli.cmd_list, argparse.Namespace(json=False))
            assert rc == 2, f"{jobs} -> {rc}, want 2"
            rc = cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="s"))
            # The pass's tick rows are written -- a clock firing into a broken
            # store is not a stopped clock -- but no row claims a job did
            # anything.
            events = {row.get("event") for row in ledger.read(base)}
            assert rc == 2 and events <= {"tick", "tick_end"}, f"{jobs} -> {rc}, {events}"
            assert not list(base.glob("jobs.json.corrupt-*")), "valid JSON is not moved aside"
    with _home() as base:
        v1 = {"x": {"interval": "5m", "command": "echo", "last_run": None, "last_status": None}}
        (base / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
        assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
        # negative twin: a minimal hand-written record loads with defaults
        (base / "jobs.json").write_text(
            json.dumps(
                {"schema": 2, "jobs": {"min": {"every": "1h", "interval_s": 3600, "run": "echo"}}}
            ),
            encoding="utf-8",
        )
        assert store.load(base)["min"]["timeout_s"] == 300


def add_refuses_names_outside_the_grammar() -> None:
    with _home() as base:
        for bad in (
            "..",
            ".",
            "sub/dir",
            "sub\\dir",
            "CON",
            "com1.txt",
            "a b",
            "a\tb",
            "-lead",
            "trail.",
            "x" * 65,
            "café",
        ):
            assert _add(bad, every="1h") == 1, f"{bad!r} accepted"
        assert store.load(base) == {}
        for good in ("a", "fleet-gates", "v2.backup", "x" * 64):
            assert _add(good, every="1h") == 0, good
        assert _add(" lead ", every="1h") == 0
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name=" lead")) == 0


def reconcile_never_copies_an_unreadable_ts_into_the_store() -> None:
    with _home() as base:
        assert _add("j", every="1h") == 0
        row = {
            "schema": 1,
            "wake_id": "w-badts001",
            "pass_id": "p-x",
            "ts": "not-a-timestamp",
            "invoker": "s",
            "host": "h",
            "pid": 1,
            "interpreter": "x",
            "job": "j",
            "event": "started",
            "state": None,
            "reason": "due",
            "timeout_s": 300,
        }
        day = ledger.day_file(base, clock.now_utc())
        day.parent.mkdir(exist_ok=True)
        with open(day, "a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(row) + "\n")
        assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 1
        assert store.load(base)["j"]["last_started_at"] is None, "garbage became a stamp"
        raw = day.read_text(encoding="utf-8")
        assert "Infinity" not in raw and "NaN" not in raw, "non-JSON number in the ledger"
        assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1


def add_refuses_timeout_ge_interval() -> None:
    """A job that cannot finish inside its window is refused at add time;
    ``--allow-overrun`` is the explicit way to accept one."""
    with _home() as base:
        assert _add("t", every="1m", timeout=60) == 1
        assert _add("t", every="1m", timeout=61) == 1
        assert store.load(base) == {}
        assert _add("t", every="1m", timeout=59) == 0
        assert _add("u", every="1m", timeout=60, allow_overrun=True) == 0
        rc = cli._dispatch(
            cli.cmd_set,
            argparse.Namespace(name="t", assignments=["timeout_s=60"], allow_overrun=False),
        )
        assert rc == 1, "set re-asks the rule when the window/timeout relation changes"


def overlap_is_skipped_while_the_lock_is_held_and_a_stale_lock_is_broken() -> None:
    with _home() as base:
        assert _add("j", every="1h") == 0
        before = (base / "jobs.json").read_bytes()
        held = lock.acquire(base, "j", {"wake_id": "w-holder01", "pass_id": "p-holder1"}, 300)
        assert isinstance(held, lock.Lock)
        fired = []
        try:
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
        finally:
            held.release()
        assert rc == 0 and fired == [], "a held lock is a skip, never a second fire"
        row = [r for r in ledger.read(base) if r["event"] == "finished"][-1]
        assert row["state"] == "skipped_overlap"
        assert row["reason"] == f"lock_held_by_w-holder01_pid_{os.getpid()}"
        assert (base / "jobs.json").read_bytes() == before, "an overlap skip is not stamped"
        # negative twin: a lock whose holder is dead is broken and the job fires
        path = lock.lock_path(base, "j")
        path.mkdir(parents=True)
        (path / "wake.json").write_text(
            json.dumps(
                {
                    "wake_id": "w-dead0001",
                    "pass_id": "p-dead",
                    "job": "j",
                    "pass_pid": 2_000_000_000,
                    "child_pid": None,
                    "started_at": clock.iso(clock.now_utc()),
                    "timeout_s": 60,
                }
            ),
            encoding="utf-8",
        )
        assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
        assert fired == [1]
        assert any(
            r["event"] == "reconciled" and r["reason"] == "broke_1_stale_locks"
            for r in ledger.read(base)
        )
        assert not path.exists(), "released after the wake"
        # and a live holder past its age bound is broken too (pid reuse)
        path.mkdir(parents=True)
        (path / "wake.json").write_text(
            json.dumps(
                {
                    "wake_id": "w-old00001",
                    "pass_id": "p-old",
                    "job": "j",
                    "pass_pid": os.getpid(),
                    "child_pid": None,
                    "started_at": "2026-01-01T00:00:00+00:00",
                    "timeout_s": 60,
                }
            ),
            encoding="utf-8",
        )
        assert lock.judge("j", path, 360) == "age_exceeded_120s"
        assert isinstance(lock.acquire(base, "j", {"wake_id": "w-x"}, 300), lock.Lock)


def a_stale_lock_is_broken_by_exactly_one_pass() -> None:
    """Two passes judge one stale lock; only one may end up holding it.

    The break is a claim (rename) + a re-judge of the claimed directory, so a
    pass that was descheduled between its verdict and its break cannot delete
    the lock the other one has already taken.
    """
    with _home() as base:
        path = lock.lock_path(base, "j")
        path.mkdir(parents=True)
        (path / "wake.json").write_text(
            json.dumps(
                {
                    "wake_id": "w-dead0001",
                    "pass_id": "p-dead",
                    "job": "j",
                    "pass_pid": 2_000_000_000,
                    "child_pid": None,
                    "started_at": clock.iso(clock.now_utc()),
                    "timeout_s": 60,
                }
            ),
            encoding="utf-8",
        )
        real_judge = lock.judge

        def slow_judge(job, target, bound, settle_s=0.0):
            verdict = real_judge(job, target, bound, settle_s=settle_s)
            if threading.current_thread().name == "A" and not isinstance(verdict, lock.Held):
                time.sleep(0.5)
            return verdict

        taken = {}

        def take(tag):
            taken[tag] = lock.acquire(base, "j", {"wake_id": "w-" + tag, "pass_id": "p-" + tag}, 60)

        lock.judge = slow_judge
        try:
            first = threading.Thread(target=take, args=("A",), name="A")
            second = threading.Thread(target=take, args=("B",), name="B")
            first.start()
            time.sleep(0.1)
            second.start()
            first.join()
            second.join()
        finally:
            lock.judge = real_judge
        holders = [tag for tag, handle in taken.items() if isinstance(handle, lock.Lock)]
        assert len(holders) == 1, f"two passes hold one lock: {taken}"
        holder = json.loads((path / "wake.json").read_text(encoding="utf-8"))
        assert holder["wake_id"] == "w-" + holders[0]
        # negative twin: with nobody racing it, a stale lock IS broken and re-taken
        handle = lock.acquire(base, "j", {"wake_id": "w-solo0001"}, 60)
        assert isinstance(handle, lock.Held), "the winner still holds it"
        taken[holders[0]].release()
        handle = lock.acquire(base, "j", {"wake_id": "w-solo0001"}, 60)
        assert isinstance(handle, lock.Lock), "a free lock is taken"
        handle.release()


def release_never_removes_another_wakes_lock() -> None:
    """A pass whose lock was broken for age while it was suspended releases
    into a directory that now belongs to someone else. It must leave it."""
    with _home() as base:
        mine = lock.acquire(base, "j", {"wake_id": "w-mine0001"}, 60)
        assert isinstance(mine, lock.Lock)
        lock._remove(mine.path)
        theirs = lock.acquire(base, "j", {"wake_id": "w-their001"}, 60)
        assert isinstance(theirs, lock.Lock)
        mine.release()
        assert mine.path.exists(), "a release deleted a lock that was not ours"
        assert isinstance(lock.acquire(base, "j", {"wake_id": "w-third001"}, 60), lock.Held)
        # negative twin: the wake that DOES hold it releases it
        theirs.release()
        assert not mine.path.exists()


def a_foreign_ledger_row_does_not_wedge_the_next_pass() -> None:
    """One hand-written line whose wake id is an object, or whose ts is a
    number, must not end every later pass in a traceback."""
    with _home() as base:
        assert _add("j", every="1h") == 0
        path = ledger.day_file(base, clock.now_utc())
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [
            {
                "schema": 1,
                "wake_id": "w-foreign1",
                "pass_id": "p-f",
                "job": "j",
                "ts": 1700000000,
                "event": "started",
                "state": None,
                "reason": "due",
            },
            {
                "schema": 1,
                "wake_id": {"a": 1},
                "pass_id": "p-f",
                "job": "j",
                "ts": clock.iso(clock.now_utc()),
                "event": "started",
                "state": None,
                "reason": "due",
            },
            {
                "schema": 1,
                "wake_id": "w-foreign3",
                "pass_id": "p-f",
                "job": ["j"],
                "ts": clock.iso(clock.now_utc()),
                "event": "finished",
                "state": "success",
                "reason": "exit_0",
            },
        ]
        with open(path, "a", encoding="utf-8", newline="\n") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        fired = []
        exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
        assert _run_due(exe) == 0, "a foreign row stopped the pass"
        assert fired == [1] and _finished_states(base) == ["success"]
        unreadable = [r for r in ledger.read(base) if r.get("event") == "unreadable"]
        assert len(unreadable) == 3, "each foreign line is reported, never dropped"
        assert all("non-string" in r["reason"] for r in unreadable)
        # negative twin: a row the writer produced is read back as itself
        assert [
            r["event"] for r in ledger.read(base) if r.get("wake_id") and r["event"] == "finished"
        ] == ["finished"]


def clock_skew_on_a_disabled_job_heals_in_one_pass() -> None:
    """Nothing runs, so nothing else can correct a stamp in the future: the
    skew row itself carries the measured clock into the store."""
    with _home() as base:
        assert _add("j", every="1h", disabled=True) == 0
        jobs = store.load(base)
        future = clock.iso(clock.now_utc() + timedelta(hours=1))
        jobs["j"]["last_started_at"] = future
        store.save(jobs, base)
        fired = []
        exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
        assert _run_due(exe) == 1
        assert _finished_states(base) == ["error", "skipped_disabled"]
        assert store.load(base)["j"]["last_started_at"] < future
        # negative twin: the next passes are quiet -- one error, not one per pass
        assert _run_due(exe) == 0 and _run_due(exe) == 0
        assert fired == [] and len(_finished_states(base)) == 2
        assert store.load(base)["j"]["consecutive_failures"] == 1


def timeout_past_the_platform_wait_is_refused() -> None:
    """A deadline the platform cannot express would leave a child running
    that nothing waited on and nothing can kill."""
    with _home() as base:
        over = clock.MAX_TIMEOUT_S + 1
        assert _add("big", every="60d", timeout=over) == 1
        assert _add("big", every="60d", timeout=over, allow_overrun=True) == 1
        assert store.load(base) == {}
        # negative twin: the bound itself is accepted, and a store that already
        # holds a bigger one errors WITHOUT spawning anything
        assert _add("big", every="60d", timeout=clock.MAX_TIMEOUT_S) == 0
        outcome = executors.run_shell({"run": "echo hi", "timeout_s": over}, {})
        assert outcome.state == "error", outcome
        assert outcome.reason.startswith("timeout_s_above_bound:") and outcome.child_pid is None


def _pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        ).stdout
        for line in out.splitlines():
            cells = [c.strip('"') for c in line.strip().split('","')]
            if len(cells) >= 2 and cells[1] == str(pid):
                return cells[0].lower() not in {"tasklist.exe", "conhost.exe"}
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_gone(pid: int, budget_s: float = 10.0) -> bool:
    deadline = time.monotonic() + budget_s
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.5)
    return not _pid_alive(pid)


def _kill_tree(pid: int) -> None:
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)], capture_output=True, timeout=30)
        return
    import signal

    for target in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
        try:
            target()
            return
        except (ProcessLookupError, PermissionError):
            continue


def timeout_kills_the_whole_process_tree() -> None:
    """A child that spawns a grandchild and waits: after the timeout NOTHING
    of the tree is left, and the pass did not hang on the grandchild's pipe."""
    with _home() as base:
        script = base / "child.py"
        pidfile = base / "grandchild.pid"
        # The path is baked into the child rather than passed through an env
        # var: the environment is process-global state this test would then
        # share with every other case, and a name only ever WRITTEN here was
        # being reported by `awrise doctor` as required config.
        script.write_text(
            "import subprocess, sys, time\n"
            "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
            f"open({str(pidfile)!r}, 'w').write(str(p.pid))\n"
            "p.wait()\n",
            encoding="utf-8",
        )
        assert _add("tree", every="1h", run=f'"{sys.executable}" "{script}"', timeout=2) == 0
        t0 = time.monotonic()
        rc = _run_due()
        elapsed = time.monotonic() - t0
        assert rc == 1
        row = [r for r in ledger.read(base) if r["event"] == "finished"][-1]
        assert row["state"] == "timeout" and row["reason"].startswith("killed after 2s"), row
        assert elapsed < 30, f"the pass hung {elapsed:.0f}s: a survivor held the pipe"
        assert pidfile.exists(), "the child never spawned its grandchild"
        grandchild = int(pidfile.read_text(encoding="utf-8").strip())
        gone = _wait_gone(grandchild)
        if not gone:
            _kill_tree(grandchild)
        assert gone, f"grandchild {grandchild} survived the timeout"
        assert store.load(base)["tree"]["last_state"] == "timeout"
        assert not lock.lock_path(base, "tree").exists()
        # negative twin: the instrument can see a live process
        probe = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            assert _pid_alive(probe.pid)
        finally:
            probe.kill()
            probe.wait(timeout=30)


def detach_closes_at_spawn_and_leaves_the_child_alive() -> None:
    with _home() as base:
        assert (
            _add(
                "bg",
                every="1h",
                run=f'"{sys.executable}" -c "import time; time.sleep(120)"',
                detach=True,
            )
            == 0
        )
        t0 = time.monotonic()
        rc = _run_due()
        elapsed = time.monotonic() - t0
        assert rc == 0 and elapsed < 20, f"the pass waited {elapsed:.0f}s on a detached child"
        row = [r for r in ledger.read(base) if r["event"] == "finished"][-1]
        assert row["state"] == "detached" and row["reason"] == f"spawned_pid_{row['child_pid']}"
        pid = row["child_pid"]
        try:
            assert _pid_alive(pid), "the detached child must outlive the pass"
            # The lock is KEPT while the child runs. Releasing it at spawn is
            # what let the next pass start a SECOND copy of a job that is still
            # in flight (review finding, 2026-09-19); the child IS the wake.
            # This case still asserted the old rule after the fix landed in
            # cli/lock and in the pytest twin, so `awrise --self-test` -- the
            # brick's own contract, and what AWP101 runs -- exited 1.
            assert lock.lock_path(base, "bg").exists(), (
                "the lock is held while the detached child is alive"
            )
            job = store.load(base)["bg"]
            assert job["last_state"] == "detached" and job["last_started_at"]
            assert (
                _run_due() == 0
                and len([r for r in ledger.read(base) if r["event"] == "finished"]) == 1
            )
        finally:
            _kill_tree(pid)
        assert _wait_gone(pid), "cleanup: the detached child is gone"
        # negative twin: without detach the same command is waited on (and times out)
        assert (
            _add(
                "fg",
                every="1h",
                run=f'"{sys.executable}" -c "import time; time.sleep(120)"',
                timeout=1,
            )
            == 0
        )
        rc = cli._dispatch(lambda a: cli.cmd_run(a), argparse.Namespace(name="fg", force=False))
        assert rc == 1
        row = [r for r in ledger.read(base) if r["event"] == "finished"][-1]
        assert row["state"] == "timeout"


def clock_skew_is_an_error_row_then_a_fire() -> None:
    with _home() as base:
        assert _add("j", every="1h") == 0
        jobs = store.load(base)
        future = clock.iso(clock.now_utc() + timedelta(hours=2))
        jobs["j"]["last_started_at"] = future
        store.save(jobs, base)
        fired = []
        rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
        assert rc == 1 and fired == [1]
        rows = [r for r in ledger.read(base) if r["event"] == "finished"]
        assert [r["state"] for r in rows] == ["error", "success"]
        assert rows[0]["reason"].startswith("clock_skew:")
        job = store.load(base)["j"]
        assert job["last_started_at"] < future and job["last_state"] == "success"
        # negative twin: an honest stamp writes no skew row and does not fire
        assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
        assert fired == [1] and len([r for r in ledger.read(base) if r["event"] == "finished"]) == 2


def healthy_store_is_byte_stable_across_passes() -> None:
    """add + run-due + run-due: jobs.json unchanged byte-for-byte and zero
    ``reconciled`` rows. The store and the row hold ONE started_at."""
    with _home() as base:
        assert _add("j", every="1h") == 0
        fired = []
        exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
        assert _run_due(exe) == 0 and fired == [1]
        first = (base / "jobs.json").read_bytes()
        assert _run_due(exe) == 0 and _run_due(exe) == 0
        assert fired == [1]
        assert (base / "jobs.json").read_bytes() == first, "a healthy pass rewrote the store"
        assert not [r for r in ledger.read(base) if r["event"] == "reconciled"], (
            "a healthy pass 'recovered' something"
        )
        started = [r for r in ledger.read(base) if r["event"] == "started"][0]
        assert store.load(base)["j"]["last_started_at"] == started["ts"]
        # negative twin: a stamp the store really lost IS recovered, exactly once
        jobs = store.load(base)
        for key in ("last_wake_id", "last_started_at", "last_finished_at", "last_state"):
            jobs["j"][key] = None
        store.save(jobs, base)
        assert _run_due(exe) == 0 and fired == [1]
        assert [r["reason"] for r in ledger.read(base) if r["event"] == "reconciled"] == [
            "recovered_1_stamps"
        ]
        assert store.load(base)["j"]["last_wake_id"] == started["wake_id"]


def readded_job_never_inherits_a_removed_jobs_wake() -> None:
    with _home() as base:
        assert _add("g", every="1d") == 0
        assert _run_due(lambda job, wake: Outcome("failure", "exit_7")) == 1
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="g")) == 0
        time.sleep(0.02)
        assert _add("g", every="1d", run="echo new") == 0
        fired = []
        assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
        assert fired == [1], "the new job fires; the removed job's wake is not copied in"
        job = store.load(base)["g"]
        assert job["last_state"] == "success" and job["consecutive_failures"] == 0
        assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0


def hostclock_renders_every_adapter_the_same_way_twice() -> None:
    ctx = hostclock.Context(
        python="/usr/bin/python3",
        home="/h",
        bin_dir="/h/bin",
        log_path="/h/logs/run-due.log",
        every_s=60,
        user="ada",
        version="0.0.0",
        sep="/",
    )
    for kind in hostclock.KINDS:
        first = hostclock.render(kind, ctx)
        assert first == hostclock.render(kind, ctx), f"{kind} renders differently twice"
        assert first, f"{kind} rendered nothing"
    # negative twin: a name that is not an adapter is refused, not rendered empty
    for unknown in ("systemd", "taskschd", ""):
        try:
            hostclock.render(unknown, ctx)
        except hostclock.HostClockError:
            continue
        raise AssertionError(f"{unknown!r} should not render")


def schtasks_payload_is_crlf_hidden_and_inside_the_length_cap() -> None:
    back = chr(92)
    ctx = hostclock.Context(
        python="C:" + back + "py" + back + "python.exe",
        home="C:" + back + "aw",
        bin_dir="C:" + back + "aw" + back + "bin",
        log_path="C:" + back + "aw" + back + "run-due.log",
        every_s=60,
        sep=back,
    )
    artifacts = hostclock.render("schtasks", ctx)
    for name in ("run-due.cmd", "run-hidden.vbs"):
        raw = artifacts[name].encode("utf-8")
        crlf = b"\r\n"
        assert crlf in raw and raw.replace(crlf, b"").count(b"\n") == 0, (
            f"{name} must be CRLF throughout: a bare line feed makes cmd.exe read "
            f"the whole payload as one mangled line, and the task then does nothing"
        )
    assert artifacts["run-due.cmd"].rstrip("\r\n").endswith("exit /b 0"), (
        "a failing job must not turn the scheduled task itself red"
    )
    assert "sh.Run(args, 0, True)" in artifacts["run-hidden.vbs"]
    assert len(hostclock.schtasks_tr(ctx)) <= hostclock.TR_MAX
    commands = hostclock.schtasks_commands(ctx)
    triggers = [argv[argv.index("/sc") + 1] for argv in commands]
    assert triggers == ["minute", "onstart"], triggers
    # The scheduler answers a read-back in UTF-16; decoded as UTF-8 no tag ever
    # matches, and the read-back silently finds nothing.
    task_xml = "<Task><Enabled>false</Enabled></Task>"
    for encoding in ("utf-16", "utf-16-le", "utf-8"):
        assert hostclock.decode(task_xml.encode(encoding)) == task_xml, encoding
    assert hostclock.decode(b"") == "" and hostclock.decode(task_xml) == task_xml
    # negative twin: a path that busts the cap is refused, never truncated
    deep = hostclock.Context(
        python="p",
        home="C:" + back + "aw",
        bin_dir="C:" + back + "d" * 240,
        log_path="l",
        every_s=60,
        sep=back,
    )
    refused = False
    try:
        hostclock.schtasks_tr(deep)
    except hostclock.HostClockError as exc:
        refused = str(hostclock.TR_MAX) in str(exc)
    assert refused, "a launch command over the cap must be refused, never truncated"


def install_print_and_dry_run_touch_nothing() -> None:
    with _home() as base:
        for kind in hostclock.KINDS:
            for mode in ("print", "dry"):
                entry = hostclock.install(
                    kind, every_s=60, base=base, print_only=mode == "print", dry_run=mode == "dry"
                )
                assert entry.installed is False and entry.lines
                assert not hostclock.record_path(base).exists(), f"{kind} {mode} wrote a record"
                assert not (base / "bin").exists(), f"{kind} {mode} wrote a payload"
        # negative twin: the real path on an OS this adapter does not target is
        # UNJUDGED, never a quiet success
        wrong = "cron" if os.name == "nt" else "schtasks"
        try:
            hostclock.install(wrong, base=base)
        except hostclock.HostClockError as exc:
            assert exc.code == 2, exc.code
        else:
            raise AssertionError("installing the wrong OS's adapter must not succeed")


def install_check_is_unjudged_without_a_record_and_red_without_the_payload() -> None:
    with _home() as base:
        code, lines = hostclock.check(base=base)
        assert code == 2 and "UNJUDGED" in lines[0], (code, lines)
        ctx = hostclock.context("schtasks", base)
        artifacts = hostclock.render("schtasks", ctx)
        hostclock._write_payloads("schtasks", ctx, artifacts)
        hostclock.write_record("schtasks", ctx, artifacts, base=base)
        real_probe = hostclock.probe
        hostclock.probe = lambda kind, c: hostclock.Definition(True, True, "registered")
        try:
            # The invoker matters: freshness is judged on SCHEDULED ticks only,
            # because a pass run by hand writes exactly this row and would
            # otherwise certify a clock that has never fired.
            ledger.append(
                base, {"event": "tick", "reason": "pass_start", "invoker": "schtasks"}
            )
            # negative twin: everything in place is a PASS, so the exit 1 below
            # is about the missing payload and nothing else
            assert hostclock.check(base=base)[0] == 0, hostclock.check(base=base)[1]
            target = hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"]
            os.remove(target)
            code, lines = hostclock.check(base=base)
            assert code == 1, (code, lines)
            assert any("payload is missing" in line for line in lines), lines
            hostclock._write_payloads("schtasks", ctx, artifacts)
            # ...and an entry the scheduler no longer holds is red too, so a
            # deleted task cannot read as a healthy clock.
            hostclock.probe = lambda kind, c: hostclock.Definition(False, True, "not there")
            code, lines = hostclock.check(base=base)
            assert code == 1 and any("is gone" in line for line in lines), (code, lines)
        finally:
            hostclock.probe = real_probe


def doctor_never_exits_zero_while_printing_a_measured_no() -> None:
    """`doctor` printed "nothing wakes run-due" and exited 0.

    The generated doctor displayed this package's local lines and read none of
    them, so the one command an operator runs when something feels wrong could
    only ever say yes. Both twins are here: a measured no is 1, a question this
    host cannot answer is 2, and a clock that is installed and ticking is 0.
    """
    from . import _doctor, doctor_local

    with _home() as base:
        # 1a. No clock of ours AND no pass has ever run: a measured NO. This is
        # the state this host was found in on 2026-09-18 -- an empty ledger
        # directory, no day file ever written, `schtasks /query /tn awrise`
        # finding nothing -- and answering it with UNJUDGED means the one
        # command an operator runs cannot report the absence of the clock.
        out = io.StringIO()
        code = _doctor.report(out=out)
        assert code == 1, (code, out.getvalue())
        assert "nothing wakes" in out.getvalue(), out.getvalue()

        # 1b. A pass that names no scheduler is STILL a measured no, and this
        # is the arm the host state of 2026-09-18 turned into a rule: the only
        # day file in that ledger had been written by hand-run passes, and read
        # unfiltered it made every surface say something was ticking a clock
        # that did not exist.
        ledger.append(
            base,
            {
                "event": "tick",
                "reason": "pass_start",
                "pass_id": "p-selftes",
                "invoker": "manual",
                "ts": clock.iso(clock.now_utc()),
            },
        )
        doctor_local._LAST = None
        out = io.StringIO()
        code = _doctor.report(out=out)
        assert code == 1, (code, out.getvalue())
        assert "hand-run" in out.getvalue(), out.getvalue()
        for day in ledger.ledger_dir(base).glob("*.jsonl"):
            day.unlink()
        doctor_local._LAST = None

        # 1c. The same absent record, but a SCHEDULER is ticking the ledger:
        # UNJUDGED, never a violation. A stranger's own cron line is a clock
        # awrise did not install, and calling that a violation teaches an
        # operator to ignore the verdict. What separates 1b from 1c is whether
        # the pass named the scheduler that started it, which is the one thing
        # about a foreign clock this brick can actually measure.
        ledger.append(
            base,
            {
                "event": "tick",
                "reason": "pass_start",
                "pass_id": "p-selftes",
                "invoker": "cron",
                "ts": clock.iso(clock.now_utc()),
            },
        )
        doctor_local._LAST = None
        out = io.StringIO()
        code = _doctor.report(out=out)
        assert code == 2, (code, out.getvalue())
        assert "UNJUDGED" in out.getvalue(), out.getvalue()
        for day in ledger.ledger_dir(base).glob("*.jsonl"):
            day.unlink()
        doctor_local._LAST = None

        # 2. A measured no: the record cannot be written, which is the one
        # thing this brick exists to do.
        blocked = ledger.ledger_dir(base)
        assert not blocked.exists() or blocked.is_dir()
        if blocked.exists():
            for child in blocked.iterdir():
                child.unlink()
            blocked.rmdir()
        blocked.write_text("not a directory\n", encoding="utf-8")
        out = io.StringIO()
        code = _doctor.report(out=out)
        blocked.unlink()
        assert code == 1, (code, out.getvalue())
        assert "NOT WRITABLE" in out.getvalue(), out.getvalue()

        # 3. The negative twin: an installed, ticking clock is a clean 0, so
        # the two answers above are about what they say and not about the
        # doctor being unable to pass at all.
        real_check, real_record = hostclock.check, hostclock.read_record
        hostclock.check = lambda kind=None, base=None: (0, ["last tick: 1s ago"])
        hostclock.read_record = lambda base=None: {
            "kind": "cron",
            "every_s": 60,
            "installed_at": clock.iso(clock.now_utc()),
            "python": sys.executable,
        }
        try:
            doctor_local._LAST = None
            out = io.StringIO()
            code = _doctor.report(out=out)
        finally:
            hostclock.check, hostclock.read_record = real_check, real_record
        assert code == 0, (code, out.getvalue())


def install_refuses_to_claim_an_entry_it_cannot_read_back() -> None:
    real_probe, real_register, real_preflight = (
        hostclock.probe,
        hostclock._register,
        hostclock.preflight,
    )
    hostclock.preflight = lambda kind: None
    # `notes` is the third argument: partial registrations (the at-startup
    # entry a boot trigger's elevation refuses) are reported, not raised.
    hostclock._register = lambda kind, ctx, notes=None: [["fake", "create"]]
    try:
        with _home() as base:
            # A create that exits 0 and registers NOTHING is the whole reason
            # the read-back exists: it must be an error, and it must leave no
            # record claiming an install that did not happen.
            # present=False with enabled=True on purpose: an "absent" arm that
            # a DISABLED entry would also satisfy proves nothing about the
            # read-back, because the enabled check would catch it either way.
            hostclock.probe = lambda kind, c: hostclock.Definition(False, True, "not there")
            try:
                hostclock.install("schtasks", base=base)
            except hostclock.HostClockError as exc:
                assert exc.code == 1, exc.code
            else:
                raise AssertionError("an entry that is not there must not be claimed")
            assert not hostclock.record_path(base).exists(), "a record for an absent entry"
            # negative twin: the same path with the entry really there records it
            hostclock.probe = lambda kind, c: hostclock.Definition(True, True, "registered")
            entry = hostclock.install("schtasks", base=base)
            assert entry.installed is True and hostclock.record_path(base).exists()
            # ...and an entry that exists but is DISABLED is not an install either
            hostclock.probe = lambda kind, c: hostclock.Definition(True, False, "disabled")
            try:
                hostclock.install("schtasks", base=base)
            except hostclock.HostClockError as exc:
                assert exc.code == 1, exc.code
            else:
                raise AssertionError("a disabled entry must not read as installed")
    finally:
        hostclock.probe, hostclock._register, hostclock.preflight = (
            real_probe,
            real_register,
            real_preflight,
        )


def every_pass_records_a_tick_with_the_gap_since_the_last_one() -> None:
    with _home() as base:
        assert _add("j", every="15m", run="echo hi") == 0
        assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
        assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
        rows = ledger.read(base)
        ticks = [row for row in rows if row.get("event") == "tick"]
        ends = [row for row in rows if row.get("event") == "tick_end"]
        assert len(ticks) == 2 and len(ends) == 2, (len(ticks), len(ends))
        assert ticks[0]["gap_s"] is None, "the first tick has no previous tick to measure"
        assert isinstance(ticks[1]["gap_s"], float) and ticks[1]["gap_s"] >= 0
        assert ends[0]["fired"] == 1 and ends[1]["fired"] == 0
        assert ends[0]["in_progress"] == 0 and ticks[0]["interpreter"]
    with _home() as base:
        # A scheduler that will not start a second copy while the first still
        # runs produces no new wake at all, so a long job looks exactly like a
        # stopped clock: the in_progress count is what tells them apart.
        assert _add("slow", every="15m", run="echo hi") == 0
        held = lock.acquire(
            base,
            "slow",
            {"wake_id": "w-live", "pass_id": "p-live", "started_at": clock.iso(clock.now_utc())},
        )
        assert not isinstance(held, lock.Held), "the lock was already taken"
        try:
            assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
        finally:
            held.release()
        ends = [row for row in ledger.read(base) if row.get("event") == "tick_end"]
        assert ends[-1]["in_progress"] == 1, ends[-1]
        assert ends[-1]["in_progress_jobs"] == ["slow"], ends[-1]
        assert ends[-1]["fired"] == 0, "a job whose lock was held did not fire"
        # negative twin: with the lock released the next pass counts none
        assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
        ends = [row for row in ledger.read(base) if row.get("event") == "tick_end"]
        assert ends[-1]["in_progress"] == 0, ends[-1]
    # negative twin: a pass whose store cannot load still ticks -- a clock
    # firing into a broken store is not a stopped clock
    with _home() as base:
        (base / "jobs.json").write_text("{ not json", encoding="utf-8")
        assert _run_due() == 2
        events = [row.get("event") for row in ledger.read(base)]
        assert events.count("tick") == 1 and events.count("tick_end") == 1, events


# --------------------------------------------------- S4: the operator verbs


@contextlib.contextmanager
def _frozen(start=None):
    """A clock the case drives, always put back -- even when the case fails."""
    state = {"now": start or clock.now_utc()}
    clock.set_now(lambda: state["now"])
    try:
        yield state
    finally:
        clock.reset_now()


def _write_row(base, event, ts, **extra) -> None:
    row = {
        "schema": 1,
        "wake_id": extra.pop("wake_id", "w-fixture1"),
        "pass_id": extra.pop("pass_id", "p-fixture1"),
        "ts": clock.iso(ts),
        "invoker": "selftest",
        "host": "h",
        "pid": 1,
        "interpreter": "x",
        "job": extra.pop("job", None),
        "event": event,
        "state": extra.pop("state", None),
        "reason": extra.pop("reason", "fixture"),
    }
    row.update(extra)
    path = ledger.day_file(base, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")


def _stamp(base, name, last_started, **extra):
    jobs = store.load(base)
    jobs[name]["last_started_at"] = clock.iso(last_started)
    jobs[name]["last_state"] = "success"
    jobs[name]["last_reason"] = "exit_0"
    jobs[name]["last_wake_id"] = "w-earlier1"
    jobs[name].update(extra)
    store.save(jobs, base)


def _rows(base, event=None, job=None):
    return [
        r
        for r in ledger.read(base)
        if (event is None or r.get("event") == event) and (job is None or r.get("job") == job)
    ]


def _ok_executor(job, wake):
    return Outcome("success", "exit_0", exit_code=0)


def _explain(name, since=None) -> int:
    return cli._dispatch(cli.cmd_explain, argparse.Namespace(name=name, since=since))


def _judge(since=None) -> int:
    return cli._dispatch(
        cli.cmd_history,
        argparse.Namespace(since=since, job=None, event=None, limit=0, json=False, judge=True),
    )


def explain_reports_overdue_windows() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=1), reason="pass_start")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            assert _explain("j") == 0
        assert "missed windows: 4" in out.getvalue(), out.getvalue()
        # negative twin: a job woken inside its window reports none, so the
        # assertion above cannot pass on any ledger at all
        _stamp(base, "j", now)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            assert _explain("j") == 0
        assert "missed windows: 0" in out.getvalue()
        # and with no tick on record there is no last pass to explain: exit 2
    with _home() as base, _frozen():
        assert _add("j", every="1m", timeout=30) == 0
        assert _explain("j") == 2


def missed_windows_are_one_row_and_one_catch_up_fire() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        missed = _rows(base, event="missed", job="j")
        assert len(missed) == 1 and missed[0]["windows"] == 4, missed
        assert missed[0]["reason"].startswith("clock_gap:"), missed[0]
        started = _rows(base, event="started", job="j")
        assert len(started) == 1 and started[0]["reason"] == "catch_up_once:4_windows"
        # ONCE: the arrears are not re-served on the next pass
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="missed", job="j")) == 1
        assert len(_rows(base, event="started", job="j")) == 1
    # negative twin: a job woken on time produces no missed row at all
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(seconds=61))
        _write_row(base, "tick", now - timedelta(seconds=61), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        assert _rows(base, event="missed") == []
        assert _rows(base, event="started", job="j")[0]["reason"] == "due"


def missed_policy_skip_drops_the_windows_without_running() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30, missed="skip") == 0
        _stamp(base, "j", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        assert _rows(base, event="started", job="j") == [], "skip must not run it"
        finished = _rows(base, event="finished", job="j")
        assert [r["state"] for r in finished] == ["skipped_missed"], finished
        assert finished[0]["reason"] == "missed_policy_skip:4_windows"
        assert clock.missed_windows(store.load(base)["j"], now) == 0, "the stamp moved"
    # negative twin: the default policy runs the same job once
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="started", job="j")) == 1
    # and a policy nobody implements is refused where it is entered
    with _home() as base, _frozen():
        assert _add("j", every="1m", timeout=30, missed="run_them_all") == 1
        assert store.load(base) == {}


def at_anchor_fires_once_past_the_anchor_and_reanchors() -> None:
    dawn = datetime(2026, 9, 18, 7, 0, tzinfo=timezone.utc)
    with _home() as base, _frozen(dawn - timedelta(hours=1)) as state:
        assert _add("dawn", every="1d", at="07:00", timeout=60) == 0
        _stamp(base, "dawn", dawn - timedelta(hours=2))
        assert _run_due(_ok_executor) == 0
        assert _rows(base, event="started", job="dawn") == [], "before the anchor"
        # the host wakes at 09:30, hours past 07:00
        state["now"] = dawn + timedelta(hours=2, minutes=30)
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="started", job="dawn")) == 1
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="started", job="dawn")) == 1, "once, not twice"
        # the anchor did NOT walk to 09:30
        job = store.load(base)["dawn"]
        assert clock.next_due(job, state["now"]) == dawn + timedelta(days=1), job
        state["now"] = dawn + timedelta(days=1, seconds=30)
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="started", job="dawn")) == 2
        assert clock.next_due(store.load(base)["dawn"], state["now"]) == dawn + timedelta(days=2)
        # a wake that landed EXACTLY on the anchor is next due the day AFTER,
        # never that same instant again -- the case that tells `<=` from `<`,
        # and the one that would re-fire a daily job every pass for a whole day
        exact = dict(store.load(base)["dawn"], last_started_at=clock.iso(dawn))
        assert clock.next_due(exact, dawn) == dawn + timedelta(days=1), exact
        assert not clock.is_due(exact, dawn)


def dry_run_says_what_would_fire_and_moves_nothing() -> None:
    def never(job, wake):
        raise AssertionError("a dry run must not execute anything")

    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("plain", every="1m", timeout=30) == 0
        _stamp(base, "plain", now - timedelta(seconds=61))
        code = cli._dispatch(
            lambda a: cli.cmd_run_due(a, executor=never),
            argparse.Namespace(quiet=True, invoker="selftest", dry_run=True, prune=None),
        )
        assert code == 0
        plain = _rows(base, event="finished", job="plain")
        assert len(plain) == 1 and plain[0]["state"] == "would_fire", plain
        assert plain[0]["reason"] == "due", plain[0]
        for path in ledger.files(base):
            path.unlink()
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="plain")) == 0
        for path in ledger.files(base):
            path.unlink()
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(minutes=5))
        before = (base / "jobs.json").read_bytes()
        code = cli._dispatch(
            lambda a: cli.cmd_run_due(a, executor=never),
            argparse.Namespace(quiet=True, invoker="selftest", dry_run=True, prune=None),
        )
        assert code == 0
        assert (base / "jobs.json").read_bytes() == before, "no stamp moved"
        rows = ledger.read(base)
        assert [r["event"] for r in rows] == ["finished"], rows
        assert rows[0]["state"] == "would_fire"
        assert rows[0]["reason"] == "catch_up_once:4_windows", rows[0]
        assert not [r for r in rows if r["event"] in ("tick", "tick_end")], (
            "a dry run is not a pass: it writes no tick"
        )
        # negative twin: the real pass DOES move the stamp and does tick
        assert _run_due(_ok_executor) == 0
        assert (base / "jobs.json").read_bytes() != before
        assert [r for r in ledger.read(base) if r["event"] == "tick"]


def history_judge_flags_drift() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now)
        _write_row(base, "tick", now - timedelta(minutes=10), reason="pass_start")
        for index, minutes in enumerate((10, 9, 8, 0)):
            _write_row(
                base,
                "started",
                now - timedelta(minutes=minutes),
                job="j",
                wake_id=f"w-drift{index:03d}",
                reason="due",
            )
        assert _judge() == 1
    # negative twin: the same span woken every minute is judged OK
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now)
        _write_row(base, "tick", now - timedelta(minutes=10), reason="pass_start")
        for minutes in range(10, -1, -1):
            _write_row(
                base,
                "started",
                now - timedelta(minutes=minutes),
                job="j",
                wake_id=f"w-ok{minutes:06d}",
                reason="due",
            )
        assert _judge() == 0


def history_judge_is_unjudged_on_a_young_ledger() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1h") == 0
        _stamp(base, "j", now - timedelta(hours=5))
        _write_row(base, "tick", now - timedelta(seconds=30), reason="pass_start")
        _write_row(base, "tick", now, reason="pass_start")
        assert _judge() == 2, "a window shorter than 2x the cadence cannot judge absence"
        assert checks.verdict(checks.run(base=base)) == 2, "the gate agrees: unjudged"
        # negative twin: widen the ledger and the SAME store is judged, and red
        _write_row(base, "tick", now - timedelta(hours=5), reason="pass_start")
        assert _judge() == 1
        # the in-brick gate agrees, and is never 0 on that silence
        assert checks.verdict(checks.run(base=base)) == 1


def a_missed_row_needs_a_measured_clock_gap() -> None:
    """Arrears are not a lost window: the clock has to have stopped.

    A 1-minute job on a 1-minute clock falls one window behind on ordinary
    jitter, and a row saying its window was lost reports a clock that never
    stopped. The negative twin is the same arrears with a measured gap.
    """
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(seconds=121))
        _write_row(base, "tick", now - timedelta(seconds=60), reason="pass_start")
        assert clock.missed_windows(store.load(base)["j"], now) == 1, "it IS in arrears"
        assert _run_due(_ok_executor) == 0
        assert _rows(base, event="missed") == [], "a healthy clock lost nothing"
        assert _rows(base, event="started", job="j")[0]["reason"] == "due"
        assert _rows(base, event="tick_end")[-1]["missed"] == 0
        # negative twin: the same arrears, a gap past 2x the window
        assert _add("k", every="1m", timeout=30) == 0
        _stamp(base, "k", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        missed = _rows(base, event="missed", job="k")
        assert len(missed) == 1 and missed[0]["reason"].startswith("clock_gap:"), missed


def a_wake_in_flight_is_not_recorded_as_lost_windows() -> None:
    """A long wake produces exactly the silence of a dead clock.

    The pass closes an overlapped job ``skipped_overlap``; it must not also
    file a row -- once per pass, for the whole life of that wake -- claiming
    the job's own windows were lost.
    """
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("slow", every="1m", timeout=30) == 0
        _stamp(base, "slow", now - timedelta(minutes=5))
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        held = lock.lock_path(base, "slow")
        held.mkdir(parents=True)
        with open(held / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
            json.dump(
                {
                    "wake_id": "w-inflight",
                    "pass_id": "p-inflight",
                    "pass_pid": os.getpid(),
                    "child_pid": 0,
                    "started_at": clock.iso(now),
                    "timeout_s": 30,
                },
                fh,
            )
        assert _run_due(_ok_executor) == 0
        assert _rows(base, event="missed") == [], "in flight, not absent"
        end = _rows(base, event="tick_end")[-1]
        assert end["missed"] == 0 and end["in_progress"] == 1, end
        assert _finished_states(base) == ["skipped_overlap"], _finished_states(base)
        # negative twin: the wake ends, the lock goes, and the windows the
        # clock really lost are recorded
        for child in sorted(held.iterdir()):
            child.unlink()
        held.rmdir()
        _write_row(base, "tick", now - timedelta(minutes=5), reason="pass_start")
        assert _run_due(_ok_executor) == 0
        assert len(_rows(base, event="missed", job="slow")) == 1


def dry_run_agrees_with_the_pass_on_a_skewed_disabled_job() -> None:
    """``--dry-run`` is what the next REAL pass would do.

    On a stamp in the future the pass writes the error row and then falls
    through to ``skipped_disabled``: the command never runs. A dry run that
    answers WOULD FIRE there answers the opposite of what it is for.
    """

    def never(job, wake):
        raise AssertionError("a disabled job must not run")

    def _dry() -> int:
        return cli._dispatch(
            lambda a: cli.cmd_run_due(a, executor=never),
            argparse.Namespace(quiet=True, invoker="selftest", dry_run=True, prune=None),
        )

    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("dis", every="10m", timeout=30, disabled=True) == 0
        _stamp(base, "dis", now + timedelta(hours=2))
        assert _dry() == 0
        rows = _rows(base, event="finished", job="dis")
        assert rows == [], f"a job that cannot run has no would_fire row: {rows}"
        assert _run_due(never) == 1, "the pass records the skew and skips"
        assert [r["state"] for r in _rows(base, event="finished", job="dis")] == [
            "error",
            "skipped_disabled",
        ]
    # negative twin: the same skew on an ENABLED job would fire, and does
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("on", every="10m", timeout=30) == 0
        _stamp(base, "on", now + timedelta(hours=2))
        assert _dry() == 0
        rows = _rows(base, event="finished", job="on")
        assert len(rows) == 1 and rows[0]["state"] == "would_fire", rows
        assert rows[0]["reason"].startswith("clock_skew:"), rows[0]
        assert _run_due(_ok_executor) == 1
        assert "success" in [r["state"] for r in _rows(base, event="finished", job="on")]


def one_unreadable_line_cannot_buy_a_verdict() -> None:
    """A truncated final line is the normal outcome of a crash mid-append.

    It is reported as a row and must lend the ledger NO span: stamped at its
    day file's midnight it bought a seconds-old ledger up to 24h of record,
    and turned the young-ledger UNJUDGED into a green verdict.
    """

    def _wl003(base: Path) -> int:
        # WL002 fires on the corrupt line by design, so the overall verdict
        # cannot tell whether the WINDOW GUARD held: ask that rule directly.
        return [f.code for f in checks.run(base=base) if f.rule == "WL003"][0]

    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="10m", timeout=30) == 0
        _stamp(base, "j", now - timedelta(hours=4))
        _write_row(base, "tick", now, reason="pass_start")
        assert _judge() == 2, "seconds of ledger: unjudged"
        assert _wl003(base) == 2, "the gate agrees"
        with open(ledger.day_file(base, now), "a", encoding="utf-8", newline="\n") as fh:
            fh.write("{truncated\n")
        assert [r for r in ledger.read(base) if r.get("event") == "unreadable"], (
            "the line is reported, never dropped"
        )
        assert cli._span_s(ledger.read(base)) is None, "and it lends no span"
        assert checks.span_s(ledger.read(base)) is None
        assert _judge() == 2, "still too young to judge"
        assert _wl003(base) == 2
        # negative twin: a real row from an earlier pass DOES buy the span
        _write_row(base, "tick", now - timedelta(hours=4), reason="pass_start")
        assert _judge() == 1
        assert _wl003(base) == 1


def the_window_guard_is_per_job_not_the_longest_cadence() -> None:
    """One monthly job must not make every 1-minute job unjudgeable.

    Keyed on the longest cadence in the store, a single long-cadence job
    blinds both the gate and ``history --judge`` for a month -- a gate that
    can never reach a verdict is not a gate.
    """
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("fast", every="1m", timeout=30) == 0
        _stamp(base, "fast", now - timedelta(hours=3))
        _write_row(base, "tick", now - timedelta(hours=3), reason="pass_start")
        _write_row(base, "tick", now, reason="pass_start")
        assert checks.verdict(checks.run(base=base)) == 1, "the starved job is red"
        assert _judge() == 1
        assert _add("monthly", every="30d") == 0  # healthy, never run
        assert checks.verdict(checks.run(base=base)) == 1, "the measured red survives"
        assert _judge() == 1
        # negative twin: per job does not mean lenient -- with nothing old
        # enough to judge, the verdict is still 2, never a green 0
        with _home() as young, _frozen(now):
            assert _add("slow", every="1h") == 0
            _stamp(young, "slow", now - timedelta(hours=5))
            _write_row(young, "tick", now - timedelta(seconds=30), reason="pass_start")
            _write_row(young, "tick", now, reason="pass_start")
            assert checks.verdict(checks.run(base=young)) == 2
            assert _judge() == 2


def prune_is_unjudged_when_the_clock_cannot_be_read() -> None:
    """An unreadable clock is a could-not-judge (2), not a bad ``--keep`` (1).

    ``ClockError`` subclasses ``ValueError``, so a blanket ``except
    ValueError`` around the prune relabelled it as an invalid interval and
    contradicted the exit contract every other verb honours on that input.
    """

    def _prune(keep: str) -> int:
        return cli._dispatch(
            cli.cmd_prune, argparse.Namespace(keep=keep, dry_run=False, force=False)
        )

    with _home() as base, _frozen():
        assert _add("j", every="1h") == 0
        assert (base / "jobs.json").exists()
    with _home():
        assert _add("j", every="1h") == 0
        previous = os.environ.get(clock.NOW_ENV)
        os.environ.update({clock.NOW_ENV: "not-a-time"})
        try:
            assert _prune("30d") == 2, "an unreadable clock is could-not-judge"
            assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2, (
                "and every other verb says the same on that clock"
            )
        finally:
            if previous is None:
                os.environ.pop(clock.NOW_ENV, None)
            else:
                os.environ.update({clock.NOW_ENV: previous})
        # negative twin: with a readable clock a bad --keep is still 1
        assert _prune("soon") == 1
        assert _prune("30d") == 0


def checks_rules_each_fire_and_each_stay_quiet() -> None:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = checks.self_test()
    assert code == 0, out.getvalue()
    text = out.getvalue()
    for rule in checks.RULES:
        assert rule in text, f"{rule} has no self-test case"
    assert "FAIL" not in text, text


def prune_removes_only_files_outside_the_window() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        for days in (0, 1, 40):
            _write_row(base, "tick", now - timedelta(days=days), reason="pass_start")
        assert len(ledger.files(base)) == 3
        assert (
            cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
            == 0
        )
        names = {p.name for p in ledger.files(base)}
        assert len(names) == 2, names
        assert now.strftime("%Y-%m-%d") + ".jsonl" in names, "today is never pruned"
        # negative twin: a dry run unlinks nothing at all
        _write_row(base, "tick", now - timedelta(days=41), reason="pass_start")
        assert (
            cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=True, force=False))
            == 0
        )
        assert len(ledger.files(base)) == 3
        # and a file holding a wake nobody closed is spared, force or not
        _write_row(
            base,
            "started",
            now - timedelta(days=42),
            job="j",
            wake_id="w-orphan09",
            reason="due",
            timeout_s=30,
        )
        assert (
            cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
            == 0
        )
        assert any(
            p.stem == (now - timedelta(days=42)).strftime("%Y-%m-%d") for p in ledger.files(base)
        ), "the open wake's evidence survived"
        assert (
            cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=True))
            == 0
        )
        assert not any(
            p.stem == (now - timedelta(days=42)).strftime("%Y-%m-%d") for p in ledger.files(base)
        ), "--force means it"


def list_json_carries_the_exact_interval_and_the_next_due() -> None:
    with _home() as base, _frozen() as state:
        now = state["now"]
        assert _add("j", every="1h30m") == 0
        _stamp(base, "j", now - timedelta(minutes=30))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
        payload = json.loads(out.getvalue())["j"]
        assert payload["every"] == "1h30m" and payload["interval_s"] == 5400.0
        assert payload["next_due"] == clock.iso(now + timedelta(minutes=60))
        assert payload["due_now"] is False and payload["due_in_s"] == 3600.0
        # negative twin: an overdue job says so, with its arrears
        _stamp(base, "j", now - timedelta(hours=5))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
        payload = json.loads(out.getvalue())["j"]
        assert payload["due_now"] is True and payload["missed_windows"] == 2


def an_unreadable_clock_override_is_a_verdict_not_a_traceback() -> None:
    with _home():
        assert _add("j") == 0
        previous = os.environ.get(clock.NOW_ENV)
        os.environ.update({clock.NOW_ENV: "half past nine"})
        try:
            assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
            # negative twin: a readable override is honoured, not refused
            os.environ.update({clock.NOW_ENV: "+3600"})
            assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
            assert clock.now_utc() > datetime.now(timezone.utc) + timedelta(minutes=50)
        finally:
            if previous is None:
                os.environ.pop(clock.NOW_ENV, None)
            else:
                os.environ.update({clock.NOW_ENV: previous})


@contextlib.contextmanager
def _only_path(directory: Path) -> Iterator[None]:
    """PATH holding nothing but ``directory`` -- the state of a host where
    the optional sinks were never installed."""
    previous = os.environ.get("PATH", "")
    os.environ.update({"PATH": str(directory)})
    try:
        yield
    finally:
        os.environ.update({"PATH": previous})


def optional_executors_never_import_their_packages_at_module_level() -> None:
    probe = (
        "import sys, awrise, awrise.cli, awrise.executors, awrise.store, awrise.selftest;"
        "print([m for m in sys.modules if m.split('.')[0] in ('awrun', 'adk')])"
    )
    done = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, timeout=120, encoding="utf-8"
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "[]", done.stdout
    # negative twin: the guarded import is still WIRED -- a job that names the
    # awrun executor reaches it and reports what it found, rather than being
    # silently unreachable.
    spec = dict(store.SPEC_DEFAULTS)
    spec.update({"executor": "awrun", "run": "ci {}", "timeout_s": 5})
    out = executors.run(spec, {"wake_id": "w-selftest"})
    assert out.state == "error" and out.reason == "awrun_kind_refused:ci", out.reason


def an_unknown_executor_never_falls_through_to_the_shell() -> None:
    with _home() as base:
        marker = base / "it-ran"
        body = "import pathlib, sys; pathlib.Path(sys.argv[1]).write_text('x')"
        command = f'"{sys.executable}" -c "{body}" "{marker}"'
        spec = dict(store.SPEC_DEFAULTS)
        spec.update({"executor": "nope", "run": command, "timeout_s": 30})
        out = executors.run(spec, {"wake_id": "w-selftest"})
        assert out.state == "error" and out.reason == "unknown_executor:nope"
        assert not marker.exists(), "an unknown kind ran the command anyway"
        # negative twin: the SAME command under the shell executor does run,
        # so the assertion above is about the dispatch and not about the command
        spec["executor"] = "shell"
        assert executors.run(spec, {"wake_id": "w-selftest"}).state == "success"
        assert marker.exists()


def a_bearer_path_outside_the_confinement_is_refused() -> None:
    with _home() as base:
        stray = base.parent / "stray-token"
        stray.write_text("value", encoding="utf-8")
        assert _add("out", bearer_file=str(stray)) == 1, "a token path anywhere is a file read"
        assert store.load(base) == {}
        # negative twin: the same file INSIDE the home is accepted
        inside = base / "token"
        inside.write_text("value", encoding="utf-8")
        assert _add("in", bearer_file=str(inside)) == 0
        assert store.load(base)["in"]["bearer_file"] == str(inside)


def a_permission_mode_outside_the_allowlist_is_refused_at_add() -> None:
    with _home() as base:
        assert _add("bad", executor="session", permission_mode="bypassPermissions") == 1
        assert store.load(base) == {}
        # negative twin: an allowlisted mode is stored as given
        assert _add("ok", executor="session", permission_mode="plan") == 0
        assert store.load(base)["ok"]["permission_mode"] == "plan"


def a_sink_that_cannot_run_is_a_row_not_a_lost_pass() -> None:
    with _home() as base:
        empty = base / "nobin"
        empty.mkdir(parents=True, exist_ok=True)
        assert (
            _add(
                "s",
                every="1h",
                run="import sys; sys.exit(4)",
                executor="python",
                card_after=1,
                report_relay="#nowhere",
            )
            == 0
        )
        with _only_path(empty):
            assert _run_due() == 1, "the job's own verdict survived the sinks"
        reasons = [r.get("reason") for r in ledger.read(base) if r.get("event") == "report_error"]
        assert sorted(reasons) == ["awask_not_installed", "awrelay_not_installed"], reasons
        job = store.load(base)["s"]
        assert job["last_state"] == "failure" and job["consecutive_failures"] == 1
        assert str(job["report"]["card_id"]).startswith("unavailable:")


def _fake_tool(bindir: Path, name: str, body: str) -> Path:
    """A real executable on PATH that runs ``body`` as Python.

    A ``.cmd`` shim on Windows and a shebang script elsewhere, because that is
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


#: A fake awask: ``ask`` mints one card id and logs its argv next to itself;
#: ``show`` reports the card still open, so nothing is applied.
_FAKE_AWASK = """
import json, pathlib, sys

log = pathlib.Path(__file__).with_name("awask.log")
with open(log, "a", encoding="utf-8", newline="\\n") as fh:
    fh.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1:2] == ["ask"]:
    print(json.dumps({"id": "d-selftest", "status": "open"}))
    sys.exit(0)
if sys.argv[1:2] == ["show"]:
    print(json.dumps({"id": sys.argv[2], "status": "open", "answer": None}))
    sys.exit(0)
sys.exit(2)
"""


@contextlib.contextmanager
def _clock_forward(seconds: int) -> Iterator[None]:
    """Move this process's clock on, so the next pass finds the job due again.
    A self-test that waited a real hour is a self-test nobody runs."""
    previous = os.environ.get(clock.NOW_ENV)
    os.environ.update({clock.NOW_ENV: f"+{seconds}"})
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(clock.NOW_ENV, None)
        else:
            os.environ.update({clock.NOW_ENV: previous})


@contextlib.contextmanager
def _local_http() -> Iterator[Tuple[str, List[dict]]]:
    """A server on loopback that answers ``/ok`` 200 and ``/500`` 500 with a
    body worth reading, and KEEPS what each request sent it. Yields
    ``(base url, requests)`` -- a fake that threw the request body away would
    let the executor send half of one with every assertion still green."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer  # noqa: PLC0415

    seen: List[dict] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            return

        def _reply(self):
            length = int(self.headers.get("Content-Length") or 0)
            seen.append(
                {
                    "path": self.path,
                    "method": self.command,
                    "body": self.rfile.read(length) if length else b"",
                }
            )
            if self.path.startswith("/500"):
                code, body = 500, b'{"error": "the widget exploded"}'
            else:
                code, body = 200, b'{"ok": true}'
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - the name the server dispatches on
            self._reply()

        def do_POST(self):  # noqa: N802 - the name the server dispatches on
            self._reply()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)


def http_gaierror_is_skipped_unresolvable() -> None:
    spec = dict(store.SPEC_DEFAULTS)
    spec.update(
        {"executor": "http", "run": "http://awrise.invalid.invalid/health", "timeout_s": 10}
    )
    out = executors.run(spec, {"wake_id": "w-selftest"})
    assert out.state == "skipped_unresolvable", f"{out.state}:{out.reason}"
    assert out.reason.startswith("dns:"), out.reason
    # negative twin: a host that IS there answers, so the verdict above is
    # about the name and not about the executor being broken
    with _local_http() as (base_url, _seen):
        spec["run"] = f"{base_url}/ok"
        good = executors.run(spec, {"wake_id": "w-selftest"})
        assert good.state == "success" and good.reason == "http_200", good.reason


def http_5xx_is_failure_with_body_tail() -> None:
    with _local_http() as (base_url, seen):
        spec = dict(store.SPEC_DEFAULTS)
        spec.update({"executor": "http", "run": f"{base_url}/500", "timeout_s": 10})
        out = executors.run(spec, {"wake_id": "w-selftest"})
        assert out.state == "failure" and out.reason == "http_500", out.reason
        assert out.exit_code == 500
        assert "the widget exploded" in out.stderr_tail, out.stderr_tail
        # negative twin: a 200 is a success whose body is the STDOUT tail, so
        # the assertion above is about the failing arm and not about tails
        spec["run"] = f"{base_url}/ok"
        good = executors.run(spec, {"wake_id": "w-selftest"})
        assert good.state == "success" and "ok" in good.stdout_tail
        assert good.stderr_tail == "", good.stderr_tail
        assert seen[-1]["body"] == b"", seen[-1]
        # and a job that names a body SENDS it whole, whether or not it named
        # a method: splitting on the first space posts `{"a":` and says
        # nothing about having cut it
        spec["run"] = f'{base_url}/thing {{"a": 1, "b": 2}}'
        sent = executors.run(spec, {"wake_id": "w-selftest"})
        assert sent.state == "success", sent.reason
        assert seen[-1]["body"] == b'{"a": 1, "b": 2}', seen[-1]
        spec["run"] = f'POST {base_url}/thing {{"a": 1, "b": 2}}'
        assert executors.run(spec, {"wake_id": "w-selftest"}).state == "success"
        assert seen[-1]["method"] == "POST" and seen[-1]["body"] == b'{"a": 1, "b": 2}'


def card_raised_after_n_failures() -> None:
    with _home() as base:
        bindir = base / "bin"
        _fake_tool(bindir, "awask", _FAKE_AWASK)
        log = bindir / "awask.log"
        assert (
            _add("f", every="1h", run="import sys; sys.exit(3)", executor="python", card_after=2)
            == 0
        )
        with _only_path(bindir):
            assert _run_due() == 1
            # negative twin: one failure is under the threshold, so no card
            assert [r for r in ledger.read(base) if r.get("event") == "card_raised"] == []
            with _clock_forward(3600):
                assert _run_due() == 1
                # and the streak asks ONCE: a third failure raises no second card
                with _clock_forward(7200):
                    assert _run_due() == 1
        raised = [r for r in ledger.read(base) if r.get("event") == "card_raised"]
        assert len(raised) == 1 and raised[0]["card_id"] == "d-selftest", raised
        assert store.load(base)["f"]["report"]["card_id"] == "d-selftest"
        asks = [
            json.loads(line)
            for line in log.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        asks = [argv for argv in asks if argv[:1] == ["ask"]]
        assert len(asks) == 1, asks
        assert "--default" in asks[0] and asks[0][asks[0].index("--default") + 1] == "keep"
        options = [asks[0][i + 1] for i, part in enumerate(asks[0]) if part == "--option"]
        assert [o.split(":", 1)[0] for o in options] == ["disable", "keep"], options


def report_block_validated_at_add() -> None:
    with _home() as base:
        assert _add("a", report_relay="ops") == 1, "a channel with no '#' was stored"
        assert _add("b", card_after=-1) == 1, "a negative card_after was stored"
        assert store.load(base) == {}
        # negative twin: a well-formed block is accepted and stored as given
        assert _add("c", report_relay="#ops", card_after=2) == 0
        block = store.load(base)["c"]["report"]
        assert block["relay"] == "#ops" and block["card_after"] == 2
        # the same judgement covers `on`, whose members must be ledger states
        assert (
            cli._dispatch(
                cli.cmd_set, argparse.Namespace(name="c", assignments=["report.on=failure,made_up"])
            )
            == 1
        )
        assert (
            cli._dispatch(
                cli.cmd_set, argparse.Namespace(name="c", assignments=["report.on=failure,timeout"])
            )
            == 0
        )
        assert store.load(base)["c"]["report"]["on"] == ["failure", "timeout"]


def cwd_missing_is_error() -> None:
    with _home() as base:
        missing = base / "not-here"
        assert (
            _add(
                "c", every="1h", run="import sys; sys.exit(0)", executor="python", cwd=str(missing)
            )
            == 0
        )
        assert _run_due() == 1
        row = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert row["state"] == "error", row
        assert row["reason"].startswith("cwd_missing:"), row
        # negative twin: a directory that EXISTS runs, so the error above is
        # about the missing path and not about `cwd` being unusable
        missing.mkdir(parents=True, exist_ok=True)
        with _clock_forward(3600):
            assert _run_due() == 0
        row = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert row["state"] == "success", row


def a_job_with_no_sinks_configured_writes_no_report_rows() -> None:
    with _home() as base:
        empty = base / "nobin"
        empty.mkdir(parents=True, exist_ok=True)
        assert (
            _add("q", every="1h", run="import sys; sys.exit(4)", executor="python", card_after=0)
            == 0
        )
        previous = os.environ.get(cli.RELAY_CHANNEL_ENV)
        os.environ.pop(cli.RELAY_CHANNEL_ENV, None)
        try:
            with _only_path(empty):
                assert _run_due() == 1
        finally:
            if previous is not None:
                os.environ.update({cli.RELAY_CHANNEL_ENV: previous})
        assert [
            r
            for r in ledger.read(base)
            if r.get("event") in ("report_error", "card_raised", "card_answered")
        ] == []


_ECHO_MEMORY_ENV = (
    "import os, sys; sys.stdout.write(os.environ.get('AWRISE_MEMORY_JSON', 'MISSING'))"
)


def memory_off_by_default_touches_neither_env_nor_store() -> None:
    with _home() as base:
        assert _add("m", run=_ECHO_MEMORY_ENV, executor="python") == 0
        assert _run_due() == 0
        row = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert row["stdout_tail"] == "MISSING", row
        assert not (base / "awm").exists()
        # negative twin: turning it on creates the store and the export
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(
                    name="m", assignments=["report.memory=true"], allow_overrun=False
                ),
            )
            == 0
        )
        with _clock_forward(3600):
            assert _run_due() == 0
        row = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert row["stdout_tail"] == "[]", row
        assert (base / "awm" / "memory.db").is_file()


def memory_on_exports_recalled_wakes_and_excludes_ancestor_scope() -> None:
    import awm

    with _home() as base:
        assert _add("m", run=_ECHO_MEMORY_ENV, executor="python") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(
                    name="m", assignments=["report.memory=true"], allow_overrun=False
                ),
            )
            == 0
        )
        assert _run_due() == 0  # wake 1: recalls [], remembers wake-1
        with _clock_forward(3700):
            assert _run_due() == 0  # wake 2: recalls [wake-1], remembers wake-2
        row2 = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert len(json.loads(row2["stdout_tail"])) == 1, row2
        # Poison the ANCESTOR scope directly. An unfiltered recall() WOULD
        # return this (that is awm's own ancestor decay, working as
        # designed) -- proving the sink's exact-scope filter is what keeps a
        # write anyone could make at `awrise:<host>:*` out of every job's env.
        poison = awm.MemoryStore(base / "awm" / "memory.db")
        poison.remember(
            awm.Scope("awrise", socket.gethostname(), "*"),
            key="wake-poison",
            value=json.dumps(
                {
                    "state": "success",
                    "reason": "poison",
                    "duration_s": 0,
                    "exit_code": 0,
                    "ts": "x",
                }
            ),
            kind="wake",
        )
        poison.close()
        with _clock_forward(7400):
            assert _run_due() == 0  # wake 3: recalls [wake-2, wake-1], never the poison
        row3 = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        payload = json.loads(row3["stdout_tail"])
        assert len(payload) == 2, payload
        assert all("poison" not in json.dumps(fact) for fact in payload), payload
        assert not [r for r in ledger.read(base) if r.get("event") == "report_error"]


def memory_failure_is_a_report_error_row_not_a_failed_wake() -> None:
    with _home() as base:
        assert _add("m", run="import sys; sys.exit(0)", executor="python") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(
                    name="m", assignments=["report.memory=true"], allow_overrun=False
                ),
            )
            == 0
        )

        def _boom(_base):
            raise RuntimeError("store exploded")

        previous = cli._memory_store
        cli._memory_store = _boom
        try:
            assert _run_due() == 0
        finally:
            cli._memory_store = previous
        row = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert row["state"] == "success", row
        reasons = " ".join(
            r.get("reason") or "" for r in ledger.read(base) if r.get("event") == "report_error"
        )
        assert "memory_recall_failed" in reasons, reasons
        assert "memory_remember_failed" in reasons, reasons


def explain_shows_the_same_recalled_memory_payload() -> None:
    with _home():
        assert _add("m", run=_ECHO_MEMORY_ENV, executor="python") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(
                    name="m", assignments=["report.memory=true"], allow_overrun=False
                ),
            )
            == 0
        )
        assert _run_due() == 0
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = cli._dispatch(cli.cmd_explain, argparse.Namespace(name="m", since=None))
        assert rc == 0
        lines = [ln for ln in out.getvalue().splitlines() if ln.startswith("memory")]
        assert lines and lines[0].startswith("memory       ["), lines


def _predict_history(base: Path, state: str, count: int) -> None:
    """*count* judged finished rows for job "p", one per due window, under
    `predict: off` so building the history never itself consults the gate."""
    for i in range(count):
        ctx = _clock_forward(3700 * i) if i else contextlib.nullcontext()
        with ctx:
            rc = _run_due(lambda job, wake, s=state: Outcome(s, f"fake_{s}"))
        assert rc == (1 if state in ledger.BAD_STATES else 0), (state, rc)


def predict_off_writes_no_prediction_and_builds_no_engine() -> None:
    with _home() as base:
        assert _add("p", run="echo hi") == 0
        assert _run_due() == 0
        row = [r for r in ledger.read(base) if r.get("event") == "started"][-1]
        assert "prediction" not in row, row
        assert str(base) not in cli._PREDICT_ENGINES


def predict_under_threshold_is_unjudged_and_still_fires() -> None:
    with _home() as base:
        assert _add("p", run="echo hi") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(name="p", assignments=["predict=warn"], allow_overrun=False),
            )
            == 0
        )
        assert _run_due() == 0
        row = [r for r in ledger.read(base) if r.get("event") == "started"][-1]
        assert row["prediction"]["verdict"] == "UNJUDGED", row
        assert "historical rows" in row["prediction"]["reason"], row
        finished = [r for r in ledger.read(base) if r.get("event") == "finished"][-1]
        assert finished["state"] == "success", finished
        assert not [r for r in ledger.read(base) if r.get("event") == "report_error"]


def predict_engine_is_built_exactly_once_across_due_checks() -> None:
    import awpredict.core.mlp as mlp

    with _home() as base:
        assert _add("p", run="echo hi") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(name="p", assignments=["predict=warn"], allow_overrun=False),
            )
            == 0
        )
        _predict_history(base, "success", cli.PREDICT_MIN_ROWS)
        builds = []
        real_init = mlp.MLPWorldModel.__init__

        def counting_init(self, *a, **k):
            builds.append(1)
            return real_init(self, *a, **k)

        mlp.MLPWorldModel.__init__ = counting_init
        try:
            # Three MORE due-checks, every one past the row threshold, so
            # every one reaches `_predict_engine` -- built on the FIRST,
            # reused by the other two.
            for i in range(3):
                with _clock_forward(3700 * (cli.PREDICT_MIN_ROWS + i)):
                    assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
        finally:
            mlp.MLPWorldModel.__init__ = real_init
        assert builds == [1], builds


def predict_timeout_fails_open_and_writes_a_report_error_row() -> None:
    with _home() as base:
        assert _add("p", run="echo hi") == 0
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(name="p", assignments=["predict=warn"], allow_overrun=False),
            )
            == 0
        )
        _predict_history(base, "success", cli.PREDICT_MIN_ROWS)

        def _hang(fn, timeout_s):
            raise TimeoutError(f"timed out after {timeout_s:g}s")

        previous = cli._predict_call_with_timeout
        cli._predict_call_with_timeout = _hang
        try:
            with _clock_forward(3700 * cli.PREDICT_MIN_ROWS):
                assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
        finally:
            cli._predict_call_with_timeout = previous
        row = [r for r in ledger.read(base) if r.get("event") == "started"][-1]
        assert row["prediction"]["verdict"] == "UNJUDGED", row
        assert "timed out" in row["prediction"]["reason"], row
        reasons = " ".join(
            r.get("reason") or "" for r in ledger.read(base) if r.get("event") == "report_error"
        )
        assert "timed out" in reasons, reasons


def predict_skip_holds_on_a_bad_verdict_and_never_skips_twice_running() -> None:
    with _home() as base:
        assert _add("p", run="echo hi") == 0
        _predict_history(base, "timeout", cli.PREDICT_MIN_ROWS)
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(name="p", assignments=["predict=skip"], allow_overrun=False),
            )
            == 0
        )
        fired = []
        with _clock_forward(3700 * cli.PREDICT_MIN_ROWS):
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake"))
        assert rc == 0
        assert fired == [], "a bad-verdict skip must never call the executor"
        rows = [r for r in ledger.read(base) if r.get("event") == "finished"]
        assert rows[-1]["state"] == "skipped_predicted", rows[-1]
        # No-starvation: the row right after `skipped_predicted` is a real
        # attempt, whatever the model still predicts.
        with _clock_forward(3700 * (cli.PREDICT_MIN_ROWS + 1)):
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake"))
        assert rc == 1
        assert fired == [1], "the row after skipped_predicted must be a real attempt"
        rows = [r for r in ledger.read(base) if r.get("event") == "finished"]
        assert rows[-1]["state"] == "timeout", rows[-1]
        started = [r for r in ledger.read(base) if r.get("event") == "started"][-1]
        assert started["prediction"]["verdict"] == "bad", started


def dry_run_predict_skip_agrees_with_the_real_pass() -> None:
    with _home() as base:
        assert _add("p", run="echo hi") == 0
        _predict_history(base, "timeout", cli.PREDICT_MIN_ROWS)
        assert (
            cli._dispatch(
                cli.cmd_set,
                argparse.Namespace(name="p", assignments=["predict=skip"], allow_overrun=False),
            )
            == 0
        )
        with _clock_forward(3700 * cli.PREDICT_MIN_ROWS):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli._dispatch(
                    cli.cmd_run_due,
                    argparse.Namespace(
                        quiet=False, invoker="selftest", dry_run=True, drain=False, prune=None
                    ),
                )
            assert rc == 0
            assert "hold" in out.getvalue(), out.getvalue()
            assert not [
                r
                for r in ledger.read(base)
                if r.get("event") == "finished" and r.get("dry_run")
            ]
            fired = []
            rc = _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake"))
        assert rc == 0
        assert fired == [], "the real pass must agree with the dry run and hold too"
        rows = [r for r in ledger.read(base) if r.get("event") == "finished"]
        assert rows[-1]["state"] == "skipped_predicted", rows[-1]


_DISK_PRESSURE_SHAPED_YAML = """
routines:
  - id: disk_pressure
    schedule:
      type: interval
      interval_minutes: 360
      jitter_minutes: 11
      jitter: true
    action:
      type: shell_command
      command: python3 dev/tools/check_disk_pressure.py --from-record --max-age-hours 8
      cwd: /app/AitherOS
      timeout_seconds: 120
"""


def _import_routine_args(
    path, apply: bool = False, i_am_the_runner: bool = False, json_out: bool = False
):
    return argparse.Namespace(
        paths=str(path), apply=apply, i_am_the_runner=i_am_the_runner, json=json_out
    )


def import_routine_translates_a_shaped_yaml_and_writes_nothing_without_apply() -> None:
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "disk-pressure.yaml"
            path.write_text(_DISK_PRESSURE_SHAPED_YAML, encoding="utf-8")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli._dispatch(cli.cmd_import_routine, _import_routine_args(path))
            assert rc == 0
            assert store.load(base) == {}, "a plan without --apply must write nothing"
            assert "routine-disk_pressure" in out.getvalue()
            assert "not carried over" in out.getvalue(), "the dropped jitter must be NAMED"


def import_routine_apply_with_i_am_the_runner_writes_the_job_exactly_once() -> None:
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "disk-pressure.yaml"
            path.write_text(_DISK_PRESSURE_SHAPED_YAML, encoding="utf-8")
            rc = cli._dispatch(
                cli.cmd_import_routine,
                _import_routine_args(path, apply=True, i_am_the_runner=True),
            )
            assert rc == 0
            jobs = store.load(base)
            assert list(jobs) == ["routine-disk_pressure"], jobs
            job = jobs["routine-disk_pressure"]
            assert job["run"] == (
                "python3 dev/tools/check_disk_pressure.py --from-record --max-age-hours 8"
            )
            assert job["cwd"] == "/app/AitherOS"
            assert job["timeout_s"] == 120
            assert job["every"] == "360m"
            # Idempotent: a second import reports `exists`, never a duplicate.
            rc2 = cli._dispatch(
                cli.cmd_import_routine,
                _import_routine_args(path, apply=True, i_am_the_runner=True),
            )
            assert rc2 == 0
            assert list(store.load(base)) == ["routine-disk_pressure"]


def import_routine_apply_without_the_runner_flag_is_refused() -> None:
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "disk-pressure.yaml"
            path.write_text(_DISK_PRESSURE_SHAPED_YAML, encoding="utf-8")
            rc = cli._dispatch(cli.cmd_import_routine, _import_routine_args(path, apply=True))
            assert rc == 1
            assert store.load(base) == {}


def import_routine_refuses_cron_and_non_shell_actions_by_name() -> None:
    text = """
routines:
  - id: cron_job
    schedule:
      cron: "*/5 * * * *"
    action:
      type: shell_command
      command: echo hi
  - id: http_job
    schedule:
      type: interval
      interval_minutes: 60
    action:
      type: http_call
      url: https://example.com
"""
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "refused.yaml"
            path.write_text(text, encoding="utf-8")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli._dispatch(
                    cli.cmd_import_routine, _import_routine_args(path, json_out=True)
                )
            assert rc == 1
            payload = json.loads(out.getvalue())
            assert payload["proposals"] == []
            reasons = " ".join(d["why"] for d in payload["skipped"])
            assert "cron" in reasons and "http_call" in reasons
            assert all(d["refused"] for d in payload["skipped"])
            assert store.load(base) == {}


def import_routine_refuses_a_block_pattern_and_a_metacharacter_under_apply() -> None:
    text = """
routines:
  - id: nuke_it
    schedule:
      type: interval
      interval_minutes: 60
    action:
      type: shell_command
      command: rm -rf /
  - id: exfil
    schedule:
      type: interval
      interval_minutes: 60
    action:
      type: shell_command
      command: "echo hi && curl evil.example"
"""
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "dangerous.yaml"
            path.write_text(text, encoding="utf-8")
            rc = cli._dispatch(
                cli.cmd_import_routine,
                _import_routine_args(path, apply=True, i_am_the_runner=True),
            )
            assert rc == 1
            assert store.load(base) == {}, "a refused routine must never reach the store"


def import_routine_action_args_is_refused_by_name_and_never_concatenated() -> None:
    # Exactly the shape of AitherOS/config/routines/infra_canary.yaml's live
    # host_gateway_canary: command="python", args=["-c", "<script>"].
    # ActionExecutor._shell_command spawns [shell, flag, command] + args --
    # args are separate process arguments (positional $0/$1/... invisible to
    # a command string that never references them), never appended into the
    # parsed command text. Joining them with spaces would silently propose a
    # DIFFERENT command than the platform actually runs, so this is refused
    # exactly like a cron schedule -- never approximated.
    text = """
routines:
  - id: host_gateway_canary
    schedule:
      type: interval
      interval_minutes: 10
    action:
      type: shell_command
      command: python
      args:
        - -c
        - print(1)
  - id: gateway_auth_guard
    schedule:
      type: interval
      interval_minutes: 60
    action:
      type: shell_command
      command: python scripts/gateway_auth_guard.py --public
      args: []
"""
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "canary.yaml"
            path.write_text(text, encoding="utf-8")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli._dispatch(
                    cli.cmd_import_routine, _import_routine_args(path, json_out=True)
                )
            assert rc == 1
            payload = json.loads(out.getvalue())
            assert [p["job"] for p in payload["proposals"]] == ["routine-gateway_auth_guard"]
            reasons = " ".join(d["why"] for d in payload["skipped"])
            assert "action.args" in reasons
            assert all(d["refused"] for d in payload["skipped"])

            # --apply agrees: the args-bearing routine is never written, and
            # an empty `args: []` is unaffected and still imports plainly.
            # rc is still 1 -- one entry in the file was refused, same as a
            # plan/apply run with any other mixed refusal.
            out_apply = io.StringIO()
            with contextlib.redirect_stdout(out_apply):
                rc_apply = cli._dispatch(
                    cli.cmd_import_routine,
                    _import_routine_args(path, apply=True, i_am_the_runner=True),
                )
            assert rc_apply == 1
            jobs = store.load(base)
            assert list(jobs) == ["routine-gateway_auth_guard"], jobs
            assert jobs["routine-gateway_auth_guard"]["run"] == (
                "python scripts/gateway_auth_guard.py --public"
            )


def history_import_fleet_is_read_only_and_flags_unparseable_records() -> None:
    with _home() as base:
        with tempfile.TemporaryDirectory(prefix="awrise-selftest-routines-") as tmp:
            path = Path(tmp) / "disk-pressure.yaml"
            path.write_text(_DISK_PRESSURE_SHAPED_YAML, encoding="utf-8")
            assert (
                cli._dispatch(
                    cli.cmd_import_routine,
                    _import_routine_args(path, apply=True, i_am_the_runner=True),
                )
                == 0
            )
            before = dict(store.load(base))
            fleet = Path(tmp) / "last-executed.json"
            fleet.write_text(
                json.dumps(
                    {
                        "disk_pressure": "2026-09-19T05:00:00+00:00",
                        "never_imported": "2026-09-18T05:00:00+00:00",
                        "bad_stamp": "not-a-timestamp",
                    }
                ),
                encoding="utf-8",
            )
            hist_args = argparse.Namespace(
                job=None,
                since=None,
                event=None,
                limit=50,
                json=True,
                judge=False,
                import_fleet=str(fleet),
            )
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                rc = cli._dispatch(cli.cmd_history, hist_args)
            assert rc == 1
            payload = json.loads(out.getvalue())
            by_id = {r["routine_id"]: r for r in payload["rows"]}
            assert by_id["disk_pressure"]["imported"] is True
            assert by_id["never_imported"]["imported"] is False
            assert any(r[0] == "bad_stamp" for r in payload["refused"])
            assert store.load(base) == before, "history --import-fleet must never write"


CASES: List[Tuple[str, Callable[[], None]]] = [
    (fn.__name__, fn)
    for fn in (
        interval_parse_accepts_s_m_h_d_w_and_compounds,
        interval_parse_rejects_empty_zero_negative_garbage,
        store_crash_mid_save_keeps_jobs,
        store_corrupt_exits_2_and_restore_returns_bak,
        store_v1_migrates_with_tz_aware_stamps_and_keeps_v1_bak,
        ledger_refuses_unknown_event_state_and_empty_reason,
        ledger_accepts_the_closed_vocabulary,
        ledger_redacts_output_tails,
        run_due_failure_exit_code_propagates,
        every_terminal_state_recorded_exactly_once,
        reconcile_closes_orphan_and_does_not_refire,
        spec_keys_accepted_equal_keys_read,
        add_refuses_empty_command_duplicate_and_bad_timeout,
        run_due_is_idempotent_inside_the_window,
        python_m_awrise_entry_exists,
        started_row_precedes_exec_and_stamp_follows_finished,
        finished_wake_whose_stamp_was_lost_is_not_refired,
        ledger_concurrent_appends_lose_nothing,
        status_is_unjudged_for_a_job_that_never_woke,
        store_refuses_malformed_records_with_exit_2,
        add_refuses_names_outside_the_grammar,
        reconcile_never_copies_an_unreadable_ts_into_the_store,
        add_refuses_timeout_ge_interval,
        overlap_is_skipped_while_the_lock_is_held_and_a_stale_lock_is_broken,
        timeout_kills_the_whole_process_tree,
        detach_closes_at_spawn_and_leaves_the_child_alive,
        clock_skew_is_an_error_row_then_a_fire,
        healthy_store_is_byte_stable_across_passes,
        readded_job_never_inherits_a_removed_jobs_wake,
        a_stale_lock_is_broken_by_exactly_one_pass,
        release_never_removes_another_wakes_lock,
        a_foreign_ledger_row_does_not_wedge_the_next_pass,
        clock_skew_on_a_disabled_job_heals_in_one_pass,
        timeout_past_the_platform_wait_is_refused,
        hostclock_renders_every_adapter_the_same_way_twice,
        schtasks_payload_is_crlf_hidden_and_inside_the_length_cap,
        install_print_and_dry_run_touch_nothing,
        install_check_is_unjudged_without_a_record_and_red_without_the_payload,
        install_refuses_to_claim_an_entry_it_cannot_read_back,
        doctor_never_exits_zero_while_printing_a_measured_no,
        every_pass_records_a_tick_with_the_gap_since_the_last_one,
        explain_reports_overdue_windows,
        missed_windows_are_one_row_and_one_catch_up_fire,
        missed_policy_skip_drops_the_windows_without_running,
        at_anchor_fires_once_past_the_anchor_and_reanchors,
        dry_run_says_what_would_fire_and_moves_nothing,
        a_missed_row_needs_a_measured_clock_gap,
        a_wake_in_flight_is_not_recorded_as_lost_windows,
        dry_run_agrees_with_the_pass_on_a_skewed_disabled_job,
        one_unreadable_line_cannot_buy_a_verdict,
        the_window_guard_is_per_job_not_the_longest_cadence,
        prune_is_unjudged_when_the_clock_cannot_be_read,
        history_judge_flags_drift,
        history_judge_is_unjudged_on_a_young_ledger,
        checks_rules_each_fire_and_each_stay_quiet,
        prune_removes_only_files_outside_the_window,
        list_json_carries_the_exact_interval_and_the_next_due,
        an_unreadable_clock_override_is_a_verdict_not_a_traceback,
        optional_executors_never_import_their_packages_at_module_level,
        an_unknown_executor_never_falls_through_to_the_shell,
        a_bearer_path_outside_the_confinement_is_refused,
        a_permission_mode_outside_the_allowlist_is_refused_at_add,
        a_sink_that_cannot_run_is_a_row_not_a_lost_pass,
        a_job_with_no_sinks_configured_writes_no_report_rows,
        http_gaierror_is_skipped_unresolvable,
        http_5xx_is_failure_with_body_tail,
        card_raised_after_n_failures,
        report_block_validated_at_add,
        cwd_missing_is_error,
        memory_off_by_default_touches_neither_env_nor_store,
        memory_on_exports_recalled_wakes_and_excludes_ancestor_scope,
        memory_failure_is_a_report_error_row_not_a_failed_wake,
        explain_shows_the_same_recalled_memory_payload,
        predict_off_writes_no_prediction_and_builds_no_engine,
        predict_under_threshold_is_unjudged_and_still_fires,
        predict_engine_is_built_exactly_once_across_due_checks,
        predict_timeout_fails_open_and_writes_a_report_error_row,
        predict_skip_holds_on_a_bad_verdict_and_never_skips_twice_running,
        dry_run_predict_skip_agrees_with_the_real_pass,
        import_routine_translates_a_shaped_yaml_and_writes_nothing_without_apply,
        import_routine_apply_with_i_am_the_runner_writes_the_job_exactly_once,
        import_routine_apply_without_the_runner_flag_is_refused,
        import_routine_refuses_cron_and_non_shell_actions_by_name,
        import_routine_refuses_a_block_pattern_and_a_metacharacter_under_apply,
        import_routine_action_args_is_refused_by_name_and_never_concatenated,
        history_import_fleet_is_read_only_and_flags_unparseable_records,
    )
]


def _indent(text: str) -> str:
    return "\n".join("        | " + line for line in text.rstrip().splitlines() if line.strip())


def run(list_only: bool = False) -> int:
    if list_only:
        for name, _fn in CASES:
            print(name)
        return 0
    print("awrise self-test")
    failed = 0
    broken = 0
    for name, fn in CASES:
        captured = io.StringIO()
        try:
            with contextlib.redirect_stdout(captured), contextlib.redirect_stderr(captured):
                fn()
        except AssertionError as exc:
            failed += 1
            print(f"  FAIL  {name}: {exc}")
            print(_indent(captured.getvalue()))
        except Exception as exc:  # noqa: BLE001 - the harness must report, not crash
            broken += 1
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
            print(_indent(captured.getvalue()))
        else:
            print(f"  PASS  {name}")
    print(f"{len(CASES) - failed - broken}/{len(CASES)} passed")
    if broken:
        return 2
    return 1 if failed else 0
