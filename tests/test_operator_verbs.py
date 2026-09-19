"""The operator verbs: explain, missed windows, `at` anchors, list --json, prune, dry-run.

Every case that needs the clock to have moved injects it -- a module-level
hook in-process, ``AWRISE_NOW`` for a subprocess -- rather than sleeping: a
test that waits for a real window is a test nobody runs.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from awrise import checks, cli, clock, ledger, lock, store
from awrise.executors import Outcome


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


@pytest.fixture
def frozen():
    """A clock the test drives. Always reset, even when the case fails."""
    state = {"now": datetime(2026, 9, 18, 6, 0, tzinfo=timezone.utc)}
    clock.set_now(lambda: state["now"])
    try:
        yield state
    finally:
        clock.reset_now()


def _add(name, every="1h", run="echo hi", **extra):
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
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _run_due(executor=None, quiet=True, dry_run=False, prune=None):
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=executor),
        argparse.Namespace(quiet=quiet, invoker="test", dry_run=dry_run, prune=prune),
    )


def _ok(job, wake):
    return Outcome("success", "exit_0", exit_code=0)


def _rows(home, event=None, job=None):
    return [
        r
        for r in ledger.read(home)
        if (event is None or r.get("event") == event) and (job is None or r.get("job") == job)
    ]


def _write_row(home: Path, event: str, ts: datetime, **extra) -> dict:
    """One raw ledger line, exactly as a pass would have written it."""
    row = {
        "schema": 1,
        "wake_id": extra.pop("wake_id", "w-fixture1"),
        "pass_id": extra.pop("pass_id", "p-fixture1"),
        "ts": clock.iso(ts),
        "invoker": "fixture",
        "host": "h",
        "pid": 1,
        "interpreter": "x",
        "job": extra.pop("job", None),
        "event": event,
        "state": extra.pop("state", None),
        "reason": extra.pop("reason", "fixture"),
    }
    row.update(extra)
    path = ledger.day_file(home, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")
    return row


def _stamp_job(home: Path, name: str, last_started: datetime, **extra):
    jobs = store.load(home)
    jobs[name]["last_started_at"] = clock.iso(last_started)
    jobs[name]["last_state"] = "success"
    jobs[name]["last_reason"] = "exit_0"
    jobs[name]["last_wake_id"] = "w-earlier1"
    jobs[name].update(extra)
    store.save(jobs, home)
    return jobs[name]


def _fixture_ledger(home: Path, now: datetime) -> None:
    """A ledger with two passes: one that woke the job, one that left it alone."""
    _write_row(
        home,
        "tick",
        now - timedelta(minutes=2),
        pass_id="p-first000",
        reason="pass_start",
        gap_s=60.0,
    )
    _write_row(
        home,
        "started",
        now - timedelta(minutes=2),
        pass_id="p-first000",
        job="j",
        wake_id="w-first000",
        reason="due",
        timeout_s=300,
    )
    _write_row(
        home,
        "finished",
        now - timedelta(minutes=2),
        pass_id="p-first000",
        job="j",
        wake_id="w-first000",
        state="success",
        reason="exit_0",
        exit_code=0,
        duration_s=0.2,
    )
    _write_row(
        home,
        "tick_end",
        now - timedelta(minutes=2),
        pass_id="p-first000",
        reason="pass_end",
        gap_s=60.0,
        jobs=1,
        due=1,
        fired=1,
    )
    _write_row(
        home,
        "tick",
        now - timedelta(minutes=1),
        pass_id="p-second00",
        reason="pass_start",
        gap_s=60.0,
    )
    _write_row(
        home,
        "tick_end",
        now - timedelta(minutes=1),
        pass_id="p-second00",
        reason="pass_end",
        gap_s=60.0,
        jobs=1,
        due=0,
        fired=0,
    )


# ------------------------------------------------------------------ explain


def test_explain_reads_a_fixture_ledger_and_names_the_last_pass(home, frozen, capsys):
    now = frozen["now"]
    assert _add("j", every="1h") == 0
    _stamp_job(home, "j", now - timedelta(minutes=2))
    _fixture_ledger(home, now)
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 0
    out = capsys.readouterr().out
    assert "last tick" in out and "p-second00" in out
    assert "wrote no row for this job: it was not due yet" in out
    assert "missed windows: 0" in out
    assert "w-first000" in out, "the recent rows name the wake the fixture recorded"


def test_explain_names_the_rows_of_the_pass_that_did_act(home, frozen, capsys):
    now = frozen["now"]
    assert _add("j", every="1h") == 0
    _stamp_job(home, "j", now - timedelta(minutes=2))
    _fixture_ledger(home, now)
    # Re-read the FIRST pass, the one that fired, by trimming the window so the
    # quiet pass falls outside it.
    rows = [r for r in ledger.read(home) if r.get("pass_id") != "p-second00"]
    for path in ledger.files(home):
        path.unlink()
    for row in rows:
        _write_row(
            home,
            row["event"],
            clock.parse_ts(row["ts"]),
            **{
                k: v
                for k, v in row.items()
                if k not in ("event", "ts", "schema", "invoker", "host", "pid", "interpreter")
            },
        )
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 0
    out = capsys.readouterr().out
    assert "the pass acted on this job" in out and "finished:success" in out


def test_explain_reports_overdue_windows(home, frozen, capsys):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(minutes=5))
    _fixture_ledger(home, now)
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 0
    out = capsys.readouterr().out
    assert "missed windows: 4" in out, out
    # negative twin: the same assertion on a job that is up to date must fail
    _stamp_job(home, "j", now)
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 0
    assert "missed windows: 0" in capsys.readouterr().out


def test_explain_is_unjudged_when_no_pass_is_on_record(home, frozen, capsys):
    assert _add("j") == 0
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 2
    assert "UNJUDGED" in capsys.readouterr().out


def test_explain_refuses_a_name_that_is_neither_job_nor_wake(home, frozen, capsys):
    assert _add("j") == 0
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="ghost", since=None)) == 1
    assert "Not found" in capsys.readouterr().err


def test_explain_still_explains_a_removed_job_from_the_ledger(home, frozen, capsys):
    now = frozen["now"]
    _fixture_ledger(home, now)
    assert cli._dispatch(cli.cmd_explain, argparse.Namespace(name="j", since=None)) == 0
    out = capsys.readouterr().out
    assert "removed" in out and "w-first000" in out


# ------------------------------------------------------------------- missed


def test_a_clock_gap_writes_one_missed_row_and_one_catch_up_fire(home, frozen):
    """The slice's own check: a stale stamp plus a measured gap > 2x period."""
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(minutes=5))
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")

    assert _run_due(_ok) == 0
    missed = _rows(home, event="missed", job="j")
    assert len(missed) == 1, missed
    assert missed[0]["reason"].startswith("clock_gap:"), missed[0]
    assert missed[0]["windows"] == 4 and missed[0]["policy"] == "catch_up_once"
    started = _rows(home, event="started", job="j")
    assert len(started) == 1 and started[0]["reason"] == "catch_up_once:4_windows"
    assert [r["state"] for r in _rows(home, event="finished", job="j")] == ["success"]

    # ONCE: the arrears are not re-served on the very next pass.
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="missed", job="j")) == 1
    assert len(_rows(home, event="started", job="j")) == 1


def test_missed_policy_skip_drops_the_windows_without_running(home, frozen):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30, missed="skip") == 0
    _stamp_job(home, "j", now - timedelta(minutes=5))
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")

    assert _run_due(_ok) == 0
    assert len(_rows(home, event="missed", job="j")) == 1
    assert _rows(home, event="started", job="j") == [], "skip must not run the command"
    finished = _rows(home, event="finished", job="j")
    assert [r["state"] for r in finished] == ["skipped_missed"]
    assert finished[0]["reason"] == "missed_policy_skip:4_windows"
    # The stamp moved, so the dropped windows are not re-found next pass.
    assert clock.missed_windows(store.load(home)["j"], now) == 0
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="missed", job="j")) == 1


def test_a_job_woken_on_time_writes_no_missed_row(home, frozen):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(seconds=61))
    _write_row(home, "tick", now - timedelta(seconds=61), reason="pass_start")
    assert _run_due(_ok) == 0
    assert _rows(home, event="missed") == []
    assert _rows(home, event="started", job="j")[0]["reason"] == "due"


def test_the_tick_end_row_counts_the_pass(home, frozen):
    now = frozen["now"]
    assert _add("fires", every="1m", timeout=30) == 0
    assert _add("off", every="1m", timeout=30, disabled=True) == 0
    _stamp_job(home, "fires", now - timedelta(minutes=5))
    _stamp_job(home, "off", now - timedelta(minutes=5))
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")
    assert _run_due(_ok) == 0
    end = _rows(home, event="tick_end")[-1]
    for field in (
        "jobs",
        "due",
        "fired",
        "skipped",
        "overlapped",
        "missed",
        "gap_s",
        "in_progress",
    ):
        assert field in end, f"tick_end is missing {field}"
    assert end["jobs"] == 2 and end["due"] == 1 and end["fired"] == 1
    assert end["missed"] == 1 and end["skipped"] == 1 and end["overlapped"] == 0


def test_add_refuses_a_missed_policy_nobody_implements(home, capsys):
    assert _add("j", missed="run_them_all") == 1
    assert "missed must be one of" in capsys.readouterr().err
    # negative twin: the two policies that exist are accepted, either spelling
    assert _add("a", missed="skip") == 0
    assert _add("b", missed="catch_up_once") == 0
    assert cli.main(["set", "--name", "b", "missed=catch-up-once"]) == 0
    assert store.load(home)["b"]["missed"] == "catch_up_once"


# --------------------------------------------------------------- at anchors


def test_at_anchor_fires_once_past_the_anchor_and_reanchors(home, frozen):
    """A host asleep through 07:00 catches up once and stays anchored at 07:00."""
    frozen["now"] = datetime(2026, 9, 18, 6, 0, tzinfo=timezone.utc)
    assert _add("dawn", every="1d", at="07:00", timeout=60) == 0
    _stamp_job(home, "dawn", datetime(2026, 9, 18, 5, 0, tzinfo=timezone.utc))
    assert _run_due(_ok) == 0, "06:00 is before the anchor"
    assert _rows(home, event="started", job="dawn") == []

    # The clock jumps past 07:00 -- a laptop opened at 09:30.
    frozen["now"] = datetime(2026, 9, 18, 9, 30, tzinfo=timezone.utc)
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="started", job="dawn")) == 1

    # ONCE: a second pass at the same clock does not fire again...
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="started", job="dawn")) == 1
    # ...and the anchor has NOT walked to 09:30: the next window is 07:00 again.
    job = store.load(home)["dawn"]
    assert clock.next_due(job, frozen["now"]) == datetime(2026, 9, 19, 7, 0, tzinfo=timezone.utc)

    # The day after, at the anchor, it fires once more.
    frozen["now"] = datetime(2026, 9, 19, 7, 0, 30, tzinfo=timezone.utc)
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="started", job="dawn")) == 2
    assert clock.next_due(store.load(home)["dawn"], frozen["now"]) == datetime(
        2026, 9, 20, 7, 0, tzinfo=timezone.utc
    )


def test_a_day_asleep_counts_one_missed_window_for_an_anchored_job(home, frozen):
    frozen["now"] = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    assert _add("dawn", every="1d", at="07:00", timeout=60) == 0
    _stamp_job(home, "dawn", datetime(2026, 9, 16, 7, 0, tzinfo=timezone.utc))
    job = store.load(home)["dawn"]
    assert clock.period_s(job) == 86400.0, "an anchored job has one window a day"
    assert clock.missed_windows(job, frozen["now"]) == 1


# -------------------------------------------------------------- list --json


def test_list_json_carries_the_exact_interval_and_the_next_due(home, frozen, capsys):
    now = frozen["now"]
    assert _add("j", every="1h30m") == 0
    _stamp_job(home, "j", now - timedelta(minutes=30))
    capsys.readouterr()
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
    out = json.loads(capsys.readouterr().out)["j"]
    assert out["every"] == "1h30m", "shown exactly as entered"
    assert out["interval_s"] == 5400.0, "exact, never rounded for display"
    assert out["next_due"] == clock.iso(now + timedelta(minutes=60))
    assert out["due_in_s"] == 3600.0 and out["due_now"] is False
    assert out["missed_windows"] == 0 and out["in_progress"] is False


def test_list_json_says_due_now_for_an_overdue_job(home, frozen, capsys):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(minutes=5))
    capsys.readouterr()
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
    out = json.loads(capsys.readouterr().out)["j"]
    assert out["due_now"] is True and out["due_in_s"] < 0
    assert out["missed_windows"] == 4


def test_list_json_on_an_empty_store_is_still_json(home, capsys):
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
    assert json.loads(capsys.readouterr().out) == {}


# -------------------------------------------------------------------- prune


def test_prune_removes_old_day_files_and_keeps_the_window(home, frozen, capsys):
    now = frozen["now"]
    for days in (0, 1, 40):
        _write_row(home, "tick", now - timedelta(days=days), reason="pass_start")
    assert len(ledger.files(home)) == 3
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
        == 0
    )
    names = {p.name for p in ledger.files(home)}
    assert len(names) == 2, names
    assert (now - timedelta(days=40)).strftime("%Y-%m-%d") + ".jsonl" not in names
    assert now.strftime("%Y-%m-%d") + ".jsonl" in names, "today is never pruned"
    assert "prune: removed 1 day file(s)" in capsys.readouterr().out


def test_prune_dry_run_removes_nothing(home, frozen, capsys):
    now = frozen["now"]
    for days in (0, 40):
        _write_row(home, "tick", now - timedelta(days=days), reason="pass_start")
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=True, force=False)) == 0
    )
    assert len(ledger.files(home)) == 2, "a dry run unlinks nothing"
    assert "would remove" in capsys.readouterr().out


def test_prune_refuses_an_unparseable_window(home, capsys):
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="soon", dry_run=False, force=False))
        == 1
    )
    assert "Error: --keep" in capsys.readouterr().err


def test_run_due_can_prune_after_the_pass(home, frozen):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _write_row(home, "tick", now - timedelta(days=40), reason="pass_start")
    assert _run_due(_ok, prune="30d") == 0
    names = {p.name for p in ledger.files(home)}
    assert (now - timedelta(days=40)).strftime("%Y-%m-%d") + ".jsonl" not in names
    assert now.strftime("%Y-%m-%d") + ".jsonl" in names, "this pass's own rows survive"


# ------------------------------------------------------------------ dry run


def test_dry_run_writes_would_fire_and_changes_nothing(home, frozen):
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(minutes=5))
    before = (home / "jobs.json").read_bytes()

    def never(job, wake):
        raise AssertionError("a dry run must not execute anything")

    assert _run_due(never, dry_run=True) == 0
    assert (home / "jobs.json").read_bytes() == before, "no stamp moved"
    rows = ledger.read(home)
    assert [r["event"] for r in rows] == ["finished"], rows
    assert rows[0]["state"] == "would_fire" and rows[0]["reason"].startswith("catch_up_once:")
    assert rows[0]["missed_windows"] == 4
    assert [r for r in rows if r["event"] in ("tick", "tick_end")] == [], (
        "a dry run is not a pass: it writes no tick"
    )


def test_dry_run_holds_a_job_that_would_not_fire(home, frozen, capsys):
    now = frozen["now"]
    assert _add("soon", every="1h") == 0
    assert _add("off", every="1m", timeout=30, disabled=True) == 0
    _stamp_job(home, "soon", now)
    _stamp_job(home, "off", now - timedelta(minutes=5))
    assert _run_due(_ok, dry_run=True, quiet=False) == 0
    out = capsys.readouterr().out
    assert "hold" in out and "not_due_for_" in out and "disabled" in out
    assert "0 of 2 job(s) would fire" in out
    assert ledger.read(home) == [], "nothing would fire, so nothing is claimed"


# ------------------------------------------------------------ history judge


def _judgeable(home, name="j", every="1m", wakes=(0,), span_minutes=10):
    """A ledger long enough to judge, with a started row at each offset."""
    now = clock.now_utc()
    _write_row(home, "tick", now - timedelta(minutes=span_minutes), reason="pass_start")
    for index, minutes in enumerate(wakes):
        _write_row(
            home,
            "started",
            now - timedelta(minutes=minutes),
            job=name,
            wake_id=f"w-judge{index:03d}",
            reason="due",
        )
    _write_row(home, "tick", now, reason="pass_start")


def test_history_judge_is_unjudged_on_a_young_ledger(home, capsys):
    assert _add("j", every="1h") == 0
    assert _run_due(_ok) == 0
    assert (
        cli._dispatch(
            cli.cmd_history,
            argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True),
        )
        == 2
    )
    assert "UNJUDGED" in capsys.readouterr().err


def test_history_judge_flags_drift(home, capsys):
    assert _add("j", every="1m", timeout=30) == 0
    _judgeable(home, wakes=(10, 9, 8, 0), span_minutes=10)
    _stamp_job(home, "j", clock.now_utc())
    assert (
        cli._dispatch(
            cli.cmd_history,
            argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True),
        )
        == 1
    )
    out = capsys.readouterr().err
    assert "NOT OK  j" in out and "more than 3x" in out


def test_history_judge_is_ok_on_a_steady_cadence(home, capsys):
    assert _add("j", every="1m", timeout=30) == 0
    _judgeable(home, wakes=tuple(range(10, -1, -1)), span_minutes=10)
    _stamp_job(home, "j", clock.now_utc())
    assert (
        cli._dispatch(
            cli.cmd_history,
            argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True),
        )
        == 0
    )
    assert "OK      j" in capsys.readouterr().out


def test_history_judge_flags_an_enabled_job_that_never_woke(home, capsys):
    assert _add("j", every="1m", timeout=30) == 0
    _judgeable(home, name="other", wakes=(10, 5, 0), span_minutes=10)
    assert (
        cli._dispatch(
            cli.cmd_history,
            argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True),
        )
        == 1
    )
    assert "enabled and never woken" in capsys.readouterr().err


# ------------------------------------------------- the clock is injectable


def test_the_clock_hook_and_the_environment_both_drive_now(monkeypatch):
    fixed = datetime(2031, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    clock.set_now(lambda: fixed)
    try:
        assert clock.now_utc() == fixed
    finally:
        clock.reset_now()
    monkeypatch.setenv(clock.NOW_ENV, clock.iso(fixed))
    assert clock.now_utc() == fixed
    monkeypatch.setenv(clock.NOW_ENV, "+3600")
    ahead = (clock.now_utc() - datetime.now(timezone.utc)).total_seconds()
    assert 3500 < ahead < 3700


def test_an_unreadable_clock_override_is_exit_2_not_a_wrong_answer(home, monkeypatch, capsys):
    assert _add("j") == 0
    monkeypatch.setenv(clock.NOW_ENV, "half past nine")
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
    assert "NOT VERIFIED" in capsys.readouterr().err
    monkeypatch.delenv(clock.NOW_ENV)
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2, "unjudged: never woke"


# -------------------------------------------------------- checks, as a verb


def test_the_checks_verb_reaches_the_same_verdict_as_the_module(home, frozen):
    assert _add("j", every="1h") == 0
    args = argparse.Namespace(since=None, json=False, self_test=False)
    assert cli._dispatch(cli.cmd_checks, args) == checks.verdict(checks.run(base=home))


def test_prune_spares_a_file_holding_a_wake_nobody_closed(home, frozen, capsys):
    """An old `started` row with no `finished` row is the only evidence that
    wake began: dropping it turns an unaccounted wake into one that never ran."""
    now = frozen["now"]
    old = now - timedelta(days=40)
    _write_row(
        home, "started", old, job="j", wake_id="w-orphan01", reason="due", timeout_s=30, pid=1
    )
    _write_row(home, "tick", now, reason="pass_start")
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
        == 0
    )
    assert len(ledger.files(home)) == 2, "the open wake's day file is spared"
    assert "never closed" in capsys.readouterr().out
    # negative twin 1: close the wake and the same prune removes the file
    _write_row(
        home, "finished", old, job="j", wake_id="w-orphan01", state="orphaned", reason="pid_gone"
    )
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
        == 0
    )
    assert len(ledger.files(home)) == 1


def test_prune_force_drops_even_an_open_wake(home, frozen):
    now = frozen["now"]
    _write_row(
        home,
        "started",
        now - timedelta(days=40),
        job="j",
        wake_id="w-orphan02",
        reason="due",
        timeout_s=30,
        pid=1,
    )
    _write_row(home, "tick", now, reason="pass_start")
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=True)) == 0
    )
    assert len(ledger.files(home)) == 1


def test_prune_is_unjudged_when_the_clock_cannot_be_read(home, monkeypatch, capsys):
    """An unreadable clock is not a bad --keep. `ClockError` subclasses
    `ValueError`, so prune used to relabel it as an invalid interval and exit
    1 (violation) on the input every other verb answers 2 (could not judge)."""
    assert _add("j", every="1h") == 0
    monkeypatch.setenv("AWRISE_NOW", "not-a-time")
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="30d", dry_run=False, force=False))
        == 2
    )
    assert "NOT VERIFIED" in capsys.readouterr().err
    # the same verdict every other verb reaches on the same clock
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
    # negative twin: with the clock readable, a bad --keep is still 1
    monkeypatch.delenv("AWRISE_NOW")
    assert (
        cli._dispatch(cli.cmd_prune, argparse.Namespace(keep="soon", dry_run=False, force=False))
        == 1
    )
    assert "Error: --keep" in capsys.readouterr().err


# ------------------------------------------- the missed row needs evidence


def _hold_the_lock(home, name, now, timeout_s=30):
    """A live lock for `name`, held by THIS process: a wake in flight."""
    import os

    path = lock.lock_path(home, name)
    path.mkdir(parents=True)
    with open(path / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {
                "wake_id": "w-inflight",
                "pass_id": "p-inflight",
                "pass_pid": os.getpid(),
                "child_pid": 0,
                "started_at": clock.iso(now),
                "timeout_s": timeout_s,
            },
            fh,
        )
    return path


def test_a_steady_clock_writes_no_missed_row_for_a_job_in_arrears(home, frozen):
    """A 1-minute job on a 1-minute clock falls one window into arrears on
    ordinary jitter. The plan's rule is measured-gap-driven, so a window is
    only lost when the clock stopped -- here it never did."""
    now = frozen["now"]
    assert _add("j", every="1m", timeout=30) == 0
    _stamp_job(home, "j", now - timedelta(seconds=121))
    _write_row(home, "tick", now - timedelta(seconds=60), reason="pass_start")
    assert clock.missed_windows(store.load(home)["j"], now) == 1, "it IS in arrears"
    assert _run_due(_ok) == 0
    assert _rows(home, event="missed") == [], "a healthy clock lost nothing"
    assert _rows(home, event="started", job="j")[0]["reason"] == "due"
    assert _rows(home, event="tick_end")[-1]["missed"] == 0
    # negative twin: the same arrears with a measured gap past 2x the window
    # IS recorded as lost (a second job, because re-stamping a job the pass
    # has already woken is a lost stamp, and `reconcile` heals that)
    assert _add("k", every="1m", timeout=30) == 0
    _stamp_job(home, "k", now - timedelta(minutes=5))
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")
    assert _run_due(_ok) == 0
    missed = _rows(home, event="missed", job="k")
    assert len(missed) == 1 and missed[0]["reason"].startswith("clock_gap:"), missed


def test_a_wake_in_flight_is_not_recorded_as_lost_windows(home, frozen):
    """A scheduler that will not start a second copy produces exactly the
    silence of a dead clock. The pass closes this job `skipped_overlap`; it
    must not ALSO file a row, once per pass, saying its windows were lost."""
    now = frozen["now"]
    assert _add("slow", every="1m", timeout=30) == 0
    _stamp_job(home, "slow", now - timedelta(minutes=5))
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")
    held = _hold_the_lock(home, "slow", now)
    assert _run_due(_ok) == 0
    assert _rows(home, event="missed") == [], "the wake is in flight, not absent"
    end = _rows(home, event="tick_end")[-1]
    assert end["missed"] == 0 and end["in_progress"] == 1
    assert [r["state"] for r in _rows(home, event="finished", job="slow")] == ["skipped_overlap"]
    # negative twin: the wake ends, the lock goes, and the same store records
    # the windows it really lost
    for child in sorted(held.iterdir()):
        child.unlink()
    held.rmdir()
    # (the last tick on record is now this pass's own, so the gap the next
    # pass measures is restored to the one the first pass saw)
    _write_row(home, "tick", now - timedelta(minutes=5), reason="pass_start")
    assert _run_due(_ok) == 0
    assert len(_rows(home, event="missed", job="slow")) == 1


def test_dry_run_holds_a_disabled_job_whose_stamp_is_in_the_future(home, frozen, capsys):
    """`--dry-run` is `what the next real pass would do`. On clock skew the
    real pass writes the error row and then falls through to
    `skipped_disabled` -- it never runs the command."""
    now = frozen["now"]
    assert _add("dis", every="10m", timeout=30, disabled=True) == 0
    _stamp_job(home, "dis", now + timedelta(hours=2))
    assert _run_due(_ok, dry_run=True, quiet=False) == 0
    out = capsys.readouterr().out
    assert "hold" in out and "disabled" in out, out
    assert "WOULD FIRE" not in out, out
    assert "0 of 1 job(s) would fire" in out

    # and the real pass agrees: an error row for the skew, then the skip,
    # and the command is never executed
    def never(job, wake):
        raise AssertionError("a disabled job must not run")

    assert _run_due(never) == 1
    states = [r["state"] for r in _rows(home, event="finished", job="dis")]
    assert states == ["error", "skipped_disabled"], states
    # negative twin: the same skew on an ENABLED job does fire, both ways
    assert _add("on", every="10m", timeout=30) == 0
    _stamp_job(home, "on", now + timedelta(hours=2))
    assert _run_due(_ok, dry_run=True, quiet=False) == 0
    assert "WOULD FIRE on (clock_skew:" in capsys.readouterr().out
    assert _run_due(_ok) == 1
    assert "success" in [r["state"] for r in _rows(home, event="finished", job="on")]


def test_history_judge_judges_a_short_cadence_job_beside_a_long_one(home, frozen, capsys):
    """The window guard is per job. Keyed on the longest cadence in the store,
    one monthly job hides a 1-minute job the clock stopped reaching."""
    assert _add("fast", every="1m", timeout=30) == 0
    _judgeable(home, name="other", wakes=(10, 5, 0), span_minutes=10)
    judge = argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True)
    assert cli._dispatch(cli.cmd_history, judge) == 1
    assert "enabled and never woken" in capsys.readouterr().err
    assert _add("monthly", every="30d") == 0  # healthy, never run, changes nothing
    assert cli._dispatch(cli.cmd_history, judge) == 1, "the measured red survives"
    err = capsys.readouterr().err
    assert "enabled and never woken" in err
    assert "UNJUDGED monthly" in err, "the job it could not judge is named"


def test_history_judge_is_unjudged_when_every_job_is_too_young(home, frozen, capsys):
    assert _add("slow", every="1h") == 0
    _judgeable(home, name="slow", wakes=(0,), span_minutes=10)
    assert (
        cli._dispatch(
            cli.cmd_history,
            argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True),
        )
        == 2
    )
    assert "UNJUDGED" in capsys.readouterr().err


def test_one_unreadable_line_cannot_buy_history_judge_a_verdict(home, frozen):
    """A truncated final line is the normal outcome of a crash mid-append. It
    is reported as a row and must lend the ledger no span: stamped at the day
    file's midnight it bought a seconds-old ledger a green verdict."""
    now = frozen["now"]
    assert _add("j", every="10m", timeout=30) == 0
    _write_row(home, "tick", now, reason="pass_start")
    judge = argparse.Namespace(since=None, job=None, event=None, limit=0, json=False, judge=True)
    assert cli._dispatch(cli.cmd_history, judge) == 2
    with open(ledger.day_file(home, now), "a", encoding="utf-8", newline="\n") as fh:
        fh.write("{truncated\n")
    assert [r for r in ledger.read(home) if r.get("event") == "unreadable"], (
        "the line is reported, never dropped"
    )
    assert cli._span_s(ledger.read(home)) is None, "and it lends the record no span"
    assert cli._dispatch(cli.cmd_history, judge) == 2, "still too young to judge"
    # negative twin: a real row from an earlier pass DOES buy the span
    _write_row(home, "tick", now - timedelta(hours=4), reason="pass_start")
    assert cli._dispatch(cli.cmd_history, judge) == 1
