"""run-due / run / reconcile: honest exit codes, every state once, no double fire."""

import argparse
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import cli, clock, ledger, store
from awrise.executors import Outcome

PKG = Path(__file__).resolve().parent.parent


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


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
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _wake_events(rows) -> list:
    """Row events for the WAKES only.

    Every pass also writes its own `tick`/`tick_end` pair -- that is the host
    clock's record, not a job's -- so an assertion about what happened to a
    job filters them out rather than counting them.
    """
    return [row["event"] for row in rows if row.get("event") not in ("tick", "tick_end")]


def _run_due(executor=None, quiet=True):
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=executor),
        argparse.Namespace(quiet=quiet, invoker="test"),
    )


def _finished(home):
    return [r for r in ledger.read(home) if r["event"] == "finished"]


def _write_started(home, wake_id, job, age, pid=1, timeout_s=300):
    ts = clock.iso(clock.now_utc() - age)
    row = {
        "schema": 1,
        "wake_id": wake_id,
        "pass_id": "p-dead0000",
        "ts": ts,
        "invoker": "t",
        "host": "h",
        "pid": pid,
        "interpreter": "x",
        "job": job,
        "event": "started",
        "state": None,
        "reason": "due",
        "timeout_s": timeout_s,
    }
    path = ledger.day_file(home, clock.parse_ts(ts))
    path.parent.mkdir(exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")
    return ts


# ----------------------------------------------------------- exit codes


def test_failure_exit_code_propagates_from_a_real_shell(home):
    py = sys.executable
    assert _add("bad", run=f'"{py}" -c "import sys; sys.exit(3)"') == 0
    assert _run_due() == 1
    row = _finished(home)[-1]
    assert row["state"] == "failure" and row["exit_code"] == 3 and row["reason"] == "exit_3"


def test_success_exits_0_and_records_output_tails(home):
    py = sys.executable
    script = "print('hello'); import sys; sys.stderr.write('warn')"
    assert _add("ok", run=f'"{py}" -c "{script}"') == 0
    assert _run_due() == 0
    row = _finished(home)[-1]
    assert row["state"] == "success" and "hello" in row["stdout_tail"]
    assert "warn" in row["stderr_tail"] and row["duration_s"] >= 0


def test_nothing_due_exits_0_and_writes_nothing(home):
    assert _add("j") == 0
    assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
    assert _run_due(lambda job, wake: Outcome("failure", "exit_1")) == 0, "not due: not fired"
    assert len(_finished(home)) == 1


def test_no_jobs_exits_0(home):
    assert _run_due() == 0


def test_bad_states_exit_1_others_exit_0(home):
    for state in sorted(ledger.STATES - {"orphaned"}):
        assert _add(f"j_{state}") == 0
        rc = _run_due(lambda job, wake, s=state: Outcome(s, f"fake_{s}"))
        assert rc == (1 if state in ledger.BAD_STATES else 0), state
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name=f"j_{state}")) == 0


def test_executor_returning_garbage_is_closed_as_error(home):
    assert _add("j") == 0
    assert _run_due(lambda job, wake: Outcome("sorta", "meh")) == 1
    row = _finished(home)[-1]
    assert row["state"] == "error" and row["reason"].startswith("executor_returned_unknown_state")


# --------------------------------------------------- every state, once


def test_every_terminal_state_is_produced_exactly_once(home):
    from_executor = sorted(ledger.STATES - {"skipped_disabled", "skipped_empty", "orphaned"})
    for state in from_executor:
        assert _add(f"j_{state}") == 0
        _run_due(lambda job, wake, s=state: Outcome(s, f"fake_{s}"))
        job = store.load(home)[f"j_{state}"]
        if state in ledger.UNSTAMPED_STATES:
            assert job["last_started_at"] is None, f"{state} must be retried next pass"
        else:
            assert job["last_state"] == state and job["last_started_at"]
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name=f"j_{state}")) == 0
    assert _add("off") == 0
    assert cli._dispatch(cli.cmd_disable, argparse.Namespace(name="off")) == 0
    assert _run_due(lambda job, wake: Outcome("success", "never")) == 0
    assert store.load(home)["off"]["last_state"] == "skipped_disabled"
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="off")) == 0
    jobs = store.load(home)
    jobs["empty"] = store.new_job("1h", "")
    store.save(jobs, home)
    assert _run_due(lambda job, wake: Outcome("success", "never")) == 0
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="empty")) == 0
    assert _add("orphan", every="1d") == 0
    _write_started(home, "w-orphan01", "orphan", timedelta(hours=1))
    assert _run_due(lambda job, wake: Outcome("success", "never")) == 1
    states = [r["state"] for r in _finished(home)]
    assert sorted(states) == sorted(ledger.STATES), states
    # every finished row for an EXECUTED wake follows its own started row
    rows = ledger.read(home)
    seen = set()
    executed = ledger.STATES - {"skipped_disabled", "skipped_empty", "orphaned"}
    for row in rows:
        if row["event"] == "started":
            seen.add(row["wake_id"])
        elif row["event"] == "finished" and row["state"] in executed:
            assert row["wake_id"] in seen, row


# --------------------------------------------- the pinned write order


def test_started_row_is_on_disk_before_the_executor_runs(home):
    """started row -> exec -> finished row -> jobs.json. The executor is the
    witness: it reads the ledger while it runs and must find its own wake."""
    assert _add("j") == 0
    seen = {}

    def peek(job, wake):
        rows = [r for r in ledger.read(home) if r.get("wake_id") == wake["wake_id"]]
        seen["events"] = [r["event"] for r in rows]
        seen["pid"] = rows[0]["pid"] if rows else None
        seen["store_stamp"] = store.load(home)["j"]["last_started_at"]
        return Outcome("success", "exit_0", exit_code=0)

    assert _run_due(peek) == 0
    assert seen["events"] == ["started"], "the started row must precede exec"
    assert seen["pid"] == os.getpid()
    assert seen["store_stamp"] is None, "jobs.json is stamped only after the finished row"
    assert _wake_events(ledger.read(home)) == ["started", "finished"]
    assert store.load(home)["j"]["last_state"] == "success"


def test_pass_that_dies_mid_exec_leaves_a_started_row_reconcile_closes_pid_gone(home):
    """A real process dies during exec: the started row must already be on
    disk, and the next pass closes that wake as orphaned:pid_gone."""
    assert _add("j") == 0
    script = (
        "import argparse, os\n"
        "from awrise import cli\n"
        "def die(job, wake):\n"
        "    os._exit(9)\n"
        "cli.cmd_run_due(argparse.Namespace(quiet=True, invoker='t'), executor=die)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=str(PKG),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "AWRISE_HOME": str(home)},
    )
    _out, err = proc.communicate(timeout=60)
    assert proc.returncode == 9, err.decode("utf-8", "replace")
    rows = [row for row in ledger.read(home) if row.get("event") not in ("tick", "tick_end")]
    assert _wake_events(ledger.read(home)) == ["started"], rows
    assert rows[0]["pid"] == proc.pid
    assert store.load(home)["j"]["last_started_at"] is None
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 1 and fired == [], "closed as orphaned, and the job is not re-fired"
    row = _finished(home)[-1]
    assert row["state"] == "orphaned" and row["reason"] == "pid_gone"
    assert row["wake_id"] == rows[0]["wake_id"]
    assert store.load(home)["j"]["last_state"] == "orphaned"


def test_executor_that_raises_is_closed_as_error_not_a_traceback(home):
    assert _add("j") == 0

    def boom(job, wake):
        raise RuntimeError("boom")

    assert _run_due(boom) == 1
    rows = [row for row in ledger.read(home) if row.get("event") not in ("tick", "tick_end")]
    assert _wake_events(ledger.read(home)) == ["started", "finished"]
    assert rows[1]["state"] == "error" and rows[1]["reason"] == "executor_crashed:RuntimeError:boom"
    assert store.load(home)["j"]["consecutive_failures"] == 1
    assert _add("k") == 0
    assert _run_due(lambda job, wake: Outcome("success", "   ")) == 1
    row = _finished(home)[-1]
    assert row["job"] == "k" and row["reason"] == "executor_returned_empty_reason:success"
    assert not ledger.open_wakes(ledger.read(home)), "no wake is left open"


def test_save_failure_after_the_finished_row_does_not_double_fire(home, monkeypatch):
    """The ledger is the memory; jobs.json is its cache. When the save fails
    AFTER the finished row (disk full, a foreign reader on Windows), the next
    pass recovers the stamp from the ledger instead of firing the job again."""
    assert _add("j") == 0
    real_save = store.save
    state = {"fail": True}

    def flaky_save(jobs, base=None):
        if state["fail"] and jobs.get("j", {}).get("last_state"):
            raise store.StoreError("cannot write jobs.json: simulated sharing violation")
        return real_save(jobs, base)

    monkeypatch.setattr(store, "save", flaky_save)
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0", exit_code=0))
    assert rc == 2 and fired == [1]
    assert _wake_events(ledger.read(home)) == ["started", "finished"]
    assert store.load(home)["j"]["last_started_at"] is None, "the stamp did not land"
    state["fail"] = False
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 0 and fired == [1], "recovered from the ledger, not fired again"
    job = store.load(home)["j"]
    started = [r for r in ledger.read(home) if r["event"] == "started"]
    assert len(started) == 1 and job["last_started_at"] == started[0]["ts"]
    assert job["last_state"] == "success" and job["last_wake_id"] == started[0]["wake_id"]
    assert any(
        r["event"] == "reconciled" and r["reason"] == "recovered_1_stamps" and r["jobs"] == ["j"]
        for r in ledger.read(home)
    )
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0
    # negative twin: a healthy store recovers nothing on the next pass
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
    assert fired == [1]
    assert (
        sum(
            r["reason"].startswith("recovered_")
            for r in ledger.read(home)
            if r["event"] == "reconciled"
        )
        == 1
    )


def test_consecutive_failures_count_and_reset(home):
    assert _add("j", every="1s", timeout=0, allow_overrun=True) == 1, "timeout 0 refused"
    assert _add("j", every="1s", timeout=1, allow_overrun=True) == 0
    for _ in range(2):
        _run_due(lambda job, wake: Outcome("failure", "exit_1"))
        import time

        time.sleep(1.05)
    assert store.load(home)["j"]["consecutive_failures"] == 2
    _run_due(lambda job, wake: Outcome("success", "exit_0"))
    assert store.load(home)["j"]["consecutive_failures"] == 0


# ---------------------------------------------------------- reconcile


def test_reconcile_closes_dead_orphan_and_does_not_refire(home):
    assert _add("once") == 0
    ts = _write_started(home, "w-orphan02", "once", timedelta(minutes=30), pid=1, timeout_s=60)
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 1 and fired == []
    job = store.load(home)["once"]
    assert job["last_state"] == "orphaned" and job["last_started_at"] == ts
    assert job["consecutive_failures"] == 1 and job["last_wake_id"] == "w-orphan02"
    rows = [r for r in ledger.read(home) if r.get("wake_id") == "w-orphan02"]
    assert [r["event"] for r in rows] == ["started", "finished"]
    assert rows[1]["state"] == "orphaned"
    assert rows[1]["reason"] in ("pid_gone", "age_exceeded_120s")
    assert any(
        r["event"] == "reconciled" and r["reason"] == "closed_1_orphaned" for r in ledger.read(home)
    )


def test_reconcile_leaves_a_live_wake_alone(home):
    assert _add("live") == 0
    _write_started(home, "w-live0001", "live", timedelta(seconds=5), pid=os.getpid())
    jobs, closed, in_progress = cli.reconcile(home, store.load(home), "p-test0001", "test")
    assert closed == [] and in_progress == ["w-live0001"]
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="live")) == 1, (
        "remove refuses while a wake is in progress"
    )


def test_reconcile_closes_a_live_pid_past_the_age_bound(home):
    assert _add("stuck") == 0
    _write_started(
        home, "w-stuck001", "stuck", timedelta(seconds=400), pid=os.getpid(), timeout_s=300
    )
    _jobs, closed, _live = cli.reconcile(home, store.load(home), "p-test0002", "test")
    assert closed == ["w-stuck001"]
    row = [r for r in ledger.read(home) if r["event"] == "finished"][-1]
    assert row["reason"] == "age_exceeded_360s"


def test_ledger_write_failure_after_exec_exits_2_and_is_closed_next_pass(home, monkeypatch):
    assert _add("j") == 0
    real_append = ledger.append
    calls = {"n": 0}

    def flaky(base, row):
        if row.get("event") == "finished":
            calls["n"] += 1
            raise OSError("disk full")
        return real_append(base, row)

    monkeypatch.setattr(ledger, "append", flaky)
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0", exit_code=0))
    assert rc == 2 and fired == [1]
    assert store.load(home)["j"]["last_started_at"] is None, "no stamp without the row"
    monkeypatch.undo()
    monkeypatch.setenv("AWRISE_HOME", str(home))
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 1 and fired == [1], "closed as orphaned, not fired again"
    row = [r for r in ledger.read(home) if r["event"] == "finished"][-1]
    assert row["state"] == "orphaned" and row["reason"] == "ledger_write_failed"
    assert row["recovered_state"] == "success" and row["recovered_exit_code"] == 0


def test_reconcile_never_copies_an_unreadable_ts_into_the_store(home):
    assert _add("j") == 0
    row = {
        "schema": 1,
        "wake_id": "w-badts001",
        "pass_id": "p-x",
        "ts": "not-a-timestamp",
        "invoker": "t",
        "host": "h",
        "pid": 1,
        "interpreter": "x",
        "job": "j",
        "event": "started",
        "state": None,
        "reason": "due",
        "timeout_s": 300,
    }
    day = ledger.day_file(home, clock.now_utc())
    day.parent.mkdir(exist_ok=True)
    with open(day, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 1
    closed = _finished(home)[-1]
    # pid 1 is absent on Windows and init (EPERM = alive) on POSIX
    assert closed["state"] == "orphaned" and closed["reason"] in ("pid_gone", "ts_unreadable")
    assert closed["orphan_age_s"] is None
    assert "Infinity" not in day.read_text(encoding="utf-8") and "NaN" not in day.read_text(
        encoding="utf-8"
    )
    job = store.load(home)["j"]
    assert job["last_started_at"] is None, "garbage never becomes a stamp"
    assert job["last_state"] == "orphaned"
    # every later verb still answers with a verdict, not a traceback
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) in (0, 1)
    # an unreadable ts on a wake whose pid is alive is closed too, and says why
    alive = dict(row, wake_id="w-badts002", pid=os.getpid())
    with open(day, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(alive) + "\n")
    _jobs, closed_ids, live = cli.reconcile(home, store.load(home), "p-t", "t")
    assert closed_ids == ["w-badts002"] and live == []
    assert _finished(home)[-1]["reason"] == "ts_unreadable"


def test_ledger_writer_refuses_non_finite_numbers_as_null(home):
    wake = ledger.append(
        home,
        {
            "event": "finished",
            "state": "orphaned",
            "job": "j",
            "reason": "x",
            "orphan_age_s": float("inf"),
            "nested": {"n": float("nan")},
        },
    )
    row = [r for r in ledger.read(home) if r["wake_id"] == wake][0]
    assert row["orphan_age_s"] is None and row["nested"] == {"n": None}
    raw = next(ledger.ledger_dir(home).glob("*.jsonl")).read_text(encoding="utf-8")
    assert "Infinity" not in raw and "NaN" not in raw
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(
            home,
            {"event": "finished", "state": "success", "job": "j", "reason": "x", "blob": object()},
        )


def test_reconcile_verb_exit_codes(home):
    assert _add("j") == 0
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 0
    _write_started(home, "w-orphan03", "j", timedelta(hours=2))
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 1
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 0


# ---------------------------------------------------------------- verbs


def test_run_name_ignores_dueness_but_not_enabled_without_force(home):
    assert _add("j") == 0
    fired = []
    exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
    assert _run_due(exe) == 0
    assert _run_due(exe) == 0 and len(fired) == 1
    run = lambda force: cli._dispatch(  # noqa: E731
        lambda a: cli.cmd_run(a, executor=exe),
        argparse.Namespace(name="j", force=force),
    )
    assert run(False) == 0 and len(fired) == 2
    assert cli._dispatch(cli.cmd_disable, argparse.Namespace(name="j")) == 0
    assert run(False) == 1 and len(fired) == 2
    assert run(True) == 0 and len(fired) == 3
    assert [r["reason"] for r in ledger.read(home) if r["event"] == "started"] == [
        "due",
        "manual",
        "forced",
    ]
    assert (
        cli._dispatch(
            lambda a: cli.cmd_run(a, executor=exe), argparse.Namespace(name="nope", force=False)
        )
        == 1
    )


def test_set_validates_keys_and_values(home):
    assert _add("j") == 0
    ok = lambda *a, **k: cli._dispatch(  # noqa: E731
        cli.cmd_set,
        argparse.Namespace(
            name="j", assignments=list(a), allow_overrun=k.get("allow_overrun", False)
        ),
    )
    assert ok("colour=red") == 1
    assert ok("every=2h") == 0 and store.load(home)["j"]["interval_s"] == 7200.0
    assert ok("every=2x") == 1
    assert ok("run=") == 1
    assert ok("timeout_s=7200") == 1
    assert ok("timeout_s=7200", allow_overrun=True) == 0
    assert ok("timeout_s=abc") == 1
    assert ok("enabled=false") == 0 and store.load(home)["j"]["enabled"] is False
    assert ok("enabled=maybe") == 1
    assert ok("at=07:00") == 0 and store.load(home)["j"]["at"] == "07:00"
    assert ok("at=25:00") == 1
    assert ok("at=null") == 0 and store.load(home)["j"]["at"] is None
    assert ok("cwd=/tmp") == 0 and store.load(home)["j"]["cwd"] == "/tmp"
    assert ok() == 1
    assert ok("noequals") == 1
    assert (
        cli._dispatch(
            cli.cmd_set,
            argparse.Namespace(name="ghost", assignments=["every=1h"], allow_overrun=False),
        )
        == 1
    )


@pytest.mark.parametrize(
    "name",
    [
        "..",
        ".",
        "sub/dir",
        "sub\\dir",
        "C:\\x",
        "CON",
        "con.txt",
        "com1",
        "LPT9",
        "a\tb",
        "a b",
        " ",
        "-lead",
        ".hidden",
        "trail.",
        "x" * 65,
        "caf\u00e9",
        "\u65e5\u672c\u8a9e",
        "a:b",
        "a*b",
        "a?b",
        "a|b",
        "a<b",
        'a"b',
    ],
)
def test_add_refuses_names_outside_the_grammar(home, name, capsys):
    assert _add(name) == 1, name
    assert store.load(home) == {}
    err = capsys.readouterr().err
    assert "Error: job name" in err or (not name.strip() and "required" in err), err
    with pytest.raises(ValueError):
        store.validate_name(name)


def test_add_accepts_directory_safe_names_and_every_verb_strips(home):
    for name in ("a", "A-1", "fleet_gates", "v2.backup", "x" * 64, "0day", "CONS", "comX"):
        assert _add(name) == 0, name
        assert store.validate_name(name) == name
    assert _add(" lead ") == 0 and "lead" in store.load(home)
    assert cli._dispatch(cli.cmd_disable, argparse.Namespace(name=" lead")) == 0
    assert cli._dispatch(cli.cmd_enable, argparse.Namespace(name="lead ")) == 0
    assert (
        cli._dispatch(
            cli.cmd_set,
            argparse.Namespace(name=" lead", assignments=["every=2h"], allow_overrun=False),
        )
        == 0
    )
    assert (
        cli._dispatch(
            lambda a: cli.cmd_run(a, executor=lambda j, w: Outcome("success", "ok")),
            argparse.Namespace(name=" lead ", force=False),
        )
        == 0
    )
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name=" lead")) == 0
    assert "lead" not in store.load(home)


def test_add_refuses_empty_duplicate_overrun_and_bad_interval(home):
    assert _add("e", run="  ") == 1 and store.load(home) == {}
    assert _add("d") == 0 and _add("d") == 1
    assert _add("t", every="1m", timeout=60) == 1
    assert _add("t", every="1m", timeout=60, allow_overrun=True) == 0
    assert _add("x", every="15x") == 1
    assert _add("a", at="7:30") == 0 and store.load(home)["a"]["at"] == "7:30"
    assert _add("b", at="24:00") == 1
    assert _add("c", disabled=True) == 0 and store.load(home)["c"]["enabled"] is False


def test_enable_disable_remove_write_rows_and_refuse_unknown(home):
    assert cli._dispatch(cli.cmd_enable, argparse.Namespace(name="nope")) == 1
    assert _add("j") == 0
    assert cli._dispatch(cli.cmd_disable, argparse.Namespace(name="j")) == 0
    assert store.load(home)["j"]["enabled"] is False
    assert cli._dispatch(cli.cmd_enable, argparse.Namespace(name="j")) == 0
    assert store.load(home)["j"]["enabled"] is True
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="j")) == 0
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="j")) == 1
    rows = ledger.read(home)
    assert [r["event"] for r in rows] == ["removed"] and rows[0]["job"] == "j"


def test_cwd_missing_is_an_error_row(home):
    assert _add("j", cwd=str(home / "gone")) == 0
    assert _run_due() == 1
    row = _finished(home)[-1]
    assert row["state"] == "error" and row["reason"].startswith("cwd_missing:")


# --------------------------------------------------------- history/status


def test_history_lists_rows_and_filters(home, capsys):
    assert _add("j") == 0
    _run_due(lambda job, wake: Outcome("success", "exit_0"))
    hist = lambda **kw: cli._dispatch(  # noqa: E731
        cli.cmd_history,
        argparse.Namespace(
            **{"job": None, "since": None, "event": None, "limit": 50, "json": False, **kw}
        ),
    )
    assert hist() == 0
    out = capsys.readouterr().out
    assert "started" in out and "finished" in out and "success" in out
    assert hist(json=True) == 0
    out = capsys.readouterr().out
    lines = [json.loads(line) for line in out.splitlines() if line.strip()]
    assert _wake_events(lines) == ["started", "finished"]
    assert hist(job="ghost") == 0
    assert "no wakes recorded" in capsys.readouterr().out
    assert hist(since="bogus") == 1
    assert hist(since="1d", limit=1) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == 3


def test_status_is_unjudged_per_job_not_per_ledger(home, capsys):
    """A row from ANOTHER job says nothing about this one: a job that never
    woke is UNJUDGED (exit 2) however busy the ledger is."""
    assert _add("a") == 0
    assert _add("b") == 0
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="b")) == 0
    assert ledger.read(home), "the removed row is in the ledger"
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
    out = capsys.readouterr().out
    assert "UNJUDGED: a never woke" in out and "OK:" not in out
    assert _add("c") == 0
    _run_due(lambda job, wake: Outcome("success", "exit_0"))
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0, "both woke: judged"
    assert _add("d") == 0
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2, "a new job is unjudged"
    assert "UNJUDGED: d never woke" in capsys.readouterr().out
    # a failure elsewhere outranks a pending job: 1, never 2
    cli._dispatch(
        lambda a: cli.cmd_run(a, executor=lambda j, w: Outcome("failure", "exit_1")),
        argparse.Namespace(name="a", force=False),
    )
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1
    assert "NOT OK: a" in capsys.readouterr().out


def test_status_is_unjudged_without_rows_then_judges(home, capsys):
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0, "no jobs: nothing to judge"
    assert _add("j") == 0
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
    assert "UNJUDGED" in capsys.readouterr().out
    _run_due(lambda job, wake: Outcome("failure", "exit_1"))
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1
    assert "NOT OK: j" in capsys.readouterr().out
    cli._dispatch(
        lambda a: cli.cmd_run(a, executor=lambda j, w: Outcome("success", "exit_0")),
        argparse.Namespace(name="j", force=False),
    )
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0
    assert "OK:" in capsys.readouterr().out
    _write_started(home, "w-stale001", "j", timedelta(hours=1))
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1
    assert "never finished" in capsys.readouterr().out


def test_list_shows_every_as_entered(home, capsys):
    assert _add("j", every="1h30m") == 0
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 0
    assert "1h30m" in capsys.readouterr().out
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=True)) == 0
    assert json.loads(capsys.readouterr().out)["j"]["interval_s"] == 5400.0


def test_main_parses_every_verb(home, capsys):
    assert cli.main(["add", "--name", "j", "--every", "1h", "--run", "echo hi"]) == 0
    assert cli.main(["list"]) == 0
    assert cli.main(["set", "--name", "j", "every=2h"]) == 0
    assert cli.main(["disable", "--name", "j"]) == 0
    assert cli.main(["enable", "--name", "j"]) == 0
    assert cli.main(["status"]) == 2
    assert cli.main(["history"]) == 0
    assert cli.main(["reconcile"]) == 0
    assert cli.main(["run", "--name", "j"]) in (0, 1)
    assert cli.main(["run-due", "--quiet", "--invoker", "cron"]) == 0
    assert cli.main(["remove", "--name", "j"]) == 0
    assert cli.main([]) == 0
    assert cli.main(["--self-test", "--list"]) == 0
    names = capsys.readouterr().out.splitlines()
    assert "store_crash_mid_save_keeps_jobs" in names


# ---------------------------------------------------- the honest memory (S2)


def test_healthy_store_is_byte_stable_across_passes(home):
    """add + run-due + run-due: jobs.json unchanged byte-for-byte and zero
    reconciled rows. Before this held, every pass after a wake wrote a false
    'recovered_1_stamps' row (two clock reads compared as one)."""
    assert _add("j") == 0
    fired = []
    exe = lambda job, wake, s="success": fired.append(1) or Outcome(s, "exit_0")  # noqa: E731
    assert _run_due(exe) == 0 and fired == [1]
    after_first = (home / "jobs.json").read_bytes()
    for _ in range(3):
        assert _run_due(exe) == 0
    assert fired == [1]
    assert (home / "jobs.json").read_bytes() == after_first
    assert not [r for r in ledger.read(home) if r["event"] == "reconciled"]
    started = [r for r in ledger.read(home) if r["event"] == "started"]
    job = store.load(home)["j"]
    assert job["last_started_at"] == started[0]["ts"], "one value in the row and the store"
    assert job["last_finished_at"] == _finished(home)[0]["ts"]
    # the failing twin: a failed wake is counted ONCE, not again by a false recovery
    assert _add("k", every="1s", timeout=1, allow_overrun=True) == 0
    assert _run_due(lambda job, wake: Outcome("failure", "exit_1")) == 1
    assert store.load(home)["k"]["consecutive_failures"] == 1
    import time

    time.sleep(1.1)
    assert _run_due(lambda job, wake: Outcome("failure", "exit_1")) == 1
    assert store.load(home)["k"]["consecutive_failures"] == 2, "no double count"
    assert not [r for r in ledger.read(home) if r["event"] == "reconciled"]


def test_clock_skew_writes_an_error_row_and_fires(home):
    """A stamp in the future (the clock went backwards) is an error row, then
    the job is due; the fire re-stamps with the measured clock, so the next
    pass is quiet."""
    assert _add("j") == 0
    jobs = store.load(home)
    future = clock.iso(clock.now_utc() + timedelta(hours=1))
    jobs["j"]["last_started_at"] = future
    jobs["j"]["last_wake_id"] = "w-future01"
    jobs["j"]["last_state"] = "success"
    store.save(jobs, home)
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 1 and fired == [1]
    rows = _finished(home)
    assert [r["state"] for r in rows] == ["error", "success"]
    assert rows[0]["reason"].startswith("clock_skew:") and rows[0]["reason"].endswith("s")
    skew = float(rows[0]["reason"][len("clock_skew:") : -1])
    assert 3500 < skew <= 3600
    job = store.load(home)["j"]
    assert job["last_started_at"] < future, "the fire stamped the measured clock, not the future"
    assert job["last_state"] == "success" and job["consecutive_failures"] == 0
    # quiet twin: the next pass writes nothing
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
    assert fired == [1] and len(_finished(home)) == 2
    # a stamp inside the tolerance is jitter, not skew
    jobs = store.load(home)
    jobs["j"]["last_started_at"] = clock.iso(clock.now_utc() + timedelta(milliseconds=500))
    store.save(jobs, home)
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0")) == 0
    assert fired == [1] and len(_finished(home)) == 2
    assert clock.skew_s({"last_started_at": None}, clock.now_utc()) is None


def test_clock_skew_on_a_disabled_job_heals_in_one_pass(home):
    """The same future stamp on a job that is NOT allowed to fire. Nothing
    runs, so nothing else can correct the stamp -- the skew row itself must,
    or every pass until the calendar catches up writes the same error and
    counts another failure."""
    assert _add("j", disabled=True) == 0
    jobs = store.load(home)
    future = clock.iso(clock.now_utc() + timedelta(hours=1))
    jobs["j"]["last_started_at"] = future
    store.save(jobs, home)
    fired = []
    exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
    assert _run_due(exe) == 1
    assert [r["state"] for r in _finished(home)] == ["error", "skipped_disabled"]
    job = store.load(home)["j"]
    assert job["last_started_at"] < future, "the skew row stamped the measured clock"
    assert job["consecutive_failures"] == 1
    # the heal is what the next passes prove: quiet, exit 0, no new rows
    assert _run_due(exe) == 0 and _run_due(exe) == 0
    assert fired == [] and len(_finished(home)) == 2
    assert store.load(home)["j"]["consecutive_failures"] == 1


def _append_row(home, row):
    path = ledger.day_file(home, clock.now_utc())
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")


@pytest.mark.parametrize(
    "field,value",
    [
        ("ts", 1700000000),
        ("wake_id", {"a": 1}),
        ("job", ["j"]),
        ("pass_id", 7),
        ("state", ["success"]),
    ],
)
def test_a_foreign_ledger_row_does_not_wedge_every_later_pass(home, field, value):
    """One hand-written or foreign line with a typed field used to end every
    later pass in a traceback: no job on the host fired again. It is reported
    as an unreadable row -- the shape a line of garbage already had -- and the
    pass judges everything else."""
    assert _add("j") == 0
    row = {
        "schema": 1,
        "wake_id": "w-foreign1",
        "pass_id": "p-foreign",
        "job": "j",
        "ts": clock.iso(clock.now_utc()),
        "invoker": "t",
        "host": "h",
        "pid": 1,
        "event": "finished",
        "state": "success",
        "reason": "exit_0",
    }
    row[field] = value
    _append_row(home, row)
    fired = []
    exe = lambda job, wake: fired.append(1) or Outcome("success", "exit_0")  # noqa: E731
    assert _run_due(exe) == 0, "a foreign row stopped the pass"
    assert fired == [1], "the job still fires"
    assert _run_due(exe) == 0 and fired == [1]
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="j")) == 0
    unreadable = [r for r in ledger.read(home) if r.get("event") == "unreadable"]
    assert len(unreadable) == 1, "the line is reported, never silently dropped"
    assert field in unreadable[0]["reason"] and "non-string" in unreadable[0]["reason"]


def test_a_foreign_started_row_is_not_closed_as_an_orphan(home):
    """A ``started`` line whose wake id is not a string names no wake: it
    cannot be paired, so it must not be reported as an orphan either."""
    assert _add("j") == 0
    _append_row(
        home,
        {
            "schema": 1,
            "wake_id": {"x": 1},
            "pass_id": "p-x",
            "job": "j",
            "ts": clock.iso(clock.now_utc()),
            "invoker": "t",
            "host": "h",
            "pid": 1,
            "event": "started",
            "state": None,
            "reason": "due",
        },
    )
    assert _run_due(lambda job, wake: Outcome("success", "exit_0")) == 0
    assert [r["state"] for r in _finished(home)] == ["success"]
    assert not [r for r in ledger.read(home) if r.get("event") == "reconciled"]


def test_removed_job_rows_never_stamp_a_new_job_with_the_same_name(home):
    """remove g, add g (a NEW spec): the next pass fires the new job instead of
    copying the removed job's last wake into it."""
    assert _add("g", every="1d") == 0
    assert _run_due(lambda job, wake: Outcome("failure", "exit_7")) == 1
    assert store.load(home)["g"]["consecutive_failures"] == 1
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="g")) == 0
    import time

    time.sleep(0.02)
    assert _add("g", every="1d", run="echo new") == 0
    fired = []
    rc = _run_due(lambda job, wake: fired.append(1) or Outcome("success", "exit_0"))
    assert rc == 0 and fired == [1], "the new job fires; nothing is 'recovered'"
    job = store.load(home)["g"]
    assert job["last_state"] == "success" and job["consecutive_failures"] == 0
    assert job["last_reason"] == "exit_0"
    assert not [r for r in ledger.read(home) if r["event"] == "reconciled"]
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 0


def test_garbage_pid_in_a_started_row_is_a_verdict_not_a_traceback(home):
    assert _add("j") == 0
    row = {
        "schema": 1,
        "wake_id": "w-badpid01",
        "pass_id": "p-x",
        "ts": clock.iso(clock.now_utc() - timedelta(minutes=5)),
        "invoker": "t",
        "host": "h",
        "pid": "not-a-pid",
        "interpreter": "x",
        "job": "j",
        "event": "started",
        "state": None,
        "reason": "due",
        "timeout_s": 300,
    }
    day = ledger.day_file(home, clock.now_utc())
    day.parent.mkdir(exist_ok=True)
    with open(day, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 1
    closed = _finished(home)[-1]
    assert closed["state"] == "orphaned" and closed["reason"] == "pid_gone"
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False)) == 0
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) in (0, 1)
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1


def test_run_name_respects_the_lock_but_not_the_recheck(home):
    from awrise import lock

    assert _add("j") == 0
    handle = lock.acquire(home, "j", {"wake_id": "w-hold0001", "pass_id": "p-h"}, 300)
    fired = []
    try:
        rc = cli._dispatch(
            lambda a: cli.cmd_run(
                a, executor=lambda j, w: fired.append(1) or Outcome("success", "exit_0")
            ),
            argparse.Namespace(name="j", force=False),
        )
    finally:
        handle.release()
    assert rc == 0 and fired == []
    assert _finished(home)[-1]["state"] == "skipped_overlap"
    assert not (home / "locks" / "j").exists()
