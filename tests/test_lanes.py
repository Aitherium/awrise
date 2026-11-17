"""The critical lane: a pager wake never waits behind a slow normal wake.

Measured 2026-11-15 on the Windows host: 853 passes in 2.5 days, p50 26 s, p90
6 min, p99 43 min, max 72 min -- one serial pass behind the host clock's single
Task Scheduler instance (``MultipleInstances=IgnoreNew``, shim waits), so the
2 min outage pager, the 5 min condition pager and the 1 min incident reflex all
started 4 to 45 minutes apart. These tests are the same shape in miniature: a
normal wake that takes seconds while the critical pass has to dispatch on time.
"""

import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from awrise import cli, hostclock, ledger, lock, store
from awrise.executors import Outcome

PY = sys.executable
PKG = Path(__file__).resolve().parent.parent


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


def _add(name, run="echo hi", every="1h", **extra):
    args = argparse.Namespace(
        name=name, every=every, run=run, timeout=None, cwd=None, at=None,
        allow_overrun=False, disabled=False, detach=False,
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _run_due(executor, invoker="test", **extra):
    ns = argparse.Namespace(quiet=True, invoker=invoker)
    for key, value in extra.items():
        setattr(ns, key, value)
    return cli._dispatch(lambda a: cli.cmd_run_due(a, executor=executor), ns)


def _started(home):
    return [r["job"] for r in ledger.read(home) if r["event"] == "started"]


def _ok(job, wake):
    return Outcome("detached" if job.get("detach") else "success", "stub")


# ---------------------------------------------------------------- the store


def test_a_job_is_normal_lane_unless_it_says_otherwise(home):
    assert _add("plain") == 0
    assert store.load(home)["plain"]["lane"] == "normal"


def test_a_critical_job_must_detach_and_must_not_wake_a_unit(home):
    assert _add("pager", lane="critical") == 1, "critical without detach is refused"
    assert _add("pager", lane="critical", detach=True, wake="x.service") == 1
    assert _add("pager", lane="critical", detach=True) == 0
    assert store.load(home)["pager"]["lane"] == "critical"
    assert _add("bad", lane="urgent") == 1
    with pytest.raises(store.StoreError):
        jobs = store.load(home)
        jobs["pager"]["detach"] = False
        store.validate_record("pager", jobs["pager"])


def test_set_lane_round_trips(home):
    assert _add("p", detach=True) == 0
    ns = argparse.Namespace(name="p", assignments=["lane=critical"])
    assert cli._dispatch(cli.cmd_set, ns) == 0
    assert store.load(home)["p"]["lane"] == "critical"


# ----------------------------------------------------------- one lane each


def test_the_normal_pass_skips_critical_jobs_and_the_critical_pass_skips_normal(home):
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0
    assert _run_due(_ok, lane="normal") == 0
    assert _started(home) == ["chore"]
    assert _run_due(_ok, lane="critical") == 0
    assert _started(home) == ["chore", "pager"]


def test_with_no_critical_job_all_is_the_one_historical_pass(home):
    assert _add("a") == 0
    assert _add("b") == 0
    assert _run_due(_ok) == 0
    rows = ledger.read(home)
    assert [r["event"] for r in rows if r["event"] in ("tick", "tick_end")] == ["tick", "tick_end"]
    assert sorted(_started(home)) == ["a", "b"]
    assert "lane" not in [r for r in rows if r["event"] == "tick"][0]


def test_all_runs_critical_then_normal_inline_with_one_tick(home):
    assert _add("chore") == 0
    assert _add("pager", detach=True, lane="critical") == 0
    assert _run_due(_ok, lane="all") == 0
    assert _started(home) == ["pager", "chore"], "the pager is dispatched FIRST"
    ticks = [r for r in ledger.read(home) if r["event"] == "tick"]
    ends = [r for r in ledger.read(home) if r["event"] == "tick_end"]
    assert len(ticks) == 1 and ticks[0]["lane"] == "critical"
    assert sorted(r["lane"] for r in ends) == ["critical", "normal"]


# ------------------------------------------- the point: no wait behind normal


def test_a_critical_pass_runs_while_a_normal_pass_holds_its_lock(home):
    """A normal pass 40 minutes into a slow inline wake used to refuse every later
    firing at the pass lock. The critical pass has its own lock and runs anyway."""
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0
    held = lock.acquire_pass(home, {"pass_id": "p-normal"}, timeout_s=600, lane="normal")
    assert not isinstance(held, lock.Held)
    try:
        assert _run_due(_ok, lane="critical") == 0
        assert _started(home) == ["pager"]
        ends = [r for r in ledger.read(home) if r["event"] == "tick_end"]
        assert ends[-1]["reason"] == "pass_end" and ends[-1]["lane"] == "critical"
        # ... while a second NORMAL pass is still refused: that rule is unchanged.
        assert _run_due(_ok, lane="normal") == 0
        refused = [r for r in ledger.read(home) if r.get("reason") == "pass_refused_overlap"]
        assert len(refused) == 1 and refused[0]["lane"] == "normal"
    finally:
        held.release()
    assert not (home / lock.CRITICAL_PASS_LOCK_NAME).exists()
    assert not (home / lock.PASS_LOCK_NAME).exists()


def test_a_slow_normal_wake_cannot_delay_a_critical_dispatch(home):
    """Two real passes on two threads: the normal one is inside a 3 s wake; the
    critical one starts 0.5 s later and must dispatch the pager before it ends."""
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("slow") == 0
    seen = {}
    release = threading.Event()

    def executor(job, wake):
        if job["run"] == "echo hi" and not job.get("detach"):
            seen["slow_started"] = time.monotonic()
            release.wait(10)
            seen["slow_done"] = time.monotonic()
        else:
            seen["pager_at"] = time.monotonic()
        return _ok(job, wake)

    normal = threading.Thread(target=_run_due, args=(executor,), kwargs={"lane": "normal"})
    normal.start()
    deadline = time.monotonic() + 10
    while "slow_started" not in seen and time.monotonic() < deadline:
        time.sleep(0.02)
    assert "slow_started" in seen
    assert _run_due(executor, lane="critical") == 0
    assert "pager_at" in seen and "slow_done" not in seen, "pager ran while the slow wake held"
    release.set()
    normal.join(15)
    assert seen["pager_at"] < seen["slow_done"]


# --------------------------------------------- the host clock frees the clock


def test_a_host_clock_firing_hands_the_normal_lane_to_a_detached_child(home, monkeypatch):
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0
    spawned = []
    monkeypatch.setattr(
        cli, "_spawn_normal_lane", lambda base, args, gap: spawned.append(gap) or 4242
    )
    assert _run_due(_ok, invoker="schtasks") == 0
    assert _started(home) == ["pager"], "the firing ran only the critical lane"
    assert len(spawned) == 1
    # a normal pass already in flight: do not spawn a second child, do not write refusals
    held = lock.acquire_pass(home, {"pass_id": "p-n"}, timeout_s=600, lane="normal")
    try:
        assert _run_due(_ok, invoker="schtasks") == 0
    finally:
        held.release()
    assert len(spawned) == 1
    assert not [r for r in ledger.read(home) if r.get("reason") == "pass_refused_overlap"]


def test_a_failed_spawn_falls_back_to_running_the_normal_lane_inline(home, monkeypatch):
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0
    monkeypatch.setattr(cli, "_spawn_normal_lane", lambda base, args, gap: None)
    assert _run_due(_ok, invoker="schtasks") == 0
    assert _started(home) == ["pager", "chore"]


def test_a_person_running_run_due_by_hand_never_detaches(home, monkeypatch):
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0

    def boom(*a, **k):
        raise AssertionError("a manual pass must not spawn")

    monkeypatch.setattr(cli, "_spawn_normal_lane", boom)
    assert _run_due(_ok, invoker="manual") == 0
    assert _started(home) == ["pager", "chore"]


def test_the_spawned_normal_lane_reuses_the_clocks_gap_and_writes_no_tick(home):
    assert _add("chore") == 0
    assert _run_due(_ok, lane="normal", clock_gap="75.5") == 0
    rows = ledger.read(home)
    assert not [r for r in rows if r["event"] == "tick"]
    end = [r for r in rows if r["event"] == "tick_end"][0]
    assert end["gap_s"] == 75.5 and end["lane"] == "normal"


def test_the_real_spawn_outlives_the_parent_and_runs_the_normal_lane(home):
    """The process the host clock really starts, end to end: critical pass inline,
    normal pass in a child that finishes after the parent has returned."""
    marker = home / "chore-ran.txt"
    code = f"open(r'{marker}', 'w').write('x')"
    assert _add("chore", run=f'"{PY}" -c "{code}"') == 0
    assert _add("pager", run=f'"{PY}" -c "pass"', detach=True, lane="critical") == 0
    env = {"AWRISE_HOME": str(home), "PATH": __import__("os").environ.get("PATH", ""),
           "SYSTEMROOT": __import__("os").environ.get("SYSTEMROOT", "")}
    parent = subprocess.run(
        [PY, "-m", "awrise", "run-due", "--quiet", "--invoker", "schtasks"],
        cwd=str(PKG), env=env, capture_output=True, timeout=600,
    )
    assert parent.returncode == 0, parent.stderr
    deadline = time.monotonic() + 600
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.2)
    assert marker.exists(), "the detached normal lane ran the chore"
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        ends = [r for r in ledger.read(home) if r["event"] == "tick_end"]
        if len(ends) >= 2:  # the child writes its tick_end after the chore, not before
            break
        time.sleep(0.2)
    rows = ledger.read(home)
    assert len([r for r in rows if r["event"] == "tick"]) == 1
    assert sorted(r["lane"] for r in rows if r["event"] == "tick_end") == ["critical", "normal"]
    assert hostclock.last_tick(home, hostclock.SCHEDULED_INVOKERS) is not None


def test_a_critical_pass_is_bounded_by_its_own_jobs_not_the_whole_stores(home):
    assert _add("chore", timeout=3600, allow_overrun=True) == 0
    assert _add("pager", detach=True, lane="critical", timeout=110, every="5m") == 0
    assert cli._pass_bound_s(home, "critical") == 300.0, "the floor, not the 3600 s chore"
    assert cli._pass_bound_s(home, "normal") == 3600.0


# ------------------------------------------------- priority and the cheap tick read


def test_last_tick_reads_the_newest_tick_not_the_whole_ledger(home):
    from datetime import timedelta

    from awrise import clock

    old = clock.now_utc() - timedelta(days=1)
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "manual",
                         "pass_id": "p-old", "ts": clock.iso(old)})
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "schtasks",
                         "pass_id": "p-mid", "ts": clock.iso(clock.now_utc())})
    ledger.append(home, {"event": "tick_end", "reason": "pass_end", "invoker": "schtasks",
                         "pass_id": "p-mid", "ts": clock.iso(clock.now_utc())})
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "manual",
                         "pass_id": "p-new", "ts": clock.iso(clock.now_utc())})
    assert hostclock.last_tick(home)["pass_id"] == "p-new"
    assert hostclock.last_tick(home, hostclock.SCHEDULED_INVOKERS)["pass_id"] == "p-mid"
    assert hostclock.last_tick(home, frozenset({"nobody"})) is None


@pytest.mark.skipif(sys.platform != "win32", reason="Task Scheduler priorities are Windows")
def test_the_critical_lane_raises_a_low_io_pass_and_the_normal_lane_lowers_it_back():
    """Task Scheduler starts the pass at Low I/O (measured: 57 KB/s ledger reads)."""
    code = "; ".join(
        [
            "from awrise import procprio as p",
            "assert p._set(p.BELOW_NORMAL_PRIORITY_CLASS, p.IO_LOW) and p.io_priority() == 1",
            "assert p.apply('critical') and p.io_priority() == 2",
            "assert p.cpu_class() == p.NORMAL_PRIORITY_CLASS",
            "assert p.apply('normal') and p.io_priority() == 1",
            "assert p.cpu_class() == p.BELOW_NORMAL_PRIORITY_CLASS",
            "assert not p.apply('all')",
            "print('ok')",
        ]
    )
    out = subprocess.run(
        [PY, "-c", code], cwd=str(PKG), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=60,
    )
    assert out.returncode == 0 and out.stdout.strip() == "ok", out.stderr


def test_a_scheduled_firing_applies_the_lane_priority_a_manual_one_does_not(home, monkeypatch):
    calls = []
    monkeypatch.setattr(cli.procprio, "apply", lambda lane: calls.append(lane) or True)
    assert _add("pager", detach=True, lane="critical") == 0
    assert _add("chore") == 0
    assert _run_due(_ok, invoker="manual") == 0
    assert calls == []
    monkeypatch.setattr(cli, "_spawn_normal_lane", lambda *a: 1)
    assert _run_due(_ok, invoker="schtasks") == 0
    assert calls == ["critical"]
    calls.clear()
    assert _run_due(_ok, invoker="schtasks", lane="normal", clock_gap="1.0") == 0
    assert calls == ["normal"]


def test_a_normal_lane_is_not_written_so_an_older_awrise_can_still_read_the_store(home):
    assert _add("chore") == 0
    assert _add("pager", detach=True, lane="critical") == 0
    raw = json.loads((home / "jobs.json").read_text("utf-8"))["jobs"]
    assert "lane" not in raw["chore"] and raw["pager"]["lane"] == "critical"
    assert store.load(home)["chore"]["lane"] == "normal"


def test_reconcile_reads_days_for_the_longest_timeout_not_a_flat_thirty(home):
    assert cli._recent_days({"a": {"timeout_s": 300}}) == 3
    assert cli._recent_days({"a": {"timeout_s": 10800}}) == 3
    assert cli._recent_days({"a": {"timeout_s": 3 * 86400}}) == 6
    assert cli._recent_days({"a": {"timeout_s": 4_000_000}}) == 30
    assert cli._recent_days({}) == 3
    assert cli._recent_days({"a": "junk", "b": {"timeout_s": None}}) == 3


def test_a_critical_job_one_clock_jitter_early_still_runs_a_normal_one_does_not(home):
    from datetime import timedelta

    from awrise import clock

    now = clock.now_utc()
    clock.set_now(lambda: now)  # frozen: a slow host must not make the chore due
    try:
        _jitter_body(home, now, timedelta, clock)
    finally:
        clock.reset_now()


def _jitter_body(home, now, timedelta, clock):
    stamp = clock.iso(now - timedelta(seconds=52))  # 8 s short of its 60 s interval
    assert _add("pager", detach=True, lane="critical", every="1m", timeout=30) == 0
    assert _add("chore", every="1m", timeout=30) == 0
    jobs = store.load(home)
    for name in ("pager", "chore"):
        jobs[name]["last_started_at"] = stamp
    store.save(jobs, home)
    assert _run_due(_ok, lane="critical") == 0
    assert _started(home) == ["pager"]
    assert _run_due(_ok, lane="normal") == 0
    assert _started(home) == ["pager"], "a normal job keeps its exact due time"


def test_the_installed_task_runs_at_normal_priority_not_the_schedulers_low_io_default():
    assert "-Priority 4" in " ".join(hostclock.battery_settings_command("awrise"))
