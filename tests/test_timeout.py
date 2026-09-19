"""No double fire, no silent kill: real processes, real clocks.

These tests spawn ``python -m awrise`` and real children. They would FAIL on
0.1.0 semantics (two passes both fire; a timeout kills only the shell and
leaves the grandchild; a long child holds the pass until it exits).
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest
from awrise import cli, clock, executors, ledger, lock, store
from awrise.executors import Outcome

PKG = Path(__file__).resolve().parent.parent
PY = sys.executable


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


def _env(home):
    return {**os.environ, "AWRISE_HOME": str(home)}


def _awrise(home, *argv, **kw):
    return subprocess.Popen(
        [PY, "-m", "awrise", *argv],
        cwd=str(PKG),
        env=_env(home),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **kw,
    )


def _add(name, run, every="1h", **extra):
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


def _finished(home):
    return [r for r in ledger.read(home) if r["event"] == "finished"]


#: The instrument's own processes: Windows may hand a just-freed pid to the
#: very ``tasklist`` (or its console host) that asks about it.
_INSTRUMENT_IMAGES = {"tasklist.exe", "conhost.exe"}


def _pid_alive(pid: int) -> bool:
    """The instrument the contract names: tasklist on Windows, kill(pid, 0) on POSIX."""
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
                return cells[0].lower() not in _INSTRUMENT_IMAGES
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


def _kill(pid: int) -> None:
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)], capture_output=True, timeout=30)
        return
    for target in (lambda: os.killpg(pid, signal.SIGKILL), lambda: os.kill(pid, signal.SIGKILL)):
        try:
            target()
            return
        except (ProcessLookupError, PermissionError):
            continue  # not a group leader, or already gone: try the next form


# ------------------------------------------------------------ the instrument


def test_the_liveness_instrument_can_see_a_process_and_its_death():
    """Negative twin for every 'is gone' assertion below: the instrument says
    alive for a running child and gone after it is killed."""
    proc = subprocess.Popen(
        [PY, "-c", "import time; time.sleep(60)"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _pid_alive(proc.pid)
    finally:
        proc.kill()
        proc.wait(timeout=30)
    assert _wait_gone(proc.pid)


# ------------------------------------------------------------- no double fire


def test_two_concurrent_passes_fire_once_and_refuse_once(home):
    """Two ``run-due`` passes at the same instant on a 3 s job: exactly one
    ``finished:success``, and the other pass REFUSED at the pass lock.

    On 0.1.0 both passes fire (two success rows). Until the pass lock landed
    the loser still walked the whole store and wrote a per-job
    ``skipped_overlap`` row -- correct, but one row per job per refused wake,
    and the refusal was only ever visible job by job. The refusal now happens
    once, before either pass looks at a job, which is what "refused, not
    queued" has to mean for a host clock that fires every minute.

    The per-job overlap path is NOT gone and is asserted next door
    (`test_a_held_lock_is_skipped_overlap_and_not_stamped`): a detached wake
    outlives the pass that started it, so its job lock is still the thing that
    stops a later pass re-firing it.
    """
    assert _add("slow", f'"{PY}" -c "import time;time.sleep(3)"') == 0
    a = _awrise(home, "run-due", "--quiet", "--invoker", "a")
    b = _awrise(home, "run-due", "--quiet", "--invoker", "b")
    out_a, err_a = a.communicate(timeout=120)
    out_b, err_b = b.communicate(timeout=120)
    assert a.returncode == 0, err_a.decode("utf-8", "replace")
    assert b.returncode == 0, err_b.decode("utf-8", "replace")
    states = sorted(r["state"] for r in _finished(home))
    assert states == ["success"], (states, err_a, err_b)
    rows = ledger.read(home)
    started = [r for r in rows if r["event"] == "started"]
    assert len(started) == 1, "exactly one wake was started"
    # Both passes ticked -- the host clock fired twice and that is a fact
    # about the clock, not about the work -- and exactly one closed refused.
    assert len([r for r in rows if r["event"] == "tick"]) == 2
    refused = [r for r in rows if r.get("reason") == "pass_refused_overlap"]
    assert len(refused) == 1, [r.get("reason") for r in rows if r["event"] == "tick_end"]
    assert refused[0]["refused"].startswith("lock_held_by_"), refused[0]["refused"]
    job = store.load(home)["slow"]
    assert job["last_state"] == "success" and job["last_wake_id"] == started[0]["wake_id"]
    assert job["last_started_at"] == started[0]["ts"]
    assert not (home / "locks" / "slow").exists(), "the lock is released after the wake"
    assert not (home / lock.PASS_LOCK_NAME).exists(), "the pass lock outlived both passes"
    assert not [r for r in rows if r["event"] == "reconciled"], "nothing was 'recovered'"


def test_a_held_lock_is_skipped_overlap_and_not_stamped(home):
    """The in-process twin: hold the lock ourselves, run-due writes the skip
    row without touching jobs.json; release, and the job fires."""
    assert _add("j", "echo hi") == 0
    before = (home / "jobs.json").read_bytes()
    handle = lock.acquire(
        home, "j", {"wake_id": "w-holder01", "pass_id": "p-holder1", "started_at": None}, 300
    )
    assert isinstance(handle, lock.Lock)
    fired = []
    try:
        rc = cli._dispatch(
            lambda a: cli.cmd_run_due(
                a, executor=lambda j, w: fired.append(1) or Outcome("success", "exit_0")
            ),
            argparse.Namespace(quiet=True, invoker="t"),
        )
    finally:
        handle.release()
    assert rc == 0 and fired == []
    row = _finished(home)[-1]
    assert row["state"] == "skipped_overlap"
    assert row["reason"] == f"lock_held_by_w-holder01_pid_{os.getpid()}"
    assert (home / "jobs.json").read_bytes() == before, "an overlap skip is not stamped"
    # released: the very next pass fires it (the skip was a retry, not a stamp)
    rc = cli._dispatch(
        lambda a: cli.cmd_run_due(
            a, executor=lambda j, w: fired.append(1) or Outcome("success", "exit_0")
        ),
        argparse.Namespace(quiet=True, invoker="t"),
    )
    assert rc == 0 and fired == [1]
    assert [r["state"] for r in _finished(home)] == ["skipped_overlap", "success"]
    assert store.load(home)["j"]["last_state"] == "success"


def test_remove_refuses_while_the_lock_is_held(home):
    assert _add("j", "echo hi") == 0
    handle = lock.acquire(
        home, "j", {"wake_id": "w-holder02", "pass_id": "p-holder2", "started_at": None}, 300
    )
    try:
        assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="j")) == 1
        assert "j" in store.load(home)
    finally:
        handle.release()
    assert cli._dispatch(cli.cmd_remove, argparse.Namespace(name="j")) == 0


def test_reconcile_breaks_a_stale_lock_and_says_so(home):
    assert _add("j", "echo hi") == 0
    path = home / "locks" / "j"
    path.mkdir(parents=True)
    (path / "wake.json").write_text(
        json.dumps(
            {
                "wake_id": "w-stale001",
                "pass_id": "p-stale01",
                "job": "j",
                "pass_pid": 2_000_000_000,
                "child_pid": None,
                "started_at": "2026-01-01T00:00:00+00:00",
                "timeout_s": 60,
            }
        ),
        encoding="utf-8",
    )
    fired = []
    rc = cli._dispatch(
        lambda a: cli.cmd_run_due(
            a, executor=lambda j, w: fired.append(1) or Outcome("success", "exit_0")
        ),
        argparse.Namespace(quiet=True, invoker="t"),
    )
    assert rc == 0 and fired == [1], "the stale lock is broken and the job fires"
    row = [r for r in ledger.read(home) if r["event"] == "reconciled"][0]
    assert row["reason"] == "broke_1_stale_locks"
    assert row["locks"][0]["job"] == "j" and row["locks"][0]["reason"] in (
        "pid_gone",
        "age_exceeded_120s",
    )
    assert not path.exists()


# ------------------------------------------------------------- no silent kill


def test_timeout_refused_at_add_when_ge_interval(home):
    assert _add("t", "echo hi", every="1m", timeout=60) == 1
    assert _add("t", "echo hi", every="1m", timeout=59) == 0
    assert _add("u", "echo hi", every="1m", timeout=60, allow_overrun=True) == 0


def test_timeout_past_the_platform_wait_is_refused_and_never_spawns(home):
    """A timeout bigger than a platform wait can express: the wait itself
    fails, so the child would run on with no pid in any row, no kill and the
    lock released -- an unrecorded, unkillable wake. It is refused where it is
    entered, and a store that already holds one errors WITHOUT spawning."""
    over = clock.MAX_TIMEOUT_S + 1
    assert _add("big", "echo hi", every="60d", timeout=over) == 1
    assert _add("big", "echo hi", every="60d", timeout=over, allow_overrun=True) == 1
    assert store.load(home) == {}
    assert _add("big", "echo hi", every="60d", timeout=clock.MAX_TIMEOUT_S) == 0
    assert (
        cli._dispatch(
            cli.cmd_set,
            argparse.Namespace(name="big", assignments=[f"timeout_s={over}"], allow_overrun=True),
        )
        == 1
    )
    # a store written before the bound existed: the wake is an error row, and
    # the ledger proves nothing was started (no child pid anywhere)
    jobs = store.load(home)
    jobs["big"]["timeout_s"] = int(over)
    store.save(jobs, home)
    started = time.monotonic()
    proc = _awrise(home, "run-due", "--quiet")
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 1, (out, err)
    assert time.monotonic() - started < 30
    row = _finished(home)[-1]
    assert row["state"] == "error" and row["reason"].startswith("timeout_s_above_bound:")
    assert row["child_pid"] is None
    assert not lock.lock_path(home, "big").exists(), "the lock is released"
    outcome = executors.run_shell({"run": "echo hi", "timeout_s": over}, {})
    assert outcome.state == "error" and outcome.child_pid is None


GRANDCHILD = (
    "import subprocess, sys, os, time\n"
    "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])\n"
    "open(os.environ['PIDFILE'], 'w').write(str(p.pid))\n"
    "p.wait()\n"
)


def test_timeout_kills_the_grandchild_too(home, tmp_path, monkeypatch):
    """A child that spawns a grandchild and waits: the timeout must leave
    NOTHING running. 0.1.0 (``subprocess.run`` + ``proc.kill``) killed the
    shell and left the tree; on Windows the grandchild's inherited pipe then
    held the pass until it exited."""
    script = tmp_path / "child.py"
    script.write_text(GRANDCHILD, encoding="utf-8")
    pidfile = tmp_path / "grandchild.pid"
    monkeypatch.setenv("PIDFILE", str(pidfile))
    assert _add("tree", f'"{PY}" "{script}"', every="1h", timeout=2) == 0
    t0 = time.monotonic()
    rc = cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t"))
    elapsed = time.monotonic() - t0
    assert rc == 1
    row = _finished(home)[-1]
    assert row["state"] == "timeout" and row["reason"].startswith("killed after 2s"), row
    assert row["child_pid"], "the child pid is in the row"
    assert elapsed < 30, f"the pass hung {elapsed:.0f}s after the timeout: a survivor held the pipe"
    assert pidfile.exists(), "the child never got as far as spawning its grandchild"
    grandchild = int(pidfile.read_text(encoding="utf-8").strip())
    gone = _wait_gone(grandchild, 10)
    if not gone:
        _kill(grandchild)
    assert gone, f"grandchild {grandchild} survived the timeout"
    assert not _pid_alive(row["child_pid"])
    assert store.load(home)["tree"]["last_state"] == "timeout"
    assert store.load(home)["tree"]["consecutive_failures"] == 1
    assert not (home / "locks" / "tree").exists()


def test_detach_closes_at_spawn_and_leaves_the_child_alive(home):
    """``detach: true``: the wake closes ``detached`` as soon as the child is
    spawned, the pass returns at once, and the child outlives it."""
    assert _add("bg", f'"{PY}" -c "import time; time.sleep(120)"', every="1h", detach=True) == 0
    assert store.load(home)["bg"]["detach"] is True
    t0 = time.monotonic()
    rc = cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t"))
    elapsed = time.monotonic() - t0
    assert rc == 0
    assert elapsed < 20, f"the pass waited {elapsed:.0f}s on a detached child"
    row = _finished(home)[-1]
    assert row["state"] == "detached" and row["reason"] == f"spawned_pid_{row['child_pid']}"
    pid = row["child_pid"]
    try:
        assert _pid_alive(pid), "the detached child must survive the pass"
        # The lock is KEPT while the child runs. Releasing it at spawn is what
        # let a later pass start a second copy (see the no-double-fire test
        # below): the child is the wake, and the wake is still in flight.
        assert (home / "locks" / "bg").exists(), "the lock is held by the live child"
        job = store.load(home)["bg"]
        assert job["last_state"] == "detached" and job["consecutive_failures"] == 0
        assert job["last_started_at"], "a detached wake is stamped: not re-fired next pass"
        rc = cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t"))
        assert rc == 0 and len(_finished(home)) == 1, "inside the window: nothing fires"
    finally:
        _kill(pid)
    assert _wait_gone(pid), "cleanup: the detached child is gone"


def test_detach_via_main_and_set(home):
    assert cli.main(["add", "--name", "d", "--every", "1h", "--run", "echo hi", "--detach"]) == 0
    assert store.load(home)["d"]["detach"] is True
    assert cli.main(["set", "--name", "d", "detach=false"]) == 0
    assert store.load(home)["d"]["detach"] is False
    assert cli.main(["set", "--name", "d", "detach=maybe"]) == 1
    jobs = store.load(home)
    jobs["d"]["detach"] = "yes"
    with pytest.raises(store.StoreError):
        store.validate_record("d", jobs["d"])


# ------------------------------------------- detach may not defeat the overlap
#
# Review finding (2026-09-19): `_run_process` returns Outcome("detached") at
# spawn and `_execute`'s `finally: handle.release()` then deleted the per-job
# lock while the child was still running, so the next pass found no lock and
# spawned another copy -- unbounded. README:83-84 states the opposite without
# qualification: "A job whose previous run still holds its lock is recorded
# skipped_overlap and is never started a second time." The overrun guard gives
# no cover either: a detached child is never waited on and never killed.


def test_a_detached_child_still_holds_its_lock_and_is_never_fired_twice(home, tmp_path):
    marks = tmp_path / "runs.txt"
    run = (
        f'"{PY}" -c "import time,os,sys; '
        f"open(sys.argv[1],'a').write(str(os.getpid())+chr(10)); time.sleep(30)\" "
        f'"{marks}"'
    )
    assert (
        _add("d", run, every="1s", detach=True, timeout=1, allow_overrun=True, executor="shell")
        == 0
    )
    pids = []
    try:
        for _pass in range(3):
            assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) == 0
            time.sleep(1.3)
        pids = [int(line) for line in marks.read_text().split() if line.strip()]
        assert len(pids) == 1, f"the detached child was started {len(pids)} times: {pids}"
        assert (home / "locks" / "d").exists(), "the live child's lock must still be there"
        states = [r["state"] for r in _finished(home)]
        assert states.count("detached") == 1, states
        assert states.count("skipped_overlap") == 2, states
    finally:
        for pid in pids:
            _kill(pid)
    for pid in pids:
        assert _wait_gone(pid), "cleanup: the detached child is gone"


def test_the_lock_of_a_detached_child_is_freed_once_the_child_is_gone(home, tmp_path):
    """The kept lock is not a permanent one: it is bounded by the child."""
    assert (
        _add(
            "d",
            f'"{PY}" -c "import time; time.sleep(30)"',
            every="1s",
            detach=True,
            timeout=1,
            allow_overrun=True,
        )
        == 0
    )
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) == 0
    pid = _finished(home)[-1]["child_pid"]
    assert lock.inspect(home, "d", 300) is not None, "held while the child runs"
    _kill(pid)
    assert _wait_gone(pid)
    # The child is gone, so nothing holds the wake: the lock is judged stale by
    # the ordinary pid rule and the next pass takes it.
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) == 0
    second = _finished(home)[-1]
    assert second["state"] == "detached", [r["state"] for r in _finished(home)]
    _kill(second["child_pid"])
    assert _wait_gone(second["child_pid"])
