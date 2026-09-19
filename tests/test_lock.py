"""The per-job lock: one holder, stale by pid + age, unreadable = held."""

import json
import os
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import clock, lock

#: A pid no OS hands out (Linux pid_max <= 4194304; Windows pids are small).
DEAD_PID = 2_000_000_000


def _wake(wake_id="w-test0001", pass_id="p-test0001", started_at=None):
    return {
        "wake_id": wake_id,
        "pass_id": pass_id,
        "started_at": started_at or clock.iso(clock.now_utc()),
    }


def test_acquire_then_held_then_release(tmp_path):
    handle = lock.acquire(tmp_path, "j", _wake(), 300)
    assert isinstance(handle, lock.Lock)
    holder = json.loads((tmp_path / "locks" / "j" / "wake.json").read_text(encoding="utf-8"))
    assert holder["pass_pid"] == os.getpid() and holder["wake_id"] == "w-test0001"
    assert holder["timeout_s"] == 300 and holder["child_pid"] is None
    second = lock.acquire(tmp_path, "j", _wake("w-test0002"), 300)
    assert isinstance(second, lock.Held)
    assert second.reason == f"lock_held_by_w-test0001_pid_{os.getpid()}"
    assert second.pass_id == "p-test0001" and second.age_s is not None
    assert lock.inspect(tmp_path, "j", 300) is not None
    handle.note_child(4242, 4242)
    holder = json.loads((tmp_path / "locks" / "j" / "wake.json").read_text(encoding="utf-8"))
    assert holder["child_pid"] == 4242
    handle.release()
    assert not (tmp_path / "locks" / "j").exists()
    assert lock.inspect(tmp_path, "j", 300) is None
    assert isinstance(lock.acquire(tmp_path, "j", _wake("w-test0003"), 300), lock.Lock)


def test_two_jobs_do_not_share_a_lock(tmp_path):
    a = lock.acquire(tmp_path, "a", _wake(), 300)
    b = lock.acquire(tmp_path, "b", _wake(), 300)
    assert isinstance(a, lock.Lock) and isinstance(b, lock.Lock)
    a.release()
    b.release()


def _plant(tmp_path, job, pass_pid, age, timeout_s=60, child_pid=None, wake_id="w-plant001"):
    path = tmp_path / "locks" / job
    path.mkdir(parents=True)
    started = clock.iso(clock.now_utc() - age)
    (path / "wake.json").write_text(
        json.dumps(
            {
                "wake_id": wake_id,
                "pass_id": "p-plant001",
                "job": job,
                "pass_pid": pass_pid,
                "child_pid": child_pid,
                "child_pgid": None,
                "started_at": started,
                "timeout_s": timeout_s,
                "host": "h",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_dead_holder_is_broken_and_retaken(tmp_path):
    _plant(tmp_path, "j", DEAD_PID, timedelta(seconds=5))
    assert lock.judge("j", tmp_path / "locks" / "j", 360) == "pid_gone"
    handle = lock.acquire(tmp_path, "j", _wake("w-new00001"), 300)
    assert isinstance(handle, lock.Lock), "a dead holder is broken, the lock re-taken"
    holder = json.loads((tmp_path / "locks" / "j" / "wake.json").read_text(encoding="utf-8"))
    assert holder["wake_id"] == "w-new00001" and holder["pass_pid"] == os.getpid()
    handle.release()


def test_live_holder_inside_the_bound_is_held(tmp_path):
    _plant(tmp_path, "j", os.getpid(), timedelta(seconds=5), timeout_s=60)
    held = lock.acquire(tmp_path, "j", _wake("w-new00002"), 300)
    assert isinstance(held, lock.Held) and held.pid == os.getpid()
    assert held.reason == f"lock_held_by_w-plant001_pid_{os.getpid()}"
    assert (tmp_path / "locks" / "j" / "wake.json").exists(), "never broken"


def test_live_holder_past_the_age_bound_is_broken(tmp_path):
    """pid reuse: a live pid does not keep a lock older than timeout + grace."""
    _plant(tmp_path, "j", os.getpid(), timedelta(seconds=200), timeout_s=60)
    assert lock.judge("j", tmp_path / "locks" / "j", 9999) == "age_exceeded_120s", (
        "the holder's own timeout is the bound, not the caller's fallback"
    )
    handle = lock.acquire(tmp_path, "j", _wake("w-new00003"), 300)
    assert isinstance(handle, lock.Lock)
    handle.release()


def test_dead_pass_with_a_live_child_is_still_held(tmp_path):
    _plant(tmp_path, "j", DEAD_PID, timedelta(seconds=5), child_pid=os.getpid())
    held = lock.acquire(tmp_path, "j", _wake("w-new00004"), 300)
    assert isinstance(held, lock.Held), "the pass died but its child runs on: one wake in flight"


def test_directory_without_wake_json_is_held_while_young_and_broken_when_old(tmp_path):
    path = tmp_path / "locks" / "j"
    path.mkdir(parents=True)
    held = lock.acquire(tmp_path, "j", _wake(), 60)
    assert isinstance(held, lock.Held) and held.wake_id is None
    assert held.reason.startswith("lock_held_by_unknown_age_")
    old = time.time() - 500
    os.utime(path, (old, old))
    assert lock.judge("j", path, 120).startswith("age_exceeded_")
    handle = lock.acquire(tmp_path, "j", _wake(), 60)
    assert isinstance(handle, lock.Lock)
    handle.release()
    # an EMPTY directory a few seconds old was never named by a holder: broken
    # well before the age bound (a release whose rmdir lost to a reader)
    path.mkdir(parents=True)
    recent = time.time() - (lock.EMPTY_LOCK_S + 2)
    os.utime(path, (recent, recent))
    assert lock.judge("j", path, 360) == "empty_lock"
    handle = lock.acquire(tmp_path, "j", _wake(), 300)
    assert isinstance(handle, lock.Lock)
    handle.release()


def test_holder_without_a_pid_is_held_until_the_age_bound(tmp_path):
    path = tmp_path / "locks" / "j"
    path.mkdir(parents=True)
    (path / "wake.json").write_text(json.dumps({"wake_id": "w-nopid001"}), encoding="utf-8")
    assert isinstance(lock.acquire(tmp_path, "j", _wake(), 60), lock.Held)
    (path / "wake.json").write_text("{not json", encoding="utf-8")
    assert isinstance(lock.acquire(tmp_path, "j", _wake(), 60), lock.Held)
    (path / "wake.json").write_text("[1, 2]", encoding="utf-8")
    assert isinstance(lock.acquire(tmp_path, "j", _wake(), 60), lock.Held)


@pytest.fixture
def unreadable_holder(tmp_path):
    """A ``wake.json`` this process cannot open: an exclusive handle on
    Windows (sharing violation), mode 000 on POSIX (EACCES).

    The directory is aged past ``EMPTY_LOCK_S`` on purpose. A lock dir younger
    than that reads as held by the empty-directory rule alone, so the test
    below would pass without the unreadable-file rule existing at all -- it
    would assert nothing about the thing it is named after.
    """
    path = _plant(tmp_path, "j", DEAD_PID, timedelta(seconds=5))
    aged = time.time() - (lock.EMPTY_LOCK_S + 2)
    os.utime(path, (aged, aged))
    target = path / "wake.json"
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateFileW.restype = ctypes.c_void_p
        kernel32.CreateFileW.argtypes = (
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        )
        kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
        generic_read, open_existing, normal = 0x80000000, 3, 0x80
        handle = kernel32.CreateFileW(
            str(target), generic_read, 0, None, open_existing, normal, None
        )
        assert handle and handle != ctypes.c_void_p(-1).value
        yield tmp_path
        kernel32.CloseHandle(handle)
    else:
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            pytest.skip("root reads mode 000")
        target.chmod(0)
        yield tmp_path
        target.chmod(0o600)


def test_unreadable_wake_json_reads_as_held_not_free(unreadable_holder):
    """The holder is DEAD by pid, but the file cannot be read: silence is a
    hold. Only the age bound may break it.

    The lock directory is older than ``EMPTY_LOCK_S``, so "no readable holder"
    cannot be answered by the young-empty-directory rule: the verdict here can
    only come from the unreadable-file rule, and reporting an unreadable file
    as an absent one turns this Held into ``empty_lock``.
    """
    tmp_path = unreadable_holder
    path = tmp_path / "locks" / "j"
    with pytest.raises(OSError):
        Path(path / "wake.json").read_bytes()
    assert (time.time() - path.stat().st_mtime) > lock.EMPTY_LOCK_S, (
        "the fixture must be old enough that the empty-directory rule cannot answer"
    )
    verdict = lock.judge("j", path, 360)
    assert isinstance(verdict, lock.Held), f"unreadable read as free: {verdict}"
    assert verdict.wake_id is None and verdict.age_s > lock.EMPTY_LOCK_S
    held = lock.acquire(tmp_path, "j", _wake(), 60)
    assert isinstance(held, lock.Held) and held.wake_id is None, held
    assert (tmp_path / "locks" / "j").exists(), "acquire must not break an unreadable holder"


def test_sweep_breaks_only_stale_locks(tmp_path):
    _plant(tmp_path, "dead", DEAD_PID, timedelta(seconds=5))
    _plant(tmp_path, "old", os.getpid(), timedelta(seconds=500), timeout_s=60)
    _plant(tmp_path, "live", os.getpid(), timedelta(seconds=5))
    jobs = {"dead": {"timeout_s": 300}, "old": {"timeout_s": 300}, "live": {"timeout_s": 300}}
    broken = lock.sweep(tmp_path, jobs)
    assert broken == [("dead", "pid_gone"), ("old", "age_exceeded_120s")]
    assert not (tmp_path / "locks" / "dead").exists()
    assert not (tmp_path / "locks" / "old").exists()
    assert (tmp_path / "locks" / "live" / "wake.json").exists()
    assert lock.sweep(tmp_path, jobs) == []
    assert lock.sweep(tmp_path / "nowhere", jobs) == []


def test_two_passes_that_both_judge_a_lock_stale_do_not_both_hold_it(tmp_path, monkeypatch):
    """The window the breaker is descheduled in: A judges the planted lock
    stale and is suspended; B breaks it, re-takes it and names itself; A wakes
    up and must not delete a lock that is now live and take it as well."""
    _plant(tmp_path, "j", DEAD_PID, timedelta(seconds=5))
    real_judge = lock.judge

    def slow_judge(job, path, bound, settle_s=0.0):
        verdict = real_judge(job, path, bound, settle_s=settle_s)
        if threading.current_thread().name == "breaker-A" and not isinstance(verdict, lock.Held):
            time.sleep(0.5)
        return verdict

    monkeypatch.setattr(lock, "judge", slow_judge)
    taken = {}

    def take(tag):
        taken[tag] = lock.acquire(tmp_path, "j", _wake(f"w-{tag}", f"p-{tag}"), 60)

    a = threading.Thread(target=take, args=("A",), name="breaker-A")
    b = threading.Thread(target=take, args=("B",), name="breaker-B")
    a.start()
    time.sleep(0.1)
    b.start()
    a.join()
    b.join()
    locks = [tag for tag, handle in taken.items() if isinstance(handle, lock.Lock)]
    assert len(locks) == 1, f"two holders of one lock: {taken}"
    holder = json.loads((tmp_path / "locks" / "j" / "wake.json").read_text(encoding="utf-8"))
    assert holder["wake_id"] == f"w-{locks[0]}", "the directory names the pass that holds it"
    assert taken[[t for t in taken if t not in locks][0]].reason.startswith("lock_held_by_")


def test_release_does_not_remove_a_lock_taken_after_ours_was_broken(tmp_path):
    """A pass suspended past its age bound is broken and the lock re-taken.
    When it resumes, its release must leave the NEW holder's lock alone --
    otherwise the next pass takes a lock that is still in flight."""
    mine = lock.acquire(tmp_path, "j", _wake("w-mine0001", "p-mine0001"), 60)
    assert isinstance(mine, lock.Lock)
    lock._remove(mine.path)  # broken for age while this pass was suspended
    theirs = lock.acquire(tmp_path, "j", _wake("w-their001", "p-their001"), 60)
    assert isinstance(theirs, lock.Lock)
    mine.release()
    assert mine.path.exists(), "released over a lock that was not ours"
    holder = json.loads((mine.path / "wake.json").read_text(encoding="utf-8"))
    assert holder["wake_id"] == "w-their001"
    assert isinstance(lock.acquire(tmp_path, "j", _wake("w-third001"), 60), lock.Held)
    theirs.release()
    assert not mine.path.exists(), "the real holder still releases"


def test_a_claim_left_by_a_dead_breaker_is_not_a_job_lock(tmp_path):
    """A breaker that died between the rename and the delete leaves a claim
    under ``locks/``. It is nobody's lock: the sweep clears it and never
    reports it as a broken job."""
    _plant(tmp_path, "live", os.getpid(), timedelta(seconds=5))
    claim = lock.locks_dir(tmp_path) / f"{lock.BREAK_PREFIX}live.123.456.abc"
    claim.mkdir(parents=True)
    (claim / "wake.json").write_text("{}", encoding="utf-8")
    assert lock.sweep(tmp_path, {"live": {"timeout_s": 300}}) == []
    assert not claim.exists(), "an abandoned claim is cleared"
    assert (tmp_path / "locks" / "live" / "wake.json").exists(), "the live lock is untouched"


def test_is_alive_reads_self_true_and_a_dead_pid_false():
    assert lock.is_alive(os.getpid())
    assert not lock.is_alive(DEAD_PID)
    assert not lock.is_alive(0) and not lock.is_alive(-1)
