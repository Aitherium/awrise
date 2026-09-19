"""Per-job lock: one wake at a time, and a lock that cannot outlive its holder.

``$AWRISE_HOME/locks/<job>/`` is taken with ``os.mkdir`` -- atomic on NTFS and
POSIX -- and holds ``wake.json`` naming the holder: wake id, pass pid, child
pid once spawned, start time, timeout. A second pass that finds it taken
writes ``skipped_overlap`` and retries next time; it never waits and never
kills.

Liveness is judged the way a lock must be judged: a pid we are not ALLOWED to
signal is a pid that EXISTS, so access-denied reads as alive on every OS; a
``wake.json`` that cannot be read (a writer mid-replace, a scanner holding it,
a directory taken a millisecond ago) reads as HELD, never as free. The lock's
age against ``timeout_s + grace`` is the final authority on both counts,
because pids are reused and a holder can be unreadable forever.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from . import clock, store

LOCKS_DIR = "locks"
HOLDER_FILE = "wake.json"
#: Past ``timeout_s + GRACE_S`` a lock is stale whatever its pid says.
GRACE_S = 60.0
DEFAULT_TIMEOUT_S = 300.0
#: A directory taken but not yet named: how long ``acquire`` waits for the
#: holder to write ``wake.json`` before reporting an unknown holder.
SETTLE_S = 1.0
#: A lock directory with no ``wake.json`` this long after its last change was
#: never named by anyone: its taker died before the write, or a release could
#: not remove it (a reader still held the file). No wake started under it.
EMPTY_LOCK_S = 5.0
#: A stale lock is CLAIMED before it is deleted, by renaming it under this
#: prefix. Exactly one renamer can win, so two passes that judged the same
#: lock stale cannot both break it and both re-take it. Job names can never
#: start with a dot, so a claim is never mistaken for a lock.
BREAK_PREFIX = ".breaking-"
#: How long ``release`` keeps re-reading an unreadable ``wake.json`` before it
#: gives up and leaves the directory for the age bound: a concurrent reader
#: holds the file for milliseconds, and a lock we cannot prove is ours must
#: not be deleted.
RELEASE_READ_S = 1.0


def locks_dir(base: Path) -> Path:
    return base / LOCKS_DIR


def lock_path(base: Path, job: str) -> Path:
    return locks_dir(base) / job


# ------------------------------------------------------------------ liveness


def is_alive(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    if sys.platform == "win32":
        return _is_alive_windows(pid)
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _is_alive_windows(pid: int) -> bool:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.OpenProcess.argtypes = (ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32)
    kernel32.GetExitCodeProcess.argtypes = (ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32))
    kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
    process_query_limited_information = 0x1000
    error_access_denied = 5
    still_active = 259
    handle = kernel32.OpenProcess(process_query_limited_information, 0, pid)
    if not handle:
        return ctypes.get_last_error() == error_access_denied
    try:
        code = ctypes.c_uint32(0)
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
            return True
        return code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


# ---------------------------------------------------------------- the lock


@dataclass
class Held:
    """The lock is taken by a wake that may still be running.

    ``wake_id`` is None when the holder could not be read: a directory whose
    ``wake.json`` is not there yet, or cannot be opened. That is still HELD.
    """

    job: str
    wake_id: Optional[str] = None
    pass_id: Optional[str] = None
    pid: Optional[int] = None
    age_s: Optional[float] = None
    #: True when the holder is a DETACHED child, not a running pass. The wake
    #: is still in flight -- that is why the lock is held -- but no `run-due`
    #: is running, so a caller asking "is a pass in progress" must not read
    #: this as yes. `hostclock.check` does exactly that, and a detached child
    #: that lives for hours would otherwise answer "a pass is in progress" for
    #: a host clock that has stopped.
    detached: bool = False

    @property
    def reason(self) -> str:
        if self.wake_id:
            return f"lock_held_by_{self.wake_id}_pid_{self.pid or 0}"
        age = 0 if self.age_s is None else int(self.age_s)
        return f"lock_held_by_unknown_age_{age}s"


@dataclass
class Lock:
    """A lock this process holds. Release it in a ``finally``; a pass that
    dies leaves the directory for the next pass to judge by pid and age."""

    job: str
    path: Path
    holder: dict

    def note_child(self, pid: Optional[int], pgid: Optional[int] = None) -> None:
        """Record the spawned child so a later judge can see it is still alive
        even when the pass that spawned it is gone.

        Written only while the directory still names THIS wake: a lock broken
        and re-taken between the acquire and the spawn belongs to another
        wake, and stamping our child into its holder would make the lock lie
        about who holds it -- and let our release delete it.
        """
        self.holder["child_pid"] = pid
        self.holder["child_pgid"] = pgid
        self._rewrite_if_ours()

    def mark_detached(self) -> None:
        """Say the child outlives the pass, so the lock is not the pass's to
        release and its age bound must not outrank the child's liveness.

        Measured 2026-09-19: a ``detach: true`` job returned ``detached`` at
        spawn and the pass's ``finally: release()`` then deleted the lock while
        the child ran on, so every later pass found no lock and spawned another
        copy -- unbounded. README:83 says the opposite without qualification.
        """
        self.holder["detached"] = True
        self._rewrite_if_ours()

    def _rewrite_if_ours(self) -> None:
        current, readable = _read_holder(self.path)
        if readable and (current or {}).get("wake_id") != self.holder.get("wake_id"):
            return
        with contextlib.suppress(OSError):
            _write_holder(self.path, self.holder)

    def release(self) -> None:
        """Remove the lock only while ``wake.json`` still names THIS wake.

        A lock broken for age while this pass was suspended belongs to another
        wake now; deleting it would hand the directory to a third pass and put
        two wakes in flight. A directory we cannot prove is ours is left for
        the age bound, which is the one authority that cannot be raced.
        """
        deadline = time.monotonic() + RELEASE_READ_S
        while True:
            holder, readable = _read_holder(self.path)
            if readable or time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        if holder is None:
            return  # gone already, or taken and not yet named: not ours to delete
        if holder.get("wake_id") != self.holder.get("wake_id"):
            return  # broken and re-taken while we ran: the new holder keeps it
        _remove(self.path)

    def __enter__(self) -> "Lock":
        return self

    def __exit__(self, *exc) -> bool:
        self.release()
        return False


def _write_holder(path: Path, holder: dict) -> None:
    """tmp + replace inside the lock directory, so a reader sees a whole
    document or none. OSError propagates to the caller."""
    tmp = path / f"{HOLDER_FILE}.tmp-{os.getpid()}"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(holder, fh, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    store.replace_file(tmp, path / HOLDER_FILE)


def _read_holder(path: Path) -> Tuple[Optional[dict], bool]:
    """(holder, readable). ``(None, True)`` = no file; ``(None, False)`` = a
    file we could not open or parse, which is a HOLD, not an absence."""
    try:
        raw = store.read_bytes(path / HOLDER_FILE)
    except FileNotFoundError:
        return None, True
    except OSError:
        return None, False
    try:
        holder = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, False
    if not isinstance(holder, dict):
        return None, False
    return holder, True


def _int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _age_s(path: Path, holder: Optional[dict]) -> Optional[float]:
    """Seconds since the lock was taken: the holder's own start stamp, else
    the directory's mtime. None when neither can be read."""
    if holder is not None:
        with contextlib.suppress(ValueError, TypeError):
            started = clock.parse_ts(holder.get("started_at"))
            if started is not None:
                return (clock.now_utc() - started).total_seconds()
    try:
        return max(0.0, clock.now_utc().timestamp() - path.stat().st_mtime)
    except OSError:
        return None


def judge(job: str, path: Path, fallback_bound_s: float, settle_s: float = 0.0) -> Union[Held, str]:
    """``Held`` while the holder may still be running, else the reason the
    lock is stale (``pid_gone`` or ``age_exceeded_<n>s``).

    With ``settle_s`` a directory that exists but has no ``wake.json`` yet
    (its taker is between ``mkdir`` and the write) is re-read for that long,
    so the verdict can name the holder instead of "unknown".
    """
    holder, readable = _read_holder(path)
    deadline = time.monotonic() + settle_s
    while holder is None and readable and time.monotonic() < deadline:
        time.sleep(0.02)
        holder, readable = _read_holder(path)
    age = _age_s(path, holder)
    bound = fallback_bound_s
    if holder is not None and _int(holder.get("timeout_s")) > 0:
        bound = float(_int(holder["timeout_s"])) + GRACE_S
    if holder is not None and holder.get("detached"):
        # The pass that started a detached wake is GONE by now, and nothing
        # waits on the child or kills it -- so the job's `timeout_s` describes
        # no deadline it will ever be held to, and both the age bound and the
        # pass pid answer about something that is not the wake. The child IS
        # the wake: alive, the lock is held; gone, the lock is stale. Ageing
        # it out instead would start a SECOND copy beside a live one.
        child = _int(holder.get("child_pid"))
        if child > 0 and is_alive(child):
            return Held(
                job,
                wake_id=holder.get("wake_id") or None,
                pass_id=holder.get("pass_id"),
                pid=child,
                age_s=age,
                detached=True,
            )
        return "detached_child_gone"
    if age is not None and age > bound:
        return f"age_exceeded_{int(bound)}s"
    if holder is None and readable and age is not None and age > EMPTY_LOCK_S:
        return "empty_lock"
    if holder is None or not readable:
        return Held(job, age_s=age)
    pass_pid = _int(holder.get("pass_pid"))
    child_pid = _int(holder.get("child_pid"))
    held = Held(
        job,
        wake_id=holder.get("wake_id") or None,
        pass_id=holder.get("pass_id"),
        pid=pass_pid or None,
        age_s=age,
    )
    if pass_pid <= 0:
        return held  # a holder with no pid cannot be judged dead; the age bound will
    if is_alive(pass_pid):
        return held
    if child_pid > 0 and is_alive(child_pid):
        return held  # the pass died but its child runs on: still one wake in flight
    return "pid_gone"


def _remove(path: Path) -> None:
    with contextlib.suppress(OSError):
        if path.is_file():
            path.unlink()
            return
    with contextlib.suppress(OSError):
        for child in path.iterdir():
            with contextlib.suppress(OSError):
                child.unlink()
    for _attempt in range(5):
        try:
            path.rmdir()
            return
        except FileNotFoundError:
            return
        except OSError:
            # Windows: a reader still holding wake.json keeps the entry until
            # its handle closes; that is milliseconds, so wait it out.
            time.sleep(0.02)


def _claim_stale(job: str, path: Path, bound_s: float) -> bool:
    """Claim a stale lock by renaming it aside, judge it again, then delete it.

    Two steps, and both are needed. The rename is atomic, so of several passes
    that judged the same lock stale only one can move it; the losers get an
    error here and retry the ``mkdir``, where they meet whatever the winner
    did. The re-judge is what makes the verdict true AT THE MOMENT OF THE
    BREAK: once renamed the directory is private to this process, so nobody
    can re-take it underneath the judgement. A directory that turns out to be
    a live lock -- another pass broke the stale one and took it while this one
    was between its judge and its rename -- is put back untouched.

    Returns True only when THIS process broke a lock it proved stale.
    """
    claim = path.parent / (
        f"{BREAK_PREFIX}{path.name}.{os.getpid()}.{threading.get_ident()}.{time.monotonic_ns():x}"
    )
    try:
        os.rename(path, claim)
    except OSError:
        # Lost the claim, or the directory cannot be moved at all (a foreign
        # handle inside it on Windows): this process did not break anything.
        return False
    if isinstance(judge(job, claim, bound_s), Held):
        with contextlib.suppress(OSError):
            os.rename(claim, path)
        return False
    _remove(claim)
    return True


def acquire(
    base: Path, job: str, wake: dict, timeout_s: Optional[float] = None
) -> Union[Lock, Held]:
    """Take ``locks/<job>/`` or say who holds it. A stale lock (dead holder,
    or older than ``timeout_s + grace``) is CLAIMED by one breaker, deleted,
    and re-taken; a pass that loses the claim retries the ``mkdir`` once and
    reports the winner as the holder.

    OSError from the locks directory itself propagates: a home that cannot
    hold a lock cannot make a no-double-fire claim, and that is exit 2.
    """
    locks_dir(base).mkdir(parents=True, exist_ok=True)
    return _take(lock_path(base, job), job, wake, timeout_s)


def _take(path: Path, job: str, wake: dict, timeout_s: Optional[float] = None) -> Union[Lock, Held]:
    """The mkdir / judge / break / re-take loop, for ONE lock directory.

    Shared by the per-job locks under ``locks/`` and the whole-pass lock
    beside them, because two locks with the same staleness rules must not
    have two implementations of them: the copy is where the pass lock
    quietly grows a way to be broken that a job lock refuses.
    """
    fallback_bound = float(_int(timeout_s) or DEFAULT_TIMEOUT_S) + GRACE_S
    for _attempt in range(2):
        try:
            os.mkdir(path)
        except FileExistsError:
            verdict = judge(job, path, fallback_bound, settle_s=SETTLE_S)
            if isinstance(verdict, Held):
                return verdict
            _claim_stale(job, path, fallback_bound)
            continue
        holder = {
            "wake_id": wake.get("wake_id"),
            "pass_id": wake.get("pass_id"),
            "job": job,
            "pass_pid": os.getpid(),
            "child_pid": None,
            "child_pgid": None,
            "started_at": wake.get("started_at") or clock.iso(clock.now_utc()),
            "timeout_s": _int(timeout_s) or None,
            "host": socket.gethostname(),
        }
        try:
            _write_holder(path, holder)
        except OSError:
            _remove(path)
            raise
        return Lock(job, path, holder)
    # Broke a stale lock and lost the re-take to another pass: it is theirs.
    return Held(job)


# ------------------------------------------------------------- the pass lock

#: The whole-pass lock. It sits BESIDE ``locks/``, never inside it: that
#: directory is one entry per job and ``sweep`` reads every name in it as a
#: job, so a pass entry there would be reported as a wake for a job nobody
#: can find.
PASS_LOCK_NAME = "pass.lock"
#: What the pass lock is called in a message. It is a label, not a path, so
#: it cannot collide with a job of the same name.
PASS_LOCK_LABEL = "run-due"


def pass_lock_path(base: Path) -> Path:
    return base / PASS_LOCK_NAME


def acquire_pass(base: Path, wake: dict, timeout_s: Optional[float] = None) -> Union[Lock, Held]:
    """Take the whole-pass lock, or name the pass that already holds it.

    Why this is awrise's job and not the scheduler's: every host clock adapter
    has a different answer to "the last run has not finished". Task Scheduler
    decides by a ``MultipleInstances`` setting its command-line front end
    cannot even set, cron fires a second copy unconditionally, and a systemd
    timer QUEUES -- which is the worst of the three, because a job that runs
    longer than its interval then accumulates a backlog of passes that each
    wait for the one before it. The only place a rule that holds on every host
    can live is here, before the pass looks at a single job.

    A refused pass is not a failure. The previous pass is doing the work.
    """
    base.mkdir(parents=True, exist_ok=True)
    return _take(pass_lock_path(base), PASS_LOCK_LABEL, wake, timeout_s)


def inspect_pass(base: Path, timeout_s: Optional[float] = None) -> Optional[Held]:
    """``Held`` while a pass is in flight, else None. Never breaks the lock."""
    path = pass_lock_path(base)
    if not path.exists():
        return None
    verdict = judge(PASS_LOCK_LABEL, path, float(_int(timeout_s) or DEFAULT_TIMEOUT_S) + GRACE_S)
    return verdict if isinstance(verdict, Held) else None


def inspect(base: Path, job: str, timeout_s: Optional[float] = None) -> Optional[Held]:
    """``Held`` when a live wake holds the job's lock, else None. Never breaks."""
    path = lock_path(base, job)
    if not path.exists():
        return None
    verdict = judge(job, path, float(_int(timeout_s) or DEFAULT_TIMEOUT_S) + GRACE_S)
    return verdict if isinstance(verdict, Held) else None


def sweep(base: Path, jobs: Dict[str, dict]) -> List[Tuple[str, str]]:
    """Break every stale lock under ``locks/``; returns ``[(job, reason)]``.

    A live lock is left alone, and so is a lock another pass claims first --
    only the break THIS pass won is reported, because a sweep that reports a
    break it did not do is how a lock gets counted twice.
    """
    broken: List[Tuple[str, str]] = []
    folder = locks_dir(base)
    if not folder.is_dir():
        return broken
    for path in sorted(folder.iterdir()):
        job = path.name
        if job.startswith(BREAK_PREFIX):
            # A claim whose breaker died between the rename and the delete.
            # It is nobody's lock; finish the job it started.
            _remove(path)
            continue
        timeout = (jobs.get(job) or {}).get("timeout_s") if isinstance(jobs, dict) else None
        bound = float(_int(timeout) or DEFAULT_TIMEOUT_S) + GRACE_S
        verdict = judge(job, path, bound)
        if isinstance(verdict, Held):
            continue
        if _claim_stale(job, path, bound):
            broken.append((job, verdict))
    return broken
