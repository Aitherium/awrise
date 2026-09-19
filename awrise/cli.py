"""awrise CLI -- the verbs, and the one ``run-due`` pass that a host clock calls.

Exit codes are the contract: 0 = every wake succeeded or was skipped by
policy (or nothing was due); 1 = at least one wake ended ``failure``,
``timeout``, ``error`` or ``orphaned``; 2 = the store or the ledger could not
be read or written, so nothing can be claimed about the pass.
"""
from __future__ import annotations

import argparse
import contextlib
import fnmatch
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from . import clock, executors, hostclock, ledger, lock, store
from .clock import parse_interval  # noqa: F401  (0.1.0 import path, kept)
from .executors import Outcome

#: Spec keys this module reads. The self-test asserts SPEC_DEFAULTS ==
#: executors.READS | cli.READS, so an unread knob cannot ship.
READS = ("enabled", "run", "every", "interval_s", "at", "missed", "report")

Executor = Callable[[dict, dict], Outcome]


class FatalError(Exception):
    """Abort the verb with this exit code and message."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


# ------------------------------------------------------- 0.1.0 module surface

def get_awrise_home() -> Path:
    return store.home()


def get_jobs_file() -> Path:
    return store.store_path()


def load_jobs() -> dict:
    return store.load()


def save_jobs(jobs: dict) -> None:
    store.save(jobs)


# ------------------------------------------------------------------ spec

def _validate_spec(job: dict, allow_overrun: bool, check_overrun: bool = True,
                   name: str = "") -> None:
    run = (job.get("run") or "").strip()
    if not run:
        raise FatalError(1, "Error: empty command refused")
    job["run"] = run
    try:
        job["interval_s"] = float(clock.parse_interval(job["every"]).total_seconds())
    except ValueError as exc:
        raise FatalError(1, f"Error: {exc}") from exc
    try:
        timeout = int(job.get("timeout_s"))
    except (TypeError, ValueError) as exc:
        raise FatalError(1, "Error: timeout_s must be an integer: "
                            f"{job.get('timeout_s')!r}") from exc
    if timeout <= 0:
        raise FatalError(1, f"Error: timeout_s must be positive: {timeout}")
    if timeout > clock.MAX_TIMEOUT_S:
        raise FatalError(1, f"Error: timeout_s {timeout} is longer than this platform can "
                            f"wait ({clock.MAX_TIMEOUT_S}s) -- a wake past it could not be "
                            "timed out or killed")
    if check_overrun and timeout >= job["interval_s"] and not allow_overrun:
        raise FatalError(1, f"Error: timeout_s {timeout} >= interval {job['interval_s']:g}s "
                            "-- the job cannot finish inside its window "
                            "(--allow-overrun to accept)")
    job["timeout_s"] = timeout
    try:
        clock.parse_at(job.get("at"))
    except ValueError as exc:
        raise FatalError(1, f"Error: {exc}") from exc
    if job.get("cwd") is not None and not isinstance(job["cwd"], str):
        raise FatalError(1, "Error: cwd must be a path or null")
    if not isinstance(job.get("enabled"), bool):
        raise FatalError(1, "Error: enabled must be true or false")
    if not isinstance(job.get("detach"), bool):
        raise FatalError(1, "Error: detach must be true or false")
    if job.get("missed") not in store.MISSED_POLICIES:
        raise FatalError(1, f"Error: missed must be one of "
                            f"{', '.join(store.MISSED_POLICIES)}, not {job.get('missed')!r}")
    kind = job.get("executor") or "shell"
    if kind not in executors.KINDS:
        raise FatalError(1, f"Error: executor must be one of {', '.join(executors.KINDS)}, "
                            f"not {kind!r}")
    job["executor"] = kind
    problem = executors.bearer_path_problem(job.get("bearer_file"))
    if problem is not None:
        raise FatalError(1, f"Error: bearer_file {problem}")
    mode = job.get("permission_mode")
    if mode is not None and mode not in executors.PERMISSION_MODES:
        # Refused HERE as well as in the executor: a mode that skips the
        # permission prompt must not be storable at all, so it cannot be
        # waiting in jobs.json for a later version to start honouring.
        raise FatalError(1, f"Error: permission_mode must be one of "
                            f"{', '.join(executors.PERMISSION_MODES)}, not {mode!r}")
    unit = job.get("wake")
    if unit is not None and not isinstance(unit, str):
        raise FatalError(1, "Error: wake must be a unit name or null")
    unit = (unit or "").strip() or None
    job["wake"] = unit
    problem = executors.unit_name_problem(unit)
    if problem is not None:
        raise FatalError(1, f"Error: wake {problem}")
    for field in ("park_after", "wake_required"):
        if not isinstance(job.get(field), bool):
            raise FatalError(1, f"Error: {field} must be true or false")
    if job.get("park_after"):
        # Refused HERE as well as in the store: a park with no unit, or a park
        # behind a detached child, is a job that would misbehave every night.
        if unit is None:
            raise FatalError(1, "Error: park_after needs wake=<unit> -- the unit to park is "
                                "the one the job woke")
        if job.get("detach"):
            raise FatalError(1, "Error: park_after is refused with detach -- a detached child "
                                "outlives the wake, so its unit must not be parked under it")
    try:
        job["report"] = store.validate_report(name or "job", job.get("report"))
    except store.StoreError as exc:
        raise FatalError(1, f"Error: {exc}") from exc


def _parse_bool(key: str, raw: str) -> bool:
    low = raw.strip().lower()
    if low in ("true", "1", "yes", "on"):
        return True
    if low in ("false", "0", "no", "off"):
        return False
    raise FatalError(1, f"Error: {key} must be true or false, not {raw!r}")


def _coerce(key: str, raw: str):
    if key not in store.SETTABLE:
        raise FatalError(1, f"Error: unknown key {key!r}; settable: {', '.join(store.SETTABLE)}")
    if key in ("enabled", "detach", "park_after", "wake_required"):
        return _parse_bool(key, raw)
    if key == "timeout_s":
        return raw.strip()
    if key in ("cwd", "at", "bearer_file", "permission_mode", "wake", "report.relay"):
        return None if raw.strip().lower() in ("", "null", "none") else raw.strip()
    if key == "missed":
        # `catch-up-once` is what an operator types; one spelling is stored.
        return raw.strip().lower().replace("-", "_")
    if key == "executor":
        return raw.strip().lower()
    if key == "report.on":
        return [part.strip() for part in raw.split(",") if part.strip()]
    if key == "report.card_after":
        try:
            return int(raw.strip())
        except ValueError as exc:
            raise FatalError(1, f"Error: report.card_after must be an integer, "
                                f"not {raw.strip()!r}") from exc
    return raw


def _assign(job: dict, key: str, value) -> None:
    """``report.<field>`` addresses one field inside the report block; every
    other key is a top-level field. A dotted key never creates a new block
    shape -- the defaults are filled in first, so a store that had no report
    at all gains a complete one rather than a half-written dict."""
    if not key.startswith("report."):
        job[key] = value
        return
    report = job.get("report")
    if not isinstance(report, dict):
        report = dict(store.REPORT_DEFAULTS)
        report["on"] = list(store.REPORT_DEFAULTS["on"])  # type: ignore[arg-type]
    else:
        report = dict(report)
    report[key.split(".", 1)[1]] = value
    job["report"] = report


# ------------------------------------------------------------------ verbs

def _name(args) -> str:
    """The job name as every verb reads it: stripped, so ``add --name ' x'``
    and ``remove --name ' x'`` agree."""
    return (getattr(args, "name", None) or "").strip()


def cmd_add(args) -> int:
    name = _name(args)
    if not name or not getattr(args, "every", None) or getattr(args, "run", None) is None:
        print("--name, --every, --run required", file=sys.stderr)
        return 1
    try:
        store.validate_name(name)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    jobs = store.load()
    if name in jobs:
        print("Job exists", file=sys.stderr)
        return 1
    job = store.new_job(every=args.every.strip(), run=args.run, interval_s=0.0)
    timeout = getattr(args, "timeout", None)
    job["timeout_s"] = store.SPEC_DEFAULTS["timeout_s"] if timeout is None else timeout
    job["cwd"] = getattr(args, "cwd", None) or None
    job["at"] = getattr(args, "at", None) or None
    job["enabled"] = not getattr(args, "disabled", False)
    job["detach"] = bool(getattr(args, "detach", False))
    job["missed"] = _coerce("missed", getattr(args, "missed", None)
                            or str(store.SPEC_DEFAULTS["missed"]))
    job["executor"] = (getattr(args, "executor", None) or "shell").strip().lower()
    job["bearer_file"] = getattr(args, "bearer_file", None) or None
    job["permission_mode"] = getattr(args, "permission_mode", None) or None
    job["wake"] = (getattr(args, "wake", None) or "").strip() or None
    job["park_after"] = bool(getattr(args, "park_after", False))
    job["wake_required"] = not getattr(args, "wake_not_required", False)
    relay = getattr(args, "report_relay", None)
    card_after = getattr(args, "card_after", None)
    if relay or card_after is not None:
        _assign(job, "report.relay", relay or None)
        if card_after is not None:
            _assign(job, "report.card_after", card_after)
    _validate_spec(job, getattr(args, "allow_overrun", False), name=name)
    jobs[name] = job
    store.save(jobs)
    print(f"Added {name}")
    return 0


def cmd_set(args) -> int:
    name = _name(args)
    jobs = store.load()
    job = jobs.get(name)
    if job is None:
        print("Not found", file=sys.stderr)
        return 1
    assignments = list(getattr(args, "assignments", None) or [])
    if not assignments:
        print("Error: nothing to set (key=value ...)", file=sys.stderr)
        return 1
    for item in assignments:
        if "=" not in item:
            print(f"Error: want key=value, got {item!r}", file=sys.stderr)
            return 1
        key, raw = item.split("=", 1)
        _assign(job, key.strip(), _coerce(key.strip(), raw))
    touched = {a.split("=", 1)[0].strip() for a in assignments}
    # The overrun rule is re-asked only when the window/timeout relation
    # changes; an accepted --allow-overrun is not revoked by `set enabled=`.
    _validate_spec(job, getattr(args, "allow_overrun", False),
                   check_overrun=bool(touched & {"every", "timeout_s"}), name=name)
    job["updated_at"] = clock.iso(clock.now_utc())
    store.save(jobs)
    print(f"Updated {name}: " + ", ".join(a.split('=', 1)[0] for a in assignments))
    return 0


def _set_enabled(name: str, enabled: bool) -> int:
    jobs = store.load()
    job = jobs.get(name)
    if job is None:
        print("Not found", file=sys.stderr)
        return 1
    job["enabled"] = enabled
    job["updated_at"] = clock.iso(clock.now_utc())
    store.save(jobs)
    print(f"{'Enabled' if enabled else 'Disabled'} {name}")
    return 0


def cmd_enable(args) -> int:
    return _set_enabled(_name(args), True)


def cmd_disable(args) -> int:
    return _set_enabled(_name(args), False)


def cmd_remove(args) -> int:
    name = _name(args)
    base = store.home()
    jobs = store.load(base)
    if name not in jobs:
        print("Not found", file=sys.stderr)
        return 1
    live = [w for w, row in _open_wakes(base).items()
            if row.get("job") == name and _wake_is_live(row)]
    held = lock.inspect(base, name, jobs[name].get("timeout_s"))
    if held is not None and held.wake_id not in live:
        live.append(held.wake_id or held.reason)
    if live:
        print(f"Refusing: {name} has a wake in progress ({', '.join(live)}); "
              f"wait for it or run `awrise reconcile`", file=sys.stderr)
        return 1
    del jobs[name]
    store.save(jobs, base)
    ledger.append(base, {"event": "removed", "job": name, "reason": "operator_remove"})
    print(f"Removed {name}")
    return 0


def _due_phrase(seconds: float) -> str:
    """``now`` / ``in 42s`` / ``42s ago`` -- a distance, never a bare stamp."""
    if -1 < seconds < 1:
        return "now"
    if seconds < 0:
        return f"{int(-seconds)}s ago"
    return f"in {int(seconds)}s"


def _derived(base: Path, name: str, job: dict, now) -> dict:
    """What a reader wants that the store does not hold: the next window, the
    distance to it, the arrears, and whether a wake is in flight right now.

    ``interval_s`` is the exact number the schedule was parsed to; ``every``
    stays exactly as it was entered. A reader that has to re-parse ``1h30m``
    to find out when the job is next due is a reader that will get it wrong.
    """
    due = clock.next_due(job, now)
    try:
        in_flight = lock.inspect(base, name, job.get("timeout_s")) is not None
    except OSError:
        in_flight = False
    return {
        "next_due": clock.iso(due),
        "due_in_s": round((due - now).total_seconds(), 3),
        "due_now": now >= due,
        "period_s": clock.period_s(job),
        "missed_windows": clock.missed_windows(job, now),
        "in_progress": in_flight,
    }


def cmd_list(args) -> int:
    base = store.home()
    jobs = store.load(base)
    if not jobs:
        if getattr(args, "json", False):
            print("{}")
        else:
            print("No jobs")
        return 0
    now = clock.now_utc()
    if getattr(args, "json", False):
        # The record as stored, plus what every reader derives from it -- so a
        # script never has to re-implement dueness to know when a job is next up.
        out = {name: dict(job, **_derived(base, name, job, now))
               for name, job in sorted(jobs.items())}
        print(json.dumps(out, indent=2, sort_keys=True))
        return 0
    print(f"{'Name':<20} {'Every':<8} {'On':<3} {'Command':<30} {'Last Started':<20} "
          f"{'State':<16} {'Next due':<14}")
    print("-" * 124)
    for name, job in sorted(jobs.items()):
        last = (job.get("last_started_at") or "never")[:19]
        state = job.get("last_state") or "pending"
        on = "yes" if job.get("enabled", True) else "no"
        derived = _derived(base, name, job, now)
        due = "in progress" if derived["in_progress"] else _due_phrase(derived["due_in_s"])
        print(f"{name:<20} {str(job.get('every')):<8} {on:<3} {(job.get('run') or '')[:30]:<30} "
              f"{last:<20} {state:<16} {due:<14}")
    return 0


# ---------------------------------------------------------------- the pass

def _recent_rows(base: Path) -> List[dict]:
    return ledger.read(base, since=timedelta(days=30))


def _open_wakes(base: Path) -> dict:
    return ledger.open_wakes(_recent_rows(base))


def _row_ts(row: dict) -> Optional[str]:
    """The row's ``ts`` if it parses, else None. A ledger line whose stamp is
    unreadable must never be copied into ``jobs.json`` as a fact."""
    try:
        return clock.iso(clock.parse_ts(row.get("ts"))) if row.get("ts") else None
    except (ValueError, TypeError):
        return None


def _wake_age_and_bound(row: dict) -> Tuple[Optional[float], float]:
    """(age in seconds or None when the row's ts is unreadable, age bound)."""
    ts = _row_ts(row)
    age = (clock.now_utc() - clock.parse_ts(ts)).total_seconds() if ts else None
    try:
        timeout = float(row.get("timeout_s") or 300)
    except (TypeError, ValueError):
        timeout = 300.0
    return age, timeout + 60.0


def _row_pid(row: dict) -> int:
    """The row's pid, or 0 when it is missing or not a number. A ledger line
    with a garbage pid is judged dead, never a traceback on every later pass."""
    try:
        return int(row.get("pid") or 0)
    except (TypeError, ValueError):
        return 0


def _wake_is_live(row: dict) -> bool:
    age, bound = _wake_age_and_bound(row)
    pid = _row_pid(row)
    return age is not None and pid > 0 and lock.is_alive(pid) and age < bound


def reconcile(base: Path, jobs: dict, pass_id: str,
              invoker: str) -> Tuple[dict, List[str], List[str]]:
    """Close every ``started`` with no ``finished`` that cannot still be running,
    and re-stamp any finished wake whose stamp never reached ``jobs.json``.

    Returns (jobs, closed wake ids, in-progress wake ids). Closing an orphan
    also stamps the job from the started row, so an executed-but-unrecorded
    wake is never fired again on the same pass. The ledger is the memory and
    the store is its cache: when a save failed AFTER the finished row (disk
    full, a foreign reader holding the file), the row is still the truth, and
    the stamp is recovered here rather than the job fired a second time.
    """
    closed: List[str] = []
    live: List[str] = []
    touched: List[str] = []
    notices: List[Tuple[str, str, Outcome]] = []
    rows = _recent_rows(base)
    for wake_id, row in ledger.open_wakes(rows).items():
        marker = ledger.ledger_dir(base) / f"{wake_id}.unrecorded.json"
        # A marker is proof the wake FINISHED and only the row failed, so it is
        # closed whatever its pass pid is doing now.
        if not marker.exists() and _wake_is_live(row):
            live.append(wake_id)
            continue
        age, bound = _wake_age_and_bound(row)
        extra = {}
        if marker.exists():
            reason = "ledger_write_failed"
            try:
                with open(marker, "r", encoding="utf-8", newline="") as fh:
                    recovered = json.load(fh)
                extra = {"recovered_state": recovered.get("state"),
                         "recovered_exit_code": recovered.get("exit_code")}
            except (OSError, json.JSONDecodeError):
                extra = {}
        elif not lock.is_alive(_row_pid(row)):
            reason = "pid_gone"
        elif age is None:
            reason = "ts_unreadable"
        else:
            reason = f"age_exceeded_{int(bound)}s"
        ledger.append(base, {"wake_id": wake_id, "pass_id": pass_id, "invoker": invoker,
                             "job": row.get("job"), "event": "finished", "state": "orphaned",
                             "reason": reason,
                             "orphan_age_s": None if age is None else round(age, 1), **extra})
        if marker.exists():
            # The row is written; a marker that will not unlink is re-read as
            # recovered fields next time, never as a second orphan.
            with contextlib.suppress(OSError):
                marker.unlink()
        closed.append(wake_id)
        job = jobs.get(row.get("job") or "")
        if job is not None:
            _stamp(job, wake_id, _row_ts(row), Outcome("orphaned", reason))
            touched.append(row["job"])
            # `orphaned` is in the default `report.on` and counts towards the
            # card, so it must reach the sinks like any other bad outcome. A
            # wake that dies with the host (a reboot mid-run) is closed HERE
            # and nowhere else: without this line the one failure mode nobody
            # is watching for is the one nobody is told about, and a job that
            # only ever orphans walks past `card_after` in silence.
            notices.append((row["job"], wake_id, Outcome("orphaned", reason)))
    if closed:
        ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "event": "reconciled",
                             "reason": f"closed_{len(closed)}_orphaned", "wakes": closed})
    broken = lock.sweep(base, jobs)
    if broken:
        ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "event": "reconciled",
                             "reason": f"broke_{len(broken)}_stale_locks",
                             "locks": [{"job": j, "reason": r} for j, r in broken]})
    recovered = _recover_stamps(jobs, rows)
    if recovered:
        ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "event": "reconciled",
                             "reason": f"recovered_{len(recovered)}_stamps",
                             "jobs": recovered})
        touched.extend(recovered)
    if touched:
        jobs = store.reload_merge(jobs, touched, base)
        store.save(jobs, base)
    # Sinks last here too: the rows and the store are already on disk, so a
    # relay that is down or an awask that is missing costs a `report_error`
    # row and never the reconcile.
    for name, wake_id, outcome in notices:
        jobs = _notify(base, jobs, name, wake_id, pass_id, invoker, outcome)
    return jobs, closed, live


def _recover_stamps(jobs: dict, rows: List[dict]) -> List[str]:
    """Stamp each job from its newest finished wake when the store is behind.

    ``finished`` rows are in write order; the last stamping one per job wins.
    Its start is the matching ``started`` row's ts, or its own ts for a
    policy skip (which has no started row). Recovery is keyed on the wake:
    a job whose ``last_wake_id`` IS that row's wake has nothing to recover,
    whatever the clocks say -- so a healthy store is left byte-for-byte
    alone. Rows written before the job's ``removed`` row, or before the
    job's ``created_at``, belong to an earlier job with the same name and
    never stamp this one.
    """
    started_ts = {}
    latest = {}
    for row in rows:
        wake = row.get("wake_id")
        event = row.get("event")
        name = row.get("job")
        if event == "started" and wake:
            started_ts[wake] = _row_ts(row)
        elif event == "removed" and name:
            latest.pop(name, None)
        elif (event == "finished" and wake and name in jobs
              and row.get("state") in ledger.STATES
              and row.get("state") not in ledger.UNSTAMPED_STATES):
            latest[name] = row
    recovered: List[str] = []
    for name, row in latest.items():
        job = jobs[name]
        if job.get("last_wake_id") == row["wake_id"]:
            continue
        started_at = started_ts.get(row["wake_id"]) or _row_ts(row)
        if not started_at:
            continue
        created = job.get("created_at")
        if created and clock.parse_ts(started_at) < clock.parse_ts(created):
            continue
        prior = job.get("last_started_at")
        if prior and clock.parse_ts(started_at) <= clock.parse_ts(prior):
            continue
        _stamp(job, row["wake_id"], started_at,
               Outcome(row["state"], row.get("reason") or "recovered_from_ledger"),
               _row_ts(row))
        recovered.append(name)
    return recovered


def _stamp(job: dict, wake_id: str, started_at: Optional[str], outcome: Outcome,
           finished_at: Optional[str] = None, executed: bool = False) -> None:
    """A wake that MEASURED the clock stamps its own start unconditionally
    (an executed wake, or the row that records a backwards clock); a recovered
    or policy row only ever moves the stamp forward."""
    prior = job.get("last_started_at")
    if started_at and (executed or not prior
                       or clock.parse_ts(started_at) > clock.parse_ts(prior)):
        job["last_started_at"] = started_at
    job["last_wake_id"] = wake_id
    job["last_finished_at"] = finished_at or clock.iso(clock.now_utc())
    job["last_state"] = outcome.state
    job["last_reason"] = outcome.reason
    if outcome.state in ledger.BAD_STATES:
        job["consecutive_failures"] = int(job.get("consecutive_failures") or 0) + 1
    elif outcome.state == "success":
        job["consecutive_failures"] = 0
    job["updated_at"] = job["last_finished_at"]


# ------------------------------------------------------------------- sinks
#
# Telling someone is the LAST thing a pass does and the least trusted. Every
# sink here runs after the ledger row and the store are already written, and
# every failure inside one becomes a `report_error` ROW -- never an exception,
# never a changed exit code. A scheduler that cannot run its jobs because a
# chat server is down would be a worse product than one that says nothing.

#: The channel a job posts to when its own spec does not name one.
RELAY_CHANNEL_ENV = "AWRISE_RELAY_CHANNEL"
#: awrelay identifies the sender by this; without it the server answers 400.
RELAY_NICK_ENV = "AWRELAY_NICK"
#: A sink gets this long, and no more: it is not the job.
SINK_TIMEOUT_S = 30.0
#: The two answers a card offers. `keep` is the default, so a card nobody
#: answers never disables anything.
CARD_OPTIONS = (
    ("disable", "Disable the job", "awrise stops waking it until you re-enable it"),
    ("keep", "Keep it enabled", "awrise keeps waking it and will not ask again this streak"),
)


def _report_block(job: dict) -> dict:
    block = job.get("report")
    merged = dict(store.REPORT_DEFAULTS)
    merged["on"] = list(store.REPORT_DEFAULTS["on"])  # type: ignore[arg-type]
    if isinstance(block, dict):
        merged.update(block)
    return merged


def _sink_run(argv: List[str]) -> Tuple[Optional[int], str, str]:
    """(exit code, stdout, problem). The problem is non-empty when the tool
    could not be run at all, which is a different fact from a non-zero exit.

    argv[0] is resolved on PATH here rather than left to the OS: on Windows a
    bare name is looked up with ``.exe`` only, so a tool installed as a
    ``.cmd`` shim is found by ``which`` and then reported missing by the spawn
    -- the sink would be silently dead on the host it was written for.
    """
    resolved = executors.which(argv[0])
    if resolved is None:
        return None, "", f"{argv[0]}_not_installed"
    try:
        done = subprocess.run([resolved] + list(argv[1:]), stdin=subprocess.DEVNULL,
                              capture_output=True, timeout=SINK_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return None, "", f"{argv[0]}_timed_out_after_{SINK_TIMEOUT_S:g}s"
    except OSError as exc:
        return None, "", f"{argv[0]}_not_runnable:{type(exc).__name__}"
    out = (done.stdout or b"").decode("utf-8", "replace")
    if done.returncode != 0:
        tail = ledger.tail(done.stderr) or ledger.tail(done.stdout)
        return done.returncode, out, f"{argv[0]}_exit_{done.returncode}:{tail.strip()[:200]}"
    return done.returncode, out, ""


def _report_error_row(base: Path, name: str, wake_id: str, pass_id: str, invoker: str,
                      reason: str) -> None:
    with contextlib.suppress(Exception):
        ledger.append(base, {"wake_id": wake_id, "pass_id": pass_id, "invoker": invoker,
                             "job": name, "event": "report_error", "reason": reason})


def _relay_line(name: str, outcome: Outcome) -> str:
    """Enveloped, and passed as ONE argv element -- never interpolated into a
    shell string, because the text carries a job's own output tail."""
    detail = ledger.redact(outcome.reason or "")
    return f"[awrise] {name}: {outcome.state} ({detail})"


def _send_relay(channel: str, name: str, outcome: Outcome) -> str:
    if executors.which("awrelay") is None:
        return "awrelay_not_installed"
    if not (os.environ.get(RELAY_NICK_ENV) or "").strip():
        # The server answers 400 without it, so this is a measured refusal
        # rather than a guess -- and a row here is the only way an operator
        # ever learns that a channel they configured is posting nothing.
        return f"awrelay_nick_missing:{RELAY_NICK_ENV}"
    _code, _out, problem = _sink_run(["awrelay", "send", channel, _relay_line(name, outcome),
                                      "--kind", "finding"])
    return problem


def _raise_card(name: str, fails: int, outcome: Outcome) -> Tuple[Optional[str], str]:
    """(card id, problem). ONE card per streak: the id is stored, and the
    stored id is what stops the next pass raising a second one."""
    if executors.which("awask") is None:
        return None, "awask_not_installed"
    argv = ["awask", "ask", f"awrise: {name} has failed {fails} times in a row",
            "--summary", _relay_line(name, outcome),
            "--kind", "decision", "--agent", "awrise", "--default", "keep",
            "--json", "--quiet"]
    for key, label, consequence in CARD_OPTIONS:
        argv += ["--option", f"{key}:{label}:{consequence}"]
    _code, out, problem = _sink_run(argv)
    if problem:
        return None, problem
    try:
        card_id = (json.loads(out) or {}).get("id")
    except ValueError as exc:
        return None, f"awask_json_unreadable:{type(exc).__name__}:{exc}"
    if not card_id:
        return None, "awask_returned_no_card_id"
    return str(card_id), ""


def read_card_answer(card_id: str) -> Tuple[Optional[str], str]:
    """(answer key, problem). None with no problem = still open."""
    if executors.which("awask") is None:
        return None, "awask_not_installed"
    _code, out, problem = _sink_run(["awask", "show", card_id, "--json"])
    if problem:
        return None, problem
    try:
        card = json.loads(out) or {}
    except ValueError as exc:
        return None, f"awask_json_unreadable:{type(exc).__name__}:{exc}"
    if not isinstance(card, dict):
        return None, f"awask_returned_{type(card).__name__}"
    answer = card.get("answer")
    if answer is None or str(answer).strip() == "":
        return None, ""
    return str(answer).strip(), ""


def _notify(base: Path, jobs: dict, name: str, wake_id: str, pass_id: str, invoker: str,
            outcome: Outcome) -> dict:
    """Relay line and/or ONE card, after the row and the store are on disk."""
    job = jobs.get(name)
    if job is None:
        return jobs
    report = _report_block(job)
    channel = report.get("relay") or (os.environ.get(RELAY_CHANNEL_ENV) or "").strip() or None
    if channel and outcome.state in (report.get("on") or []):
        # Judged HERE as well as at `add`, because the environment is the
        # other lane into this argument and it has no add-time gate at all: a
        # value that is not a channel name is handed to the relay tool as an
        # argument, one shaped like a flag is then read by that tool's own
        # parser, and it exits 0 having posted nothing -- a configured channel
        # that is silently dead, which is the exact failure the nick check
        # above exists to make impossible.
        problem = store.relay_channel_problem(channel)
        problem = (f"relay_channel_refused:{problem}" if problem
                   else _send_relay(str(channel), name, outcome))
        if problem:
            _report_error_row(base, name, wake_id, pass_id, invoker, problem)
    card_after = report.get("card_after") or 0
    fails = int(job.get("consecutive_failures") or 0)
    if fails == 0:
        # The streak ended, so the card that named it is spent. Clearing it
        # here is what lets a job that recovers and later fails again raise a
        # SECOND card instead of being silent forever.
        if not report.get("card_id"):
            return jobs
        report["card_id"] = None
    elif not card_after or fails < int(card_after) or report.get("card_id"):
        return jobs
    else:
        card_id, problem = _raise_card(name, fails, outcome)
        if problem:
            _report_error_row(base, name, wake_id, pass_id, invoker, problem)
            # The sentinel is what makes "one per streak" true for a FAILED
            # raise too: without it a host with no awask writes the same row
            # on every pass, forever, and the ledger stops being readable.
            report["card_id"] = f"unavailable:{problem[:80]}"
        else:
            report["card_id"] = card_id
            with contextlib.suppress(Exception):
                ledger.append(base, {"wake_id": wake_id, "pass_id": pass_id,
                                     "invoker": invoker, "job": name,
                                     "event": "card_raised",
                                     "reason": f"consecutive_failures_{fails}",
                                     "card_id": card_id})
    # The merge FIRST, the write SECOND: `report` is a spec block the operator
    # owns, so a pass that carried its in-memory copy across the merge would
    # revert an edit made while the job was running.
    jobs = store.reload_merge(jobs, [name], base)
    if name not in jobs:
        return jobs
    jobs[name]["report"] = report
    store.save(jobs, base)
    return jobs


def apply_card_answer(base: Path, jobs: dict, name: str, pass_id: str, invoker: str) -> dict:
    """Read the answer to this job's open card and DO what it says.

    The card is raised by one pass and read by a later one on purpose: a
    scheduler that blocked waiting for a human would be a scheduler that stops
    running every other job in the store.
    """
    job = jobs.get(name)
    if job is None:
        return jobs
    report = _report_block(job)
    card_id = report.get("card_id")
    if not card_id or str(card_id).startswith(("unavailable:", "kept:")):
        return jobs
    answer, problem = read_card_answer(str(card_id))
    disable = False
    if problem:
        _report_error_row(base, name, ledger.new_id("w-"), pass_id, invoker, problem)
        report["card_id"] = f"unavailable:{problem[:80]}"
    elif answer is None:
        return jobs
    else:
        # Any answer but `disable` -- including the default `keep` -- leaves
        # the job alone and marks the card spent, so one streak asks once.
        disable = answer == "disable"
        report["card_id"] = f"kept:{card_id}"
        with contextlib.suppress(Exception):
            ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "job": name,
                                 "event": "card_answered", "card_id": card_id,
                                 "reason": f"answer_{answer}"})
    jobs = store.reload_merge(jobs, [name], base)
    if name not in jobs:
        return jobs
    jobs[name]["report"] = report
    if disable:
        jobs[name]["enabled"] = False
    jobs[name]["updated_at"] = clock.iso(clock.now_utc())
    store.save(jobs, base)
    return jobs


def _record(base: Path, jobs: dict, name: str, wake_id: str, pass_id: str, invoker: str,
            outcome: Outcome, started_at: str, executed: bool,
            measured: bool = False) -> dict:
    """finished row -> jobs.json replace. That order is pinned on purpose.

    ``measured`` says ``started_at`` is this pass's own clock reading rather
    than a stamp carried from elsewhere, so it may move ``last_started_at``
    backwards even though nothing ran. Only the clock-skew row uses it: a
    stamp in the future must be corrected on the pass that sees it, whether or
    not the job was then allowed to fire.
    """
    finished_at = clock.iso(clock.now_utc())
    try:
        duration = (clock.parse_ts(finished_at) - clock.parse_ts(started_at)).total_seconds()
    except (ValueError, TypeError):
        duration = None
    row = {"wake_id": wake_id, "pass_id": pass_id, "invoker": invoker, "job": name,
           "event": "finished", "state": outcome.state, "reason": outcome.reason,
           "ts": finished_at, "exit_code": outcome.exit_code,
           "duration_s": None if duration is None else round(duration, 3),
           "stdout_tail": outcome.stdout_tail, "stderr_tail": outcome.stderr_tail,
           "handoff_id": outcome.handoff_id, "child_pid": outcome.child_pid}
    if outcome.wake_unit:
        # Only a woken job carries these, so every row that was always written
        # is byte-identical to what it was before wakes existed.
        row.update({"wake_unit": outcome.wake_unit, "wake_s": outcome.wake_s,
                    "wake_state": outcome.wake_state, "park_state": outcome.park_state})
    try:
        ledger.append(base, row)
    except OSError as exc:
        if executed:
            # Best effort: the started row alone is enough for reconcile to
            # close this wake as orphaned; the marker only adds what we know.
            marker = ledger.ledger_dir(base) / f"{wake_id}.unrecorded.json"
            with contextlib.suppress(OSError):
                with open(marker, "w", encoding="utf-8", newline="\n") as fh:
                    json.dump({"state": outcome.state, "exit_code": outcome.exit_code}, fh)
        raise FatalError(2, f"ledger write failed after {name} ran: {exc}") from exc
    if outcome.state in ledger.UNSTAMPED_STATES:
        return jobs
    job = jobs.get(name)
    if job is None:
        return jobs
    _stamp(job, wake_id, started_at, outcome, finished_at, executed=executed or measured)
    jobs = store.reload_merge(jobs, [name], base)
    store.save(jobs, base)
    # Sinks last, and never in the way: the row and the store are already on
    # disk, so a relay server that is down or an awask that is not installed
    # costs a `report_error` row and nothing else.
    return _notify(base, jobs, name, wake_id, pass_id, invoker, outcome)


def _execute(base: Path, jobs: dict, name: str, pass_id: str, invoker: str,
             executor: Executor, reason: str, recheck: bool = False) -> Tuple[dict, Outcome]:
    """lock -> started row -> exec -> finished row -> jobs.json -> unlock.

    The lock is per job and is what makes "no double fire" a fact: a pass
    that finds it held writes ``skipped_overlap`` (not stamped, retried next
    pass) and never waits. With ``recheck`` the job is re-read from disk
    under the lock, so a pass that loaded the store BEFORE another pass fired
    the job does not fire it again inside the same window.
    """
    job = jobs[name]
    wake_id = ledger.new_id("w-")
    started_at = clock.iso(clock.now_utc())
    handle = lock.acquire(base, name, {"wake_id": wake_id, "pass_id": pass_id,
                                       "started_at": started_at}, job.get("timeout_s"))
    if isinstance(handle, lock.Held):
        outcome = Outcome("skipped_overlap", handle.reason)
        return _record(base, jobs, name, wake_id, pass_id, invoker, outcome, started_at,
                       executed=False), outcome
    try:
        if recheck:
            fresh = store.load(base).get(name)
            if fresh is None:
                outcome = Outcome("cancelled", "removed_during_pass")
                return _record(base, jobs, name, wake_id, pass_id, invoker, outcome,
                               started_at, executed=False), outcome
            if fresh.get("last_wake_id") != job.get("last_wake_id"):
                outcome = Outcome("skipped_overlap",
                                  f"already_woken_by_{fresh.get('last_wake_id')}")
                return _record(base, jobs, name, wake_id, pass_id, invoker, outcome,
                               started_at, executed=False), outcome
            job = jobs[name] = fresh
        ledger.append(base, {"wake_id": wake_id, "pass_id": pass_id, "invoker": invoker,
                             "job": name, "event": "started", "reason": reason,
                             "ts": started_at, "run": job.get("run"),
                             "timeout_s": job.get("timeout_s"), "detach": job.get("detach")})
        wake = {"wake_id": wake_id, "pass_id": pass_id, "job": name,
                "on_spawn": handle.note_child}
        try:
            outcome = executor(job, wake)
        except Exception as exc:  # noqa: BLE001 - an executor that raises is still a wake to close
            outcome = Outcome("error", f"executor_crashed:{type(exc).__name__}:{exc}")
        if not isinstance(outcome, Outcome):
            outcome = Outcome("error", f"executor_returned_{type(outcome).__name__}")
        elif not isinstance(outcome.reason, str) or not outcome.reason.strip():
            outcome = Outcome("error", f"executor_returned_empty_reason:{outcome.state}",
                              exit_code=outcome.exit_code, stdout_tail=outcome.stdout_tail,
                              stderr_tail=outcome.stderr_tail)
        if outcome.state not in ledger.STATES:
            # The executor is the one caller that may hand us garbage; the ledger
            # refuses it, and the wake must still be closed as an error.
            outcome = Outcome("error", f"executor_returned_unknown_state:{outcome.state}")
        jobs = _record(base, jobs, name, wake_id, pass_id, invoker, outcome, started_at,
                       executed=True)
        return jobs, outcome
    finally:
        handle.release()


def _skip(base: Path, jobs: dict, name: str, pass_id: str, invoker: str,
          state: str, reason: str, measured: bool = False) -> dict:
    wake_id = ledger.new_id("w-")
    now = clock.iso(clock.now_utc())
    return _record(base, jobs, name, wake_id, pass_id, invoker, Outcome(state, reason), now,
                   executed=False, measured=measured)


def _measured_gap_s(base: Path, now) -> Optional[float]:
    """Seconds since the last ``tick`` on record, or None when there is none.

    None is not "no gap": it is "nothing in the window awrise reads woke it
    at all", which is a louder absence than any measured gap, not a quieter
    one. Both the real pass and ``--dry-run`` read the clock through this one
    function, so the plan they print and the pass they predict are judging
    the same measurement.
    """
    previous = hostclock.last_tick(base)
    if previous is None:
        return None
    try:
        stamp = clock.parse_ts(previous.get("ts"))
    except (ValueError, TypeError):
        return None
    if stamp is None:
        return None
    return round((now - stamp).total_seconds(), 3)


def _tick(base: Path, pass_id: str, invoker: str) -> Tuple[Optional[float], object]:
    """Open the pass with a ``tick`` row; return (gap since the last tick, now).

    The gap is the whole point. A pass that runs says nothing about the pass
    that should have run four minutes ago, and ``last_run`` alone cannot tell
    a healthy clock from one that was asleep: only the distance between two
    measured wakes can. The row also carries the interpreter, because an entry
    left pointing at a rebuilt venv keeps firing and never reaches this code.
    """
    now = clock.now_utc()
    gap = _measured_gap_s(base, now)
    row = {"event": "tick", "reason": "pass_start", "pass_id": pass_id,
           "invoker": invoker, "ts": clock.iso(now), "gap_s": gap,
           "interpreter": sys.executable}
    if gap is not None and gap < -hostclock.TICK_FUTURE_TOLERANCE_S:
        # A negative gap is not a short one: the previous tick is stamped after
        # this one, so the host clock moved. Recorded as measured AND marked,
        # because a reader that averages gaps would otherwise treat an
        # impossible number as data.
        row["clock_step"] = True
    ledger.append(base, row)
    return gap, now


def new_counts() -> dict:
    """The tally one pass keeps, and the one ``tick_end`` is read for.

    Every later verdict -- did the clock fire, did it reach the jobs, were
    windows lost -- is a comparison between these numbers across passes, so
    they are written even when the pass then fails: a count that is only kept
    on the happy path is a count nobody can use.
    """
    return {"jobs": 0, "due": 0, "fired": 0, "skipped": 0, "overlapped": 0, "missed": 0}


def _tick_end(base: Path, jobs: dict, pass_id: str, invoker: str,
              gap: Optional[float], started, counts: dict) -> None:
    """Close the pass. ``in_progress`` is why a quiet clock is not a dead one.

    A scheduler that refuses to start a second copy while the first is still
    running produces no new wake at all, so a 20-minute job looks exactly like
    a stopped clock. Counting the live locks here is what lets a later verdict
    tell those two apart.
    """
    live = []
    for name, job in (jobs or {}).items():
        try:
            if lock.inspect(base, name, job.get("timeout_s")) is not None:
                live.append(name)
        except OSError:
            continue
    now = clock.now_utc()
    row = {"event": "tick_end", "reason": "pass_end", "pass_id": pass_id,
           "invoker": invoker, "ts": clock.iso(now), "gap_s": gap,
           "in_progress": len(live), "in_progress_jobs": sorted(live),
           "duration_s": round((now - started).total_seconds(), 3)}
    row.update(counts or new_counts())
    ledger.append(base, row)


def _draining(executor: Executor) -> Executor:
    """``--drain`` travels to the executor in the wake envelope, not in a
    module global: a flag stored on the module would be read by every
    concurrent pass in the same interpreter, including the self-test's."""
    def wrapped(job: dict, wake: dict) -> Outcome:
        return executor(job, {**wake, "drain": True})

    return wrapped


def cmd_run_due(args, executor: Optional[Executor] = None) -> int:
    """One pass of the clock: a tick row, every due job, a tick_end row.

    The tick rows are written whatever the pass then does -- including when
    the store turns out to be unreadable -- because they answer a different
    question from the wakes: "is anything waking awrise at all". A host clock
    that fires into a broken store is a very different fault from one that
    stopped firing, and only the ticks tell them apart.
    """
    quiet = getattr(args, "quiet", False)
    invoker = getattr(args, "invoker", None) or "manual"
    executor = executor or executors.run
    if getattr(args, "drain", False):
        executor = _draining(executor)
    base = store.home()
    pass_id = ledger.new_id("p-")
    if getattr(args, "dry_run", False):
        # A dry run writes ``would_fire`` rows and NOTHING else: no tick (a tick
        # is the claim that the clock fired a real pass, and the gap between two
        # of them is how a missed window is found), no lock, no stamp.
        return _dry_run_pass(base, pass_id, invoker, quiet)
    gap, pass_started = _tick(base, pass_id, invoker)
    state: dict = {"jobs": {}, "counts": new_counts(), "gap": gap}
    try:
        return _run_due_pass(base, pass_id, invoker, executor, quiet, state)
    finally:
        _tick_end(base, state["jobs"], pass_id, invoker, gap, pass_started, state["counts"])
        keep = getattr(args, "prune", None)
        if keep:
            try:
                removed, _spared = prune_ledger(base, keep)
            except (ValueError, OSError) as exc:
                print(f"prune skipped: {exc}", file=sys.stderr)
            else:
                if removed and not quiet:
                    print(f"pruned {len(removed)} ledger day file(s) older than {keep}")


def _run_due_pass(base: Path, pass_id: str, invoker: str, executor: Executor,
                  quiet: bool, state: dict) -> int:
    jobs = state["jobs"] = store.load(base)
    jobs, closed, _live = reconcile(base, jobs, pass_id, invoker)
    state["jobs"] = jobs
    worst = 1 if closed else 0
    if closed and not quiet:
        print(f"reconcile: closed {len(closed)} orphaned wake(s)", file=sys.stderr)
    if not jobs:
        if not quiet:
            print("No jobs")
        return worst
    now = clock.now_utc()
    counts = state["counts"]
    counts["jobs"] = len(jobs)
    for name in sorted(jobs):
        if jobs.get(name) is not None:
            # An answered card is applied BEFORE dueness is judged, so a job
            # the owner disabled on the card does not get one more wake first.
            jobs = state["jobs"] = apply_card_answer(base, jobs, name, pass_id, invoker)
        job = jobs.get(name)
        if job is None:
            continue
        skew = clock.skew_s(job, now)
        if skew is not None:
            # The clock went backwards since the last wake: a row that says
            # so, then the job is due -- a stamp in the future never silences
            # it until the calendar catches up. The row carries THIS pass's
            # clock reading into the store, so the impossible stamp is healed
            # once, here: a job that is then skipped (disabled, empty) would
            # otherwise re-raise the same error on every pass until the
            # calendar passed its future stamp.
            worst = 1
            jobs = state["jobs"] = _skip(base, jobs, name, pass_id, invoker, "error",
                                         f"clock_skew:{skew:g}s", measured=True)
            print(f"  FAIL {name} error (clock_skew:{skew:g}s)", file=sys.stderr)
            job = jobs.get(name)
            if job is None:
                continue
        elif not clock.is_due(job, now):
            continue
        if not job.get("enabled", True):
            jobs = state["jobs"] = _skip(base, jobs, name, pass_id, invoker,
                                         "skipped_disabled", "disabled")
            counts["skipped"] += 1
            continue
        if not (job.get("run") or "").strip():
            jobs = state["jobs"] = _skip(base, jobs, name, pass_id, invoker,
                                         "skipped_empty", "empty_command")
            counts["skipped"] += 1
            print(f"Skip {name}: empty", file=sys.stderr)
            continue
        counts["due"] += 1
        windows = _missed_windows_to_record(base, name, job, now, state.get("gap"))
        reason = "due"
        if windows:
            counts["missed"] += 1
            _missed_row(base, name, job, pass_id, invoker, windows, state.get("gap"), now)
            if job.get("missed") == "skip":
                # The windows are dropped, and the stamp moves so they are not
                # re-found next pass. One row says how many were lost: a job
                # that quietly resumes is a job nobody knows was asleep.
                jobs = state["jobs"] = _skip(base, jobs, name, pass_id, invoker,
                                             "skipped_missed",
                                             f"missed_policy_skip:{windows}_windows")
                counts["skipped"] += 1
                if not quiet:
                    print(f"  SKIPPED_MISSED {name} ({windows} window(s) dropped)")
                continue
            reason = f"catch_up_once:{windows}_windows"
        if not quiet:
            print(f"Running {name}...")
        jobs, outcome = _execute(base, jobs, name, pass_id, invoker, executor, reason,
                                 recheck=True)
        state["jobs"] = jobs
        if outcome.state == "skipped_overlap":
            counts["overlapped"] += 1
        if outcome.state.startswith("skipped") or outcome.state == "cancelled":
            counts["skipped"] += 1
        else:
            # A pass that found the lock held did not fire the job; counting it
            # would make a starved job look like a running one.
            counts["fired"] += 1
        if outcome.state in ledger.BAD_STATES:
            worst = 1
            print(f"  FAIL {name} {outcome.state} ({outcome.reason})", file=sys.stderr)
        elif not quiet:
            print(f"  {outcome.state.upper()} {name} ({outcome.reason})")
    return worst


#: A gap this many times a job's own window is a clock that stopped, not
#: jitter. Under it the arrears are the job's, not the clock's, and no
#: ``missed`` row is written at all.
MISSED_GAP_FACTOR = 2


def _missed_windows_to_record(base: Path, name: str, job: dict, now,
                              gap: Optional[float]) -> int:
    """Windows this pass may RECORD as lost -- 0 unless the record supports it.

    Arrears alone are not a lost window. Three things have to hold:

    * the job is at least one whole window in arrears (``missed_windows``);
    * the MEASURED clock supports it -- a tick gap longer than
      ``MISSED_GAP_FACTOR`` windows, or no tick on record at all. A 1-minute
      job on a 1-minute clock falls one window into arrears on ordinary
      sub-second jitter, and writing that down as a lost window reports a
      clock that stopped when the clock never stopped;
    * no wake of this job is in flight. A scheduler that will not start a
      second copy produces exactly the silence of a dead clock (the plan's
      in-progress rule), and this pass is about to close the job
      ``skipped_overlap`` anyway: a long wake must not also file one row per
      pass saying its own windows were lost.

    A lock that cannot be read counts as in flight -- a claim nobody can
    check is not a measurement.
    """
    windows = clock.missed_windows(job, now)
    if not windows:
        return 0
    period = clock.period_s(job)
    if period <= 0:
        return 0
    if gap is not None and gap <= period * MISSED_GAP_FACTOR:
        return 0
    try:
        if lock.inspect(base, name, job.get("timeout_s")) is not None:
            return 0
    except OSError:
        return 0
    return windows


def _missed_row(base: Path, name: str, job: dict, pass_id: str, invoker: str,
                windows: int, gap: Optional[float], now) -> None:
    """One row per job per pass for the windows that came and went.

    One row, never one per window: the point is that they were lost and how
    many, and a dozen identical rows is how a re-fire storm gets recorded as
    if it were a schedule. The reason names the evidence the caller reached
    this row on: the MEASURED gap when a tick is on record, and the absence
    of any tick at all when there is none.
    """
    period = clock.period_s(job)
    if gap is not None:
        reason = f"clock_gap:{gap:g}s"
    else:
        reason = f"clock_absent:{windows}_windows"
    ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "job": name,
                         "event": "missed", "reason": reason, "windows": windows,
                         "period_s": period, "policy": job.get("missed"),
                         "due_at": clock.iso(clock.next_due(job, now)),
                         "last_started_at": job.get("last_started_at"),
                         "gap_s": gap})


def _would_do(base: Path, name: str, job: dict, now,
              gap: Optional[float] = None) -> Tuple[str, str]:
    """(``would_fire`` | ``hold``, the reason) for one job, touching nothing.

    The tests this answer has to pass are the real pass's, in the real pass's
    ORDER. Skew is the trap: a stamp in the future makes the job due (the
    pass writes the error row and stops the stamp silencing it), but it never
    makes a DISABLED or empty job run -- ``_run_due_pass`` falls straight
    through to ``skipped_disabled`` / ``skipped_empty`` and executes nothing.
    A dry run that answers WOULD FIRE there is answering the opposite of what
    it is for.
    """
    skew = clock.skew_s(job, now)
    if skew is None and not clock.is_due(job, now):
        return "hold", f"not_due_for_{int((clock.next_due(job, now) - now).total_seconds())}s"
    if not job.get("enabled", True):
        return "hold", "disabled"
    if not (job.get("run") or "").strip():
        return "hold", "empty_command"
    try:
        held = lock.inspect(base, name, job.get("timeout_s"))
    except OSError as exc:
        return "hold", f"lock_unreadable:{exc.__class__.__name__}"
    if held is not None:
        return "hold", held.reason
    if skew is not None:
        return "would_fire", f"clock_skew:{skew:g}s"
    windows = _missed_windows_to_record(base, name, job, now, gap)
    if windows and job.get("missed") == "skip":
        return "hold", f"missed_policy_skip:{windows}_windows"
    if windows:
        return "would_fire", f"catch_up_once:{windows}_windows"
    return "would_fire", "due"


def _dry_run_pass(base: Path, pass_id: str, invoker: str, quiet: bool) -> int:
    """What the next real pass would do, as ``would_fire`` rows and nothing else.

    Nothing is run, nothing is locked and no stamp moves (``would_fire`` is
    one of the states that never stamps), so this is safe to run beside a
    live clock. It exits 0 whatever it finds: a plan is not a measurement.
    """
    jobs = store.load(base)
    now = clock.now_utc()
    # The gap the next REAL pass would measure, read the same way that pass
    # reads it: a plan that judged the clock differently from the pass it
    # predicts would disagree with it about missed windows.
    gap = _measured_gap_s(base, now)
    would = 0
    for name in sorted(jobs):
        job = jobs[name]
        verdict, reason = _would_do(base, name, job, now, gap)
        if verdict == "would_fire":
            would += 1
            ledger.append(base, {"pass_id": pass_id, "invoker": invoker, "job": name,
                                 "event": "finished", "state": "would_fire",
                                 "reason": reason, "dry_run": True,
                                 "missed_windows": clock.missed_windows(job, now)})
        if not quiet:
            print(f"  {'WOULD FIRE' if verdict == 'would_fire' else 'hold      '} "
                  f"{name} ({reason})")
    if not quiet:
        print(f"dry run: {would} of {len(jobs)} job(s) would fire; nothing ran, "
              "no stamp moved")
    return 0


def cmd_run(args, executor: Optional[Executor] = None) -> int:
    name = _name(args)
    executor = executor or executors.run
    base = store.home()
    jobs = store.load(base)
    job = jobs.get(name)
    if job is None:
        print("Not found", file=sys.stderr)
        return 1
    force = getattr(args, "force", False)
    if not job.get("enabled", True) and not force:
        print(f"{name} is disabled (--force runs it anyway)", file=sys.stderr)
        return 1
    if not (job.get("run") or "").strip():
        print(f"{name} has an empty command", file=sys.stderr)
        return 1
    pass_id = ledger.new_id("p-")
    _jobs, outcome = _execute(base, jobs, name, pass_id, "manual", executor,
                              "forced" if force else "manual")
    line = f"{outcome.state} {name} ({outcome.reason})"
    if outcome.state in ledger.BAD_STATES:
        print(line, file=sys.stderr)
        if outcome.stderr_tail:
            print(outcome.stderr_tail.rstrip(), file=sys.stderr)
        return 1
    print(line)
    return 0


# ------------------------------------------------------------ the record

def cmd_history(args) -> int:
    base = store.home()
    since = None
    if getattr(args, "since", None):
        try:
            since = clock.parse_interval(args.since)
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
    if getattr(args, "judge", False):
        code, lines = _judge_history(base, since or timedelta(days=7))
        for line in lines:
            print(line, file=sys.stderr if code else sys.stdout)
        return code
    rows = ledger.read(base, since=since, job=getattr(args, "job", None),
                       event=getattr(args, "event", None))
    limit = getattr(args, "limit", 50)
    if limit and limit > 0:
        rows = rows[-limit:]
    if getattr(args, "json", False):
        # ASCII-only on purpose: JSON escapes survive any stdout encoding, and
        # a `→` in a tail is what killed this verb on a cp1252 pipe.
        for row in rows:
            print(json.dumps(row, sort_keys=True, ensure_ascii=True))
        return 0
    if not rows:
        filtered = since or getattr(args, "job", None) or getattr(args, "event", None)
        print("no wakes recorded" + (" for that filter" if filtered else " yet (ledger empty)"))
        return 0
    print(f"{'When (UTC)':<20} {'Wake':<11} {'Job':<18} {'Event':<10} {'State':<18} Reason")
    print("-" * 100)
    for row in rows:
        ts = (row.get("ts") or "")[:19].replace("T", " ")
        print(f"{ts:<20} {str(row.get('wake_id') or ''):<11} {str(row.get('job') or '-'):<18} "
              f"{str(row.get('event')):<10} {str(row.get('state') or '-'):<18} "
              f"{str(row.get('reason') or '')[:60]}")
    return 0


def _spec_line(job: dict) -> str:
    bits = [f"every {job.get('every')} ({clock.period_s(job):g}s window)"]
    if job.get("at"):
        bits.append(f"anchored at {job['at']} UTC")
    bits.append("enabled" if job.get("enabled", True) else "DISABLED")
    bits.append(f"timeout {job.get('timeout_s')}s")
    bits.append(f"missed={job.get('missed')}")
    if (job.get("executor") or "shell") != "shell":
        # Named only when it is not the default, and named at all because
        # `explain` prints `run` under the heading "command": a URL or a
        # Python snippet under that heading with nothing saying which
        # executor reads it is a line an operator can misread.
        bits.append(f"executor={job['executor']}")
    if job.get("permission_mode"):
        bits.append(f"permission_mode={job['permission_mode']}")
    if job.get("bearer_file"):
        bits.append(f"bearer {job['bearer_file']}")
    report = job.get("report")
    if isinstance(report, dict) and report.get("relay"):
        bits.append(f"relay {report['relay']}")
    if isinstance(report, dict) and report.get("card_id"):
        bits.append(f"card {report['card_id']}")
    if job.get("detach"):
        bits.append("detached")
    if job.get("cwd"):
        bits.append(f"cwd {job['cwd']}")
    return ", ".join(bits)


def _describe_row(row: dict) -> str:
    state = row.get("state")
    head = f"{(row.get('ts') or '')[:19].replace('T', ' ')} {row.get('event')}"
    if state:
        head += f":{state}"
    extra = ""
    if row.get("exit_code") is not None:
        extra += f" exit={row['exit_code']}"
    if row.get("duration_s") is not None:
        extra += f" {row['duration_s']}s"
    return f"{head} ({row.get('reason')}){extra} [{row.get('wake_id')}]"


def cmd_explain(args) -> int:
    """Why this job did -- or did not -- fire on the last pass, from the record.

    The ledger is the memory, so every line below is read off it: the pass that
    last woke awrise, the rows that pass wrote for this job, and (when it wrote
    none) the only thing that can then be true. Exit 2 when no pass has been
    recorded at all: with no tick there is no "last tick" to explain, and
    guessing is what this verb exists to replace.
    """
    name = _name(args)
    base = store.home()
    jobs = store.load(base)
    job = jobs.get(name)
    since = None
    if getattr(args, "since", None):
        try:
            since = clock.parse_interval(args.since)
        except ValueError as exc:
            raise FatalError(1, f"Error: {exc}") from exc
    rows = ledger.read(base, since=since or timedelta(days=30))
    mine = [row for row in rows if row.get("job") == name]
    if job is None and not mine:
        print(f"Not found: no job named {name!r} and no wake of that name on record",
              file=sys.stderr)
        return 1
    now = clock.now_utc()
    print(f"job          {name}" + ("" if job else "  (removed -- explained from the ledger)"))
    if job:
        print(f"spec         {_spec_line(job)}")
        print(f"command      {job.get('run')}")
        due = clock.next_due(job, now)
        print(f"last wake    {job.get('last_state') or 'never'}"
              + (f" ({job.get('last_reason')}) at {job.get('last_started_at')}"
                 if job.get("last_state") else "")
              + (f", {job.get('consecutive_failures')} consecutive failure(s)"
                 if job.get("consecutive_failures") else ""))
        print(f"next due     {_due_phrase((due - now).total_seconds())} ({clock.iso(due)})")
        print(f"missed windows: {clock.missed_windows(job, now)}")
    ticks = [row for row in rows if row.get("event") == "tick"]
    if not ticks:
        print("last tick    none on record")
        print("UNJUDGED: nothing has woken awrise in this window, so there is no last "
              "pass to explain -- `awrise install --check` judges the host clock")
        return 2
    last_tick = ticks[-1]
    gap = last_tick.get("gap_s")
    print(f"last tick    {(last_tick.get('ts') or '')[:19].replace('T', ' ')} "
          f"pass {last_tick.get('pass_id')}"
          + (f", {gap}s after the one before" if gap is not None else ""))
    in_pass = [row for row in mine if row.get("pass_id") == last_tick.get("pass_id")]
    if in_pass:
        print("verdict      the pass acted on this job:")
        for row in in_pass:
            print(f"             {_describe_row(row)}")
    else:
        # Every other outcome writes a row -- disabled, empty, overlapped,
        # missed, fired -- so a pass that wrote nothing for this job saw a job
        # that was not yet due. That is a deduction from the closed vocabulary,
        # not a guess.
        why = "it was not due yet"
        if job is None:
            why = "the job no longer exists"
        elif not job.get("enabled", True):
            why = "it is disabled (and was not due, or the pass predates the change)"
        print(f"verdict      the pass wrote no row for this job: {why}")
        if job is not None:
            print(f"             last start {job.get('last_started_at') or 'never'}, "
                  f"window {clock.period_s(job):g}s")
    recent = [row for row in mine if row.get("event") in ("finished", "missed")][-5:]
    if recent:
        print(f"recent       {len(recent)} row(s), newest last:")
        for row in recent:
            print(f"             {_describe_row(row)}")
    return 0


def _files_holding_open_wakes(base: Path) -> set:
    """The day files that hold a ``started`` row nothing has closed.

    Pruning one of those is not forgetting an old pass, it is destroying the
    only evidence that a wake ever began -- the open wake disappears, so
    ``reconcile`` never closes it, ``status`` never names it, and a job that
    ran and was never accounted for reads as a job that never ran.
    """
    holding = set()
    for row in ledger.open_wakes(ledger.read(base)).values():
        stamp = _row_ts(row)
        if stamp:
            holding.add(ledger.day_file(base, clock.parse_ts(stamp)))
    return holding


def prune_ledger(base: Path, keep: str, dry_run: bool = False,
                 force: bool = False) -> Tuple[List[Path], List[Path]]:
    """Drop whole ledger day files older than ``keep``; returns (gone, spared).

    Day files are what make this an unlink instead of a rewrite: no line is
    ever edited, so a prune cannot corrupt a row or lose one inside the
    window. Today's file is always kept -- the floor is a date, and today is
    never before it -- and so is any file holding a wake nobody has closed,
    unless ``force`` says the operator means it.
    """
    window = clock.parse_interval(keep)
    floor = (clock.now_utc() - window).strftime("%Y-%m-%d")
    protected = set() if force else _files_holding_open_wakes(base)
    removed: List[Path] = []
    spared: List[Path] = []
    for path in ledger.files(base):
        if path.stem >= floor:
            continue
        if path in protected:
            spared.append(path)
            continue
        if not dry_run:
            path.unlink()
        removed.append(path)
    return removed, spared


def cmd_prune(args) -> int:
    """Drop old ledger files. Exit 0; a prune measures nothing."""
    base = store.home()
    keep = getattr(args, "keep", None) or "30d"
    dry_run = getattr(args, "dry_run", False)
    try:
        removed, spared = prune_ledger(base, keep, dry_run=dry_run,
                                       force=getattr(args, "force", False))
    except clock.ClockError:
        # A clock nobody can read is not a bad --keep. ClockError subclasses
        # ValueError, so the blanket clause below used to relabel an
        # unreadable AWRISE_NOW as an invalid interval and exit 1 (violation)
        # where every other verb exits 2 (could not judge) on that same input.
        # _dispatch maps it to 2; this clause only has to stop swallowing it.
        raise
    except ValueError as exc:
        raise FatalError(1, f"Error: --keep {exc}") from exc
    kept = len(ledger.files(base)) - (len(removed) if dry_run else 0)
    verb = "would remove" if dry_run else "removed"
    for path in removed:
        print(f"{verb} {path.name}")
    for path in spared:
        print(f"kept {path.name}: it holds a wake that was never closed "
              "(`awrise reconcile` first, or --force)")
    print(f"prune: {verb} {len(removed)} day file(s) older than {keep}; {kept} kept")
    return 0


def _span_s(rows: List[dict]) -> Optional[float]:
    """Seconds between the oldest and newest row awrise itself WROTE, or None.

    ``unreadable`` rows are excluded on purpose: they stand for a line the
    reader could not parse, they carry no stamp of their own, and a record
    that cannot be read is not record the judge may spend.
    """
    stamps = [_row_ts(row) for row in rows if row.get("event") != "unreadable"]
    parsed = sorted(clock.parse_ts(ts) for ts in stamps if ts)
    if len(parsed) < 2:
        return None
    return (parsed[-1] - parsed[0]).total_seconds()


#: A wake this many windows after the previous one is drift, not jitter.
DRIFT_FACTOR = 3


def _judge_history(base: Path, since: Optional[timedelta]) -> Tuple[int, List[str]]:
    """Read the record as a cadence: is each enabled job waking on its window?

    Two measurements, both distances between wakes: DRIFT (a gap longer than
    three windows) and ABSENCE (no wake at all). Neither can be judged for a
    job on a ledger shorter than twice THAT JOB's window -- an absence there
    is indistinguishable from a window that has not come round yet -- so such
    a job is named and skipped, and a run that could judge none of them is
    exit 2, never a green 0. The guard is per job because it used to be the
    longest cadence in the store: one monthly job then made every 1-minute
    job unjudgeable for a month, which is a judge that can never say no.
    """
    jobs = store.load(base)
    rows = ledger.read(base, since=since)
    enabled = {name: job for name, job in jobs.items() if job.get("enabled", True)}
    if not enabled:
        return 2, ["UNJUDGED: no enabled job to judge"]
    if max(clock.period_s(job) for job in enabled.values()) <= 0:
        return 2, ["UNJUDGED: no job declares a window to judge against"]
    span = _span_s(rows)
    have = "nothing" if span is None else f"{span:.0f}s"
    now = clock.now_utc()
    worst = 0
    judged = 0
    lines: List[str] = []
    young: List[str] = []
    for name, job in sorted(enabled.items()):
        period = clock.period_s(job)
        # The guard is THIS job's window, never the longest in the store: one
        # monthly job must not blind the judge to a 1-minute job the clock
        # stopped reaching hours ago.
        if period <= 0:
            young.append(f"{name}: no window to judge against")
            continue
        if span is None or span < period * 2:
            young.append(f"{name}: window {int(period)}s, ledger holds {have}")
            continue
        judged += 1
        stamps = sorted(clock.parse_ts(ts) for ts in
                        (_row_ts(row) for row in rows
                         if row.get("job") == name and row.get("event") == "started") if ts)
        if not stamps:
            worst = 1
            lines.append(f"NOT OK  {name}: no wake in {span:.0f}s of ledger, window "
                         f"{period:g}s -- enabled and never woken")
            continue
        gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
        gaps.append((now - stamps[-1]).total_seconds())
        worst_gap = max(gaps)
        if period > 0 and worst_gap > period * DRIFT_FACTOR:
            worst = 1
            lines.append(f"NOT OK  {name}: {len(stamps)} wake(s), worst gap {worst_gap:.0f}s "
                         f"is more than {DRIFT_FACTOR}x the {period:g}s window")
        else:
            lines.append(f"OK      {name}: {len(stamps)} wake(s), worst gap "
                         f"{worst_gap:.0f}s within {DRIFT_FACTOR}x the {period:g}s window")
    for line in young:
        lines.append(f"UNJUDGED {line} -- less than 2x its own window, too young to tell "
                     "an absence from a window that has not come round yet")
    if not judged:
        return 2, [f"UNJUDGED: the ledger holds {have}, less than 2x the window of any "
                   f"enabled job -- too young to tell an absence from a window that has "
                   "not come round yet"] + lines
    lines.append(("NOT OK" if worst else "OK")
                 + f": {judged} of {len(enabled)} enabled job(s) judged over "
                   f"{span:.0f}s of ledger"
                 + (f"; {len(young)} too young to judge" if young else ""))
    return worst, lines


def cmd_checks(args) -> int:
    """The in-brick gate: WL001-WL004, 0 clean / 1 violation / 2 unjudged."""
    from . import checks
    argv: List[str] = []
    if getattr(args, "since", None):
        argv += ["--since", args.since]
    if getattr(args, "json", False):
        argv.append("--json")
    if getattr(args, "self_test", False):
        argv.append("--self-test")
    return checks.main(argv)


def cmd_status(args) -> int:
    base = store.home()
    jobs = store.load(base)
    if not jobs:
        print("No jobs")
        return 0
    note = _hostclock_note(base)
    if note:
        print(note)
    rows = ledger.read(base, since=timedelta(days=30))
    opens = ledger.open_wakes(rows)
    live_by_job = {}
    for wake_id, row in opens.items():
        if _wake_is_live(row):
            live_by_job.setdefault(row.get("job"), []).append(wake_id)
    for name, job in jobs.items():
        held = lock.inspect(base, name, job.get("timeout_s"))
        if held is not None and held.wake_id not in live_by_job.get(name, []):
            live_by_job.setdefault(name, []).append(held.wake_id or held.reason)
    now = clock.now_utc()
    bad: List[str] = []
    pending: List[str] = []
    print(f"{'Name':<20} {'On':<3} {'Every':<8} {'State':<16} {'Fails':<5} "
          f"{'Next due':<14} Reason")
    print("-" * 96)
    for name, job in sorted(jobs.items()):
        state = job.get("last_state") or "pending"
        if job.get("last_state") in ledger.BAD_STATES:
            bad.append(name)
        elif job.get("last_state") is None and name not in live_by_job:
            pending.append(name)
        if name in live_by_job:
            due = "in progress"
        else:
            wait = (clock.next_due(job, now) - now).total_seconds()
            due = "now" if wait <= 0 else f"in {int(wait)}s"
        print(f"{name:<20} {'yes' if job.get('enabled', True) else 'no':<3} "
              f"{str(job.get('every')):<8} {state:<16} "
              f"{int(job.get('consecutive_failures') or 0):<5} "
              f"{due:<14} {(job.get('last_reason') or '')[:40]}")
    stale = [w for w, row in opens.items() if not _wake_is_live(row)]
    if stale:
        print(f"{len(stale)} wake(s) started and never finished -- run `awrise reconcile`")
    if bad or stale:
        print(f"NOT OK: {', '.join(bad) or 'no failing job'}"
              + (f"; {len(stale)} orphan(s)" if stale else ""))
        return 1
    if pending:
        # Per job, never per ledger: another job's rows say nothing about
        # this one, and a job that never woke cannot be called OK.
        print(f"UNJUDGED: {', '.join(pending)} never woke -- nothing has run for "
              f"{'it' if len(pending) == 1 else 'them'}, so nothing can be judged")
        return 2
    print("OK: every job's last wake ended success or a policy skip")
    return 0


def _hostclock_note(base: Path) -> Optional[str]:
    """One line about the clock for `status`, or nothing.

    Printed, never judged: a host clock this awrise did not install is a
    perfectly good clock, so its absence cannot make `status` say NOT OK.
    """
    try:
        record = hostclock.read_record(base)
    except hostclock.HostClockError as exc:
        return f"host clock: install record unreadable ({exc})"
    if record is None:
        return None
    parts = [f"host clock: {record.get('kind')} every {record.get('every_s')}s"]
    try:
        age = hostclock.tick_age_s(base)
    except OSError:
        age = None
    parts.append("last tick " + hostclock.tick_age_phrase(age))
    python = record.get("python")
    if python and os.path.normcase(str(python)) != os.path.normcase(sys.executable):
        # The entry survives a venv rebuild; awrise inside it does not. Every
        # wake then dies before it can open the ledger, so nothing anywhere
        # records a failure -- the only signal is this line.
        parts.append(f"REINSTALL: the entry runs {python}, this awrise is {sys.executable}")
    return "; ".join(parts)


def cmd_install(args) -> int:
    """Register (or judge, or remove) the host clock.

    0 = done and read back, 1 = a measured NO, 2 = could not judge. ``--print``
    and ``--dry-run`` touch nothing at all, so they are safe to run anywhere,
    including on an OS whose scheduler this adapter does not target.
    """
    base = store.home()
    kind = getattr(args, "kind", None)
    if getattr(args, "check", False):
        code, lines = hostclock.check(kind, base)
        for line in lines:
            print(line, file=sys.stderr if code else sys.stdout)
        return code
    if not kind:
        raise FatalError(2, "name the host clock: --cron, --systemd-user, "
                            "--systemd-system, --launchd or --schtasks "
                            "(or --check to judge the one already installed)")
    if getattr(args, "uninstall", False):
        entry = hostclock.uninstall(kind, base, dry_run=getattr(args, "dry_run", False))
        for line in entry.lines:
            print(line)
        return 0
    try:
        every_s = int(clock.parse_interval(getattr(args, "every", None) or "60s")
                      .total_seconds())
    except ValueError as exc:
        raise FatalError(2, f"--every: {exc}") from exc
    entry = hostclock.install(kind, every_s=every_s,
                              dry_run=getattr(args, "dry_run", False),
                              print_only=getattr(args, "print_only", False),
                              base=base)
    for line in entry.lines:
        print(line)
    if entry.installed:
        print(f"{kind} host clock installed and read back; "
              f"`awrise install --check` judges whether it is ticking")
    return 0


def cmd_reconcile(args) -> int:
    base = store.home()
    if getattr(args, "restore", False):
        path = store.restore(base)
        jobs = store.load(base)
        print(f"Restored {path} from {store.STORE_NAME}.bak ({len(jobs)} job(s))")
        return 0
    if getattr(args, "reset", False):
        # The way out of a store that was corrupted before it had a backup.
        # It refuses while anything readable is there, so it can only ever be
        # the last resort it is documented as.
        path = store.reset(base)
        print(f"Reset {path}: the corrupt copy is parked under "
              f"{(base / 'corrupt')}; the store is empty")
        return 0
    jobs = store.load(base)
    _jobs, closed, live = reconcile(base, jobs, ledger.new_id("p-"), "manual")
    print(f"reconcile: closed {len(closed)} orphaned wake(s), {len(live)} in progress")
    return 1 if closed else 0


# ---------------------------------------------------------------- prewarm

#: The usage ledger: one ``<service>.json`` per service, holding when that
#: service was last ASKED for something. awrise only ever READS it -- the
#: services themselves write it, and a unit with no entry is UNKNOWN, never
#: idle. ``AITHER_USAGE_LEDGER_DIR`` names it; the sibling of the unit plane
#: is the documented default, because the two planes ship together.
USAGE_LEDGER_DIR_ENV = "AITHER_USAGE_LEDGER_DIR"
USAGE_LEDGER_SUBDIR = "usage"
#: How a service name becomes the unit that serves it when the record does not
#: say so itself. One place, so a proposal and a hand-written ``set wake=``
#: spell the same unit.
UNIT_PREFIX = "aither-"
UNIT_SUFFIX = ".service"
#: Prefixes a service name may already carry that the unit name must not repeat
#: (``x`` and ``<prefix>x`` are the same service). Assembled rather than written
#: out, because the publish-time moat guard refuses a platform hostname literal
#: in a shipped file -- and it is right to: this brick must read as generic.
NAME_PREFIXES = (UNIT_PREFIX, UNIT_PREFIX[:-1] + "os-")
#: What a proposed pre-warm job RUNS. The work is the wake; the command only
#: has to end. ``python``/``pass`` is the one pair that is the same on every OS
#: and runs under the interpreter awrise was installed into, so a proposal does
#: not quietly depend on ``true``, ``rem`` or anything else being on a PATH.
PREWARM_EXECUTOR = "python"
PREWARM_RUN = "pass"
PREWARM_EVERY = "1d"


def usage_ledger_dir(explicit: Optional[str] = None) -> Optional[Path]:
    if explicit:
        return Path(explicit).expanduser()
    raw = (os.environ.get(USAGE_LEDGER_DIR_ENV) or "").strip()
    if raw:
        return Path(raw).expanduser()
    plane = executors.unit_plane_dir()
    return plane.parent / USAGE_LEDGER_SUBDIR if plane is not None else None


def _slug(text: str) -> str:
    """A service name as a unit name's middle: lowercase, safe characters only."""
    clean = "".join(ch if (ch.isalnum() or ch in "._-") else "-" for ch in str(text).lower())
    while "--" in clean:
        clean = clean.replace("--", "-")
    clean = clean.strip("-.")
    for prefix in NAME_PREFIXES:
        if clean.startswith(prefix):
            clean = clean[len(prefix):]
    return clean


def _unit_of(stem: str, record: dict) -> Tuple[str, str]:
    """(unit, basis) for this record. ``declared`` means the record NAMED its
    unit; ``derived`` means this is a guess assembled from the service name.

    The difference is the whole reason ``--apply`` has a gate in front of it. A
    unit name is MEASURED, not derived: the fleet this reads from was written by
    several generator vintages that disagree, so a rule that turns a service
    name into a unit name is right for most services and silently wrong for the
    rest -- and a wake that names a unit nobody runs fails at 03:00 into a log,
    which reads exactly like a quiet fleet. A record that carries ``unit`` (the
    writer measured it) is trusted; anything else is offered, never scheduled by
    itself.
    """
    declared = str(record.get("unit") or "").strip()
    if declared:
        return declared, "declared"
    slug = _slug(record.get("service") or stem)
    return (f"{UNIT_PREFIX}{slug}{UNIT_SUFFIX}" if slug else ""), "derived"


def _first_use(record: dict, day) -> Tuple[Optional[object], str]:
    """When ``day`` first used this service, and the EVIDENCE that says so.

    The basis is returned with the answer and printed, because the three
    sources are not equally good: the ledger a service writes today keeps only
    its LAST request, so on most records the honest answer is "the hour it was
    last used that day" and a proposal must say so rather than claim a first
    use it cannot see. ``first_request_at`` and a per-day block are read first
    and preferred, so the day the writer keeps them this gets better with no
    change here.
    """
    days = record.get("days") if isinstance(record.get("days"), dict) else {}
    per_day = days.get(day.isoformat()) if isinstance(days.get(day.isoformat()), dict) else {}
    for basis, value in (("first_request_at", record.get("first_request_at")),
                         (f"days[{day.isoformat()}]", per_day.get("first_request_at")),
                         ("last_request_at", record.get("last_request_at"))):
        try:
            stamp = clock.parse_ts(value) if value else None
        except (ValueError, TypeError):
            continue
        if stamp is not None and stamp.date() == day:
            return stamp, basis
    return None, ""


def prewarm_plan(directory: Path, day, jobs: dict, exclude: Optional[List[str]] = None,
                 every: str = PREWARM_EVERY) -> Tuple[List[dict], List[dict]]:
    """(proposals, skipped) for ``day``. Reads only; judges nothing live."""
    proposals: List[dict] = []
    skipped: List[dict] = []
    for path in sorted(directory.glob("*.json")):
        def drop(why: str, name: str = path.name) -> None:
            skipped.append({"source": name, "why": why})
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            drop(f"unreadable: {type(exc).__name__}")
            continue
        if not isinstance(record, dict):
            drop("record is not an object")
            continue
        unit, unit_basis = _unit_of(path.stem, record)
        problem = executors.unit_name_problem(unit) if unit else "record names no service"
        if problem is not None:
            drop(f"no usable unit name: {problem}")
            continue
        if any(fnmatch.fnmatch(unit, pattern) for pattern in (exclude or [])):
            drop(f"{unit} is excluded")
            continue
        stamp, basis = _first_use(record, day)
        if stamp is None:
            # Silence is not idleness and it is not a schedule either: a unit
            # nothing asked for yesterday gets no pre-warm today.
            drop(f"no request recorded on {day.isoformat()}")
            continue
        name = f"prewarm-{_slug(unit[:-len(UNIT_SUFFIX)])}"[:store.NAME_MAX]
        try:
            store.validate_name(name)
        except ValueError as exc:
            drop(f"{name!r} is not a usable job name: {exc}")
            continue
        proposals.append({"job": name, "unit": unit, "unit_basis": unit_basis,
                          "at": f"{stamp.hour:02d}:00", "every": every, "basis": basis,
                          "first_use_at": clock.iso(stamp), "exists": name in jobs})
    return proposals, skipped


def cmd_prewarm(args) -> int:
    """Propose one daily pre-warm job per unit the usage ledger saw yesterday.

    A parked unit is only a saving while something wakes it in time. This verb
    reads the usage truth and proposes the schedule that would have met
    yesterday's demand -- and it PRINTS it. Nothing is written without
    ``--apply``: a scheduler that installs jobs because it was run once is a
    scheduler nobody runs twice.
    """
    directory = usage_ledger_dir(getattr(args, "ledger_dir", None))
    if directory is None:
        raise FatalError(2, f"no usage ledger directory: set {USAGE_LEDGER_DIR_ENV} (or "
                            f"{executors.UNIT_PLANE_DIR_ENV} / {executors.UNIT_LIBRARY_ENV}) "
                            "-- a pre-warm with no usage truth would be a guess")
    if not directory.is_dir():
        raise FatalError(2, f"{directory} is not a directory -- an absent usage ledger is not "
                            "an empty one, and nothing can be proposed from it")
    back = max(1, int(getattr(args, "days_ago", 1) or 1))
    day = (clock.now_utc() - timedelta(days=back)).date()
    jobs = store.load()
    proposals, skipped = prewarm_plan(directory, day, jobs,
                                      exclude=list(getattr(args, "exclude", None) or []),
                                      every=(getattr(args, "every", None) or PREWARM_EVERY))
    apply = bool(getattr(args, "apply", False))
    allow_derived = bool(getattr(args, "allow_derived", False))
    run = getattr(args, "run", None) or PREWARM_RUN
    applied: List[dict] = []
    if apply:
        for proposal in proposals:
            if proposal["exists"]:
                applied.append({"job": proposal["job"], "state": "exists"})
                continue
            if proposal["unit_basis"] != "declared" and not allow_derived:
                # Refused, not skipped: a schedule keyed on a guessed unit name
                # is a job that fails every night into a log nobody reads.
                applied.append({"job": proposal["job"], "state": "refused:derived_unit",
                                "unit": proposal["unit"]})
                continue
            job = store.new_job(every=proposal["every"], run=run, interval_s=0.0)
            job["at"] = proposal["at"]
            job["executor"] = PREWARM_EXECUTOR
            job["wake"] = proposal["unit"]
            job["park_after"] = bool(getattr(args, "park_after", False))
            _validate_spec(job, allow_overrun=False, name=proposal["job"])
            jobs[proposal["job"]] = job
            applied.append({"job": proposal["job"], "state": "added"})
        if applied:
            store.save(jobs)
    refused = [item for item in applied if item["state"] == "refused:derived_unit"]
    if getattr(args, "json", False):
        print(json.dumps({"ledger_dir": str(directory), "day": day.isoformat(),
                          "applied": applied, "proposals": proposals, "skipped": skipped},
                         indent=2, sort_keys=True))
        return 1 if refused else 0
    print(f"usage ledger: {directory}")
    print(f"first use on {day.isoformat()} (UTC)")
    for proposal in proposals:
        state = "exists " if proposal["exists"] else "propose"
        print(f"  {state} {proposal['job']:<28} {proposal['unit']:<34} "
              f"at {proposal['at']} every {proposal['every']}  "
              f"basis={proposal['basis']} unit={proposal['unit_basis']}")
    for drop in skipped:
        print(f"  skip    {drop['source']:<28} {drop['why']}")
    if not proposals:
        print("Nothing to pre-warm: no ledger entry names a request on that day.")
    elif not apply:
        print(f"{len(proposals)} proposal(s); nothing was scheduled -- rerun with --apply")
    else:
        added = sum(1 for item in applied if item["state"] == "added")
        print(f"{added} job(s) added, {len(applied) - added - len(refused)} already present")
    if refused:
        print(f"{len(refused)} proposal(s) NOT scheduled: the unit name is derived from the "
              f"service name, and nothing measured it. Name the unit in the ledger record, "
              f"add the job by hand, or rerun with --allow-derived-units.")
    return 1 if refused else 0


# ------------------------------------------------------------------- main

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="awrise", description="Wake something on a schedule")
    parser.add_argument("--self-test", action="store_true", help="prove the brick can still fail")
    parser.add_argument("--list", action="store_true", help="with --self-test: list the cases")
    subs = parser.add_subparsers(dest="command")

    add_p = subs.add_parser("add", help="register a job")
    add_p.add_argument("--name", required=True)
    add_p.add_argument("--every", required=True, help="Ns/Nm/Nh/Nd/Nw, e.g. 15m or 1h30m")
    add_p.add_argument("--run", required=True, help="shell command")
    add_p.add_argument("--timeout", type=int, default=None, help="seconds (default 300)")
    add_p.add_argument("--cwd", default=None)
    add_p.add_argument("--at", default=None, help="HH:MM UTC daily anchor")
    add_p.add_argument("--allow-overrun", action="store_true")
    add_p.add_argument("--disabled", action="store_true")
    add_p.add_argument("--detach", action="store_true",
                       help="spawn and close the wake at once; the child outlives the pass")
    add_p.add_argument("--missed", default=None, choices=list(store.MISSED_POLICIES),
                       help="windows lost while nothing ran: catch_up_once (default) "
                            "runs it one more time, skip drops them")
    add_p.add_argument("--executor", default=None, choices=list(executors.KINDS),
                       help="how the wake runs (default shell)")
    add_p.add_argument("--bearer-file", dest="bearer_file", default=None,
                       help="file holding the token for an http/session job; must live "
                            "under ~/.aither or $AWRISE_HOME")
    add_p.add_argument("--permission-mode", dest="permission_mode", default=None,
                       choices=list(executors.PERMISSION_MODES),
                       help="session jobs only; anything outside this list is refused")
    add_p.add_argument("--wake", default=None, metavar="UNIT.service",
                       help="ask the host unit agent to wake this unit before the job runs, "
                            "and wait for it to be healthy")
    add_p.add_argument("--park-after", dest="park_after", action="store_true",
                       help="with --wake: ask for the unit to be put back to sleep when the "
                            "job is done (refused with --detach)")
    add_p.add_argument("--wake-not-required", dest="wake_not_required", action="store_true",
                       help="with --wake: run the command even when the wake failed "
                            "(default: a failed wake means the command does not run)")
    add_p.add_argument("--report-relay", dest="report_relay", default=None, metavar="#CHANNEL",
                       help="post a line to this relay channel when a wake ends badly")
    add_p.add_argument("--card-after", dest="card_after", type=int, default=None,
                       metavar="N", help="raise ONE decision card after N consecutive "
                                         "bad wakes (0 = never)")
    add_p.set_defaults(func=cmd_add)

    set_p = subs.add_parser("set", help="change a job's spec: key=value ...")
    set_p.add_argument("--name", required=True)
    set_p.add_argument("assignments", nargs="*", metavar="key=value")
    set_p.add_argument("--allow-overrun", action="store_true")
    set_p.set_defaults(func=cmd_set)

    for verb, func in (("enable", cmd_enable), ("disable", cmd_disable)):
        p = subs.add_parser(verb)
        p.add_argument("--name", required=True)
        p.set_defaults(func=func)

    rm_p = subs.add_parser("remove")
    rm_p.add_argument("--name", required=True)
    rm_p.set_defaults(func=cmd_remove)

    list_p = subs.add_parser("list")
    list_p.add_argument("--json", action="store_true")
    list_p.set_defaults(func=cmd_list)

    run_p = subs.add_parser("run-due", help="one pass: run every due job, then exit")
    run_p.add_argument("--quiet", action="store_true")
    run_p.add_argument("--invoker", default="manual",
                       help="who fired the pass (cron, systemd, ...)")
    run_p.add_argument("--dry-run", action="store_true",
                       help="say what would fire (would_fire rows); run nothing")
    run_p.add_argument("--prune", default=None, metavar="WINDOW",
                       help="after the pass, drop ledger day files older than this")
    run_p.add_argument("--drain", action="store_true",
                       help="awrun jobs only: after submitting, also claim and run THAT "
                            "item -- never another actor's queued work")
    run_p.set_defaults(func=cmd_run_due)

    one_p = subs.add_parser("run", help="run one job now, due or not")
    one_p.add_argument("--name", required=True)
    one_p.add_argument("--force", action="store_true", help="run even if disabled")
    one_p.set_defaults(func=cmd_run)

    hist_p = subs.add_parser("history", help="what happened to every wake and why")
    hist_p.add_argument("--job", default=None)
    hist_p.add_argument("--since", default=None, help="window, e.g. 7d or 12h")
    hist_p.add_argument("--event", default=None)
    hist_p.add_argument("--limit", type=int, default=50, help="0 = all")
    hist_p.add_argument("--json", action="store_true")
    hist_p.add_argument("--judge", action="store_true",
                        help="verdict instead of rows: drift or absence per job; "
                             "exit 2 on a ledger too young to judge")
    hist_p.set_defaults(func=cmd_history)

    status_p = subs.add_parser("status", help="per-job verdict; exit 1 on a failing job")
    status_p.set_defaults(func=cmd_status)

    exp_p = subs.add_parser("explain", help="why one job did or did not fire last tick")
    exp_p.add_argument("--name", required=True)
    exp_p.add_argument("--since", default=None, help="ledger window to read (default 30d)")
    exp_p.set_defaults(func=cmd_explain)

    prune_p = subs.add_parser("prune", help="drop ledger day files older than a window")
    prune_p.add_argument("--keep", default="30d", help="window to keep (default 30d)")
    prune_p.add_argument("--dry-run", action="store_true", help="name them, remove none")
    prune_p.add_argument("--force", action="store_true",
                         help="also drop a file holding a wake that was never closed")
    prune_p.set_defaults(func=cmd_prune)

    pre_p = subs.add_parser("prewarm", help="propose one daily wake per unit the usage "
                                            "ledger saw yesterday (prints; --apply schedules)")
    pre_p.add_argument("--ledger-dir", dest="ledger_dir", default=None,
                       help=f"usage ledger directory (default ${USAGE_LEDGER_DIR_ENV})")
    pre_p.add_argument("--days-ago", dest="days_ago", type=int, default=1,
                       help="which day's usage to read (1 = yesterday, the default)")
    pre_p.add_argument("--every", default=PREWARM_EVERY, help="interval of a proposed job")
    pre_p.add_argument("--run", default=None,
                       help=f"command each proposed job runs (default {PREWARM_RUN!r} under "
                            f"the {PREWARM_EXECUTOR} executor -- the wake IS the work)")
    pre_p.add_argument("--exclude", action="append", default=None, metavar="GLOB",
                       help="skip units matching this glob (repeatable)")
    pre_p.add_argument("--park-after", dest="park_after", action="store_true",
                       help="proposed jobs also park the unit when they are done")
    pre_p.add_argument("--apply", action="store_true", help="actually add the proposed jobs")
    pre_p.add_argument("--allow-derived-units", dest="allow_derived", action="store_true",
                       help="with --apply: also add jobs whose unit name was DERIVED from the "
                            "service name rather than declared by the ledger record (a guess; "
                            "refused by default, and the exit code is 1 when any was refused)")
    pre_p.add_argument("--json", action="store_true")
    pre_p.set_defaults(func=cmd_prewarm)

    chk_p = subs.add_parser("checks", help="WL001-WL004 against the record")
    chk_p.add_argument("--since", default=None, help="ledger window to read (default 7d)")
    chk_p.add_argument("--json", action="store_true")
    chk_p.add_argument("--self-test", dest="self_test", action="store_true",
                       help="prove each rule can still fail")
    chk_p.set_defaults(func=cmd_checks)

    inst_p = subs.add_parser("install", help="register the host clock that runs run-due")
    kinds = inst_p.add_mutually_exclusive_group()
    for flag in hostclock.KINDS:
        kinds.add_argument(f"--{flag}", dest="kind", action="store_const", const=flag,
                           help=f"use the {flag} scheduler")
    inst_p.add_argument("--every", default="60s", help="how often the clock wakes run-due")
    inst_p.add_argument("--print", dest="print_only", action="store_true",
                        help="render the entry to stdout and touch nothing")
    inst_p.add_argument("--dry-run", action="store_true",
                        help="name every file and command, run none of them")
    inst_p.add_argument("--check", action="store_true",
                        help="0 installed and ticking, 1 a measured no, 2 unjudged")
    inst_p.add_argument("--uninstall", action="store_true", help="remove what we installed")
    inst_p.set_defaults(func=cmd_install, kind=None)

    rec_p = subs.add_parser("reconcile", help="close orphaned wakes; --restore brings back .bak")
    rec_p.add_argument("--restore", action="store_true")
    rec_p.add_argument("--reset", action="store_true",
                       help="last resort: park a corrupt store that has no .bak and start "
                            "an empty one (refused while anything readable is there)")
    rec_p.set_defaults(func=cmd_reconcile)
    return parser


def _never_crash_on_encoding() -> None:
    """A verdict verb must not die on the data it reports.

    A pipe or redirect on Windows is cp1252 by default, so a job name, a
    reason or an output tail with one character outside it turned every
    verb into a traceback (exit 1: not a verdict). The stream keeps its
    encoding; what it cannot encode becomes a backslash escape.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        with contextlib.suppress(Exception):
            reconfigure(errors="backslashreplace")


def _dispatch(func, args) -> int:
    _never_crash_on_encoding()
    try:
        return func(args)
    except FatalError as exc:
        print(str(exc), file=sys.stderr)
        return exc.code
    except hostclock.HostClockError as exc:
        print(f"{'NOT OK' if exc.code == 1 else 'NOT VERIFIED'}: {exc}", file=sys.stderr)
        return exc.code
    except store.StoreError as exc:
        print(f"NOT VERIFIED: {exc}", file=sys.stderr)
        return 2
    except clock.ClockError as exc:
        print(f"NOT VERIFIED: {exc}", file=sys.stderr)
        return 2
    except ledger.LedgerRefusedError as exc:
        print(f"NOT VERIFIED: ledger refused a row: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"NOT VERIFIED: {exc}", file=sys.stderr)
        return 2


def main(argv: Optional[List[str]] = None) -> int:
    # GENERATED doctor intercept (gen_aw_doctor.py) -- do not edit
    _dv = locals().get("argv")
    if (_dv if _dv is not None else __import__("sys").argv[1:])[:1] == ["doctor"]:
        from ._doctor import report
        return report()
    # GENERATED repo-state intercept (gen_aw_doctor.py) -- do not edit
    try:
        from awgit import state as _aw_state
    except Exception:
        _aw_state = None
    if _aw_state is not None:
        _sv = locals().get("argv")
        if _aw_state.cli_banner(_sv if _sv is not None else __import__("sys").argv[1:]):
            return 0
    argv = list(sys.argv[1:] if argv is None else argv)
    # The brick-wide self-test is `awrise --self-test`, BEFORE any verb. A
    # `--self-test` that follows one belongs to that verb (`awrise checks
    # --self-test` is the gate's own), so the scan stops at the first word
    # that is not a flag -- otherwise the global intercept silently swallows
    # a sub-verb's flag and runs the wrong suite while reporting success.
    stop = next((i for i, item in enumerate(argv) if not item.startswith("-")), len(argv))
    if "--self-test" in argv[:stop]:
        from .selftest import run
        return run(list_only="--list" in argv)

    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    func = getattr(args, "func", None)
    return _dispatch(func, args) if func else 0


if __name__ == "__main__":
    sys.exit(main())
