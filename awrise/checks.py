"""awrise's own gate: four rules read straight off the record, and a verdict.

``python -m awrise.checks`` (or ``awrise checks``) answers, without running
anything, whether the wake ledger still says what it is supposed to say:

* **WL001** every ``started`` row reached a ``finished`` row -- unless that
  wake can still be running. An open wake nothing can close is a job whose
  outcome nobody knows.
* **WL002** every row's vocabulary is inside the closed sets. A line that is
  not JSON, or whose event/state is not one the writer can produce, was not
  written by awrise, and a reader that accepts it is a reader that can be
  lied to.
* **WL003** an enabled job that has been due for more than two of its own
  windows with no wake in the record: the clock is not reaching it. Suppressed
  while one of its wakes is still in flight, because a scheduler that refuses
  to start a second copy produces exactly the same silence as a dead clock.
  Judged per job against that job's OWN window, so a weekly job cannot make a
  starved 1-minute job unjudgeable.
* **WL004** a lock directory whose holder is gone or past its age bound. It is
  what makes the next pass skip the job, so it is a starved job in waiting.

Exit codes are the contract, and silence is never a pass: 0 = judged clean,
1 = at least one measured violation, 2 = the record cannot support a verdict
(no jobs, no rows, or -- for every job in it -- a ledger younger than twice
that job's own window: a record younger than the thing it is being asked
about can only guess).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from . import clock, ledger, lock, store

#: How much record a cadence needs before a verdict about it means anything.
#: The same factor the platform's routine gates use: under two full windows
#: an absence is indistinguishable from "it has not come round yet".
WINDOW_FACTOR = 2
#: How many windows a job may be overdue before WL003 calls the clock dead.
OVERDUE_FACTOR = 2
#: Default window of ledger to read.
DEFAULT_SINCE = "7d"

RULES = ("WL001", "WL002", "WL003", "WL004")

OK, VIOLATION, UNJUDGED = 0, 1, 2


class Finding:
    """One rule's verdict, with the evidence it was reached from."""

    def __init__(
        self, rule: str, code: int, summary: str, details: Optional[List[str]] = None
    ) -> None:
        self.rule = rule
        self.code = code
        self.summary = summary
        self.details = details or []

    def as_dict(self) -> dict:
        return {
            "rule": self.rule,
            "code": self.code,
            "verdict": _word(self.code),
            "summary": self.summary,
            "details": self.details,
        }

    def lines(self) -> List[str]:
        head = f"{_word(self.code):<9} {self.rule}  {self.summary}"
        return [head] + [f"            {detail}" for detail in self.details]


def _word(code: int) -> str:
    return {OK: "OK", VIOLATION: "NOT OK", UNJUDGED: "UNJUDGED"}.get(code, "UNJUDGED")


# ------------------------------------------------------------------ reading


def _row_ts(row: dict) -> Optional[datetime]:
    try:
        return clock.parse_ts(row.get("ts"))
    except (ValueError, TypeError):
        return None


def _int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _wake_is_live(row: dict) -> bool:
    """Could this open wake still be running? pid alive AND inside its bound."""
    stamp = _row_ts(row)
    if stamp is None:
        return False
    age = (clock.now_utc() - stamp).total_seconds()
    bound = float(_int(row.get("timeout_s")) or 300) + 60.0
    pid = _int(row.get("pid"))
    return pid > 0 and age < bound and lock.is_alive(pid)


def span_s(rows: Sequence[dict]) -> Optional[float]:
    """Seconds between the oldest and the newest row awrise WROTE, or None.

    ``unreadable`` rows never count. They stand for a line the reader could
    not parse -- the ordinary outcome of a crash mid-append -- and they carry
    no stamp of their own, so counting them would let one truncated line lend
    the record a history it does not have and turn the window guard below
    from UNJUDGED into a green verdict on a ledger seconds old.
    """
    stamps = [
        ts
        for ts in (_row_ts(row) for row in rows if row.get("event") != "unreadable")
        if ts is not None
    ]
    if len(stamps) < 2:
        return None
    return (max(stamps) - min(stamps)).total_seconds()


def longest_cadence_s(jobs: Dict[str, dict]) -> float:
    periods = [clock.period_s(job) for job in jobs.values() if job.get("enabled", True)]
    return max(periods) if periods else 0.0


# -------------------------------------------------------------------- rules


def wl001_every_started_wake_was_closed(rows: Sequence[dict], **_kw) -> Finding:
    opens = ledger.open_wakes(rows)
    stale = {wake: row for wake, row in opens.items() if not _wake_is_live(row)}
    live = len(opens) - len(stale)
    if not stale:
        return Finding(
            "WL001", OK, f"every started wake reached a finished row ({live} still in flight)"
        )
    details = [
        f"{wake} job={row.get('job')} started {row.get('ts')} -- no finished row; "
        "`awrise reconcile` closes it"
        for wake, row in sorted(stale.items())
    ]
    return Finding("WL001", VIOLATION, f"{len(stale)} wake(s) started and never finished", details)


def wl002_the_ledger_vocabulary_is_closed(rows: Sequence[dict], **_kw) -> Finding:
    bad: List[str] = []
    for row in rows:
        event = row.get("event")
        if event == "unreadable":
            bad.append(f"unreadable line: {row.get('reason')}")
            continue
        if event not in ledger.EVENTS:
            bad.append(f"{row.get('ts')} unknown event {event!r}")
            continue
        state = row.get("state")
        if state is not None and state not in ledger.STATES:
            bad.append(f"{row.get('ts')} {event} unknown state {state!r}")
            continue
        reason = row.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            bad.append(f"{row.get('ts')} {event} has no reason")
    if not bad:
        return Finding(
            "WL002",
            OK,
            f"{len(rows)} row(s), every event, state and reason inside the closed vocabulary",
        )
    return Finding("WL002", VIOLATION, f"{len(bad)} row(s) awrise could not have written", bad[:20])


def wl003_every_enabled_job_is_being_woken(
    rows: Sequence[dict], jobs: Dict[str, dict], base: Path, **_kw
) -> Finding:
    enabled = {name: job for name, job in jobs.items() if job.get("enabled", True)}
    if not enabled:
        return Finding("WL003", UNJUDGED, "no enabled job to judge")
    if longest_cadence_s(enabled) <= 0:
        return Finding("WL003", UNJUDGED, "no job declares a window to judge against")
    span = span_s(rows)
    have = "nothing" if span is None else f"{span:.0f}s"
    now = clock.now_utc()
    starved: List[str] = []
    young: List[str] = []
    judged = 0
    for name, job in sorted(enabled.items()):
        period = clock.period_s(job)
        if period <= 0:
            young.append(f"{name}: declares no window to judge against")
            continue
        # The guard is THIS job's own window. Keyed on the longest cadence in
        # the store instead, one monthly job blinds the rule to every
        # 1-minute job for a month -- a gate that can never reach a verdict,
        # which is the opposite of what a gate is for.
        if span is None or span < period * WINDOW_FACTOR:
            young.append(f"{name}: window {int(period)}s, the ledger holds {have}")
            continue
        judged += 1
        if lock.inspect(base, name, job.get("timeout_s")) is not None:
            continue  # a wake is in flight: the silence is the job, not the clock
        overdue = (now - clock.next_due(job, now)).total_seconds()
        if overdue > period * OVERDUE_FACTOR:
            windows = int(overdue // period)
            last = job.get("last_started_at") or "never"
            starved.append(
                f"{name}: due {int(overdue)}s ago ({windows} window(s) "
                f"of {int(period)}s), last wake {last}"
            )
    if starved:
        return Finding(
            "WL003",
            VIOLATION,
            f"{len(starved)} enabled job(s) overdue with no wake -- the clock is "
            "not reaching awrise",
            starved + _too_young(young),
        )
    if not judged:
        return Finding(
            "WL003",
            UNJUDGED,
            f"the ledger holds {have}, less than {WINDOW_FACTOR}x the window of "
            "any enabled job -- an absence here cannot be told from a window "
            "that has not come round yet",
            young,
        )
    return Finding(
        "WL003",
        OK,
        f"{judged} of {len(enabled)} enabled job(s) judged, none overdue by more "
        f"than {OVERDUE_FACTOR} windows" + (f"; {len(young)} too young to judge" if young else ""),
        _too_young(young),
    )


def _too_young(young: Sequence[str]) -> List[str]:
    """The jobs the window guard skipped, always named rather than implied."""
    return [f"not judged -- {line} (less than {WINDOW_FACTOR}x its own window)" for line in young]


def wl004_no_lock_outlives_its_wake(base: Path, jobs: Dict[str, dict], **_kw) -> Finding:
    folder = lock.locks_dir(base)
    if not folder.is_dir():
        return Finding("WL004", OK, "no lock directory yet")
    stale: List[str] = []
    held = 0
    try:
        entries = sorted(folder.iterdir())
    except OSError as exc:
        return Finding("WL004", UNJUDGED, f"locks/ cannot be listed: {exc}")
    for path in entries:
        job = path.name
        if job.startswith(lock.BREAK_PREFIX):
            stale.append(f"{job}: a broken lock nobody finished removing")
            continue
        timeout = (jobs.get(job) or {}).get("timeout_s")
        bound = float(_int(timeout) or lock.DEFAULT_TIMEOUT_S) + lock.GRACE_S
        verdict = lock.judge(job, path, bound)
        if isinstance(verdict, lock.Held):
            held += 1
            continue
        stale.append(f"{job}: {verdict} -- every pass skips this job until it is swept")
    if not stale:
        return Finding("WL004", OK, f"{held} live lock(s), none stale")
    return Finding("WL004", VIOLATION, f"{len(stale)} stale lock(s)", stale)


# --------------------------------------------------------------------- main


def run(base: Optional[Path] = None, since: Optional[timedelta] = None) -> List[Finding]:
    """Every rule, in order. Raises StoreError/OSError to the caller."""
    base = base or store.home()
    jobs = store.load(base)
    rows = ledger.read(base, since=since)
    findings = [
        wl001_every_started_wake_was_closed(rows=rows, jobs=jobs, base=base),
        wl002_the_ledger_vocabulary_is_closed(rows=rows, jobs=jobs, base=base),
        wl003_every_enabled_job_is_being_woken(rows=rows, jobs=jobs, base=base),
        wl004_no_lock_outlives_its_wake(rows=rows, jobs=jobs, base=base),
    ]
    if not jobs and not rows:
        findings.insert(
            0, Finding("WL000", UNJUDGED, "no jobs and an empty ledger: nothing to judge")
        )
    return findings


def verdict(findings: Sequence[Finding]) -> int:
    """A measured NO outranks a could-not-judge; silence is never a pass."""
    if any(f.code == VIOLATION for f in findings):
        return VIOLATION
    if any(f.code == UNJUDGED for f in findings):
        return UNJUDGED
    return OK if findings else UNJUDGED


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="awrise checks",
        description="WL001-WL004 against the wake ledger: 0 clean, 1 violation, 2 unjudged",
    )
    parser.add_argument(
        "--since", default=DEFAULT_SINCE, help=f"ledger window to read (default {DEFAULT_SINCE})"
    )
    parser.add_argument("--json", action="store_true", help="one JSON object per rule")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="prove each rule can still fail, and its twin still pass",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.self_test:
        return self_test()
    try:
        since = clock.parse_interval(args.since)
    except ValueError as exc:
        print(f"UNJUDGED: --since {exc}", file=sys.stderr)
        return UNJUDGED
    try:
        findings = run(since=since)
    except (store.StoreError, ledger.LedgerRefusedError, clock.ClockError, OSError) as exc:
        print(f"UNJUDGED: {exc}", file=sys.stderr)
        return UNJUDGED
    code = verdict(findings)
    if args.json:
        print(
            json.dumps(
                {"verdict": _word(code), "code": code, "findings": [f.as_dict() for f in findings]},
                indent=2,
                sort_keys=True,
            )
        )
        return code
    # Every rule's line goes to ONE stream, in rule order: a report split
    # across stdout and stderr interleaves unpredictably and reads as if the
    # rules ran out of order. Only the verdict follows the exit code.
    for finding in findings:
        for line in finding.lines():
            print(line)
    print(f"{_word(code)}: {len(findings)} rule(s) judged", file=sys.stderr if code else sys.stdout)
    return code


# ---------------------------------------------------------------- self-test


def _home_with(rows: Sequence[dict], jobs: Optional[dict] = None) -> Path:
    """A throwaway AWRISE_HOME holding exactly these rows and jobs."""
    base = Path(tempfile.mkdtemp(prefix="awrise-checks-"))
    (base / "ledger").mkdir()
    for row in rows:
        stamp = clock.parse_ts(row["ts"])
        path = ledger.day_file(base, stamp)
        with open(path, "a", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(row) + "\n")
    if jobs:
        previous = os.environ.get("AWRISE_HOME")
        os.environ.update({"AWRISE_HOME": str(base)})
        try:
            store.save(jobs, base)
        finally:
            if previous is None:
                os.environ.pop("AWRISE_HOME", None)
            else:
                os.environ.update({"AWRISE_HOME": previous})
    return base


def _row(event: str, ts: datetime, **extra) -> dict:
    row = {
        "schema": 1,
        "wake_id": extra.pop("wake_id", "w-aaaaaaaa"),
        "pass_id": "p-aaaaaaaa",
        "ts": clock.iso(ts),
        "invoker": "selftest",
        "host": "h",
        "pid": 1,
        "interpreter": "x",
        "job": extra.pop("job", None),
        "event": event,
        "state": extra.pop("state", None),
        "reason": extra.pop("reason", "selftest"),
    }
    row.update(extra)
    return row


def _job(every: str = "1m", **extra) -> dict:
    job = store.new_job(every=every, run="echo hi")
    job.update(extra)
    return job


def _code(base: Path, rule: str, since: timedelta = timedelta(days=7)) -> int:
    for finding in run(base=base, since=since):
        if finding.rule == rule:
            return finding.code
    return UNJUDGED


def self_test() -> int:
    """Each rule fired by a fixture that breaks it, and quiet on its twin."""
    now = clock.now_utc()
    cases = []

    # WL001: a started row with no finished row, whose pid is long gone.
    open_wake = _home_with(
        [_row("started", now - timedelta(hours=2), job="j", wake_id="w-open0001", pid=0x7FFFFFFE)]
    )
    closed_wake = _home_with(
        [
            _row(
                "started", now - timedelta(hours=2), job="j", wake_id="w-shut0001", pid=0x7FFFFFFE
            ),
            _row(
                "finished",
                now - timedelta(hours=2),
                job="j",
                wake_id="w-shut0001",
                state="success",
                reason="exit_0",
            ),
        ]
    )
    cases.append(("WL001 fires on an open wake", _code(open_wake, "WL001"), VIOLATION))
    cases.append(("WL001 quiet on a closed wake", _code(closed_wake, "WL001"), OK))

    # WL002: a line awrise could not have written.
    foreign = _home_with([_row("started", now, job="j")])
    with open(ledger.day_file(foreign, now), "a", encoding="utf-8", newline="\n") as fh:
        fh.write("not json at all\n")
    cases.append(("WL002 fires on a foreign line", _code(foreign, "WL002"), VIOLATION))
    cases.append(("WL002 quiet on a written ledger", _code(closed_wake, "WL002"), OK))

    # WL003: an enabled job whose last wake is many windows old, with a ledger
    # span long enough to judge it.
    old = clock.iso(now - timedelta(hours=3))
    starved = _home_with(
        [
            _row("tick", now - timedelta(hours=3), reason="pass_start"),
            _row("tick", now, reason="pass_start"),
        ],
        {
            "j": _job(
                "1m",
                last_started_at=old,
                last_state="success",
                last_reason="exit_0",
                last_wake_id="w-old00001",
            )
        },
    )
    fresh = _home_with(
        [
            _row("tick", now - timedelta(hours=3), reason="pass_start"),
            _row("tick", now, reason="pass_start"),
        ],
        {
            "j": _job(
                "1m",
                last_started_at=clock.iso(now),
                last_state="success",
                last_reason="exit_0",
                last_wake_id="w-new00001",
            )
        },
    )
    young = _home_with(
        [_row("tick", now, reason="pass_start")], {"j": _job("1h", last_started_at=old)}
    )
    cases.append(("WL003 fires on a starved job", _code(starved, "WL003"), VIOLATION))
    cases.append(("WL003 quiet on a job just woken", _code(fresh, "WL003"), OK))
    cases.append(("WL003 unjudged on a young ledger", _code(young, "WL003"), UNJUDGED))

    # WL003's window guard is per job: one monthly job must not make a starved
    # 1-minute job unjudgeable (a gate that can never reach a verdict), and
    # per job must not mean lenient either -- the twin is still OK, not green
    # by accident.
    mixed = _home_with(
        [
            _row("tick", now - timedelta(hours=3), reason="pass_start"),
            _row("tick", now, reason="pass_start"),
        ],
        {
            "j": _job(
                "1m",
                last_started_at=old,
                last_state="success",
                last_reason="exit_0",
                last_wake_id="w-old00001",
            ),
            "monthly": _job("30d"),
        },
    )
    mixed_fresh = _home_with(
        [
            _row("tick", now - timedelta(hours=3), reason="pass_start"),
            _row("tick", now, reason="pass_start"),
        ],
        {
            "j": _job(
                "1m",
                last_started_at=clock.iso(now),
                last_state="success",
                last_reason="exit_0",
                last_wake_id="w-new00001",
            ),
            "monthly": _job("30d"),
        },
    )
    cases.append(("WL003 fires beside a long-cadence job", _code(mixed, "WL003"), VIOLATION))
    cases.append(("WL003 quiet beside a long-cadence job", _code(mixed_fresh, "WL003"), OK))

    # One unreadable line must not age the ledger: stamped at its day file's
    # midnight it bought a seconds-old record up to 24h of span, and the
    # young-ledger UNJUDGED became a green verdict.
    torn = _home_with(
        [_row("tick", now, reason="pass_start")], {"j": _job("10m", last_started_at=old)}
    )
    with open(ledger.day_file(torn, now), "a", encoding="utf-8", newline="\n") as fh:
        fh.write("{truncated\n")
    cases.append(
        ("WL003 unjudged with one torn line as the only age", _code(torn, "WL003"), UNJUDGED)
    )
    cases.append(("WL002 fires on that same torn line", _code(torn, "WL002"), VIOLATION))

    # WL004: a lock directory whose holder cannot be alive.
    stale_lock = _home_with([_row("tick", now, reason="pass_start")], {"j": _job("1m")})
    path = lock.lock_path(stale_lock, "j")
    path.mkdir(parents=True)
    with open(path / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {
                "wake_id": "w-lock0001",
                "pass_pid": 0x7FFFFFFE,
                "child_pid": 0,
                "started_at": clock.iso(now),
                "timeout_s": 300,
            },
            fh,
        )
    cases.append(("WL004 fires on a stale lock", _code(stale_lock, "WL004"), VIOLATION))
    cases.append(("WL004 quiet with no locks", _code(fresh, "WL004"), OK))

    # The verdict itself: a violation outranks an unjudged rule, and an empty
    # home is never a pass.
    empty = _home_with([])
    cases.append(("an empty home is unjudged", verdict(run(base=empty)), UNJUDGED))
    cases.append(
        (
            "a violation outranks unjudged",
            verdict([Finding("WL001", VIOLATION, "x"), Finding("WL003", UNJUDGED, "y")]),
            VIOLATION,
        )
    )

    failed = 0
    print("awrise.checks self-test")
    for label, got, want in cases:
        ok = got == want
        failed += 0 if ok else 1
        print(f"  {'PASS' if ok else 'FAIL'}  {label} (want {_word(want)}, got {_word(got)})")
    print(f"{len(cases) - failed}/{len(cases)} passed")
    return OK if not failed else VIOLATION


if __name__ == "__main__":
    raise SystemExit(main())
