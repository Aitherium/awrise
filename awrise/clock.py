"""Time, as awrise reads it: tz-aware UTC only, intervals as entered, dueness.

Every timestamp awrise writes is an ISO-8601 string with an explicit UTC
offset. A naive string read back from an older store is taken as UTC rather
than raising, so a 0.1.0 ``last_run`` never turns a pass into a traceback.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, time, timedelta, timezone
from typing import Callable, Optional, Tuple

_UNIT_SECONDS = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}
_TOKEN = re.compile(r"(\d+(?:\.\d+)?)\s*([smhdw])")
#: Longest interval accepted: 100 years. Past this ``last + interval`` can
#: leave the calendar, and a schedule nobody will live to see is a typo.
MAX_INTERVAL_S = 36500 * 86400.0
#: Longest timeout one wake may declare, ~46 days. The bound is the platform's,
#: not a taste: a wait is expressed in whole milliseconds in a 32-bit counter
#: here, so a deadline past this cannot be waited on at all -- the wait itself
#: raises, the child is never waited on and never killed, and the wake becomes
#: an unrecorded process nobody can find. A number that cannot be honoured is
#: refused where it is entered, not clamped where it is used.
MAX_TIMEOUT_S = 4_000_000
#: A stamp this far ahead of the clock is a clock that went backwards, not
#: jitter: one coarse tick on Windows is ~16 ms, an NTP slew never steps back.
SKEW_TOLERANCE_S = 1.0


class ClockError(ValueError):
    """The clock cannot be read as configured.

    A clock nobody can read is not a clock that says "now": every verdict in
    this brick is a distance between two times, so a misconfigured override
    is exit 2 (could not judge), never a silent fall back to the host clock.
    """


#: Test/operator override, in order: a hook set in-process, then ``AWRISE_NOW``.
#: The environment form is what a subprocess can be given: ``+900`` / ``-3600``
#: shifts the real clock by that many seconds (time still MOVES, which is what
#: a catch-up or a re-anchor has to be watched across), and an ISO stamp
#: freezes it. Anything else is refused rather than ignored.
_NOW_HOOK: Optional[Callable[[], datetime]] = None
NOW_ENV = "AWRISE_NOW"
_env_cache: Tuple[Optional[str], Optional[Tuple[str, object]]] = (None, None)


def set_now(hook: Optional[Callable[[], datetime]]) -> None:
    """Install (or with None remove) the process-wide clock hook."""
    global _NOW_HOOK
    _NOW_HOOK = hook


def reset_now() -> None:
    set_now(None)


def _parse_now_env(raw: str) -> Tuple[str, object]:
    global _env_cache
    cached_raw, cached = _env_cache
    if cached_raw == raw and cached is not None:
        return cached
    text = raw.strip()
    if text[:1] in "+-":
        try:
            parsed: Tuple[str, object] = ("offset", float(text))
        except ValueError as exc:
            raise ClockError(f"{NOW_ENV}={raw!r} is not an offset in seconds") from exc
    else:
        try:
            stamp = parse_ts(text)
        except ValueError as exc:
            raise ClockError(
                f"{NOW_ENV}={raw!r} is neither an offset in seconds (+900) nor an ISO timestamp"
            ) from exc
        if stamp is None:
            raise ClockError(f"{NOW_ENV}={raw!r} is empty")
        parsed = ("fixed", stamp)
    _env_cache = (raw, parsed)
    return parsed


def now_utc() -> datetime:
    if _NOW_HOOK is not None:
        value = _NOW_HOOK()
        if not isinstance(value, datetime):
            raise ClockError(f"the clock hook returned {type(value).__name__}, not a datetime")
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    raw = os.environ.get(NOW_ENV)
    if raw:
        kind, value = _parse_now_env(raw)
        if kind == "offset":
            return datetime.now(timezone.utc) + timedelta(seconds=float(value))
        return value  # type: ignore[return-value]
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    """Canonical form: seconds precision is not enough for a ledger, keep micros."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds")


def parse_ts(value: Optional[str]) -> Optional[datetime]:
    """ISO string -> aware datetime; naive input is read as UTC. None/"" -> None.

    Raises ValueError on an unparseable string -- and on a value that is not a
    string at all, because a number or a list where a stamp belongs is exactly
    as unreadable, and an AttributeError deep in a caller is not a verdict.
    Callers decide what that means (a field that cannot be read is a reason,
    never a silent "due").
    """
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError(f"not a timestamp: {value!r}")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_interval(interval_str: str) -> timedelta:
    """``Ns / Nm / Nh / Nd / Nw`` and compounds such as ``1h30m``.

    The string is stored exactly as entered and shown exactly as entered; only
    the derived ``interval_s`` is arithmetic. Zero, negative and unparseable
    input raise ValueError.
    """
    text = (interval_str or "").strip().lower()
    if not text:
        raise ValueError("Interval empty")
    pos = 0
    total = 0.0
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if not match:
            raise ValueError(f"Invalid: {interval_str}")
        total += float(match.group(1)) * _UNIT_SECONDS[match.group(2)]
        pos = match.end()
    if total <= 0:
        raise ValueError(f"Interval must be positive: {interval_str}")
    if total > MAX_INTERVAL_S:
        raise ValueError(f"Interval longer than 100 years: {interval_str}")
    return timedelta(seconds=total)


def parse_at(value: Optional[str]) -> Optional[time]:
    """``HH:MM`` (UTC) or None. Anything else raises ValueError."""
    if value is None or value == "":
        return None
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", str(value).strip())
    if not match:
        raise ValueError(f"Invalid --at (want HH:MM UTC): {value}")
    hour, minute = int(match.group(1)), int(match.group(2))
    if hour > 23 or minute > 59:
        raise ValueError(f"Invalid --at (want HH:MM UTC): {value}")
    return time(hour=hour, minute=minute, tzinfo=timezone.utc)


def next_due(job: dict, now: datetime) -> datetime:
    """When this job is next due, from its own ``last_started_at``.

    Interval jobs: last start + interval (never started -> now). Anchored
    jobs (``at``): the first anchor strictly after the last start, so a host
    asleep through the anchor catches up once and re-anchors instead of
    walking later by the job's duration.

    An anchored job that has NEVER started is anchored on when it was CREATED,
    by the same "first anchor strictly after" rule. Measured 2026-09-19: the
    branch returned ``now``, so ``add --every 1d --at 03:00`` at 14:00 ran the
    03:00 work at 14:00 and only began honouring the anchor after that
    accidental fire -- and ``prewarm --apply``, which creates every proposal
    with an anchor and no stamp, woke every proposed unit at once on the very
    next pass. A job with neither stamp (a hand-written record) is anchored on
    ``now``, which is the nearest honest reading of "it has not run yet".
    """
    last = parse_ts(job.get("last_started_at"))
    anchor = parse_at(job.get("at"))
    if anchor is not None:
        if last is None:
            try:
                since = parse_ts(job.get("created_at"))
            except (ValueError, TypeError):
                since = None
            if since is None:
                first = datetime.combine(now.date(), anchor)
                return first if first >= now else first + timedelta(days=1)
            last = since
        candidate = datetime.combine(last.date(), anchor)
        if candidate <= last:
            candidate += timedelta(days=1)
        return candidate
    if last is None:
        return now
    return last + timedelta(seconds=float(job.get("interval_s") or 0))


def is_due(job: dict, now: datetime) -> bool:
    return now >= next_due(job, now)


def period_s(job: dict) -> float:
    """How long one of this job's windows is.

    An anchored job has one window a day whatever its ``every`` says, so a
    missed-window count for ``at: 07:00`` counts days, not intervals.
    """
    if job.get("at"):
        return 86400.0
    try:
        return float(job.get("interval_s") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def missed_windows(job: dict, now: datetime) -> int:
    """Whole windows that came and went BEYOND the one now being served.

    A job due at 10:00, 11:00 and 12:00 whose host slept until 12:30 has
    missed two: the window it is about to be served for is the third. Zero
    for a job that is not due, and for a job with no measurable period.
    """
    due = next_due(job, now)
    if now < due:
        return 0
    period = period_s(job)
    if period <= 0:
        return 0
    return int((now - due).total_seconds() // period)


def skew_s(job: dict, now: datetime) -> Optional[float]:
    """Seconds the job's last start lies AHEAD of ``now`` (beyond tolerance),
    or None. A positive answer means the clock went backwards since that
    wake: the pass records it as an error and treats the job as due, so a
    stamp in the future never silences a job until the calendar catches up.
    """
    last = parse_ts(job.get("last_started_at"))
    if last is None:
        return None
    ahead = (last - now).total_seconds()
    if ahead <= SKEW_TOLERANCE_S:
        return None
    return round(ahead, 3)
