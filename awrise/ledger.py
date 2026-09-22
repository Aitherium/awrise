"""The wake ledger: an append-only record of every wake and why.

``$AWRISE_HOME/ledger/YYYY-MM-DD.jsonl``, one JSON object per line, one
``write + fsync`` per row. The writer REFUSES a row whose event or state is
outside the closed sets below, or whose reason is empty -- so a row can only
ever say something the code measured. Output tails are redacted before they
touch the disk.

Two passes may append at the same instant, so the append itself must be
atomic. POSIX ``O_APPEND`` is. The Windows C runtime's ``O_APPEND`` is NOT
(it seeks to the end and then writes, and two writers that seek together
overwrite each other with every writer reporting success), so on Windows a
row is written through a handle opened with ``FILE_APPEND_DATA`` only, which
the kernel appends atomically.
"""

from __future__ import annotations

import json
import math
import os
import re
import secrets
import socket
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from . import clock

SCHEMA = 1
ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz"
TAIL_CHARS = 1024

EVENTS = frozenset(
    {
        "tick",
        "tick_end",
        "started",
        "finished",
        "missed",
        "reconciled",
        "removed",
        "card_raised",
        "card_answered",
        "report_error",
    }
)
STATES = frozenset(
    {
        "success",
        "failure",
        "timeout",
        "error",
        "detached",
        "queued",
        "cancelled",
        "skipped_overlap",
        "skipped_disabled",
        "skipped_missed",
        "skipped_empty",
        "skipped_unresolvable",
        "skipped_predicted",
        "orphaned",
        "would_fire",
    }
)
#: A pass that produced one of these exits 1.
BAD_STATES = frozenset({"failure", "timeout", "error", "orphaned"})
#: Outcomes that do NOT stamp ``last_started_at`` -- the job is retried next pass.
UNSTAMPED_STATES = frozenset({"skipped_overlap", "would_fire"})

_REDACT = (
    (re.compile(r"\bBearer\s+[A-Za-z0-9._\-]{6,}"), "<redacted:bearer>"),
    (re.compile(r"\baither_sk_[A-Za-z0-9_\-]{4,}"), "<redacted:aither_sk>"),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{6,}"), "<redacted:sk>"),
    (re.compile(r"\bgh[ps]_[A-Za-z0-9]{6,}"), "<redacted:gh>"),
    (re.compile(r"\bAKIA[A-Z0-9]{8,}"), "<redacted:akia>"),
    (re.compile(r"\bxox[bp]-[A-Za-z0-9\-]{6,}"), "<redacted:slack>"),
)


class LedgerRefusedError(ValueError):
    """A row outside the closed vocabulary. This is a programming error, and
    it must be loud: a templated or empty reason is how a ledger starts lying."""


def new_id(prefix: str) -> str:
    return prefix + "".join(secrets.choice(ALPHABET) for _ in range(8))


def redact(text: str) -> str:
    for pattern, mask in _REDACT:
        text = pattern.sub(mask, text)
    return text


def tail(data: Optional[bytes], limit: int = TAIL_CHARS) -> str:
    """Last ``limit`` chars of a byte stream, decoded leniently, redacted."""
    if not data:
        return ""
    text = data.decode("utf-8", "replace")
    if len(text) > limit:
        text = text[-limit:]
    return redact(text)


def ledger_dir(base: Path) -> Path:
    return base / "ledger"


def day_file(base: Path, ts: datetime) -> Path:
    return ledger_dir(base) / (ts.strftime("%Y-%m-%d") + ".jsonl")


def validate(row: dict) -> None:
    event = row.get("event")
    if event not in EVENTS:
        raise LedgerRefusedError(f"unknown event {event!r}; known: {sorted(EVENTS)}")
    state = row.get("state")
    if event == "finished" and state is None:
        raise LedgerRefusedError("a finished row needs a state")
    if state is not None and state not in STATES:
        raise LedgerRefusedError(f"unknown state {state!r}; known: {sorted(STATES)}")
    reason = row.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise LedgerRefusedError(f"row {event!r} has an empty reason")
    if event in ("started", "finished", "missed", "removed") and not row.get("job"):
        raise LedgerRefusedError(f"a {event} row names a job")


def append(base: Path, row: dict) -> str:
    """Validate, fill the envelope, append one line. Returns the wake_id.

    OSError propagates: a ledger that cannot be written is exit 2 for the
    caller, never a pass that "worked".
    """
    validate(row)
    if row.get("ts") is not None:
        # A caller-supplied stamp (the pass's own started_at) so the row and
        # the store hold ONE value, never two clock reads that disagree.
        try:
            ts = clock.parse_ts(row["ts"])
        except (ValueError, TypeError) as exc:
            raise LedgerRefusedError(
                f"row {row['event']!r} ts is not a timestamp: {row['ts']!r}"
            ) from exc
        if ts is None:
            raise LedgerRefusedError(f"row {row['event']!r} ts is empty")
    else:
        ts = clock.now_utc()
    full = {
        "schema": SCHEMA,
        "wake_id": row.get("wake_id") or new_id("w-"),
        "pass_id": row.get("pass_id"),
        "ts": clock.iso(ts),
        "invoker": row.get("invoker", "manual"),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "interpreter": sys.executable,
        "job": row.get("job"),
        "event": row["event"],
        "state": row.get("state"),
        "reason": row["reason"],
    }
    for key, value in row.items():
        if key not in full:
            full[key] = redact(value) if isinstance(value, str) else value
    for key in ("stdout_tail", "stderr_tail"):
        if isinstance(full.get(key), str):
            full[key] = redact(full[key])
    full = _json_finite(full)
    try:
        line = json.dumps(full, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    except (TypeError, ValueError) as exc:
        raise LedgerRefusedError(f"row {row['event']!r} is not JSON: {exc}") from exc
    path = day_file(base, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    _append_bytes(path, line.encode("utf-8"))
    return full["wake_id"]


def _json_finite(value):
    """NaN and +/-Infinity are not JSON; a strict reader would drop the row."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: _json_finite(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_finite(v) for v in value]
    return value


def _append_bytes(path: Path, data: bytes) -> None:
    """One atomic append + fsync. OSError propagates."""
    if sys.platform == "win32":
        _append_bytes_windows(path, data)
        return
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def _append_bytes_windows(path: Path, data: bytes) -> None:
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    c_void_p, c_uint32 = ctypes.c_void_p, ctypes.c_uint32
    kernel32.CreateFileW.restype = c_void_p
    kernel32.CreateFileW.argtypes = (
        ctypes.c_wchar_p,
        c_uint32,
        c_uint32,
        c_void_p,
        c_uint32,
        c_uint32,
        c_void_p,
    )
    kernel32.WriteFile.argtypes = (c_void_p, c_void_p, c_uint32, ctypes.POINTER(c_uint32), c_void_p)
    kernel32.FlushFileBuffers.argtypes = (c_void_p,)
    kernel32.CloseHandle.argtypes = (c_void_p,)
    file_append_data, synchronize = 0x0004, 0x00100000
    share_read_write_delete = 0x1 | 0x2 | 0x4
    open_always, file_attribute_normal = 4, 0x80
    invalid_handle = c_void_p(-1).value
    handle = kernel32.CreateFileW(
        str(path),
        file_append_data | synchronize,
        share_read_write_delete,
        None,
        open_always,
        file_attribute_normal,
        None,
    )
    if handle is None or handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        written = c_uint32(0)
        buf = ctypes.create_string_buffer(data, len(data))
        if not kernel32.WriteFile(handle, buf, len(data), ctypes.byref(written), None):
            raise ctypes.WinError(ctypes.get_last_error())
        if written.value != len(data):
            raise OSError(f"short append to {path}: {written.value} of {len(data)} bytes")
        if not kernel32.FlushFileBuffers(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel32.CloseHandle(handle)


def files(base: Path, since: Optional[timedelta] = None) -> List[Path]:
    folder = ledger_dir(base)
    if not folder.is_dir():
        return []
    names = sorted(p for p in folder.glob("*.jsonl") if p.is_file())
    if since is None:
        return names
    floor = (clock.now_utc() - since).strftime("%Y-%m-%d")
    return [p for p in names if p.stem >= floor]


_NOT_JSON = object()

#: Fields every reader treats as text (or absent). A line that types one of
#: them as a number, a list or an object is as unreadable as garbage: it can
#: be keyed on nothing and compared with nothing, and handing it to a reader
#: is how one hand-written line wedges every later pass with a traceback.
TEXT_FIELDS = ("wake_id", "pass_id", "job", "event", "state", "reason", "ts")


def _unreadable(path: Path, lineno: int, why: str) -> dict:
    """A line nobody can read, reported as a row -- and deliberately UNSTAMPED.

    The obvious stamp for this row is the day file it came from, and that is
    exactly what it must not carry: every judge here measures the ledger's
    span between its oldest and newest stamp, so a truncated final line (the
    normal outcome of a crash or ENOSPC mid-append, which is why the reader
    tolerates it at all) stamped at MIDNIGHT would hand a seconds-old ledger
    up to 24h of fabricated record -- and the window guard that exists to say
    UNJUDGED would pass it as judged. A row with no ``ts`` is counted, named
    and reported; it just cannot lend the record a history it does not have.
    """
    return {
        "event": "unreadable",
        "state": None,
        "job": None,
        "reason": f"{path.name}:{lineno} {why}",
        "ts": None,
        "wake_id": None,
        "day_file": path.name,
    }


def _mistyped(row: dict) -> Optional[str]:
    """The fields this row types wrongly, or None when it is readable."""
    bad = [key for key in TEXT_FIELDS if row.get(key) is not None and not isinstance(row[key], str)]
    return "has a non-string " + ", ".join(bad) if bad else None


def read(
    base: Path,
    since: Optional[timedelta] = None,
    job: Optional[str] = None,
    event: Optional[str] = None,
) -> List[dict]:
    """Rows in write order. A malformed line is reported as a row, never dropped."""
    rows: List[dict] = []
    for path in files(base, since):
        with open(path, "r", encoding="utf-8", newline="") as fh:
            for lineno, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    why = "is not a JSON object"
                except json.JSONDecodeError:
                    row = _NOT_JSON
                    why = "is not JSON"
                if not isinstance(row, dict):
                    # Valid JSON that is not an object (null, 42, a list) is as
                    # unreadable as garbage: reported as a row, never dropped,
                    # and never handed to a caller that expects ``.get``.
                    row = _unreadable(path, lineno, why)
                else:
                    mistyped = _mistyped(row)
                    if mistyped:
                        # A foreign or hand-written line whose wake id is a
                        # dict, or whose ts is a number: reported the same way,
                        # so ONE such line cannot stop every job on the host.
                        row = _unreadable(path, lineno, mistyped)
                if job and row.get("job") != job:
                    continue
                if event and row.get("event") != event:
                    continue
                rows.append(row)
    return rows


def open_wakes(rows: Iterable[dict]) -> Dict[str, dict]:
    """``started`` rows whose wake never got a ``finished`` row.

    Paired by wake id, not by order: a ``started`` row that lands in a
    later-named day file than its ``finished`` row (a clock stepped back
    between the two) is still closed.
    """
    rows = [row for row in rows if isinstance(row, dict) and not _mistyped(row)]
    finished = {
        row["wake_id"] for row in rows if row.get("event") == "finished" and row.get("wake_id")
    }
    started: Dict[str, dict] = {}
    for row in rows:
        wake = row.get("wake_id")
        if row.get("event") == "started" and wake and wake not in finished:
            started[wake] = row
    return started
