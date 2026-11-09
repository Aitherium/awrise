"""Why a wake failed, in one line -- and what kind of failure it is.

A ledger that says ``exit_2`` 1,713 times tells the owner nothing he can act
on; the job's own last error line ("no llama-server on spark.local:8118") tells
him everything. Measured 2026-11-08: the desk's wakes panel listed 21 broken
wakes with only an exit code each, and running two of them by hand answered
both in seconds. This module is that "by hand", done on every bad wake:

* :func:`output_tail` -- the last ``TAIL_LINES`` lines of stdout+stderr, capped
  at ``TAIL_BYTES``, redacted before it touches the ledger;
* :func:`extract_why` -- the most informative error line of that tail;
* :func:`classify` -- a ``cause`` bucket, so a dependency that is down is held
  (backed off) instead of piling up failures, and a code bug raises ONE card;
* :func:`receipt_why` -- the same answer for a detached job, from its receipt.

Pure functions, no I/O except :func:`receipt_why` reading the receipt the job
itself declared.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from . import ledger

TAIL_LINES = 40
TAIL_BYTES = 4096
WHY_CHARS = 200
#: The same ``why`` this many wakes in a row is a pattern, not a blip: a
#: dependency-down cause starts being held at this streak.
HOLD_AFTER = 3
#: A code-bug ``why`` this many wakes in a row raises ONE decision card.
CARD_AFTER = 5
#: The hold never grows past an hour: a dependency that comes back is noticed
#: within the hour, whatever the streak.
HOLD_CAP_S = 3600.0
HOLD_FLOOR_S = 60.0

CAUSES = ("dependency-down", "code-bug", "timeout", "config", "unknown")

#: A line that names a failure. Scored: the LAST matching line wins, because a
#: traceback's final line (the exception) is the informative one.
_INFORMATIVE = re.compile(
    r"error|exception|refused|failed|failure|denied|no such|not found|"
    r"unreachable|timed? ?out|cannot|can't|could not|unable|invalid|missing|"
    r"\bno [a-z][\w.-]* (on|at|in|for)\b|HTTP [45]\d\d|\b[45]\d\d [A-Z][a-z]",
    re.IGNORECASE,
)
#: Lines that look like errors and are not: a traceback frame header, a
#: separator, a bare "Traceback" banner.
_NOISE = re.compile(r"^\s*(File \"|Traceback \(most recent call last\)|[-=~^]{3,}\s*$|at \S+ \()")

_DEPENDENCY = re.compile(
    r"connection refused|econnrefused|actively refused|no llama-server|"
    r"unreachable|no route to host|name or service not known|getaddrinfo|"
    r"could not resolve|failed to connect|could not connect|cannot connect|"
    r"connection (reset|aborted|timed out)|ssh failed|ssh: connect|"
    r"service unavailable|bad gateway|gateway time-?out|HTTP 50[234]|\b50[234]\b|"
    r"is not running|not reachable|daemon unreachable|is down\b|host is down|"
    r"max retries exceeded|remote end closed|wsl.*(not running|unavailable)",
    re.IGNORECASE,
)
_CONFIG = re.compile(
    r"no such file|filenotfounderror|not found|cwd_missing|spawn_failed|"
    r"is not recognized as|command not found|permission denied|access is denied|"
    r"not set\b|missing (env|config|setting|credential|token|key)|"
    r"no module named|modulenotfounderror|executable_not_found|bearer",
    re.IGNORECASE,
)
_CODE_BUG = re.compile(
    r"traceback|exception|error:|\w+error\b|HTTP 4\d\d|\b4\d\d\b.*(bad request|"
    r"unprocessable|validation)|validation|assert|must declare|refused a|"
    r"raise failed|unexpected|invalid",
    re.IGNORECASE,
)


def _lines(text: str) -> List[str]:
    return [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]


def output_tail(
    stdout: Optional[bytes] = None,
    stderr: Optional[bytes] = None,
    lines: int = TAIL_LINES,
    limit: int = TAIL_BYTES,
) -> str:
    """stdout then stderr, last ``lines`` lines, at most ``limit`` chars, redacted.

    Redaction runs on the WHOLE decoded stream before it is cut, so a token
    split by the cut cannot leave its tail unredacted on disk.
    """
    parts = []
    for data in (stdout, stderr):
        if data:
            text = data.decode("utf-8", "replace") if isinstance(data, bytes) else str(data)
            parts.append(ledger.redact(text))
    return cut_tail("\n".join(p.rstrip("\r\n") for p in parts if p.strip()), lines, limit)


def cut_tail(text: str, lines: int = TAIL_LINES, limit: int = TAIL_BYTES) -> str:
    """The last ``lines`` non-trailing-blank lines of ``text``, capped at ``limit`` chars."""
    if not text:
        return ""
    kept = _lines(ledger.redact(text))
    while kept and not kept[-1].strip():
        kept.pop()
    joined = "\n".join(kept[-lines:])
    if len(joined) > limit:
        joined = joined[-limit:]
        # Never start mid-line: drop the partial first line unless it is all we have.
        cut = joined.find("\n")
        if 0 <= cut < len(joined) - 1:
            joined = joined[cut + 1 :]
    return joined


def _clip(line: str) -> str:
    line = " ".join(line.split())
    return line if len(line) <= WHY_CHARS else line[: WHY_CHARS - 3] + "..."


def extract_why(tail: str, fallback: str = "", prefer: str = "") -> str:
    """The most informative last error line, one line, at most ``WHY_CHARS``.

    ``prefer`` (stderr) is searched first; then the whole tail; then the last
    non-blank line of either; then ``fallback`` (the outcome's own reason, e.g.
    ``exit_2`` or ``killed after 300s``). Never empty when ``fallback`` is not.
    """
    for text in (prefer, tail):
        candidates = [ln for ln in _lines(text or "") if ln.strip() and not _NOISE.match(ln)]
        for line in reversed(candidates):
            if _INFORMATIVE.search(line):
                return _clip(ledger.redact(line))
    for text in (prefer, tail):
        candidates = [ln for ln in _lines(text or "") if ln.strip() and not _NOISE.match(ln)]
        if candidates:
            return _clip(ledger.redact(candidates[-1]))
    return _clip(ledger.redact(fallback or ""))


#: Digit runs, hex ids and quoted paths vary per run without changing the cause.
_VARIABLE = re.compile(r"0x[0-9a-f]+|\b[0-9a-f]{8,}\b|\d+(\.\d+)?", re.IGNORECASE)


def why_key(why: Optional[str]) -> str:
    """A short stable key for "the same why": volatile numbers and ids masked."""
    if not why:
        return ""
    norm = _VARIABLE.sub("#", why.strip().lower())
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:12]


def classify(why: str, tail: str = "", state: str = "failure") -> str:
    """One of :data:`CAUSES`. Judged on the ``why`` first, the tail second.

    Order matters: a timeout is a timeout whatever it printed; a dependency
    that is down is not a code bug even when it surfaces as an exception; an
    HTTP 4xx is the caller's bug, never a missing file, even if the body says
    "not found".
    """
    text = why or ""
    if state == "timeout" or re.search(r"killed after \d", text):
        return "timeout"
    for probe in (text, tail or ""):
        if _DEPENDENCY.search(probe):
            return "dependency-down"
        if re.search(r"HTTP 4\d\d", probe):
            return "code-bug"
        if _CONFIG.search(probe):
            return "config"
        if _CODE_BUG.search(probe):
            return "code-bug"
    return "unknown"


def hold_seconds(streak: int, period_s: float) -> float:
    """How long a dependency-down job is held after ``streak`` identical failures.

    TWO windows (a window floored at a minute) at ``HOLD_AFTER``, doubling per
    further identical failure, capped at ``HOLD_CAP_S``. Two, not one: a hold
    of exactly one window expires as the next window opens and skips nothing.
    """
    if streak < HOLD_AFTER:
        return 0.0
    base = max(float(period_s or 0), HOLD_FLOOR_S)
    exponent = min(streak - HOLD_AFTER + 1, 20)
    return min(base * (2**exponent), HOLD_CAP_S)


#: Receipt keys that carry a human reason, most specific first.
_RECEIPT_TEXT_KEYS = ("error", "why", "message", "detail", "reason", "stderr_tail", "output_tail")
_RECEIPT_LIST_KEYS = ("errors", "warnings", "notes")
_RECEIPT_LOG_KEYS = ("log", "log_path", "logfile")


def _texts(value: object) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                yield item
            elif isinstance(item, dict):
                for key in ("message", "error", "detail"):
                    if isinstance(item.get(key), str):
                        yield item[key]


def receipt_why(path: object, exit_code: Optional[int] = None) -> Tuple[str, str]:
    """(why, tail) for a detached job, read off its own receipt (and its log).

    A receipt that names a reason (``error``/``detail``/``errors``...) gives it;
    one that names a log file gives that log's tail. Otherwise the why is the
    receipt's exit code -- honest, and no worse than before.
    """
    fallback = f"receipt exit_code {exit_code}"
    if not path:
        return fallback, ""
    try:
        data = json.loads(Path(os.path.expanduser(str(path))).read_text("utf-8", "replace"))
    except (OSError, ValueError):
        return fallback, ""
    if not isinstance(data, dict):
        return fallback, ""
    if exit_code is None:
        fallback = f"receipt exit_code {data.get('exit_code')}"
    texts: List[str] = []
    for key in _RECEIPT_TEXT_KEYS:
        texts.extend(t for t in _texts(data.get(key)) if t.strip() and t.strip().lower() != "ok")
    for key in _RECEIPT_LIST_KEYS:
        texts.extend(t for t in _texts(data.get(key)) if t.strip())
    for key in _RECEIPT_LOG_KEYS:
        log = data.get(key)
        if isinstance(log, str) and log.strip():
            try:
                raw = Path(os.path.expanduser(log)).read_bytes()[-TAIL_BYTES * 2 :]
            except OSError:
                continue
            texts.append(raw.decode("utf-8", "replace"))
            break
    tail = cut_tail("\n".join(texts))
    return extract_why(tail, fallback), tail
