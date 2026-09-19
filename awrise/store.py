"""``jobs.json`` -- the one file awrise owns, and the rules that keep it honest.

* every write is ``tmp + fsync + os.replace``; the previous copy becomes
  ``jobs.json.bak`` first, so a crash at any instruction leaves either the old
  file or the new one, never a truncated one;
* a store that cannot be parsed is an exit-2 refusal, moved aside as
  ``jobs.json.corrupt-<ts>`` and NEVER replaced by ``{}`` -- ``reconcile
  --restore`` brings the ``.bak`` back;
* a home or store another user can write is refused (that is the trust
  boundary: whoever writes ``jobs.json`` runs shell commands as you);
* a 0.1.0 store (``interval`` as a string, naive ``last_run``) migrates in
  memory on first load and is written back once, with ``jobs.json.v1.bak``
  kept;
* every record is validated on load -- a field the code would choke on
  (an unparseable stamp, a non-numeric timeout, a name outside the grammar)
  is an exit-2 refusal that names the job and the field, never a traceback
  half-way through a pass;
* on Windows the store is read through a handle that shares DELETE, so a
  concurrent ``run-due`` reading ``jobs.json`` never makes another pass's
  ``os.replace`` fail; a foreign reader (editor, indexer) is retried past.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import stat
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

from . import clock, executors, ledger

SCHEMA = 2
STORE_NAME = "jobs.json"

#: Job names are keys in ``jobs.json`` AND (next slice) lock directory names,
#: so the grammar is what a directory name can safely be on every OS: start
#: with a letter or digit, then letters, digits, ``.``, ``_``, ``-``; at most
#: 64 chars; no trailing dot; no Windows reserved device name.
NAME_MAX = 64
NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_RESERVED_NAMES = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}
)


def validate_name(name: object) -> str:
    """The name, or ValueError saying which rule it broke."""
    if not isinstance(name, str):
        raise ValueError(f"job name must be a string, not {type(name).__name__}")
    if not name:
        raise ValueError("job name is empty")
    if len(name) > NAME_MAX:
        raise ValueError(f"job name longer than {NAME_MAX} chars: {name[:20]!r}...")
    if not NAME_RE.fullmatch(name):
        raise ValueError(f"job name {name!r} must be letters, digits, '.', '_' or '-' "
                         "and start with a letter or digit")
    if name.endswith("."):
        raise ValueError(f"job name {name!r} must not end with '.'")
    if name.split(".", 1)[0].upper() in _RESERVED_NAMES:
        raise ValueError(f"job name {name!r} is a reserved device name")
    return name


#: Spec fields the operator sets and the code READS. A self-test asserts this
#: set equals the keys the runner actually reads, so a knob nobody honours
#: cannot exist in a shipped store.
SPEC_DEFAULTS: Dict[str, object] = {
    "every": None,
    "interval_s": None,
    "run": None,
    "timeout_s": 300,
    "enabled": True,
    "cwd": None,
    "at": None,
    "detach": False,
    "missed": "catch_up_once",
    "executor": "shell",
    "bearer_file": None,
    "permission_mode": None,
    "report": None,
    "wake": None,
    "park_after": False,
    "wake_required": True,
}
#: What a job says about telling someone. Every field is off or inert by
#: default except ``on`` (which only says WHICH outcomes would be reported if
#: a channel were configured) -- a brick that starts messaging a chat room
#: because it was installed would be a brick nobody installs twice.
REPORT_DEFAULTS: Dict[str, object] = {
    "relay": None,
    "on": ["failure", "timeout", "error", "orphaned"],
    "card_after": 3,
    "card_id": None,
}
#: What a job does about windows that came and went while nothing was running.
#: ``catch_up_once`` runs it ONE more time, whatever the arrears; ``skip``
#: drops them and waits for the next window. There is deliberately no "run
#: them all": a host asleep for half a day would wake to a dozen copies of
#: the same command, which is a documented way to lose an afternoon.
MISSED_POLICIES = ("catch_up_once", "skip")
#: Keys ``set`` accepts (``interval_s`` is derived from ``every``). The dotted
#: ones address one field inside ``report``.
SETTABLE = ("every", "run", "timeout_s", "enabled", "cwd", "at", "detach", "missed",
            "executor", "bearer_file", "permission_mode", "wake", "park_after",
            "wake_required", "report.relay", "report.on", "report.card_after")

#: Fields awrise itself writes. ``reload_merge`` carries exactly these.
STATE_DEFAULTS: Dict[str, object] = {
    "last_wake_id": None,
    "last_started_at": None,
    "last_finished_at": None,
    "last_state": None,
    "last_reason": None,
    "consecutive_failures": 0,
    "created_at": None,
    "updated_at": None,
}
STATE_KEYS = tuple(STATE_DEFAULTS)
Jobs = Dict[str, dict]


class StoreError(Exception):
    """The store cannot be read or written safely. The CLI maps this to exit 2."""


def home() -> Path:
    env = os.environ.get("AWRISE_HOME")
    path = Path(env) if env else Path.home() / ".aither" / "awrise"
    try:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    except OSError as exc:
        raise StoreError(f"cannot create {path}: {exc}") from exc
    return path


def store_path(base: Optional[Path] = None) -> Path:
    return (base or home()) / STORE_NAME


# ---------------------------------------------------------------- permissions

def perms_problems(base: Path) -> list:
    """Reasons this home is not trustworthy. Empty list = owner-only."""
    problems = []
    targets = [base]
    jobs_file = base / STORE_NAME
    if jobs_file.exists():
        targets.append(jobs_file)
    for target in targets:
        try:
            info = target.stat()
        except OSError as exc:
            problems.append(f"{target}: cannot stat ({exc})")
            continue
        if sys.platform == "win32":
            mine = _windows_owned_by_me(target)
            if mine is False:
                problems.append(f"{target}: owned by another account")
            continue
        mode = stat.S_IMODE(info.st_mode)
        if mode & (stat.S_IWGRP | stat.S_IWOTH):
            problems.append(f"{target}: group/world-writable (mode {mode:04o})")
        if hasattr(os, "getuid") and info.st_uid != os.getuid():
            problems.append(f"{target}: owned by uid {info.st_uid}, not {os.getuid()}")
    return problems


def _windows_owned_by_me(path: Path) -> Optional[bool]:
    """True/False, or None when the DACL could not be read (not judged)."""
    try:
        import ctypes
    except ImportError:
        return None
    try:
        advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        c_void_p, byref = ctypes.c_void_p, ctypes.byref
        advapi32.GetNamedSecurityInfoW.restype = ctypes.c_uint32
        advapi32.GetNamedSecurityInfoW.argtypes = (
            ctypes.c_wchar_p, ctypes.c_int, ctypes.c_uint32,
            ctypes.POINTER(c_void_p), ctypes.POINTER(c_void_p),
            ctypes.POINTER(c_void_p), ctypes.POINTER(c_void_p),
            ctypes.POINTER(c_void_p),
        )
        advapi32.OpenProcessToken.argtypes = (c_void_p, ctypes.c_uint32,
                                              ctypes.POINTER(c_void_p))
        advapi32.GetTokenInformation.argtypes = (
            c_void_p, ctypes.c_int, c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32),
        )
        advapi32.EqualSid.argtypes = (c_void_p, c_void_p)
        kernel32.GetCurrentProcess.restype = c_void_p
        kernel32.CloseHandle.argtypes = (c_void_p,)
        kernel32.LocalFree.argtypes = (c_void_p,)
        kernel32.LocalFree.restype = c_void_p

        owner, descriptor = c_void_p(), c_void_p()
        se_file_object, owner_security_information = 1, 0x1
        rc = advapi32.GetNamedSecurityInfoW(
            str(path), se_file_object, owner_security_information,
            byref(owner), None, None, None, byref(descriptor),
        )
        if rc != 0 or not owner.value:
            return None
        try:
            token = c_void_p()
            token_query = 0x8
            me = kernel32.GetCurrentProcess()
            if not advapi32.OpenProcessToken(me, token_query, byref(token)):
                return None
            try:
                # TokenUser = 1, TokenOwner = 4. An elevated shell creates files
                # owned by the Administrators group, which is the token OWNER,
                # not the token USER; accept either as "me".
                for info_class in (1, 4):
                    size = ctypes.c_uint32(0)
                    advapi32.GetTokenInformation(token, info_class, None, 0, byref(size))
                    buf = ctypes.create_string_buffer(size.value or 1)
                    if not advapi32.GetTokenInformation(token, info_class, buf, size, byref(size)):
                        continue
                    sid = ctypes.cast(buf, ctypes.POINTER(c_void_p))[0]
                    if sid and advapi32.EqualSid(owner, sid):
                        return True
                return False
            finally:
                kernel32.CloseHandle(token)
        finally:
            kernel32.LocalFree(descriptor)
    except (OSError, AttributeError, ValueError):
        return None


# --------------------------------------------------------------------- load

def relay_channel_problem(channel: object) -> Optional[str]:
    """Why this is not a channel name, or None when it is one.

    One judgement for both lanes. A channel named in a job's spec is judged
    at ``add`` time; a channel named in the environment is judged by the pass
    that would post to it. Judging only the spec lane leaves the other one
    handing whatever the variable holds to the relay tool as an argument --
    and a value shaped like a flag is then read by that tool's own parser,
    which exits 0 having posted nothing, so a channel an operator configured
    is silently dead and no row says so.
    """
    if not isinstance(channel, str) or not channel.strip():
        return "must be a channel name or null"
    if not channel.startswith("#"):
        return f"must start with '#': {channel!r}"
    return None


def validate_report(name: str, raw: object) -> Optional[dict]:
    """A complete ``report`` block, or None when the job has no sinks at all.

    ``None`` is kept as ``None`` rather than filled in with defaults so that a
    store written before sinks existed round-trips byte-for-byte: a migration
    that rewrites every record to add an inert block is a diff nobody asked
    for and a backup nobody can compare.
    """
    if raw is None:
        return None

    def bad(field: str, why: str) -> StoreError:
        return StoreError(f"job {name!r}: report.{field} {why}")

    if not isinstance(raw, dict):
        raise StoreError(f"job {name!r}: report must be an object or null")
    unknown = set(raw) - set(REPORT_DEFAULTS)
    if unknown:
        raise StoreError(f"job {name!r}: report has unknown key(s) {sorted(unknown)}")
    full = dict(REPORT_DEFAULTS)
    full["on"] = list(REPORT_DEFAULTS["on"])  # type: ignore[arg-type]
    full.update(raw)
    relay = full["relay"]
    if relay is not None:
        problem = relay_channel_problem(relay)
        if problem:
            raise bad("relay", problem)
    on = full["on"]
    if not isinstance(on, list) or not all(isinstance(s, str) for s in on):
        raise bad("on", "must be a list of ledger states")
    unknown_states = [s for s in on if s not in ledger.STATES]
    if unknown_states:
        raise bad("on", f"names states the ledger does not have: {unknown_states}")
    full["on"] = list(on)
    card_after = full["card_after"]
    if isinstance(card_after, bool) or not isinstance(card_after, int) or card_after < 0:
        raise bad("card_after", "must be a non-negative integer (0 = never raise a card)")
    if full["card_id"] is not None and not isinstance(full["card_id"], str):
        raise bad("card_id", "must be a string or null")
    return full


def validate_record(name: str, job: object) -> dict:
    """A complete, typed record, or StoreError naming the job and the field.

    Missing spec/state keys take their defaults (a hand-written minimal record
    is fine); an unknown key is refused (a knob nobody reads must not exist in
    a store); every stamp must parse; ``last_state`` must be a ledger state.
    """
    try:
        validate_name(name)
    except ValueError as exc:
        raise StoreError(f"job {name!r}: {exc}") from exc
    if not isinstance(job, dict):
        raise StoreError(f"job {name!r}: record is not an object")
    unknown = set(job) - set(SPEC_DEFAULTS) - set(STATE_DEFAULTS)
    if unknown:
        raise StoreError(f"job {name!r}: unknown key(s) {sorted(unknown)}")
    full = dict(SPEC_DEFAULTS)
    full.update(STATE_DEFAULTS)
    full.update(job)

    def bad(field: str, why: str) -> StoreError:
        return StoreError(f"job {name!r}: {field} {why}: {full.get(field)!r}")

    if not isinstance(full["run"], str):
        raise bad("run", "must be a string")
    if not isinstance(full["every"], str):
        raise bad("every", "must be a string")
    try:
        parsed = float(clock.parse_interval(full["every"]).total_seconds())
    except ValueError as exc:
        raise bad("every", f"is not an interval ({exc})") from exc
    if isinstance(full["interval_s"], bool) or not isinstance(full["interval_s"], (int, float)):
        raise bad("interval_s", "must be a number")
    if float(full["interval_s"]) != parsed:
        raise bad("interval_s", f"disagrees with every={full['every']!r} ({parsed:g})")
    full["interval_s"] = float(full["interval_s"])
    if isinstance(full["timeout_s"], bool) or not isinstance(full["timeout_s"], int):
        raise bad("timeout_s", "must be an integer")
    if full["timeout_s"] <= 0:
        raise bad("timeout_s", "must be positive")
    if not isinstance(full["enabled"], bool):
        raise bad("enabled", "must be true or false")
    if not isinstance(full["detach"], bool):
        raise bad("detach", "must be true or false")
    if full["cwd"] is not None and not isinstance(full["cwd"], str):
        raise bad("cwd", "must be a path or null")
    try:
        clock.parse_at(full["at"])
    except ValueError as exc:
        raise bad("at", f"is not HH:MM ({exc})") from exc
    if full["missed"] not in MISSED_POLICIES:
        raise bad("missed", f"must be one of {', '.join(MISSED_POLICIES)}")
    if full["executor"] not in executors.KINDS:
        raise bad("executor", f"must be one of {', '.join(executors.KINDS)}")
    problem = executors.bearer_path_problem(full["bearer_file"])
    if problem is not None:
        raise bad("bearer_file", problem)
    if full["permission_mode"] is not None:
        if not isinstance(full["permission_mode"], str):
            raise bad("permission_mode", "must be a string or null")
        if full["permission_mode"] not in executors.PERMISSION_MODES:
            raise bad("permission_mode",
                      f"must be one of {', '.join(executors.PERMISSION_MODES)}")
    # The wake block. A record written before these keys existed simply takes
    # the defaults above (no wake, nothing parked, a wake that is required if
    # one is asked for), so a 0.2.0 store loads unchanged and gains the keys on
    # its next save -- the same additive shape as `report`, and the reason this
    # is not a new SCHEMA: nothing already in the file means something else now.
    if full["wake"] is not None:
        if not isinstance(full["wake"], str):
            raise bad("wake", "must be a unit name or null")
        problem = executors.unit_name_problem(full["wake"])
        if problem is not None:
            raise bad("wake", problem)
    for field in ("park_after", "wake_required"):
        if not isinstance(full[field], bool):
            raise bad(field, "must be true or false")
    if full["park_after"]:
        if full["wake"] is None:
            raise bad("park_after", "needs wake=<unit>: the unit to park is the one the job "
                                    "woke, and a job that woke nothing has nothing to park")
        if full["detach"]:
            raise bad("park_after", "is refused with detach=true: a detached child outlives "
                                    "the wake, so parking its unit would stop it mid-run")
    full["report"] = validate_report(name, full["report"])
    for field in ("last_started_at", "last_finished_at", "created_at", "updated_at"):
        value = full[field]
        if value is None:
            continue
        if not isinstance(value, str):
            raise bad(field, "must be an ISO timestamp or null")
        try:
            clock.parse_ts(value)
        except ValueError as exc:
            raise bad(field, f"is not an ISO timestamp ({exc})") from exc
    if full["last_state"] is not None and full["last_state"] not in ledger.STATES:
        raise bad("last_state", "is not a ledger state")
    for field in ("last_wake_id", "last_reason"):
        if full[field] is not None and not isinstance(full[field], str):
            raise bad(field, "must be a string or null")
    fails = full["consecutive_failures"]
    if isinstance(fails, bool) or not isinstance(fails, int) or fails < 0:
        raise bad("consecutive_failures", "must be a non-negative integer")
    return full


def _validate_all(jobs: object, path: Path) -> Jobs:
    if not isinstance(jobs, dict):
        raise StoreError(f"{path}: `jobs` is not an object")
    return {name: validate_record(name, job) for name, job in jobs.items()}


def _migrate_v1(raw: dict) -> Jobs:
    jobs: Jobs = {}
    for name, old in raw.items():
        if not isinstance(old, dict):
            raise StoreError(f"v1 record {name!r} is not an object")
        try:
            seconds = float(old.get("interval") or 0)
        except (TypeError, ValueError) as exc:
            raise StoreError(f"v1 record {name!r}: interval is not a number of seconds: "
                             f"{old.get('interval')!r}") from exc
        if seconds <= 0:
            raise StoreError(f"v1 record {name!r}: interval must be positive seconds, "
                             f"got {old.get('interval')!r}")
        every = _seconds_to_every(seconds)
        last_started = old.get("last_run")
        if last_started:
            try:
                last_started = clock.iso(clock.parse_ts(last_started))
            except ValueError:
                last_started = None
        job = new_job(every=every, run=old.get("command", ""), interval_s=seconds)
        job["last_started_at"] = last_started
        job["last_state"] = old.get("last_status")
        job["last_reason"] = "migrated_from_v1" if old.get("last_status") else None
        jobs[name] = job
    return jobs


def _seconds_to_every(seconds: float) -> str:
    for unit, span in (("w", 604800.0), ("d", 86400.0), ("h", 3600.0), ("m", 60.0)):
        if seconds >= span and seconds % span == 0:
            return f"{int(seconds // span)}{unit}"
    if seconds == int(seconds):
        return f"{int(seconds)}s"
    return f"{seconds}s"


def new_job(every: str, run: str, interval_s: Optional[float] = None) -> dict:
    now = clock.iso(clock.now_utc())
    job = dict(SPEC_DEFAULTS)
    job.update(STATE_DEFAULTS)
    job["every"] = every
    job["interval_s"] = float(interval_s if interval_s is not None
                              else clock.parse_interval(every).total_seconds())
    job["run"] = run
    job["created_at"] = now
    job["updated_at"] = now
    return job


def read_bytes(path: Path) -> bytes:
    with open_shared_read(path) as fh:
        return fh.read()


def open_shared_read(path: Path):
    """A binary reader that never blocks a concurrent ``os.replace`` of ``path``.

    A plain ``open()`` on Windows shares READ and WRITE but not DELETE, and
    ``os.replace`` needs DELETE on its target -- so one pass reading the store
    made another pass's save fail with "Access is denied" after its job had
    already run. This handle shares DELETE; the writer's replace succeeds and
    this reader keeps the bytes it opened.
    """
    if sys.platform != "win32":
        return open(path, "rb")
    import ctypes
    import msvcrt

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    c_void_p, c_uint32 = ctypes.c_void_p, ctypes.c_uint32
    kernel32.CreateFileW.restype = c_void_p
    kernel32.CreateFileW.argtypes = (ctypes.c_wchar_p, c_uint32, c_uint32, c_void_p,
                                     c_uint32, c_uint32, c_void_p)
    generic_read = 0x80000000
    share_read_write_delete = 0x1 | 0x2 | 0x4
    open_existing, file_attribute_normal = 3, 0x80
    handle = kernel32.CreateFileW(str(path), generic_read, share_read_write_delete, None,
                                  open_existing, file_attribute_normal, None)
    if handle is None or handle == c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    # open_osfhandle owns the handle from here; os.fdopen owns the fd.
    fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    return os.fdopen(fd, "rb")


#: How long ``save`` waits for a foreign reader to let go of ``jobs.json``.
REPLACE_BUDGET_S = 2.0


def _replace(src: Path, dst: Path) -> None:
    """Atomic rename-over. On Windows, ``os.replace`` (``MoveFileEx``) refuses
    a target ANY process holds open, DELETE-sharing or not; a rename with
    POSIX semantics (Windows 10 1607+, NTFS) replaces a target whose readers
    share DELETE and leaves them the bytes they opened. A reader that does
    not share DELETE still refuses (sharing violation), which the caller
    retries. Where the flag is unsupported the plain replace is used.
    """
    if sys.platform != "win32":
        os.replace(src, dst)
        return
    import ctypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    c_void_p, c_uint32 = ctypes.c_void_p, ctypes.c_uint32
    kernel32.CreateFileW.restype = c_void_p
    kernel32.CreateFileW.argtypes = (ctypes.c_wchar_p, c_uint32, c_uint32, c_void_p,
                                     c_uint32, c_uint32, c_void_p)
    kernel32.SetFileInformationByHandle.argtypes = (c_void_p, ctypes.c_int, c_void_p, c_uint32)
    kernel32.CloseHandle.argtypes = (c_void_p,)
    delete, synchronize = 0x00010000, 0x00100000
    share_read_write_delete = 0x1 | 0x2 | 0x4
    open_existing, file_attribute_normal = 3, 0x80
    handle = kernel32.CreateFileW(str(src), delete | synchronize, share_read_write_delete,
                                  None, open_existing, file_attribute_normal, None)
    if handle is None or handle == c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        name = str(dst).encode("utf-16-le")
        # FILE_RENAME_INFO for FileRenameInfoEx (22): DWORD Flags at 0 (padded
        # to 8), HANDLE RootDirectory at 8, DWORD FileNameLength at 16,
        # WCHAR FileName[] at 20.
        size = 20 + len(name) + 2
        info = ctypes.create_string_buffer(size)
        replace_if_exists, posix_semantics = 0x1, 0x2
        ctypes.memmove(info, ctypes.byref(c_uint32(replace_if_exists | posix_semantics)), 4)
        ctypes.memmove(ctypes.addressof(info) + 16, ctypes.byref(c_uint32(len(name))), 4)
        ctypes.memmove(ctypes.addressof(info) + 20, name, len(name))
        file_rename_info_ex = 22
        if kernel32.SetFileInformationByHandle(handle, file_rename_info_ex, info, size):
            return
        code = ctypes.get_last_error()
    finally:
        kernel32.CloseHandle(handle)
    error_not_supported, error_invalid_parameter = 50, 87
    if code in (error_not_supported, error_invalid_parameter):
        os.replace(src, dst)
        return
    raise ctypes.WinError(code)


def _replace_retrying(src: Path, dst: Path, budget_s: Optional[float] = None) -> None:
    """``_replace`` that waits out a transient foreign reader (Windows).

    An editor, an antivirus scan or an indexer holding ``jobs.json`` open
    without DELETE sharing makes the replace fail; most such holds last
    milliseconds. Past the budget the error propagates: a store that cannot
    be written is exit 2, never a silent skip.
    """
    deadline = time.monotonic() + (REPLACE_BUDGET_S if budget_s is None else budget_s)
    delay = 0.02
    while True:
        try:
            _replace(src, dst)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.25)


def replace_file(src: Path, dst: Path) -> None:
    """Atomic rename-over that tolerates a DELETE-sharing reader (Windows) and
    waits out a transient foreign one. OSError propagates past the budget."""
    _replace_retrying(src, dst)


def load(base: Optional[Path] = None) -> Jobs:
    """The jobs, or StoreError. Never ``{}`` for a file that exists and is wrong."""
    base = base or home()
    problems = perms_problems(base)
    if problems:
        raise StoreError("refusing an untrusted store: " + "; ".join(problems))
    path = store_path(base)
    if not path.exists():
        corrupt = sorted(base.glob(STORE_NAME + ".corrupt-*"))
        if corrupt:
            raise StoreError(
                f"{path} is absent but {corrupt[-1].name} exists -- the store was corrupt; "
                f"run `awrise reconcile --restore` to bring back {STORE_NAME}.bak, "
                f"or `awrise reconcile --reset` to start an empty store")
        return {}
    try:
        raw = json.loads(read_bytes(path).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        aside = base / f"{STORE_NAME}.corrupt-{time.strftime('%Y%m%dT%H%M%S')}"
        try:
            os.replace(path, aside)
        except OSError:
            aside = path
        raise StoreError(
            f"{path} is not valid JSON ({exc}); moved to {aside.name}. "
            f"Run `awrise reconcile --restore` to bring back {STORE_NAME}.bak, "
            f"or `awrise reconcile --reset` to start an empty store") from exc
    except OSError as exc:
        raise StoreError(f"cannot read {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise StoreError(f"{path}: top level is not an object")
    if raw.get("schema") == SCHEMA:
        return _validate_all(raw.get("jobs"), path)
    if "schema" in raw:
        raise StoreError(f"{path}: unknown schema {raw.get('schema')!r} "
                         f"(this awrise knows {SCHEMA})")
    # v1: a bare {name: record} map.
    jobs = _validate_all(_migrate_v1(raw), path)
    try:
        _copy(path, base / f"{STORE_NAME}.v1.bak")
    except OSError as exc:
        raise StoreError(f"cannot keep the v1 backup: {exc}") from exc
    save(jobs, base)
    return jobs


# --------------------------------------------------------------------- save

def _copy(src: Path, dst: Path) -> None:
    """Byte copy through the DELETE-sharing reader (see ``read_bytes``)."""
    data = read_bytes(src)
    with open(dst, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())


def save(jobs: Jobs, base: Optional[Path] = None) -> None:
    """tmp + fsync + os.replace; the previous file becomes ``.bak`` first."""
    base = base or home()
    path = store_path(base)
    bak = base / f"{STORE_NAME}.bak"
    payload = json.dumps({"schema": SCHEMA, "jobs": jobs}, indent=2, sort_keys=True) + "\n"
    tmp = base / f"{STORE_NAME}.tmp-{os.getpid()}"
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        if path.exists():
            _copy(path, bak)
        _replace_retrying(tmp, path)
    except OSError as exc:
        # The tmp is the casualty of the failed write, not the store.
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise StoreError(f"cannot write {path}: {exc}") from exc
    if not bak.exists():
        # The FIRST save has no previous version to keep, so without this the
        # store has no backup until its second one -- and a store corrupted in
        # that window has nothing to restore, which makes every verb exit 2
        # forever, naming a restore that cannot work. The first version is its
        # own backup, so the advertised recovery is true from the first job on.
        try:
            _copy(path, bak)
        except OSError as exc:
            raise StoreError(f"cannot write {bak}: {exc}") from exc
    # Inert on drvfs/NTFS; the DACL/mode check on load is the gate.
    with contextlib.suppress(OSError):
        os.chmod(path, 0o600)


def reload_merge(jobs: Jobs, names_touched: Iterable[str], base: Optional[Path] = None) -> Jobs:
    """Re-read the store and overlay ONLY the touched jobs' state fields.

    Two passes, or a pass and an operator's ``set``, would otherwise take turns
    overwriting each other's edits with a stale in-memory copy. A job removed
    from disk while we ran stays removed.
    """
    fresh = load(base)
    for name in names_touched:
        if name in fresh and name in jobs:
            for key in STATE_KEYS:
                if key == "created_at":
                    continue
                fresh[name][key] = jobs[name].get(key, STATE_DEFAULTS[key])
    return fresh


def restore(base: Optional[Path] = None) -> Path:
    """Bring ``jobs.json.bak`` back as ``jobs.json``; park corrupt copies."""
    base = base or home()
    bak = base / f"{STORE_NAME}.bak"
    if not bak.exists():
        raise StoreError(
            f"nothing to restore: {bak} does not exist. The corrupt copy is kept; "
            f"`awrise reconcile --reset` parks it and starts an empty store")
    try:
        json.loads(read_bytes(bak).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        raise StoreError(f"{bak} is itself unreadable ({exc}); nothing safe to restore") from exc
    path = store_path(base)
    parked = base / "corrupt"
    stamp = time.strftime("%Y%m%dT%H%M%S")
    try:
        if path.exists():
            parked.mkdir(exist_ok=True)
            _replace_retrying(path, parked / f"{STORE_NAME}.corrupt-{stamp}")
        for stray in base.glob(STORE_NAME + ".corrupt-*"):
            parked.mkdir(exist_ok=True)
            _replace_retrying(stray, parked / stray.name)
        _copy(bak, path)
    except OSError as exc:
        raise StoreError(f"restore failed: {exc}") from exc
    return path


def reset(base: Optional[Path] = None) -> Path:
    """Park the corrupt store and start an empty one. The LAST way out.

    A store that was corrupted before it ever had a backup cannot be
    restored, and every verb exits 2 while the corrupt copy is on disk, so
    without this the only escape is deleting a file by hand -- an instruction
    no product should have to give. It refuses while a readable store exists,
    so it can never be the command that loses a working set of jobs, and the
    corrupt bytes are parked rather than removed.
    """
    base = base or home()
    path = store_path(base)
    parked = base / "corrupt"
    stamp = time.strftime("%Y%m%dT%H%M%S")
    readable = False
    if path.exists():
        try:
            readable = isinstance(json.loads(read_bytes(path).decode("utf-8")), dict)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            readable = False
    if readable:
        raise StoreError(f"refusing to reset: {path} is readable; "
                         f"remove the jobs you do not want with `awrise remove`")
    bak = base / f"{STORE_NAME}.bak"
    if bak.exists():
        raise StoreError(f"refusing to reset: {bak.name} exists; "
                         f"run `awrise reconcile --restore` first")
    corrupt = list(base.glob(STORE_NAME + ".corrupt-*"))
    if parked.is_dir():
        corrupt += list(parked.glob(STORE_NAME + ".corrupt-*"))
    if not path.exists() and not corrupt:
        raise StoreError(f"nothing to reset: {path} is absent and no corrupt copy is parked")
    try:
        if path.exists():
            parked.mkdir(exist_ok=True)
            _replace_retrying(path, parked / f"{STORE_NAME}.corrupt-{stamp}")
        for stray in base.glob(STORE_NAME + ".corrupt-*"):
            parked.mkdir(exist_ok=True)
            _replace_retrying(stray, parked / stray.name)
    except OSError as exc:
        raise StoreError(f"reset failed: {exc}") from exc
    save({}, base)
    return path
