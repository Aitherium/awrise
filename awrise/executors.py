"""How a wake actually runs. ``shell`` is the default and needs nothing installed.

An executor takes a job spec and a wake envelope and returns an ``Outcome``
whose ``state`` is one of the ledger's closed states. It never raises for a
job that could not run -- that is an ``error`` outcome with the reason the
code can prove, so the ledger records it instead of a traceback.

Five more kinds are OPTIONAL and none of them is ever reached unless the job
spec names it: ``python`` (a snippet run as its own subprocess, so a callable
that hangs still hits the timeout instead of wedging the pass), ``http`` (a
request whose bearer comes from a confined FILE path, never a value in the
store), ``awrun`` (submit to the local run queue), ``agent`` and ``session``
(hand the work to an agent harness). Every one of their imports is made
inside the function that needs it, so this module -- and the whole package --
imports on a machine where none of those tools exists.

Every child is spawned as the leader of its own process group (a new session
on POSIX, ``CREATE_NEW_PROCESS_GROUP`` on Windows), so a timeout kills the
WHOLE tree -- ``os.killpg`` there, ``taskkill /T /F`` here -- and a grandchild
holding the output pipe cannot keep the pass alive after its parent is dead.
A ``detach`` job gets no pipes at all: its wake closes ``detached`` at spawn
and the child outlives the pass.
"""
from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from . import clock, ledger

#: Spec keys this module reads (asserted against store.SPEC_DEFAULTS by the
#: self-test, together with the keys the CLI reads).
READS = ("run", "timeout_s", "cwd", "detach", "executor", "bearer_file", "permission_mode",
         "wake", "park_after", "wake_required")

#: After the group is signalled TERM, this long before KILL (POSIX).
KILL_GRACE_S = 1.0
#: After the tree is killed, this long to drain what it wrote before giving up
#: on the pipe (a survivor that inherited it would otherwise hold the pass).
KILL_DRAIN_S = 5.0

#: Executor kinds. ``shell`` is the default; the rest are opt-in per job.
KINDS = ("shell", "python", "http", "awrun", "agent", "session")

#: Run-queue kinds this brick refuses to submit, by name. Both of them are
#: dispatched by a worker that resolves its OWN credentials from its process
#: environment, so a queued item is an instruction to spend someone else's
#: authority. A scheduler that can post them is a privilege escalation with a
#: cron entry in front of it; naming them here is the whole gate.
#: `tunnel` joined 2026-09-19 in the SAME commit that added the kind to awrun's
#: store: a wake must not be able to schedule opening a public hostname.
AWRUN_REFUSED_KINDS = ("ci", "comet-deploy", "tunnel")

#: Permission modes a ``session`` job may ask the harness daemon for. Anything
#: else -- notably a mode that skips the permission prompt entirely -- is
#: refused at ``add`` time and again here: an unattended wake must not be the
#: thing that grants an agent more than a human sitting at the keyboard has.
PERMISSION_MODES = ("default", "acceptEdits", "plan")

#: Where the harness daemon answers, and the token file it mints for itself.
SESSION_URL_ENV = "AWRISE_SESSION_URL"
DEFAULT_SESSION_URL = "http://127.0.0.1:8362"
DEFAULT_TOKEN_NAME = "harness_token"
#: The event kind that says one turn finished. Pinned by a fixture against a
#: fake daemon, because a signal nobody measured is a poll that never ends.
TURN_COMPLETE = "turn.completed"
SESSION_EXITED = "session.exited"
SESSION_POLL_S = 0.25


@dataclass
class Outcome:
    state: str
    reason: str
    exit_code: Optional[int] = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    handoff_id: Optional[str] = None
    child_pid: Optional[int] = None
    #: Set only for a job that asked for a unit to be woken first. They are
    #: carried into the finished row, so "the command took 4s" and "waking the
    #: thing it talks to took 11 minutes" are two numbers in the record rather
    #: than one duration nobody can explain.
    wake_unit: Optional[str] = None
    wake_s: Optional[float] = None
    wake_state: Optional[str] = None
    park_state: Optional[str] = None


def _spawn_kwargs(detach: bool) -> dict:
    if sys.platform == "win32":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP
        if detach:
            flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return {"creationflags": flags}
    return {"start_new_session": True}


def kill_tree(proc: subprocess.Popen) -> str:
    """Kill the child and everything under it. Returns a note for the reason
    when the kill was not the clean one ('' when it was)."""
    if sys.platform == "win32":
        try:
            done = subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                                  stdin=subprocess.DEVNULL, capture_output=True, timeout=15)
            note = "" if done.returncode == 0 else f"; taskkill_rc_{done.returncode}"
        except (OSError, subprocess.TimeoutExpired) as exc:
            note = f"; taskkill_failed:{type(exc).__name__}"
        if note:
            # The tree walk failed; the leader alone is the fallback, and the
            # note in the reason says the kill may be incomplete.
            with contextlib.suppress(OSError):
                proc.kill()
        return note
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return ""  # already gone, group and all
    except OSError as exc:
        proc.kill()
        return f"; killpg_failed:{type(exc).__name__}"
    deadline = time.monotonic() + KILL_GRACE_S
    while time.monotonic() < deadline and proc.poll() is None:
        time.sleep(0.05)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return ""  # TERM was enough
    except OSError as exc:
        proc.kill()
        return f"; killpg_failed:{type(exc).__name__}"
    return ""


def _run_process(job: dict, wake: dict, command, *, shell: bool,
                 missing_reason: str = "spawn_failed") -> Outcome:
    """Spawn ``command``, wait for it inside the job's timeout, return an Outcome.

    Shared by every executor that ends in a child process -- shell, python and
    the ``adk chat`` fallback -- so the timeout, the process-group kill, the
    detach rule and the redaction of tails are written once and behave the same
    whichever kind the operator chose.
    """
    timeout = float(job.get("timeout_s") or 300)
    if timeout > clock.MAX_TIMEOUT_S:
        # Nothing is spawned: a deadline this platform cannot express is a
        # child that could never be timed out or killed, so the wake is an
        # error the operator can fix with one `set`, not a process nobody
        # recorded and nobody can find.
        return Outcome("error", f"timeout_s_above_bound:{timeout:.0f}")
    cwd = job.get("cwd") or None
    if cwd and not Path(cwd).is_dir():
        return Outcome("error", f"cwd_missing:{cwd}")
    detach = bool(job.get("detach"))
    on_spawn = wake.get("on_spawn") if isinstance(wake, dict) else None
    stdio = subprocess.DEVNULL if detach else subprocess.PIPE
    try:
        proc = subprocess.Popen(command, shell=shell, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=stdio, stderr=stdio, **_spawn_kwargs(detach))
    except FileNotFoundError as exc:
        return Outcome("error", f"{missing_reason}:{exc.filename or exc}")
    except OSError as exc:
        return Outcome("error", f"spawn_failed:{type(exc).__name__}:{exc}")
    pgid = proc.pid  # session leader on POSIX; group leader on Windows
    if callable(on_spawn):
        on_spawn(proc.pid, pgid)
    if detach:
        return Outcome("detached", f"spawned_pid_{proc.pid}", child_pid=proc.pid)
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        note = kill_tree(proc)
        try:
            out, err = proc.communicate(timeout=KILL_DRAIN_S)
        except subprocess.TimeoutExpired as exc:
            out, err = exc.stdout, exc.stderr
            note += "; pipe_still_open"
        return Outcome("timeout", f"killed after {timeout:g}s{note}", exit_code=proc.returncode,
                       stdout_tail=ledger.tail(out), stderr_tail=ledger.tail(err),
                       child_pid=proc.pid)
    except Exception as exc:  # noqa: BLE001 - a leaked child is the worse outcome
        # The wait itself failed (an I/O error on the pipe, a deadline the
        # platform cannot express). The child is already running, so the one
        # thing that must not happen is returning without killing it: that is
        # an unrecorded wake with no pid in the row and nothing to stop it.
        note = kill_tree(proc)
        with contextlib.suppress(Exception):
            proc.communicate(timeout=KILL_DRAIN_S)
        return Outcome("error", f"wait_failed:{type(exc).__name__}:{exc}{note}",
                       child_pid=proc.pid)
    state = "success" if proc.returncode == 0 else "failure"
    return Outcome(state, f"exit_{proc.returncode}", exit_code=proc.returncode,
                   stdout_tail=ledger.tail(out), stderr_tail=ledger.tail(err),
                   child_pid=proc.pid)


def run_shell(job: dict, wake: dict) -> Outcome:
    command = (job.get("run") or "").strip()
    if not command:
        return Outcome("skipped_empty", "empty_command")
    return _run_process(job, wake, command, shell=True)


# ------------------------------------------------------------------- python

def run_python(job: dict, wake: dict) -> Outcome:
    """Run the job's ``run`` as Python source in ITS OWN interpreter.

    Deliberately a subprocess and not ``exec``: an in-process callable that
    blocks cannot be timed out (no thread can be killed in CPython) and takes
    the whole pass down with it -- every other job in the store simply never
    runs. A child hits the same deadline and the same process-group kill as a
    shell job, and the interpreter is THIS one, so a job runs under the
    interpreter awrise was installed into rather than whatever `python` means
    on the PATH of whatever fired the clock.
    """
    source = (job.get("run") or "").strip()
    if not source:
        return Outcome("skipped_empty", "empty_command")
    return _run_process(job, wake, [sys.executable, "-c", source], shell=False,
                        missing_reason="no_interpreter")


# --------------------------------------------------------------------- http

def bearer_roots() -> List[Path]:
    """The only directories a bearer file may live in.

    A token is a file PATH in the spec and never a value, so the store can be
    read (or backed up, or pasted into a bug report) without leaking one. The
    confinement is what stops the path itself becoming the exploit: without
    it, ``bearer_file`` is an arbitrary-file-read primitive that posts the
    bytes to a URL the same record chooses.
    """
    roots = [Path.home() / ".aither"]
    env = (os.environ.get("AWRISE_HOME") or "").strip()
    if env:
        roots.append(Path(env))
    out = []
    for root in roots:
        with contextlib.suppress(OSError):
            # realpath, not abspath: a symlink placed inside the home is the
            # obvious way past a confinement that only compares spellings.
            out.append(Path(os.path.realpath(str(root))))
    return out


def bearer_path_problem(raw: object) -> Optional[str]:
    """Why this bearer path is refused, or None. Used by ``add``/``set`` too,
    so a job that could never read its token is refused when it is written
    rather than at 3am on the first wake."""
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        return "must be a path to a file holding the token"
    path = Path(os.path.realpath(os.path.expanduser(raw.strip())))
    roots = bearer_roots()
    if not any(path == root or root in path.parents for root in roots):
        return ("must live under " + " or ".join(str(r) for r in roots)
                + " (a token path outside it is an arbitrary-file read)")
    return None


def read_bearer(job: dict) -> Tuple[Optional[str], Optional[Outcome]]:
    """(token, None) or (None, the error Outcome that says why not)."""
    raw = job.get("bearer_file")
    if raw is None:
        return None, None
    problem = bearer_path_problem(raw)
    if problem is not None:
        return None, Outcome("error", f"bearer_file_refused:{problem}")
    path = Path(os.path.realpath(os.path.expanduser(str(raw).strip())))
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return None, Outcome("error", f"bearer_file_unreadable:{type(exc).__name__}")
    if not token:
        return None, Outcome("error", "bearer_file_empty")
    return token, None


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse to follow a redirect.

    urllib re-sends the headers it was given, so a 302 is enough to make a job
    hand its bearer to whatever host the answer names. A scheduler firing
    unattended must not do that on its own; a moved endpoint is a one-line
    ``set run=`` by a human who can see where it moved to.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code,
                                     f"redirect refused to {newurl}", headers, fp)


_OPENER = urllib.request.build_opener(_NoRedirects)


def _parse_http_run(source: str) -> Tuple[str, str, Optional[bytes], Optional[str]]:
    """``[METHOD ]URL[ BODY]`` -> (method, url, body, problem).

    The body is EVERYTHING after the URL, whitespace and all. Splitting it on
    the first space would cut ``{"a": 1}`` down to ``{"a":`` and post that,
    with nothing in the row saying it had been cut -- and it would do so only
    for the shape that omits the method, so the same request written with a
    leading ``POST`` would be sent whole.
    """
    parts = source.split(None, 2)
    if not parts:
        return "", "", None, "empty_command"
    first = parts[0]
    if "://" in first:
        method, url = "GET", first
        after_url = source.split(None, 1)
        rest = after_url[1:]
    elif len(parts) >= 2:
        method, url, rest = first.upper(), parts[1], parts[2:]
    else:
        return "", "", None, f"not_a_url:{first}"
    scheme = urllib.parse.urlsplit(url).scheme.lower()
    if scheme not in ("http", "https"):
        return "", "", None, f"unsupported_scheme:{scheme or 'none'}"
    body = rest[0].encode("utf-8") if rest and rest[0].strip() else None
    return method, url, body, None


def run_http(job: dict, wake: dict) -> Outcome:
    """One request, one verdict.

    A name that does not resolve is ``skipped_unresolvable``, not a failure:
    a laptop off the fleet network has not run a job badly, it has not been
    able to run it at all, and a ledger that says ``failure`` there would have
    an operator hunting a service that is fine.
    """
    source = (job.get("run") or "").strip()
    if not source:
        return Outcome("skipped_empty", "empty_command")
    method, url, body, problem = _parse_http_run(source)
    if problem is not None:
        return Outcome("error", problem) if problem != "empty_command" else Outcome(
            "skipped_empty", "empty_command")
    token, refusal = read_bearer(job)
    if refusal is not None:
        return refusal
    timeout = float(job.get("timeout_s") or 300)
    headers = {"User-Agent": "awrise"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    host = urllib.parse.urlsplit(url).hostname or url
    try:
        with _OPENER.open(request, timeout=timeout) as response:
            payload = response.read(64 * 1024)
            code = int(response.status or 0)
        return Outcome("success", f"http_{code}", exit_code=code,
                       stdout_tail=ledger.tail(payload))
    except urllib.error.HTTPError as exc:
        payload = b""
        with contextlib.suppress(Exception):
            payload = exc.read(64 * 1024)
        if exc.code in (401, 403):
            return Outcome("error", f"http_{exc.code}", exit_code=exc.code,
                           stderr_tail=ledger.tail(payload))
        return Outcome("failure", f"http_{exc.code}", exit_code=exc.code,
                       stderr_tail=ledger.tail(payload))
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.gaierror):
            return Outcome("skipped_unresolvable", f"dns:{host}")
        if isinstance(exc.reason, socket.timeout):
            return Outcome("timeout", f"killed after {timeout:g}s")
        return Outcome("error", f"unreachable:{type(exc.reason).__name__}:{exc.reason}")
    except socket.gaierror:
        return Outcome("skipped_unresolvable", f"dns:{host}")
    except socket.timeout:
        return Outcome("timeout", f"killed after {timeout:g}s")
    except OSError as exc:
        return Outcome("error", f"unreachable:{type(exc).__name__}:{exc}")


# -------------------------------------------------------------------- awrun

def _awrun_store():
    """The local run queue, or None when awrun is not installed here.

    Imported inside the function on purpose: awrise is a standalone brick and
    must import on a machine that has never heard of the run queue.
    """
    from awrun.store import RunStore  # noqa: PLC0415 - optional by contract

    return RunStore(os.environ.get("AITHER_AWRUN_DIR"))


class _ClaimOnlyMine:
    """A run-queue view that offers a claimer exactly ONE item: ours.

    The drain runs through the queue package's own dispatcher, and that
    dispatcher claims the highest-priority queued item of ANY kind. Handed
    the bare queue it will therefore claim, start and finish work another
    actor submitted -- including the kinds this module refuses by name, whose
    handlers spend the host owner's credentials -- and it will do it with a
    cron entry in front of it. Narrowing what ``claim_next`` may hand back is
    what makes the refused-kind list a gate on the drain and not only on the
    submit, and it is also what keeps the wake's record about the run this
    wake actually queued. Everything else (leases, heartbeats, the finish)
    stays the queue's own code, reached through this one override.
    """

    def __init__(self, queue: object, item_id: str) -> None:
        self._queue = queue
        self._item_id = item_id

    def claim_next(self, *, worker_id, kind=None, skip=None, now=None):
        def only_mine(item) -> bool:
            if getattr(item, "id", None) != self._item_id:
                return True
            return bool(skip(item)) if skip is not None else False

        return self._queue.claim_next(worker_id=worker_id, kind=kind,  # type: ignore[attr-defined]
                                      skip=only_mine, now=now)

    def __getattr__(self, name: str):
        return getattr(self._queue, name)


def _parse_awrun_run(source: str) -> Tuple[str, dict, int, Optional[str]]:
    """``<kind>[ <json spec>]`` -> (kind, spec, priority, problem)."""
    parts = source.split(None, 1)
    kind = parts[0]
    raw = parts[1].strip() if len(parts) > 1 else ""
    spec: dict = {}
    if raw:
        try:
            spec = json.loads(raw)
        except ValueError as exc:
            return kind, {}, 0, f"spec_not_json:{exc}"
        if not isinstance(spec, dict):
            return kind, {}, 0, f"spec_not_an_object:{type(spec).__name__}"
    priority = spec.pop("priority", 0)
    if isinstance(priority, bool) or not isinstance(priority, int):
        return kind, {}, 0, f"priority_not_an_integer:{priority!r}"
    return kind, spec, priority, None


def run_awrun(job: dict, wake: dict) -> Outcome:
    """Submit one item to the local run queue; ``queued`` is a real outcome.

    A submit that was accepted is NOT a success -- nothing has run yet -- so
    the wake closes ``queued`` carrying the run id, and that id is what ties
    the two records together afterwards. With ``--drain`` the same pass also
    claims and runs THAT item -- its own, never another actor's -- so a host
    with no worker still makes progress on what it queued, and the row it
    writes describes the run the row names.
    """
    source = (job.get("run") or "").strip()
    if not source:
        return Outcome("skipped_empty", "empty_command")
    kind, spec, priority, problem = _parse_awrun_run(source)
    if problem is not None:
        return Outcome("error", problem)
    if kind in AWRUN_REFUSED_KINDS:
        return Outcome("error", f"awrun_kind_refused:{kind}")
    try:
        queue = _awrun_store()
    except ImportError:
        return Outcome("error", "awrun_not_installed")
    except OSError as exc:
        return Outcome("error", f"awrun_dir_unusable:{type(exc).__name__}:{exc}")
    try:
        item = queue.submit(kind, spec, priority=priority)
    except Exception as exc:  # noqa: BLE001 - the queue's own refusal is a reason, not a crash
        return Outcome("error", f"awrun_submit_refused:{type(exc).__name__}:{exc}")
    handoff = getattr(item, "id", None)
    if not (isinstance(wake, dict) and wake.get("drain")):
        return Outcome("queued", f"awrun_{kind}_queued", handoff_id=handoff)
    worker = f"awrise:{(wake.get('wake_id') if isinstance(wake, dict) else None) or 'wake'}"
    if not handoff:
        return Outcome("queued", f"awrun_{kind}_queued; drain_needs_an_item_id")
    try:
        from awrun.dispatcher import dispatch_once  # noqa: PLC0415 - optional by contract

        finished = dispatch_once(_ClaimOnlyMine(queue, handoff), worker_id=worker)
    except ImportError:
        return Outcome("queued", f"awrun_{kind}_queued; drain_unavailable",
                       handoff_id=handoff)
    except Exception as exc:  # noqa: BLE001 - a dispatcher that raised still closes the wake
        return Outcome("error", f"awrun_drain_failed:{type(exc).__name__}:{exc}",
                       handoff_id=handoff)
    if finished is None:
        # Somebody else got there first, or the queue is holding it back. The
        # item is still the wake's own handoff, so the wake is `queued`.
        return Outcome("queued", f"awrun_{kind}_queued; drain_did_not_claim_it",
                       handoff_id=handoff)
    if getattr(finished, "id", None) != handoff:
        # Belt and braces for the narrowing above: a stranger's result is
        # never this wake's verdict, whatever came back.
        return Outcome("queued", f"awrun_{kind}_queued; drain_claimed_another_item",
                       handoff_id=handoff)
    status = getattr(finished, "status", "") or "unknown"
    result = getattr(finished, "result", None) or {}
    code = result.get("code") if isinstance(result, dict) else None
    message = result.get("message") if isinstance(result, dict) else None
    if status == "queued":
        # The queue put it BACK -- a lease door said "not now", or a path
        # lease was lost to a peer. That is contention the queue handled, and
        # paging an operator for it would teach them to ignore the pager.
        return Outcome("queued", f"awrun_{kind}_requeued", handoff_id=handoff)
    state = "success" if status == "done" else "failure"
    return Outcome(state, f"awrun_drained:{status}",
                   exit_code=code if isinstance(code, int) else None,
                   stdout_tail=ledger.tail(str(message).encode("utf-8")) if message else "",
                   handoff_id=handoff)


# -------------------------------------------------------------------- agent

def _public_run_agent():
    """awrun's public inline agent entry point, or None.

    There is no such name in the queue package today -- the only runner is
    private -- so this returns None and the wake falls back to the CLI. The
    lookup stays because the alias is a one-line addition upstream, and when
    it lands a job gets the inline path with no change here.
    """
    import importlib  # noqa: PLC0415 - optional by contract

    module = importlib.import_module("awrun.dispatcher")
    fn = getattr(module, "run_agent", None)
    return fn if callable(fn) else None


def run_agent(job: dict, wake: dict) -> Outcome:
    """``<agent> <task>`` -- inline if the queue package offers a public entry
    point, otherwise the ``adk chat`` CLI, and the reason says which."""
    source = (job.get("run") or "").strip()
    if not source:
        return Outcome("skipped_empty", "empty_command")
    parts = source.split(None, 1)
    agent = parts[0]
    task = parts[1].strip() if len(parts) > 1 else ""
    if not task:
        return Outcome("error", f"agent_task_missing:{agent}")
    inline = None
    with contextlib.suppress(ImportError, AttributeError):
        inline = _public_run_agent()
    if inline is not None:
        try:
            from awrun.store import RunItem  # noqa: PLC0415 - optional by contract

            item = RunItem(id=str(wake.get("wake_id") or "w-inline"), kind="agent",
                           spec={"agent": agent, "task": task})
            result = inline(item)
        except Exception as exc:  # noqa: BLE001 - an inline runner that raised is a reason
            return Outcome("error", f"agent_inline_failed:{type(exc).__name__}:{exc}")
        if not (isinstance(result, tuple) and len(result) == 2
                and isinstance(result[0], int)):
            return Outcome("error", f"agent_inline_returned_{type(result).__name__}")
        code, message = result
        state = "success" if code == 0 else "failure"
        return Outcome(state, f"agent_inline:exit_{code}", exit_code=code,
                       stdout_tail=ledger.tail(str(message).encode("utf-8")))
    # Resolved here, not left to the OS: a bare name on Windows is searched
    # with `.exe` only, so a console script installed as a `.cmd` shim exists,
    # answers `--version`, and cannot be spawned.
    adk = which("adk")
    if adk is None:
        return Outcome("error", "no_agent_executor:adk")
    outcome = _run_process(job, wake, [adk, "chat", agent, task], shell=False,
                           missing_reason="no_agent_executor")
    if outcome.state == "error" and outcome.reason.startswith("no_agent_executor"):
        return outcome
    return Outcome(outcome.state, f"fallback_adk_chat:{outcome.reason}",
                   exit_code=outcome.exit_code, stdout_tail=outcome.stdout_tail,
                   stderr_tail=outcome.stderr_tail, child_pid=outcome.child_pid)


# ------------------------------------------------------------------ session

def session_base_url() -> str:
    return (os.environ.get(SESSION_URL_ENV) or "").strip() or DEFAULT_SESSION_URL


def _session_token(job: dict) -> Tuple[Optional[str], Optional[Outcome]]:
    if job.get("bearer_file"):
        return read_bearer(job)
    default = Path.home() / ".aither" / DEFAULT_TOKEN_NAME
    try:
        token = default.read_text(encoding="utf-8").strip()
    except OSError:
        return None, Outcome("error", "daemon_token_missing")
    if not token:
        return None, Outcome("error", "daemon_token_missing")
    return token, None


def _session_call(url: str, token: str, *, method: str, payload: Optional[dict],
                  timeout: float) -> Tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Authorization": f"Bearer {token}", "User-Agent": "awrise"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    # The same no-redirect opener: the daemon is local, and a 302 out of it
    # would hand the harness bearer to wherever the answer pointed.
    with _OPENER.open(request, timeout=timeout) as response:
        raw = response.read(256 * 1024)
        code = int(response.status or 0)
    try:
        body = json.loads(raw.decode("utf-8")) if raw else {}
    except ValueError:
        body = {}
    return code, body if isinstance(body, dict) else {}


def run_session(job: dict, wake: dict) -> Outcome:
    """POST a session, send the task, poll until the turn completes, ALWAYS delete.

    The delete is in a ``finally`` because the failure this executor exists to
    avoid is a scheduler that leaks a live agent process per wake: every
    minute, forever, each one holding a model connection and a working
    directory. A wake that could not even be created deletes nothing; a wake
    that got an id deletes it whatever happened next.
    """
    text = (job.get("run") or "").strip()
    if not text:
        return Outcome("skipped_empty", "empty_command")
    mode = job.get("permission_mode")
    if mode is not None and mode not in PERMISSION_MODES:
        return Outcome("error", f"permission_mode_refused:{mode}")
    token, refusal = _session_token(job)
    if refusal is not None:
        return refusal
    timeout = float(job.get("timeout_s") or 300)
    base = session_base_url().rstrip("/")
    host = urllib.parse.urlsplit(base).hostname or base
    body: dict = {}
    if job.get("cwd"):
        body["cwd"] = str(job["cwd"])
    if mode:
        body["permission_mode"] = mode
    deadline = time.monotonic() + timeout
    session_id = None
    try:
        _code, created = _session_call(f"{base}/sessions", token, method="POST",
                                       payload=body, timeout=timeout)
        session_id = created.get("id")
        if not session_id:
            return Outcome("error", "daemon_returned_no_session_id")
        _session_call(f"{base}/sessions/{session_id}/input", token, method="POST",
                      payload={"text": text}, timeout=timeout)
        since = 0
        chunks: List[str] = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return Outcome("timeout", f"killed after {timeout:g}s",
                               stdout_tail=ledger.tail("".join(chunks).encode("utf-8")),
                               handoff_id=session_id)
            _code, page = _session_call(
                f"{base}/sessions/{session_id}/events?since={since}", token,
                method="GET", payload=None, timeout=min(remaining, timeout))
            events = page.get("events") or []
            for event in events:
                if not isinstance(event, dict):
                    continue
                since = max(since, int(event.get("seq") or since))
                kind = event.get("kind") or ""
                if kind in ("text.delta", "notice"):
                    chunks.append(str(event.get("text") or ""))
                if kind == TURN_COMPLETE:
                    tail = ledger.tail("".join(chunks).encode("utf-8"))
                    return Outcome("success", "turn_completed", stdout_tail=tail,
                                   handoff_id=session_id)
                if kind == "error":
                    return Outcome("failure", f"session_error:{event.get('text') or 'unknown'}",
                                   stderr_tail=ledger.tail(str(
                                       event.get("text") or "").encode("utf-8")),
                                   handoff_id=session_id)
                if kind == SESSION_EXITED:
                    return Outcome("failure", "session_exited_before_turn_completed",
                                   stdout_tail=ledger.tail("".join(chunks).encode("utf-8")),
                                   handoff_id=session_id)
            # EVERY poll pauses, not only a poll that came back empty. A live
            # turn streams deltas, so the page is almost never empty and a
            # sleep guarded by emptiness never runs: the loop then asks the
            # local daemon for events as fast as the socket allows, for the
            # whole turn, on every wake. The same pause is what stops a page
            # whose events carry no sequence number -- so `since` cannot
            # advance and the same page comes back -- from spinning too.
            time.sleep(min(SESSION_POLL_S, max(deadline - time.monotonic(), 0.0)))
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return Outcome("error", f"http_{exc.code}", exit_code=exc.code,
                           handoff_id=session_id)
        return Outcome("failure", f"http_{exc.code}", exit_code=exc.code,
                       handoff_id=session_id)
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, socket.gaierror):
            return Outcome("skipped_unresolvable", f"dns:{host}")
        if isinstance(exc.reason, (socket.timeout, TimeoutError)):
            # The deadline was the job's own, so it is a timeout and not an
            # unreachable daemon: those two send an operator to different
            # places, and only one of them is the daemon's fault.
            return Outcome("timeout", f"killed after {timeout:g}s", handoff_id=session_id)
        return Outcome("error", f"daemon_unreachable:{exc.reason}", handoff_id=session_id)
    except socket.gaierror:
        return Outcome("skipped_unresolvable", f"dns:{host}")
    except (socket.timeout, TimeoutError):
        return Outcome("timeout", f"killed after {timeout:g}s", handoff_id=session_id)
    except (OSError, ValueError) as exc:
        return Outcome("error", f"daemon_unreachable:{type(exc).__name__}:{exc}",
                       handoff_id=session_id)
    finally:
        if session_id:
            with contextlib.suppress(Exception):
                _session_call(f"{base}/sessions/{session_id}", token, method="DELETE",
                              payload=None, timeout=min(30.0, timeout))


# ---------------------------------------------------------------- unit plane

#: A job may name a UNIT to wake before it runs (``wake``) and ask for it to be
#: put back to sleep afterwards (``park_after``). The unit is started by a host
#: agent that owns systemd; awrise never does. They meet on a directory both can
#: write -- one file per request, one per answer, no port, no token, no daemon of
#: our own:
#:
#:     <plane>/requests/<id>.json   written here  {id, action, unit, wait_s, ...}
#:     <plane>/results/<id>.json    the answer    {id, ok, state, reason, wake_s}
#:     <plane>/agent.json           heartbeat     {alive_at, ...}
#:
#: This client is VENDORED -- 90 lines of stdlib -- on purpose. awrise is a
#: standalone brick: importing the fleet library that owns the other half would
#: make ``awrise run-due`` a ``ModuleNotFoundError`` on every machine that does
#: not have the monorepo, which is every machine but one.
#: Both sides must name the SAME directory:
#:   1. ``AITHER_GPU_UNIT_PLANE_DIR`` (explicit; what a test and a container set)
#:   2. ``<AITHER_LIBRARY>/Data/compute/gpu_unit_plane``
#: With neither set there is NO plane and a wake is refused at once, rather than
#: a request written where nothing reads it and a budget spent waiting for it.
UNIT_PLANE_DIR_ENV = "AITHER_GPU_UNIT_PLANE_DIR"
UNIT_LIBRARY_ENV = "AITHER_LIBRARY"
UNIT_PLANE_SUBPATH = ("Data", "compute", "gpu_unit_plane")
#: A heartbeat older than this means nobody is serving the plane, so nothing is
#: written: an unclaimable request must not cost a job its whole wake budget.
AGENT_TTL_S = 30.0
#: How long the agent may take to bring a unit up and prove it healthy. A model
#: reload on a loaded host is minutes, not seconds, so this is deliberately far
#: longer than any sane command timeout; ``AWRISE_WAKE_BUDGET_S`` overrides it.
WAKE_BUDGET_S = 600.0
WAKE_BUDGET_ENV = "AWRISE_WAKE_BUDGET_S"
#: The agent answers a wake only once the unit is HEALTHY, so this side waits
#: PAST the budget it asked for. A client that gives up first turns a slow
#: success into a reported failure -- the exact lie this plane exists to avoid.
WAKE_GRACE_S = 20.0
#: A park is a stop: the agent answers as soon as it has decided, not after a
#: reload, so it needs nothing like the wake budget.
PARK_WAIT_S = 200.0
#: Closed states that mean the WORK IS STILL OUTSTANDING: the wake ended, and
#: the thing it started has not. Parking on one of these stops the unit out from
#: under the very job that woke it -- a detached child still running, or a run
#: queue item submitted but not yet dispatched. The spec refuses `park_after`
#: with `detach`; this catches the same hazard from the OUTCOME side, so an
#: executor that closes early (awrun without --drain does) cannot reintroduce it.
UNFINISHED_STATES = ("detached", "queued")
PLANE_POLL_S = 0.5
PLANE_TTL_S = 900
#: What the agent accepts as a unit name. Checked HERE too, so a typo is a
#: refusal at ``add`` time instead of a wake that fails every night at 03:00.
UNIT_RE = re.compile(r"[a-z0-9][a-z0-9._-]{2,80}\.service")


def unit_plane_dir() -> Optional[Path]:
    """The plane directory, or None when this machine has no plane configured."""
    override = (os.environ.get(UNIT_PLANE_DIR_ENV) or "").strip()
    if override:
        return Path(override)
    library = (os.environ.get(UNIT_LIBRARY_ENV) or "").strip()
    if library:
        return Path(library).joinpath(*UNIT_PLANE_SUBPATH)
    return None


def unit_name_problem(unit: object) -> Optional[str]:
    """Why ``unit`` is not a unit name the agent would look at, or None."""
    if unit is None:
        return None
    if not isinstance(unit, str) or not unit.strip():
        return "must be a unit name like my-model.service"
    name = unit.strip()
    if not UNIT_RE.fullmatch(name):
        return (f"{name!r} is not a plain .service name (lowercase letters, digits, "
                "'.', '_' and '-', ending in .service)")
    return None


def agent_alive(plane: Path, now: Optional[float] = None) -> bool:
    """Is someone serving this plane right now? A missing, stale or unreadable
    heartbeat is False -- an unanswerable question is never a yes."""
    try:
        doc = json.loads((plane / "agent.json").read_text(encoding="utf-8"))
        alive_at = float(doc.get("alive_at") or 0)
    except (OSError, ValueError, TypeError, AttributeError):
        return False
    return (time.time() if now is None else now) - alive_at <= AGENT_TTL_S


def _refused(unit: str, reason: str, state: str = "failed") -> dict:
    return {"ok": False, "state": state, "unit": unit, "reason": reason}


def ask_unit_agent(action: str, unit: str, wait_s: float, plane: Optional[Path] = None,
                   extra: Optional[dict] = None, poll_s: float = PLANE_POLL_S) -> dict:
    """Ask the host agent to ``action`` ``unit`` and wait for ITS verdict.

    Fail-closed in both directions: nothing is written when no agent is alive,
    and an unanswered request is ``failed`` (never ``done``) AND withdrawn, so
    an agent that wakes up late cannot start or stop a unit for a job that
    already gave up on it.
    """
    directory = Path(plane) if plane is not None else unit_plane_dir()
    if directory is None:
        return _refused(unit, f"no unit plane on this machine: set {UNIT_PLANE_DIR_ENV}")
    if not agent_alive(directory):
        return _refused(unit, f"no unit agent heartbeat in {directory} within "
                              f"{AGENT_TTL_S:.0f}s -- nothing would claim the request")
    rid = uuid.uuid4().hex[:16]
    request = directory / "requests" / f"{rid}.json"
    result = directory / "results" / f"{rid}.json"
    payload = {"id": rid, "action": action, "unit": unit, "token": "",
               "ttl_s": PLANE_TTL_S, "created_at": time.time()}
    payload.update(extra or {})
    try:
        request.parent.mkdir(parents=True, exist_ok=True)
        tmp = request.with_name(request.name + f".tmp-{os.getpid()}")
        with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(payload, fh)
        os.replace(tmp, request)
    except OSError as exc:
        return _refused(unit, f"cannot write the {action} request: {type(exc).__name__}: {exc}")
    deadline = time.monotonic() + max(1.0, float(wait_s))
    while True:
        try:
            answer = json.loads(result.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            answer = None
        if isinstance(answer, dict):
            return answer
        if answer is not None:
            return _refused(unit, "the agent's answer is not an object")
        if time.monotonic() >= deadline:
            break
        time.sleep(poll_s)
    try:
        request.unlink()
    except FileNotFoundError:
        return _refused(unit, "the agent took the request and never answered")
    except OSError as exc:
        return _refused(unit, f"no answer within {wait_s:.0f}s and the request could not be "
                              f"withdrawn: {type(exc).__name__}: {exc}")
    return _refused(unit, f"no agent answered within {wait_s:.0f}s")


def wake_budget_s() -> float:
    raw = (os.environ.get(WAKE_BUDGET_ENV) or "").strip()
    try:
        value = float(raw) if raw else WAKE_BUDGET_S
    except ValueError:
        value = WAKE_BUDGET_S
    return max(1.0, min(value, float(clock.MAX_TIMEOUT_S)))


def _verdict(answer: dict) -> Tuple[bool, str, str]:
    ok = bool(answer.get("ok"))
    state = str(answer.get("state") or ("done" if ok else "failed")).strip() or "failed"
    reason = str(answer.get("reason") or "").strip() or "the agent gave no reason"
    return ok, state, reason


def wake_unit(unit: str, plane: Optional[Path] = None) -> Tuple[dict, float]:
    """Ask for ``unit`` and return (answer, wake_s). ``wake_s`` is the agent's
    own measurement when it made one -- it knows when the unit became healthy --
    and this side's wall clock otherwise, so the ledger always has a number."""
    budget = wake_budget_s()
    started = time.monotonic()
    answer = ask_unit_agent("wake", unit, wait_s=budget + WAKE_GRACE_S, plane=plane,
                            extra={"wait_s": budget})
    waited = round(time.monotonic() - started, 3)
    try:
        wake_s = round(float(answer["wake_s"]), 3)
    except (KeyError, TypeError, ValueError):
        wake_s = waited
    return answer, wake_s


def park_unit(job: dict, unit: str, outcome: Optional[Outcome] = None,
              plane: Optional[Path] = None) -> Optional[str]:
    """Put ``unit`` back to sleep after the job ran. Returns what happened, or
    None when the job did not ask. A park NEVER changes the job's own state: the
    command ran and its exit code is the truth; whether the unit went back to
    sleep is a separate fact, recorded separately.

    It is also refused whenever the work is still outstanding -- a detached
    child, or a queued run-queue item -- because stopping the unit then is the
    one way this feature could break the job it exists to serve.
    """
    if not job.get("park_after"):
        return None
    if job.get("detach"):
        # The child outlives this pass, so parking now would stop the unit out
        # from under the thing that was just started to use it.
        return "skipped:detached child is still running"
    state = getattr(outcome, "state", None)
    if state in UNFINISHED_STATES:
        return f"skipped:{state} -- the work that needs the unit has not finished"
    ok, state, reason = _verdict(ask_unit_agent("park", unit, wait_s=PARK_WAIT_S, plane=plane))
    return state if ok else f"{state}:{reason}"


def run_woken(job: dict, wake: dict, inner: Callable[[dict, dict], Outcome]) -> Outcome:
    """Wake the unit the job names, run it, then park the unit if asked.

    ``wake_required`` (default true) is the whole policy: a job whose command
    only makes sense against a live unit must NOT run when the wake failed --
    it would fail in some way that reads like the command's own bug. A job that
    says ``wake_required: false`` runs anyway, and the row says the wake failed.
    """
    unit = str(job.get("wake") or "").strip()
    plane = wake.get("unit_plane") if isinstance(wake, dict) else None
    problem = unit_name_problem(unit)
    if problem:
        return Outcome("error", f"wake_refused:{problem}", wake_unit=unit,
                       wake_state="refused")
    required = job.get("wake_required")
    required = True if required is None else bool(required)
    answer, wake_s = wake_unit(unit, plane)
    ok, state, reason = _verdict(answer)
    if not ok:
        if required:
            # Nothing ran: no child was spawned, and the reason names the unit
            # and the agent's own words for why it is not up.
            return Outcome("error", f"wake_failed:{unit}:{state}:{reason}",
                           wake_unit=unit, wake_s=wake_s, wake_state=state)
        outcome = inner(job, wake)
        return replace(outcome, wake_unit=unit, wake_s=wake_s,
                       wake_state=f"{state}:not_required:{reason}",
                       park_state="skipped:the unit was never woken"
                       if job.get("park_after") else None)
    outcome = inner(job, wake)
    return replace(outcome, wake_unit=unit, wake_s=wake_s, wake_state=state,
                   park_state=park_unit(job, unit, outcome, plane))


EXECUTORS = {
    "shell": run_shell,
    "python": run_python,
    "http": run_http,
    "awrun": run_awrun,
    "agent": run_agent,
    "session": run_session,
}


def run(job: dict, wake: dict) -> Outcome:
    """Dispatch on the job's ``executor``. An unknown kind is an error row,
    never a default that silently runs something else as a shell command.

    A job that names a ``wake`` unit is wrapped: the unit is woken first and
    parked afterwards if asked. The wrap is HERE rather than inside each
    executor so every kind gets it once and none can forget it.
    """
    kind = job.get("executor") or "shell"
    fn = EXECUTORS.get(kind)
    if fn is None:
        return Outcome("error", f"unknown_executor:{kind}")
    if job.get("wake"):
        return run_woken(job, wake, fn)
    return fn(job, wake)


def which(name: str) -> Optional[str]:
    """``shutil.which`` behind one name, so the sinks and their tests agree
    about what 'installed' means."""
    return shutil.which(name)
