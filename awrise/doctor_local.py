"""``awrise doctor``'s local half: is the clock installed, is it ticking, can
the record be written.

Three questions, no network. The generated doctor answers "is the family
installed"; this answers "would a wake actually happen on THIS host", which is
the only question an operator is really asking. Every line is a measurement or
says it could not measure -- there is no line here that is printed because
things are usually fine.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Optional

from . import clock, hostclock, ledger, store

_CMD_HOME_RE = re.compile(r'(?im)^\s*set\s+"?AWRISE_HOME=([^"\r\n]+)"?\s*$')
_SH_HOME_RE = re.compile(
    r'(?m)^\s*(?:export\s+|Environment="?|)AWRISE_HOME=(?:"([^"\r\n]+)"|\'([^\'\r\n]+)\'|([^\s"\r\n]+))'
)
_WIN_DRIVE_RE = re.compile(r"^([A-Za-z]):[\\/](.*)$")
_WSL_MOUNT_RE = re.compile(r"^(?:/var)?/mnt/([A-Za-z])/(.*)$")
_SPLIT_SIBLING_FRESH_S = 600.0


def _home() -> Path:
    return store.home()


def _clock_line(base: Path) -> "tuple[str, list, list]":
    """The host-clock line, and what it means for the exit code.

    Returns (line, problems, unjudged). The verdict comes from
    `hostclock.check`, which is the same judgement `awrise install --check`
    publishes -- two answers to "is a clock running" that could disagree would
    be worse than one.
    """
    try:
        record = hostclock.read_record(base)
    except hostclock.HostClockError as exc:
        return (
            f"hostclock  unreadable install record: {exc}",
            [],
            [f"the install record could not be read: {exc}"],
        )
    if record is None:
        # No record of OURS is not automatically a no: a stranger's own cron
        # line is a perfectly good clock this brick did not install, and
        # calling that a violation teaches an operator to ignore the verdict.
        # So the ledger decides. No entry of ours AND no pass has ever run is
        # not "cannot judge" -- it is the measured absence of a clock, which
        # is the state this host was found in on 2026-09-18: an empty ledger
        # directory, no day file ever written, and every surface reporting
        # awrise health reporting on a clock that had never ticked.
        line = (
            "hostclock  NOT INSTALLED by this awrise -- nothing wakes `run-due` "
            "unless you have your own entry. `awrise install-clock`"
        )
        try:
            # SCHEDULED ticks only. A pass run by hand writes the same row, so
            # the unfiltered reading answered "something else is ticking it"
            # about a host whose clock had never once fired -- the exact silence
            # this line exists to break.
            age = hostclock.tick_age_s(base, hostclock.SCHEDULED_INVOKERS)
        except OSError as exc:
            return (
                line,
                [],
                [f"no host clock of ours, and the ledger window could not be read: {exc}"],
            )
        if age is None:
            hand = hostclock.tick_age_s(base)
            return (
                line,
                [
                    "nothing wakes `run-due` on this host: no clock installed by "
                    "this awrise, and no pass has ever been started by a scheduler"
                    + (
                        ""
                        if hand is None
                        else f" (the last pass, {hostclock.tick_age_phrase(hand)}, "
                        f"was hand-run; if it was really yours, launch it as "
                        f"`run-due --invoker <kind>` so it can be told apart)"
                    )
                    + " (`awrise install-clock`)"
                ],
                [],
            )
        return (
            line + f"\n          something else is ticking it: last scheduled pass "
            f"{hostclock.tick_age_phrase(age)}",
            [],
            [
                "a host clock this awrise did not install is ticking; its schedule "
                "is not ours to judge"
            ],
        )
    kind = record.get("kind")
    every = record.get("every_s")
    installed = record.get("installed_at")
    line = f"hostclock  {kind} every {every}s, installed {installed}"
    problems: list = []
    unjudged: list = []
    python = record.get("python")
    if python and os.path.normcase(str(python)) != os.path.normcase(sys.executable):
        # A rebuilt venv leaves the entry pointing at an interpreter that no
        # longer has awrise: the scheduler keeps firing and every wake dies
        # before the ledger is opened, so nothing anywhere records a failure.
        line += f"\n          REINSTALL: the entry runs {python}, this awrise is {sys.executable}"
        problems.append(f"the installed entry runs {python}, not this interpreter")
    # The at-startup entry is registered best effort (it needs elevation), so a
    # missing one is a real gap in an otherwise healthy clock: printed here on
    # every run, with its one-line fix, rather than found after a reboot. It is
    # not a verdict -- the clock this line judges IS running.
    try:
        ctx = hostclock.context(str(kind), base=base, every_s=int(every or 60))
        for note in hostclock.boot_entry_notes(str(kind), ctx):
            if note.startswith("NOTICE") or note.startswith("boot entry: UNJUDGED"):
                line += "\n          " + note.replace("\n", "\n          ")
    except (hostclock.HostClockError, OSError, TypeError, ValueError) as exc:
        # Printed, never swallowed: a context we cannot build says nothing
        # about whether the clock ticks (the check below is that judgement),
        # but it does mean this line could not look at the boot entry, and a
        # question that was not asked must not look like one answered no.
        line += f"\n          boot entry: UNJUDGED -- {exc}"
    try:
        code, said = hostclock.check(base=base)
    except (hostclock.HostClockError, OSError) as exc:
        return line, problems, unjudged + [f"the host clock could not be probed: {exc}"]
    verdict = next((s for s in reversed(said) if s.startswith(("NOT OK", "UNJUDGED"))), None)
    if code == 1:
        problems.append(verdict or f"the {kind} entry is registered and not running")
    elif code == 2:
        unjudged.append(verdict or f"the {kind} entry could not be judged")
    return line, problems, unjudged


def _tick_line(base: Path) -> "tuple[str, list, list]":
    """When a pass last ran. The verdict on that belongs to `_clock_line`.

    "No tick" on its own is not a no: an awrise installed a minute ago has
    none, and an awrise with no clock of ours was already reported UNJUDGED
    above. What IS judged here is a window that could not be read at all.
    """
    try:
        age = hostclock.tick_age_s(base, hostclock.SCHEDULED_INVOKERS)
        any_age = hostclock.tick_age_s(base)
    except OSError as exc:
        return (
            f"last tick  UNREADABLE: {exc}",
            [],
            [f"the ledger window could not be read: {exc}"],
        )
    if age is None and any_age is None:
        return "last tick  never in the last 2 days -- no pass has run", [], []
    if age is None:
        # The distinction is the whole point: rows exist, and NONE of them was
        # written by a scheduler. "last tick 3m ago" over hand-run passes is how
        # a clock that has never fired reads as healthy on every surface.
        return (
            "last tick  never by a host clock; last hand-run pass "
            + hostclock.tick_age_phrase(any_age),
            [],
            [],
        )
    # Never `int(age)` alone: a tick stamped in the future prints as a negative
    # age that reads like a measurement, and it is a clock that moved.
    return "last tick  " + hostclock.tick_age_phrase(age) + " (host clock)", [], []


def _ledger_line(base: Path) -> "tuple[str, list, list]":
    """Prove the ledger is writable by writing, not by looking at a mode bit.

    A directory that lists fine and refuses a write is the case that matters
    (a read-only mount, a Windows ACL, a full disk), and only a write finds it.
    """
    probe = ledger.ledger_dir(base) / ".doctor-write-probe"
    try:
        ledger.ledger_dir(base).mkdir(parents=True, exist_ok=True)
        with open(probe, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(clock.iso(clock.now_utc()) + "\n")
        os.remove(probe)
    except OSError as exc:
        return (
            f"ledger     NOT WRITABLE at {ledger.ledger_dir(base)}: {exc}",
            [f"the record cannot be written at {ledger.ledger_dir(base)}: {exc}"],
            [],
        )
    return f"ledger     writable at {ledger.ledger_dir(base)}", [], []


def _normalize_home_cmp(raw: str) -> str:
    """Canonical comparable key for an AWRISE_HOME path across Windows/WSL."""
    text = str(raw).strip().rstrip("\\/")
    win = _WIN_DRIVE_RE.match(text)
    if win:
        rest = win.group(2).replace("\\", "/").strip("/")
        return f"win:{win.group(1).lower()}:/{rest.lower()}"
    wsl = _WSL_MOUNT_RE.match(text)
    if wsl:
        rest = wsl.group(2).replace("\\", "/").strip("/")
        return f"win:{wsl.group(1).lower()}:/{rest.lower()}"
    return os.path.normcase(os.path.normpath(text))


def _resolve_wrapper_target(raw: str, base: Path) -> Optional[Path]:
    """Translate a wrapper's AWRISE_HOME string to a readable Path when possible."""
    text = str(raw).strip()
    if not text:
        return None
    win = _WIN_DRIVE_RE.match(text)
    if win and os.name != "nt":
        drive = win.group(1).lower()
        rest = win.group(2).replace("\\", "/")
        base_str = str(base)
        prefix = "/var/mnt" if base_str.startswith("/var/mnt/") else "/mnt"
        return Path(f"{prefix}/{drive}/{rest}")
    return Path(text)


def _extract_wrapper_home(wrapper: Path) -> Optional[str]:
    try:
        text = wrapper.read_text(encoding="utf-8", errors="replace")[:16384]
    except OSError:
        return None
    match = _CMD_HOME_RE.search(text)
    if match:
        return match.group(1).strip()
    match = _SH_HOME_RE.search(text)
    if match:
        return (match.group(1) or match.group(2) or match.group(3) or "").strip()
    return None


def _split_home_line(base: Path) -> "tuple[Optional[str], list, list]":
    """Detect split AWRISE_HOME where the host clock ticks a DIFFERENT store.

    Measured in production: ``~/.aither/awrise/bin/run-due.cmd`` set
    ``AWRISE_HOME=...\\awrise-wzns`` while readers without ``AWRISE_HOME`` read
    ``~/.aither/awrise`` -- making every dashboard claim the scheduler clock
    stopped ticking while the real clock ticked every 60s in the sibling home.
    """
    base_key = _normalize_home_cmp(str(base))
    bin_dir = base / "bin"
    for name in ("run-due.cmd", "run-due.sh"):
        wrapper = bin_dir / name
        if not wrapper.is_file():
            continue
        raw_target = _extract_wrapper_home(wrapper)
        if not raw_target:
            continue
        if _normalize_home_cmp(raw_target) != base_key:
            target_path = _resolve_wrapper_target(raw_target, base)
            tick_note = ""
            if target_path is not None and target_path.is_dir():
                try:
                    t_age = hostclock.tick_age_s(target_path, hostclock.SCHEDULED_INVOKERS)
                except OSError:
                    t_age = None
                if t_age is not None:
                    tick_note = f" (last scheduled tick {hostclock.tick_age_phrase(t_age)})"
            line = (
                f"split-home SPLIT HOME: {wrapper.name} sets AWRISE_HOME={raw_target}"
                f"{tick_note}, but this awrise reads {base}\n"
                f"           set AWRISE_HOME={raw_target} or run `awrise install-clock --force` "
                f"from the intended home"
            )
            return (
                line,
                [
                    f"split AWRISE_HOME: {wrapper} writes to {raw_target}, not {base}; "
                    f"reconcile AWRISE_HOME so clock and readers use the same store"
                ],
                [],
            )

    # Even without a local wrapper in base/bin, catch a stale store sitting next
    # to an actively ticking sibling store (e.g. awrise vs awrise-<user>).
    try:
        own_age = hostclock.tick_age_s(base, hostclock.SCHEDULED_INVOKERS)
    except OSError:
        own_age = None
    if own_age is not None and own_age <= _SPLIT_SIBLING_FRESH_S:
        return None, [], []

    parent = base.parent
    candidates: list[Path] = []
    try:
        if parent.is_dir():
            prefix = base.name + "-"
            for entry in sorted(parent.iterdir()):
                if entry != base and entry.is_dir() and entry.name.startswith(prefix):
                    candidates.append(entry)
            if "-" in base.name:
                stem = parent / base.name.split("-", 1)[0]
                if stem != base and stem.is_dir():
                    candidates.append(stem)
    except OSError:
        candidates = []

    for sib in candidates:
        if not ledger.ledger_dir(sib).is_dir():
            continue
        try:
            sib_age = hostclock.tick_age_s(sib, hostclock.SCHEDULED_INVOKERS)
        except OSError:
            continue
        if sib_age is not None and sib_age <= _SPLIT_SIBLING_FRESH_S:
            own_phrase = "never" if own_age is None else hostclock.tick_age_phrase(own_age)
            sib_phrase = hostclock.tick_age_phrase(sib_age)
            line = (
                f"split-home SPLIT HOME: sibling store {sib} ticked {sib_phrase} "
                f"while this home ({base}) is stale ({own_phrase})\n"
                f"           set AWRISE_HOME={sib} or consolidate both stores to one AWRISE_HOME"
            )
            return (
                line,
                [
                    f"split AWRISE_HOME: sibling store {sib} ticked {sib_phrase} while "
                    f"{base} is stale ({own_phrase})"
                ],
                [],
            )
    return None, [], []


def _detached_stale_line(base: Path) -> "tuple[Optional[str], list, list]":
    """Surface detached jobs with no receipt carrying frozen consecutive_failures.

    A detached job without ``receipt`` spawns and returns ``detached``; it never
    records ``success``, so any ``consecutive_failures`` accumulated before
    ``detach=true`` stays frozen forever. Informational here (WL006 already
    judges receiptless detached jobs as UNJUDGED in ``awrise status``).
    """
    try:
        jobs = store.load(base)
    except (store.StoreError, OSError):
        return None, [], []
    frozen = []
    for name, job in jobs.items():
        if not job.get("enabled", True):
            continue
        if not job.get("detach") or job.get("receipt") or job.get("last_state") != "detached":
            continue
        fails = int(job.get("consecutive_failures") or 0)
        if fails > 0:
            frozen.append((fails, name))
    if not frozen:
        return None, [], []
    frozen.sort(key=lambda item: (-item[0], item[1]))
    shown = ", ".join(f"{name} ({fails}x)" for fails, name in frozen[:3])
    if len(frozen) > 3:
        shown += f", +{len(frozen) - 3} more"
    line = (
        f"detached   {len(frozen)} detached job(s) without receipt carry frozen "
        f"consecutive_failures: {shown}\n"
        "           set `receipt=<path>` or clear stale counts with "
        "`awrise reset-failures --detached-no-receipt --dry-run`"
    )
    return line, [], []


#: The most recent measurement, so the verdict the generated doctor asks for is
#: the verdict for the lines it just printed. Two independent passes could
#: disagree -- a clock ticking between them is enough -- and a verdict that does
#: not match the display is the kind of thing nobody believes twice.
_LAST: "tuple | None" = None


def _measure() -> tuple:
    """(lines, problems, unjudged) -- one pass, three answers."""
    lines: list = []
    problems: list = []
    unjudged: list = []
    try:
        base = _home()
    except store.StoreError as exc:
        return ([f"home       UNUSABLE: {exc}"], [f"$AWRISE_HOME is unusable: {exc}"], [])
    lines.append(f"home       {base}")
    for measure in (
        _clock_line,
        _tick_line,
        _ledger_line,
        _split_home_line,
        _detached_stale_line,
    ):
        line, said_no, could_not = measure(base)
        if line:
            lines.append(line)
        problems.extend(said_no)
        unjudged.extend(could_not)
    return lines, problems, unjudged


def _doctor_local() -> list:
    """Display lines for the generated doctor. Never raises: a doctor that
    crashes tells an operator nothing about the thing they asked about."""
    global _LAST
    _LAST = _measure()
    return list(_LAST[0])


def _doctor_local_verdict() -> tuple:
    """(problems, unjudged) for the generated doctor's exit code.

    Without this hook every line above is decoration: the generated doctor
    printed "nothing wakes run-due" and exited 0, which teaches an operator
    that awrise's exit codes carry no information.
    """
    global _LAST
    if _LAST is None:
        _LAST = _measure()
    return list(_LAST[1]), list(_LAST[2])
