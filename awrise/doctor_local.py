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
import sys
from pathlib import Path

from . import clock, hostclock, ledger, store


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
    for measure in (_clock_line, _tick_line, _ledger_line):
        line, said_no, could_not = measure(base)
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
