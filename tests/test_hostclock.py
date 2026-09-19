"""The host clock: what each adapter renders, and what it must never touch.

The render tests are golden-file tests on purpose. An adapter's output is a
contract with a scheduler that will not tell us it misread the file -- cron
silently ignores a line it cannot parse, systemd refuses a unit and logs it
where nobody looks, and a .cmd with the wrong line endings runs as one mangled
line. Comparing bytes to a fixture is the only check that fails when a
plausible-looking edit changes what the scheduler is handed.

Every fixture is compared against a Context built from literal strings, so the
tests read the same on every OS. Nothing here registers anything on this host:
the `--print` and `--dry-run` paths are asserted to write no file and to run no
command at all, and the real install path is exercised only through a fake
scheduler.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import cli, clock, hostclock, ledger, store

FIXTURES = Path(__file__).parent / "fixtures" / "hostclock"

POSIX_CTX = hostclock.Context(
    python="/usr/bin/python3",
    home="/home/ada/.aither/awrise",
    bin_dir="/home/ada/.aither/awrise/bin",
    log_path="/home/ada/.aither/awrise/logs/run-due.log",
    every_s=60,
    user="ada",
    version="0.2.0",
    sep="/",
)
WINDOWS_CTX = hostclock.Context(
    python="C:\\Python\\python.exe",
    home="C:\\aw",
    bin_dir="C:\\aw\\bin",
    log_path="C:\\aw\\logs\\run-due.log",
    every_s=60,
    user="ada",
    version="0.2.0",
    sep="\\",
)


def _ctx_for(kind: str) -> hostclock.Context:
    return WINDOWS_CTX if kind == "schtasks" else POSIX_CTX


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    return store.home()


@pytest.fixture
def no_subprocess(monkeypatch):
    """Any subprocess at all is a failure for the paths that use this."""

    def _boom(*args, **kwargs):
        raise AssertionError(f"this path must run no command; it ran {args!r}")

    monkeypatch.setattr(subprocess, "run", _boom)
    monkeypatch.setattr(hostclock.subprocess, "run", _boom)


# ------------------------------------------------------------------ renders


@pytest.mark.parametrize("kind", hostclock.KINDS)
def test_render_matches_the_golden_file(kind):
    artifacts = hostclock.render(kind, _ctx_for(kind))
    for name, text in artifacts.items():
        fixture = FIXTURES / f"{kind}__{name}"
        assert fixture.exists(), f"no golden file for {kind} {name} ({fixture})"
        assert text.encode("utf-8") == fixture.read_bytes(), (
            f"{kind} {name} no longer renders what the fixture holds"
        )


@pytest.mark.parametrize("kind", hostclock.KINDS)
def test_render_is_pure_and_repeatable(kind):
    first = hostclock.render(kind, _ctx_for(kind))
    second = hostclock.render(kind, _ctx_for(kind))
    assert first == second


def test_render_refuses_an_unknown_kind():
    with pytest.raises(hostclock.HostClockError):
        hostclock.render("systemd", POSIX_CTX)


def test_cron_schedule_covers_minutes_hours_and_days():
    assert hostclock.cron_schedule(60) == "*/1 * * * *"
    assert hostclock.cron_schedule(900) == "*/15 * * * *"
    assert hostclock.cron_schedule(4 * 3600) == "0 */4 * * *"
    assert hostclock.cron_schedule(7 * 86400) == "0 0 * * *"


def test_cron_line_carries_the_marker_and_the_home():
    line = hostclock.render("cron", POSIX_CTX)["crontab-line"]
    assert line.rstrip("\n").endswith(hostclock.CRON_MARKER)
    assert 'AWRISE_HOME="/home/ada/.aither/awrise"' in line
    assert "-m awrise run-due --quiet --invoker cron" in line


def test_systemd_system_names_a_user_and_the_user_unit_does_not():
    system = hostclock.render("systemd-system", POSIX_CTX)["awrise.service"]
    user = hostclock.render("systemd-user", POSIX_CTX)["awrise.service"]
    assert "User=ada" in system
    assert "User=" not in user
    timer = hostclock.render("systemd-user", POSIX_CTX)["awrise.timer"]
    for key in ("OnBootSec=90", "OnUnitActiveSec=60", "AccuracySec=10", "Persistent=true"):
        assert key in timer


def test_launchd_plist_is_parseable_and_has_the_interval():
    import plistlib

    text = hostclock.render("launchd", POSIX_CTX)[hostclock.LAUNCHD_LABEL + ".plist"]
    data = plistlib.loads(text.encode("utf-8"))
    assert data["Label"] == hostclock.LAUNCHD_LABEL
    assert data["StartInterval"] == 60
    assert data["ProgramArguments"][:4] == ["/usr/bin/python3", "-m", "awrise", "run-due"]


# ------------------------------------------------------------------ schtasks


def test_schtasks_payload_is_crlf_and_ends_with_exit_zero():
    artifacts = hostclock.render("schtasks", WINDOWS_CTX)
    for name in ("run-due.cmd", "run-hidden.vbs"):
        raw = artifacts[name].encode("utf-8")
        assert b"\r\n" in raw, f"{name} must use CRLF"
        assert raw.replace(b"\r\n", b"").count(b"\n") == 0, (
            f"{name} has a bare LF: cmd.exe reads the file as one mangled line"
        )
        assert raw.endswith(b"\r\n")
    cmd = artifacts["run-due.cmd"]
    assert cmd.rstrip("\r\n").endswith("exit /b 0"), (
        "a failing job must not turn the scheduled task red"
    )


def test_schtasks_shim_is_the_gui_subsystem_launcher():
    vbs = hostclock.render("schtasks", WINDOWS_CTX)["run-hidden.vbs"]
    assert 'CreateObject("WScript.Shell")' in vbs
    assert "sh.Run(args, 0, True)" in vbs, "window style 0 and WAIT, or the exit code is lost"
    creates = hostclock.render("schtasks", WINDOWS_CTX)["schtasks-create.txt"]
    assert "wscript.exe //B //Nologo" in creates
    source = (Path(hostclock.__file__)).read_text(encoding="utf-8")
    assert "wscript" in source, (
        "the source-level scan for task-creating code looks for this literal here"
    )


def test_schtasks_registers_an_onstart_trigger_as_well_as_the_interval():
    commands = hostclock.schtasks_commands(WINDOWS_CTX)
    assert len(commands) == 2
    assert commands[0][commands[0].index("/sc") + 1] == "minute"
    assert commands[0][commands[0].index("/mo") + 1] == "1"
    assert commands[1][commands[1].index("/sc") + 1] == "onstart"
    assert commands[1][commands[1].index("/tn") + 1] == hostclock.BOOT_TASK_NAME
    assert commands[0][commands[0].index("/tr") + 1] == commands[1][commands[1].index("/tr") + 1]


def test_schtasks_tr_value_is_inside_the_cap():
    value = hostclock.schtasks_tr(WINDOWS_CTX)
    assert len(value) <= hostclock.TR_MAX


def test_schtasks_tr_over_the_cap_is_refused_not_truncated():
    deep = hostclock.Context(
        python="C:\\Python\\python.exe",
        home="C:\\aw",
        bin_dir="C:\\" + "d" * 240,
        log_path="C:\\aw\\log",
        every_s=60,
        sep="\\",
    )
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.schtasks_tr(deep)
    assert str(hostclock.TR_MAX) in str(caught.value)


def test_a_long_home_falls_back_to_a_shorter_bin_dir(monkeypatch):
    monkeypatch.setenv("LOCALAPPDATA", "C:\\Users\\a\\AppData\\Local")
    deep = "C:\\Users\\a\\" + "\\".join(["directory-with-a-long-name"] * 6)
    chosen = hostclock._short_enough_bin_dir(deep, "\\")
    assert chosen.startswith("C:\\Users\\a\\AppData\\Local")
    assert len(hostclock._tr_value(chosen, "\\")) <= hostclock.TR_MAX


def test_schtasks_minutes_follow_the_interval():
    ctx = hostclock.Context(python="p", home="h", bin_dir="b", log_path="l", every_s=900, sep="\\")
    assert ctx.minutes == 15
    assert hostclock.schtasks_commands(ctx)[0][8] == "15"


# ------------------------------------------------- --print / --dry-run purity


def _target_state(kind: str, home: Path) -> dict:
    """Existence + mtime of every file this adapter would write.

    Some adapters write OUTSIDE the temporary home (a unit directory, a launch
    agents directory), so the assertion is "this call created or changed
    nothing", not "these paths are empty" -- a file another tool left there
    must not be read as this call touching it, and must not hide one either.
    """
    state = {}
    for target in hostclock.artifact_targets(kind, hostclock.context(kind, home)).values():
        try:
            state[target] = os.stat(target).st_mtime_ns
        except OSError:
            state[target] = None
    return state


@pytest.mark.parametrize("kind", hostclock.KINDS)
def test_install_print_touches_nothing(kind, home, no_subprocess):
    before = _target_state(kind, home)
    entry = hostclock.install(kind, every_s=60, print_only=True, base=home)
    assert entry.installed is False
    assert entry.lines, "--print prints the artifacts"
    assert not (home / "bin").exists()
    assert not hostclock.record_path(home).exists()
    assert _target_state(kind, home) == before, "--print wrote or changed a file"


@pytest.mark.parametrize("kind", hostclock.KINDS)
def test_install_dry_run_touches_nothing_and_names_everything(kind, home, no_subprocess):
    before = _target_state(kind, home)
    entry = hostclock.install(kind, every_s=60, dry_run=True, base=home)
    assert entry.installed is False
    assert not hostclock.record_path(home).exists()
    assert not (home / "bin").exists()
    assert _target_state(kind, home) == before, "--dry-run wrote or changed a file"
    text = "\n".join(entry.lines)
    assert "would record" in text
    if kind != "cron":
        assert "would write" in text
    assert any("would run" in line for line in entry.lines)


def test_cli_install_print_exits_zero_and_writes_nothing(home, no_subprocess, capsys):
    args = argparse.Namespace(
        kind="schtasks", every="60s", print_only=True, dry_run=False, check=False, uninstall=False
    )
    assert cli._dispatch(cli.cmd_install, args) == 0
    assert "wscript.exe" in capsys.readouterr().out
    assert not hostclock.record_path(home).exists()


def test_cli_install_without_a_kind_is_unjudged(home):
    args = argparse.Namespace(
        kind=None, every="60s", print_only=False, dry_run=False, check=False, uninstall=False
    )
    assert cli._dispatch(cli.cmd_install, args) == 2


def test_cli_install_refuses_an_unparseable_interval(home):
    args = argparse.Namespace(
        kind="cron", every="soon", print_only=True, dry_run=False, check=False, uninstall=False
    )
    assert cli._dispatch(cli.cmd_install, args) == 2


# ----------------------------------------------------------- install --check


def _record(home: Path, kind: str = "schtasks", **over) -> dict:
    ctx = hostclock.context(kind, home)
    record = hostclock.write_record(kind, ctx, hostclock.render(kind, ctx), base=home)
    if over:
        record.update(over)
        with open(hostclock.record_path(home), "w", encoding="utf-8", newline="\n") as fh:
            json.dump(record, fh)
    return record


def _payloads(home: Path, kind: str = "schtasks") -> None:
    ctx = hostclock.context(kind, home)
    hostclock._write_payloads(kind, ctx, hostclock.render(kind, ctx))


def _probe(monkeypatch, present=True, enabled=True, detail="registered"):
    monkeypatch.setattr(
        hostclock, "probe", lambda kind, ctx: hostclock.Definition(present, enabled, detail)
    )


def test_check_without_a_record_is_unjudged_never_a_violation(home):
    code, lines = hostclock.check(base=home)
    assert code == 2
    assert "UNJUDGED" in lines[0]


def test_check_is_one_when_the_registered_payload_is_missing(home, monkeypatch):
    _record(home)
    _payloads(home)
    _probe(monkeypatch)
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "schtasks"})
    assert hostclock.check(base=home)[0] == 0
    ctx = hostclock.context("schtasks", home)
    os.remove(hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"])
    code, lines = hostclock.check(base=home)
    assert code == 1
    assert any("payload is missing" in line for line in lines)


def test_check_is_one_when_the_definition_is_gone(home, monkeypatch):
    _record(home)
    _payloads(home)
    _probe(monkeypatch, present=False, enabled=True, detail="not registered")
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "schtasks"})
    code, lines = hostclock.check(base=home)
    assert code == 1
    assert any("is gone" in line for line in lines)


def test_check_is_one_when_the_clock_stopped_ticking(home, monkeypatch):
    _record(home, installed_at="2020-01-01T00:00:00+00:00")
    _payloads(home)
    _probe(monkeypatch)
    code, lines = hostclock.check(base=home)
    assert code == 1
    assert any("last scheduled tick was never" in line for line in lines)


def test_check_is_unjudged_while_the_install_is_younger_than_the_bound(home, monkeypatch):
    _record(home)
    _payloads(home)
    _probe(monkeypatch)
    code, lines = hostclock.check(base=home)
    assert code == 2
    assert any("UNJUDGED" in line for line in lines)


def test_check_is_unjudged_when_the_record_holds_another_kind(home, monkeypatch):
    _record(home, kind="cron")
    assert hostclock.check("schtasks", base=home)[0] == 2


def test_check_refuses_to_guess_from_an_unreadable_record(home):
    with open(hostclock.record_path(home), "w", encoding="utf-8", newline="\n") as fh:
        fh.write("{not json")
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.check(base=home)
    assert caught.value.code == 2


def test_check_accepts_a_live_pass_instead_of_a_fresh_tick(home, monkeypatch):
    from awrise import lock

    _record(home, installed_at="2020-01-01T00:00:00+00:00")
    _payloads(home)
    _probe(monkeypatch)
    assert (
        cli._dispatch(
            cli.cmd_add,
            argparse.Namespace(
                name="slow",
                every="15m",
                run="echo hi",
                timeout=None,
                cwd=None,
                at=None,
                allow_overrun=False,
                disabled=False,
                detach=False,
            ),
        )
        == 0
    )
    held = lock.acquire(
        home,
        "slow",
        {"wake_id": "w-live", "pass_id": "p-live", "started_at": clock.iso(clock.now_utc())},
    )
    try:
        code, lines = hostclock.check(base=home)
    finally:
        held.release()
    assert code == 0
    assert any("in progress" in line for line in lines)


def test_cli_install_check_prints_and_returns_the_code(home, capsys):
    assert (
        cli._dispatch(
            cli.cmd_install,
            argparse.Namespace(
                kind=None, check=True, every="60s", print_only=False, dry_run=False, uninstall=False
            ),
        )
        == 2
    )
    assert "UNJUDGED" in capsys.readouterr().err


# ------------------------------------------------------------ install proper


def test_install_reads_the_entry_back_and_refuses_to_claim_an_absent_one(home, monkeypatch):
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    monkeypatch.setattr(hostclock, "_register", lambda kind, ctx, notes=None: [["fake", "create"]])
    # enabled=True so only the PRESENT check can produce this refusal
    _probe(monkeypatch, present=False, enabled=True, detail="not registered")
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.install("schtasks", base=home)
    assert caught.value.code == 1
    assert not hostclock.record_path(home).exists(), (
        "an install that could not be read back must not leave a record claiming it was"
    )


def test_install_writes_payload_record_and_digests(home, monkeypatch):
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    monkeypatch.setattr(hostclock, "_register", lambda kind, ctx, notes=None: [["fake", "create"]])
    _probe(monkeypatch)
    entry = hostclock.install("schtasks", every_s=60, base=home)
    assert entry.installed is True
    record = hostclock.read_record(home)
    assert record["kind"] == "schtasks" and record["every_s"] == 60
    assert set(record["artifacts"]) == {"run-due.cmd", "run-hidden.vbs", "schtasks-create.txt"}
    ctx = hostclock.context("schtasks", home)
    for target in hostclock.artifact_targets("schtasks", ctx).values():
        assert os.path.exists(target)
    with open(hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"], "rb") as handle:
        assert b"\r\n" in handle.read(), "the payload must keep its CRLF on disk"


def test_install_on_the_wrong_os_is_unjudged(home):
    kind = "cron" if os.name == "nt" else "schtasks"
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.install(kind, base=home)
    assert caught.value.code == 2


# -------------------------------------------------------------- tick rows


def _run_due(invoker="cron"):
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=None), argparse.Namespace(quiet=True, invoker=invoker)
    )


def test_run_due_writes_a_tick_and_a_tick_end_with_a_gap(home):
    assert _run_due() == 0
    assert _run_due() == 0
    rows = ledger.read(home)
    ticks = [row for row in rows if row.get("event") == "tick"]
    ends = [row for row in rows if row.get("event") == "tick_end"]
    assert len(ticks) == 2 and len(ends) == 2
    assert ticks[0]["gap_s"] is None, "the first tick has nothing to measure a gap against"
    assert ticks[1]["gap_s"] is not None and ticks[1]["gap_s"] >= 0
    assert ends[1]["gap_s"] == ticks[1]["gap_s"]
    assert ticks[0]["interpreter"], "an entry pointing at a dead venv is only visible here"
    assert ends[0]["in_progress"] == 0 and ends[0]["fired"] == 0


def test_tick_end_counts_a_pass_in_progress(home):
    from awrise import lock

    assert (
        cli._dispatch(
            cli.cmd_add,
            argparse.Namespace(
                name="slow",
                every="15m",
                run="echo hi",
                timeout=None,
                cwd=None,
                at=None,
                allow_overrun=False,
                disabled=False,
                detach=False,
            ),
        )
        == 0
    )
    held = lock.acquire(
        home,
        "slow",
        {"wake_id": "w-live", "pass_id": "p-live", "started_at": clock.iso(clock.now_utc())},
    )
    try:
        assert _run_due() == 0
    finally:
        held.release()
    ends = [row for row in ledger.read(home) if row.get("event") == "tick_end"]
    assert ends[-1]["in_progress"] == 1
    assert ends[-1]["in_progress_jobs"] == ["slow"]


def test_the_tick_is_written_even_when_the_store_is_corrupt(home):
    with open(store.store_path(home), "w", encoding="utf-8", newline="\n") as handle:
        handle.write("{ not json")
    assert _run_due() == 2
    events = [row.get("event") for row in ledger.read(home)]
    assert events.count("tick") == 1 and events.count("tick_end") == 1, (
        "a clock firing into a broken store is not a stopped clock"
    )


def test_fired_counts_only_wakes_that_ran(home):
    assert (
        cli._dispatch(
            cli.cmd_add,
            argparse.Namespace(
                name="a",
                every="15m",
                run="echo hi",
                timeout=None,
                cwd=None,
                at=None,
                allow_overrun=False,
                disabled=False,
                detach=False,
            ),
        )
        == 0
    )
    assert _run_due() == 0
    ends = [row for row in ledger.read(home) if row.get("event") == "tick_end"]
    assert ends[-1]["fired"] == 1
    assert _run_due() == 0
    ends = [row for row in ledger.read(home) if row.get("event") == "tick_end"]
    assert ends[-1]["fired"] == 0, "nothing was due on the second pass"


# ------------------------------------------------------------- doctor_local


def test_doctor_local_reports_clock_tick_and_ledger(home):
    from awrise import doctor_local

    lines = doctor_local._doctor_local()
    text = "\n".join(lines)
    assert "NOT INSTALLED" in text
    assert "never in the last 2 days" in text
    assert "ledger     writable" in text
    _run_due()
    lines = doctor_local._doctor_local()
    assert any(line.startswith("last tick  ") and "never" not in line for line in lines)


def test_doctor_local_names_an_interpreter_that_moved(home, monkeypatch):
    from awrise import doctor_local

    _record(home, python="/nowhere/python")
    lines = "\n".join(doctor_local._doctor_local())
    assert "REINSTALL" in lines


def test_status_prints_the_clock_line_without_changing_its_verdict(home, capsys):
    assert (
        cli._dispatch(
            cli.cmd_add,
            argparse.Namespace(
                name="a",
                every="15m",
                run="echo hi",
                timeout=None,
                cwd=None,
                at=None,
                allow_overrun=False,
                disabled=False,
                detach=False,
            ),
        )
        == 0
    )
    assert _run_due() == 0
    before = cli._dispatch(cli.cmd_status, argparse.Namespace())
    capsys.readouterr()
    _record(home)
    after = cli._dispatch(cli.cmd_status, argparse.Namespace())
    out = capsys.readouterr().out
    assert before == after == 0
    assert "host clock: schtasks every 60s" in out


# ------------------------------------------- the source-level task-generator rule


def _task_generators(source: str):
    """(function name, literals) for every function that creates a task.

    A local twin of the platform's source scan for task-creating code: that
    scan reads TRACKED files, so it says nothing about a module until it is
    committed, and "the gate was green" would then mean "the gate never looked".
    """
    import ast

    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        literals = " ".join(
            child.value
            for child in ast.walk(node)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        ).lower()
        if "schtasks" in literals and "/create" in literals:
            yield node.name, literals


def test_the_task_generator_is_recognisable_and_carries_the_shim():
    source = Path(hostclock.__file__).read_text(encoding="utf-8")
    generators = dict(_task_generators(source))
    assert generators, (
        "no function here reads as a task generator, so the source-level scan "
        "would skip this module entirely rather than clear it"
    )
    for name, literals in generators.items():
        assert "wscript" in literals or "onlogon" not in literals, (
            f"{name} registers an interactive-logon task with no GUI-subsystem shim: "
            f"it would open a console that steals focus on whatever host it is installed on"
        )


def test_no_adapter_registers_an_interactive_logon_trigger():
    for argv in hostclock.schtasks_commands(WINDOWS_CTX):
        assert "onlogon" not in [part.lower() for part in argv]
        assert argv[argv.index("/tr") + 1].startswith("wscript.exe //B //Nologo")


# ------------------------------------- the register + read-back round trip


def _stub_scheduler(tmp_path: Path, log: Path, enabled: bool = True, owner: str = "awrise") -> str:
    """A real executable standing in for the task scheduler.

    Not a patched function: the thing most likely to break here is the
    ARGUMENT PASSING -- a launch command with quotes and forward-slash
    switches, handed to a process. A stub that is really executed is what
    proves the switches and the quoted paths arrive whole; a fake `run()`
    would happily accept a command line no scheduler would.

    ``owner`` decides what the read-back says the task DOES, because that is
    what ownership is read from: a task already registered under our name that
    launches something else belongs to somebody else, and neither an install
    nor an uninstall may touch it.
    """
    action = (
        "wscript.exe //B //Nologo run-hidden.vbs run-due.cmd"
        if owner == "awrise"
        else "C:\\payroll\\nightly-close.exe"
    )
    tag = f"{'on' if enabled else 'off'}-{owner}"
    if os.name == "nt":
        stub = tmp_path / f"fake-schtasks-{tag}.cmd"
        body = (
            "@echo off\r\n"
            f'>>"{log}" echo %*\r\n'
            'if "%1"=="/query" echo ^<Task^>^<Settings^>^<Enabled^>'
            + ("true" if enabled else "false")
            + "^</Enabled^>"
            "^</Settings^>^<Actions^>^<Exec^>^<Command^>"
            + action.replace("^", "^^")
            + "^</Command^>^</Exec^>^</Actions^>"
            "^</Task^>\r\n"
            "exit /b 0\r\n"
        )
        with open(stub, "w", encoding="utf-8", newline="") as handle:
            handle.write(body)
        return str(stub)
    stub = tmp_path / f"fake-schtasks-{tag}.sh"
    flag = "true" if enabled else "false"
    body = (
        "#!/bin/sh\n"
        f'echo "$@" >> "{log}"\n'
        'if [ "$1" = "/query" ]; then\n'
        f"  echo '<Task><Settings><Enabled>{flag}</Enabled></Settings>'\n"
        f"  echo '<Actions><Exec><Command>{action}</Command></Exec></Actions></Task>'\n"
        "fi\n"
        "exit 0\n"
    )
    with open(stub, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(body)
    os.chmod(stub, 0o755)
    return str(stub)


def test_register_and_read_back_round_trip_through_a_real_process(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log)
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)

    entry = hostclock.install("schtasks", every_s=60, base=home)
    assert entry.installed is True
    seen = log.read_text(encoding="utf-8", errors="replace")
    assert seen.count("/create") == 2, seen
    assert "/sc minute /mo 1" in seen and "/sc onstart" in seen, seen
    assert "wscript.exe //B //Nologo" in seen, "the launch command lost its shim on the way"
    assert "run-hidden.vbs" in seen and "run-due.cmd" in seen, (
        "the quoted payload paths did not survive the hop to the scheduler"
    )
    assert "/query" in seen, "success was claimed without reading the entry back"

    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "schtasks"})
    assert hostclock.check(base=home)[0] == 0

    ctx = hostclock.context("schtasks", home)
    os.remove(hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"])
    code, lines = hostclock.check(base=home)
    assert code == 1 and any("payload is missing" in line for line in lines), lines


def test_uninstall_removes_the_entry_and_the_record(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log)
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    hostclock.install("schtasks", every_s=60, base=home)
    monkeypatch.setattr(
        hostclock, "probe", lambda kind, ctx: hostclock.Definition(False, False, "gone")
    )
    entry = hostclock.uninstall("schtasks", base=home)
    assert "/delete" in log.read_text(encoding="utf-8", errors="replace")
    assert not hostclock.record_path(home).exists()
    ctx = hostclock.context("schtasks", home)
    for target in hostclock.artifact_targets("schtasks", ctx).values():
        assert not os.path.exists(target)
    assert any("read back: gone" in line for line in entry.lines)
    assert hostclock.check(base=home)[0] == 2, "with the record gone there is nothing to judge"


def test_scheduler_output_is_decoded_whatever_it_encoded_it_in():
    xml = "<Task><Settings><Enabled>false</Enabled></Settings></Task>"
    assert hostclock.decode(xml.encode("utf-16")) == xml, "UTF-16 with a BOM"
    assert hostclock.decode(xml.encode("utf-16-le")) == xml, "UTF-16 without a BOM"
    assert hostclock.decode(xml.encode("utf-8")) == xml
    assert hostclock.decode(b"") == "" and hostclock.decode(None) == ""


def test_a_disabled_entry_is_not_read_as_installed(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log, enabled=False)
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.install("schtasks", every_s=60, base=home)
    assert caught.value.code == 1
    assert "disabled" in str(caught.value).lower()
    assert not hostclock.record_path(home).exists()


def test_fired_does_not_count_a_wake_that_was_skipped_for_overlap(home):
    from awrise import lock

    assert (
        cli._dispatch(
            cli.cmd_add,
            argparse.Namespace(
                name="slow",
                every="15m",
                run="echo hi",
                timeout=None,
                cwd=None,
                at=None,
                allow_overrun=False,
                disabled=False,
                detach=False,
            ),
        )
        == 0
    )
    held = lock.acquire(
        home,
        "slow",
        {"wake_id": "w-live", "pass_id": "p-live", "started_at": clock.iso(clock.now_utc())},
    )
    try:
        assert _run_due() == 0
    finally:
        held.release()
    ends = [row for row in ledger.read(home) if row.get("event") == "tick_end"]
    assert ends[-1]["fired"] == 0, "the lock was held, so nothing ran"
    assert ends[-1]["in_progress"] == 1


# ------------------------------------------------- what it may not touch


def test_install_refuses_to_overwrite_a_task_it_did_not_create(tmp_path, home, monkeypatch):
    """A host with its own task called `awrise` keeps it.

    `/create /f` overwrites whatever is under the name without asking, and the
    thing it would overwrite is somebody's job, not ours.
    """
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log, owner="stranger")
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.install("schtasks", every_s=60, base=home)
    assert caught.value.code == 1
    assert "did not create" in str(caught.value)
    seen = log.read_text(encoding="utf-8", errors="replace")
    assert "/create" not in seen, "the stranger's task was overwritten anyway"
    assert not hostclock.record_path(home).exists()


def test_uninstall_refuses_to_delete_a_task_it_did_not_create(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log, owner="stranger")
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.uninstall("schtasks", base=home)
    assert caught.value.code == 1
    assert "/delete" not in log.read_text(encoding="utf-8", errors="replace"), (
        "an uninstall deleted a task this awrise never registered"
    )


def test_a_task_that_launches_our_payload_is_ours(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    monkeypatch.setattr(
        hostclock, "_schtasks_exe", lambda: _stub_scheduler(tmp_path, log, owner="awrise")
    )
    ctx = hostclock.context("schtasks", home)
    assert hostclock.foreign_entries("schtasks", ctx) == []
    monkeypatch.setattr(
        hostclock, "_schtasks_exe", lambda: _stub_scheduler(tmp_path, log, owner="stranger")
    )
    assert hostclock.foreign_entries("schtasks", ctx) == [
        hostclock.TASK_NAME,
        hostclock.BOOT_TASK_NAME,
    ]


def test_a_unit_file_written_by_somebody_else_is_never_overwritten(tmp_path, home, monkeypatch):
    """The unit and plist paths are fixed names in shared directories."""
    target = tmp_path / "awrise.service"
    target.write_text("[Unit]\nDescription=a unit somebody else wrote\n", encoding="utf-8")
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.refuse_foreign_file(target)
    assert caught.value.code == 1
    assert hostclock.GENERATED_MARKER in str(caught.value)
    assert "somebody else wrote" in target.read_text(encoding="utf-8")
    # Ours carries the marker, so a reinstall may replace it.
    ctx = hostclock.context("systemd-user", home)
    target.write_text(hostclock.render("systemd-user", ctx)["awrise.service"], encoding="utf-8")
    hostclock.refuse_foreign_file(target)


def test_writing_the_payload_never_overwrites_a_strangers_file(tmp_path, home, monkeypatch):
    """The refusal has to sit on the write path, not only in a helper."""
    ctx = hostclock.context("schtasks", home)
    targets = hostclock.artifact_targets("schtasks", ctx)
    stranger = Path(targets["run-due.cmd"])
    stranger.parent.mkdir(parents=True, exist_ok=True)
    stranger.write_text(
        "@echo off\r\nREM somebody else's payload\r\n", encoding="utf-8", newline=""
    )
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock._write_payloads("schtasks", ctx, hostclock.render("schtasks", ctx))
    assert caught.value.code == 1
    assert "somebody else" in stranger.read_text(encoding="utf-8")


def test_uninstall_leaves_a_payload_file_it_did_not_write(tmp_path, home, monkeypatch):
    log = tmp_path / "schtasks-argv.log"
    stub = _stub_scheduler(tmp_path, log)
    monkeypatch.setattr(hostclock, "_schtasks_exe", lambda: stub)
    monkeypatch.setattr(hostclock, "preflight", lambda kind: None)
    hostclock.install("schtasks", every_s=60, base=home)
    ctx = hostclock.context("schtasks", home)
    payload = Path(hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"])
    payload.write_text("@echo off\r\nREM not ours\r\n", encoding="utf-8", newline="")
    monkeypatch.setattr(
        hostclock, "probe", lambda kind, ctx_: hostclock.Definition(False, False, "gone")
    )
    entry = hostclock.uninstall("schtasks", base=home)
    assert payload.exists(), "a file this awrise did not write was deleted anyway"
    assert any("left" in line and "marker" in line for line in entry.lines), entry.lines


# ------------------------------------------------------------------ cron


def _fake_crontab(monkeypatch, listed, list_rc=0, stderr=""):
    """Stand in for the crontab binary; record what would be written."""
    sent = {}

    def fake(argv, input_text=None, timeout=60, binary=False):
        argv = list(argv)
        if argv == ["crontab", "-l"]:
            return subprocess.CompletedProcess(argv, list_rc, "" if list_rc else listed, stderr)
        sent["argv"] = argv
        sent["input"] = input_text
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(hostclock, "_run", fake)
    return sent


USER_CRONTAB = "0 3 * * * /usr/local/bin/backup.sh\n*/5 * * * * /opt/pay/settle.py\n"


def test_cron_install_keeps_every_other_entry(monkeypatch):
    sent = _fake_crontab(monkeypatch, USER_CRONTAB + "*/1 * * * * old # awrise\n")
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    hostclock._register("cron", ctx)
    written = sent["input"]
    assert "/usr/local/bin/backup.sh" in written and "/opt/pay/settle.py" in written
    assert written.count(hostclock.CRON_MARKER) == 1, "the old awrise line was not replaced"


def test_cron_install_refuses_when_the_crontab_cannot_be_read(monkeypatch):
    """An unreadable crontab is UNJUDGED, never 'empty'.

    `crontab -l` prints nothing on stdout when the spool is locked or
    unreadable, and rewriting from that deletes every entry the user has.
    """
    sent = _fake_crontab(
        monkeypatch, "", list_rc=1, stderr="crontab: error reading /var/spool/cron/crontabs/ada"
    )
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock._register("cron", ctx)
    assert caught.value.code == 2
    assert "input" not in sent, "a crontab was written from a read that failed"


def test_cron_install_treats_no_crontab_as_empty(monkeypatch):
    sent = _fake_crontab(monkeypatch, "", list_rc=1, stderr="no crontab for ada")
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    hostclock._register("cron", ctx)
    assert sent["input"].rstrip("\n").endswith(hostclock.CRON_MARKER)


def test_cron_uninstall_keeps_every_other_entry(monkeypatch):
    sent = _fake_crontab(monkeypatch, USER_CRONTAB + "*/1 * * * * ours # awrise\n")
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    hostclock._unregister("cron", ctx, [])
    written = sent["input"]
    assert "/usr/local/bin/backup.sh" in written and "/opt/pay/settle.py" in written
    assert hostclock.CRON_MARKER not in written


def test_cron_uninstall_refuses_when_the_crontab_cannot_be_read(monkeypatch):
    sent = _fake_crontab(monkeypatch, "", list_rc=1, stderr="crontab: error reading spool")
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock._unregister("cron", ctx, [])
    assert caught.value.code == 2
    assert "input" not in sent, "the crontab was rewritten from a read that failed"


def test_cron_uninstall_of_the_only_line_removes_the_crontab(monkeypatch):
    sent = _fake_crontab(monkeypatch, "*/1 * * * * ours # awrise\n")
    ctx = hostclock.context("cron", base=Path("/home/ada/.aither/awrise"), every_s=60)
    hostclock._unregister("cron", ctx, [])
    assert sent["argv"] == ["crontab", "-r"]
    assert sent["input"] is None, "a blank line was written where nothing was meant"


# ------------------------------------------ the interval the scheduler does


@pytest.mark.parametrize("every_s", [1, 10, 59, 60, 90, 900, 3600, 4 * 3600, 86400, 7 * 86400])
def test_the_effective_cron_interval_matches_the_schedule_it_writes(every_s):
    """The recorded number and the crontab field cannot drift apart."""
    field = hostclock.cron_schedule(every_s).split()
    minutes, hours = field[0], field[1]
    if minutes.startswith("*/"):
        expected = int(minutes[2:]) * 60
    elif hours.startswith("*/"):
        expected = int(hours[2:]) * 3600
    else:
        expected = 86400
    assert hostclock.effective_every_s("cron", every_s) == expected


def test_the_effective_schtasks_interval_is_the_one_minute_floor():
    assert hostclock.effective_every_s("schtasks", 10) == 60
    assert hostclock.effective_every_s("schtasks", 900) == 900
    ctx = hostclock.Context(python="p", home="h", bin_dir="b", log_path="l", every_s=10, sep="\\")
    assert ctx.minutes * 60 == hostclock.effective_every_s("schtasks", 10)


def test_check_judges_a_sub_minute_request_by_what_the_scheduler_really_does(home, monkeypatch):
    """A 10s request fires once a minute; a tick 45s old is not a dead clock."""
    _record(home, every_s=10, effective_every_s=hostclock.effective_every_s("schtasks", 10))
    _payloads(home)
    _probe(monkeypatch)
    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "invoker": "schtasks",
            "ts": clock.iso(clock.now_utc() - timedelta(seconds=45)),
        },
    )
    code, lines = hostclock.check(base=home)
    assert code == 0, lines
    assert any("judged against 60s" in line for line in lines), lines


def test_check_of_an_old_record_derives_the_effective_interval(home, monkeypatch):
    """A record written before this number existed is still judged correctly."""
    record = _record(home, every_s=10)
    record.pop("effective_every_s")
    with open(hostclock.record_path(home), "w", encoding="utf-8", newline="\n") as fh:
        json.dump(record, fh)
    _payloads(home)
    _probe(monkeypatch)
    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "invoker": "schtasks",
            "ts": clock.iso(clock.now_utc() - timedelta(seconds=45)),
        },
    )
    assert hostclock.check(base=home)[0] == 0


def test_install_says_when_the_scheduler_rounds_the_request_up(home, no_subprocess):
    entry = hostclock.install("schtasks", every_s=10, print_only=True, base=home)
    assert any("floor rounds" in line and "60s" in line for line in entry.lines), entry.lines


# ------------------------------------------------ a tick that has not happened


def test_check_refuses_a_tick_stamped_in_the_future(home, monkeypatch):
    """One future-stamped row must not certify a clock dead for hours."""
    _record(home, installed_at="2020-01-01T00:00:00+00:00")
    _payloads(home)
    _probe(monkeypatch)
    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "ts": clock.iso(clock.now_utc() - timedelta(hours=5)),
        },
    )
    assert hostclock.check(base=home)[0] == 1
    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "ts": clock.iso(clock.now_utc() + timedelta(hours=5)),
        },
    )
    code, lines = hostclock.check(base=home)
    assert code == 1, lines
    assert any("FUTURE" in line for line in lines), lines


def test_doctor_local_does_not_print_a_future_tick_as_an_age(home):
    from awrise import doctor_local

    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "ts": clock.iso(clock.now_utc() + timedelta(hours=5)),
        },
    )
    line, problems, unjudged = doctor_local._tick_line(home)
    assert "FUTURE" in line and "-17" not in line, line
    # A clock that moved backwards is judged by `_clock_line` (it is the same
    # judgement `install --check` makes); the tick line reports it and claims
    # nothing, so it contributes neither a no nor an unjudged of its own.
    assert (problems, unjudged) == ([], []), (problems, unjudged)


def test_a_pass_after_a_backwards_clock_step_marks_the_gap(home):
    ledger.append(
        home,
        {
            "event": "tick",
            "reason": "pass_start",
            "ts": clock.iso(clock.now_utc() + timedelta(hours=1)),
        },
    )
    assert _run_due() == 0
    # The pass's OWN tick, not the last row on file: ledger.read orders by day
    # FILE, so a seed stamped an hour ahead lands in tomorrow's file for one
    # hour of every UTC day and sorts after the row this test is about.
    ticks = [row for row in ledger.read(home) if row.get("event") == "tick" and "gap_s" in row]
    assert ticks, "the pass wrote no tick carrying a gap"
    assert ticks[-1]["gap_s"] < 0
    assert ticks[-1].get("clock_step") is True, (
        "an impossible gap was recorded as if it were a measurement"
    )


# --------------------------------------- what the payload cannot carry


@pytest.mark.parametrize("hostile", ["&", "%", "^"])
def test_a_home_cmd_cannot_carry_is_refused_not_silently_broken(hostile, tmp_path):
    """Measured on Windows: each of these kills the payload with no console.

    `&` ends the command even inside quotes, `%` is expanded before the quoting
    is looked at, and `^` is eaten as the escape character -- the path loses it.
    """
    home = f"C:\\aw{hostile}x"
    ctx = hostclock.Context(
        python="C:\\Python\\python.exe",
        home=home,
        bin_dir=home + "\\bin",
        log_path=home + "\\logs\\run-due.log",
        every_s=60,
        sep="\\",
    )
    with pytest.raises(hostclock.HostClockError) as caught:
        hostclock.render("schtasks", ctx)
    assert caught.value.code == 2
    assert repr(hostile) in str(caught.value)


@pytest.mark.skipif(os.name != "nt", reason="the payload is cmd.exe's to run")
@pytest.mark.parametrize("exits, expected", [("exit 0", "exit=0"), ("exit 3", "exit=1")])
def test_the_rendered_payload_logs_the_exit_code_it_really_got(tmp_path, exits, expected):
    """Run the real payload and read the log the task leaves behind.

    The golden file pins bytes, and bytes cannot show that cmd.exe reads the
    digit left of `>>` as a file handle: the line then logs `exit=` with the
    code gone, or goes to a console the hidden shim does not have. Only
    executing it can fail on that, and this is the ONLY host-side evidence
    when the ledger itself is what failed.
    """
    base = tmp_path / "home"
    (base / "logs").mkdir(parents=True)
    ctx = hostclock.context("schtasks", base=base, every_s=60)
    artifacts = hostclock.render("schtasks", ctx)
    hostclock._write_payloads("schtasks", ctx, artifacts)
    package_root = Path(hostclock.__file__).resolve().parent.parent
    env = {**os.environ, "AWRISE_HOME": str(base)}
    add = subprocess.run(
        [
            ctx.python,
            "-m",
            "awrise",
            "add",
            "--name",
            "j",
            "--every",
            "10m",
            "--run",
            exits,
            "--timeout",
            "30",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=str(package_root),
    )
    assert add.returncode == 0, add.stderr
    payload = hostclock.artifact_targets("schtasks", ctx)["run-due.cmd"]
    done = subprocess.run(
        [payload],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(package_root),
    )
    assert done.returncode == 0, "a failing job must not turn the task red"
    log = (base / "logs" / hostclock.LOG_NAME).read_text(encoding="utf-8", errors="replace")
    assert expected in log, (
        f"the payload logged {log!r}: the awrise exit code never reached the log"
    )
    rows = [row.get("event") for row in ledger.read(base)]
    assert "tick" in rows, "the payload never reached run-due"


# ------------------------------------------------------- the generated doctor


def test_the_doctor_demands_only_config_the_shipped_code_really_reads():
    """`doctor` exits 1 on a name no shipped code path reads.

    ENV_REQUIRED is generated by scanning this package for `os.environ["X"]`,
    and a subscript ASSIGNMENT is a write, not a requirement. A name that only
    a test harness sets made every fresh install report itself misconfigured.
    """
    import ast

    from awrise import _doctor

    package = Path(hostclock.__file__).parent
    strict_reads = set()
    for path in sorted(package.glob("*.py")):
        if path.name in ("_doctor.py", "selftest.py"):
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Subscript)
                and isinstance(node.value, ast.Attribute)
                and node.value.attr == "environ"
                and isinstance(node.ctx, ast.Load)
                and isinstance(node.slice, ast.Constant)
            ):
                strict_reads.add(node.slice.value)
    assert set(_doctor.ENV_REQUIRED) <= strict_reads, (
        f"doctor demands {sorted(set(_doctor.ENV_REQUIRED) - strict_reads)}, which no "
        f"shipped code path reads with os.environ[...]"
    )


def test_doctor_reports_that_nothing_wakes_run_due(tmp_path):
    """The exit code has to mean what the lines say.

    This test used to assert 0 on a fresh home, which is exactly the shape the
    README forbids: `doctor` printed "hostclock NOT INSTALLED -- nothing wakes
    run-due" and "last tick never", then exited 0. No entry of ours AND no pass
    ever recorded is a measured absence, not a shrug.
    """
    package_root = Path(hostclock.__file__).resolve().parent.parent
    env = {**os.environ, "AWRISE_HOME": str(tmp_path / "home")}
    done = subprocess.run(
        [sys.executable, "-m", "awrise", "doctor"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=str(package_root),
    )
    assert done.returncode == 1, done.stdout + done.stderr
    assert "MISSING REQUIRED" not in done.stdout
    assert "nothing wakes" in done.stdout, done.stdout


def test_doctor_does_not_call_a_stranger_s_own_clock_a_violation(tmp_path):
    """The twin of the test above, and the reason it is not simply "no record".

    Someone else's cron line running `awrise run-due` is a perfectly good clock
    this brick did not install. What proves it is a tick THAT NAMES THE
    SCHEDULER THAT STARTED IT (`run-due --invoker cron`), so a ledger with one
    turns the measured no above into an honest UNJUDGED. A tick that names no
    scheduler is not that evidence: a pass run by hand writes exactly the same
    row, and on 2026-09-18 that reading had this host reporting a live clock
    while `schtasks /query /tn awrise` found nothing at all.
    """
    home = tmp_path / "home"
    home.mkdir(parents=True)
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "schtasks"})
    package_root = Path(hostclock.__file__).resolve().parent.parent
    env = {**os.environ, "AWRISE_HOME": str(home)}
    done = subprocess.run(
        [sys.executable, "-m", "awrise", "doctor"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=str(package_root),
    )
    assert done.returncode == 2, done.stdout + done.stderr
    assert "something else is ticking it" in done.stdout, done.stdout


def test_doctor_exits_one_when_the_record_cannot_be_written(tmp_path):
    """A brick whose whole product is the record must go red when it has none."""
    package_root = Path(hostclock.__file__).resolve().parent.parent
    home = tmp_path / "home"
    home.mkdir(parents=True)
    # A FILE where the ledger directory belongs: the mkdir fails, which is the
    # read-only mount / full disk / ACL case in the one shape a test can make.
    (home / "ledger").write_text("not a directory\n", encoding="utf-8")
    env = {**os.environ, "AWRISE_HOME": str(home)}
    done = subprocess.run(
        [sys.executable, "-m", "awrise", "doctor"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        cwd=str(package_root),
    )
    assert done.returncode == 1, done.stdout + done.stderr
    assert "NOT WRITABLE" in done.stdout, done.stdout


def test_doctor_exits_zero_when_the_clock_is_installed_and_ticking(tmp_path, monkeypatch):
    """The negative twin: the two verdicts above are about what they measured,
    not a doctor that can no longer pass."""
    import io

    from awrise import _doctor, clock, doctor_local

    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(hostclock, "check", lambda kind=None, base=None: (0, ["last tick: 1s ago"]))
    monkeypatch.setattr(
        hostclock,
        "read_record",
        lambda base=None: {
            "kind": "cron",
            "every_s": 60,
            "installed_at": clock.iso(clock.now_utc()),
            "python": sys.executable,
        },
    )
    doctor_local._LAST = None
    out = io.StringIO()
    assert _doctor.report(out=out) == 0, out.getvalue()
