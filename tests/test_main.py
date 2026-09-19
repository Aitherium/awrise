"""``python -m awrise`` exists and the self-test can fail."""

import subprocess
import sys
from pathlib import Path

PKG = Path(__file__).resolve().parent.parent


def _run(*argv, env=None):
    return subprocess.run(
        [sys.executable, "-m", "awrise", *argv],
        cwd=str(PKG),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
        env=env,
    )


def test_python_m_awrise_self_test_exits_0():
    proc = _run("--self-test")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "FAIL" not in proc.stdout and "ERROR" not in proc.stdout


def test_self_test_list_names_every_case():
    proc = _run("--self-test", "--list")
    assert proc.returncode == 0
    names = proc.stdout.split()
    assert "store_corrupt_exits_2_and_restore_returns_bak" in names
    assert "every_terminal_state_recorded_exactly_once" in names
    assert "ledger_refuses_unknown_event_state_and_empty_reason" in names


def test_self_test_reports_a_broken_case_as_nonzero(monkeypatch, capsys):
    from awrise import selftest

    def broken():
        raise AssertionError("negative twin")

    monkeypatch.setattr(selftest, "CASES", [("broken_case", broken)])
    assert selftest.run() == 1
    assert "FAIL  broken_case" in capsys.readouterr().out

    def crashed():
        raise RuntimeError("harness fault")

    monkeypatch.setattr(selftest, "CASES", [("crashed_case", crashed)])
    assert selftest.run() == 2


def test_verbs_answer_on_a_cp1252_pipe_with_non_ascii_data(tmp_path):
    """A pipe on Windows is cp1252; a reason or a tail with one character
    outside it must not turn a verdict into a UnicodeEncodeError traceback."""
    import json
    import os

    env = {**os.environ, "AWRISE_HOME": str(tmp_path), "PYTHONIOENCODING": "cp1252"}
    env.pop("PYTHONUTF8", None)
    # the reason carries the path: cwd_missing:<...café → gone>
    gone = str(tmp_path / "café → 日本 gone")
    proc = _run("add", "--name", "j", "--every", "1h", "--run", "echo hi", "--cwd", gone, env=env)
    assert proc.returncode == 0, proc.stderr
    tail = "import sys; sys.stdout.buffer.write('caf\\u00e9 \\u2192 done'.encode('utf-8'))"
    proc = _run(
        "add", "--name", "k", "--every", "1h", "--run", f'"{sys.executable}" -c "{tail}"', env=env
    )
    assert proc.returncode == 0, proc.stderr
    proc = _run("run-due", env=env)  # not --quiet: prints the names and reasons
    assert proc.returncode == 1 and "Traceback" not in proc.stderr, proc.stderr
    assert "cwd_missing" in proc.stderr
    for argv, want in (
        (["history"], 0),
        (["history", "--json"], 0),
        (["status"], 1),
        (["list"], 0),
        (["run", "--name", "j"], 1),
    ):
        proc = _run(*argv, env=env)
        assert proc.returncode == want, (argv, proc.stdout, proc.stderr)
        assert "Traceback" not in proc.stderr, (argv, proc.stderr)
    proc = _run("history", "--json", "--limit", "0", env=env)
    rows = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    tails = [r["stdout_tail"] for r in rows if r.get("job") == "k" and r["event"] == "finished"]
    assert tails == ["café → done"], "JSON escapes survive the pipe intact"
    reasons = [r["reason"] for r in rows if r.get("job") == "j" and r["event"] == "finished"]
    assert reasons and all(r.startswith("cwd_missing:") for r in reasons)


def test_help_exits_0_and_names_the_verbs():
    proc = _run("--help")
    assert proc.returncode == 0
    for verb in (
        "add",
        "remove",
        "list",
        "run-due",
        "run",
        "history",
        "status",
        "set",
        "enable",
        "disable",
        "reconcile",
    ):
        assert verb in proc.stdout, verb
