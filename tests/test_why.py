"""Why a wake failed: the line, the cause, the hold, the ONE card, the brief.

Measured 2026-11-08: the desk's wakes panel listed 21 broken wakes with only an
exit code each ("moe-tiers-publish 1713x · exited 2"). These pin that every bad
wake now carries its own last error line, that a dependency that is down is
HELD instead of piling up failures, and that a repeated code bug raises
exactly one card whose options have real consumers.
"""

import argparse
import json
from datetime import timedelta

import pytest
from awrise import cli, clock, executors, ledger, store, why
from awrise.executors import Outcome

MOE = "publish_moe_tiers: no llama-server on spark.local:8118 (or ssh failed)"
CARDS = (
    "relocate-sync C:\\x: raised 0, raise errors 1\n"
    "  RAISE FAILED plan rel-20260929T021647Z-da111d: RelocateRefusedError: POST /decisions: "
    "HTTP 400 {'detail': 'a card offering options must declare default_key'}"
)


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    monkeypatch.setenv("AITHER_DECISIONS_POPUP", "0")
    monkeypatch.setenv("AITHER_NO_GUI_PROMPT", "1")
    return tmp_path


def _add(name, every="1h", run="echo hi"):
    args = argparse.Namespace(
        name=name, every=every, run=run, timeout=60, cwd=None, at=None,
        allow_overrun=False, disabled=False,
    )
    assert cli._dispatch(cli.cmd_add, args) == 0


def _run_due(executor):
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=executor),
        argparse.Namespace(quiet=True, invoker="test"),
    )


def _make_due(home, name):
    jobs = store.load(home)
    jobs[name]["last_started_at"] = clock.iso(clock.now_utc() - timedelta(hours=2))
    store.save(jobs, home)


def _failing(stderr="", stdout="", state="failure", reason="exit_2"):
    def run(job, wake):
        return Outcome(state, reason, exit_code=2, stdout_tail=stdout, stderr_tail=stderr)

    return run


# ------------------------------------------------------------------ pure parts


def test_extract_why_takes_the_last_informative_line_of_a_traceback():
    tail = (
        "starting\nTraceback (most recent call last):\n"
        '  File "x.py", line 3, in <module>\n    main()\n'
        "ValueError: bad config value 42\n"
    )
    assert why.extract_why(tail, "exit_1") == "ValueError: bad config value 42"


def test_extract_why_prefers_stderr_and_falls_back_to_the_reason():
    assert why.extract_why("all good\n", "exit_1", prefer=MOE + "\r\n") == MOE
    assert why.extract_why("", "exit_3") == "exit_3"
    # No error-shaped line: the last non-blank line is still better than nothing.
    assert why.extract_why("step 1\nstep 2\n\n", "exit_1") == "step 2"


@pytest.mark.parametrize(
    ("line", "state", "cause"),
    [
        (MOE, "failure", "dependency-down"),
        ("httpx.ConnectError: [Errno 111] Connection refused", "failure", "dependency-down"),
        ("HTTP 503 Service Unavailable", "failure", "dependency-down"),
        (CARDS.splitlines()[-1], "failure", "code-bug"),
        ("killed after 300s", "timeout", "timeout"),
        ("FileNotFoundError: [Errno 2] No such file or directory: 'x.json'", "failure", "config"),
        ("cwd_missing:E:\\gone", "error", "config"),
        ("exit_2 (no output captured)", "failure", "unknown"),
    ],
)
def test_classify_buckets(line, state, cause):
    assert why.classify(line, "", state) == cause


def test_why_key_ignores_volatile_numbers_but_not_the_message():
    a = why.why_key("timed out after 31.2s on pid 4411")
    b = why.why_key("timed out after 29.9s on pid 9001")
    assert a == b
    assert a != why.why_key("connection refused on pid 4411")


def test_output_tail_is_capped_redacted_and_line_aligned():
    out = "\n".join(f"line {i}" for i in range(200)).encode()
    err = b"Authorization: Bearer abcdefghijklmnop\ntoken=sekritvalue123\nfatal: sk-abcdef123456"
    tail = why.output_tail(out, err)
    lines = tail.splitlines()
    assert len(lines) == why.TAIL_LINES
    assert lines[-1].startswith("fatal: <redacted:sk>")
    assert "abcdefghijklmnop" not in tail and "sekritvalue123" not in tail
    big = why.output_tail(("x" * 300 + "\n").encode() * 40)
    assert len(big) <= why.TAIL_BYTES and big.startswith("x")


def test_hold_is_exponential_from_two_windows_and_capped_at_an_hour():
    assert why.hold_seconds(2, 300) == 0
    assert why.hold_seconds(3, 300) == 600  # two windows: one would skip nothing
    assert why.hold_seconds(4, 300) == 1200
    assert why.hold_seconds(5, 30) == 480  # floored at one minute, doubled thrice
    assert why.hold_seconds(40, 300) == why.HOLD_CAP_S


def test_receipt_why_reads_the_reason_and_the_log(tmp_path):
    log = tmp_path / "run.log"
    log.write_text("phase 1 ok\nERROR: upstream refused the push\n", encoding="utf-8")
    receipt = tmp_path / "r.json"
    receipt.write_text(json.dumps({"exit_code": 1, "log": str(log)}), encoding="utf-8")
    line, tail = why.receipt_why(receipt, 1)
    assert line == "ERROR: upstream refused the push"
    assert "phase 1 ok" in tail
    receipt.write_text(json.dumps({"exit_code": 0, "detail": "ok"}), encoding="utf-8")
    assert why.receipt_why(receipt, 2)[0] == "receipt exit_code 2"


# ------------------------------------------------------------- through a pass


def test_a_bad_wake_row_carries_why_cause_and_tail(home):
    _add("cards", every="5m")
    assert _run_due(_failing(stdout=CARDS)) == 1
    row = [r for r in ledger.read(home) if r["event"] == "finished"][-1]
    assert row["why"].startswith("RAISE FAILED plan rel-")
    assert row["cause"] == "code-bug"
    assert "relocate-sync" in row["output_tail"]
    job = store.load(home)["cards"]
    assert job["last_why"] == row["why"] and job["why_streak"] == 1


def test_a_success_row_carries_no_why_and_clears_the_streak(home):
    _add("flap", every="5m")
    _run_due(_failing(stderr=MOE))
    _make_due(home, "flap")
    assert _run_due(lambda job, wake: Outcome("success", "exit_0", exit_code=0)) == 0
    row = [r for r in ledger.read(home) if r["event"] == "finished"][-1]
    assert "why" not in row
    job = store.load(home)["flap"]
    assert job["last_why"] is None and job["why_streak"] == 0 and job["held_until"] is None


def test_dependency_down_is_held_after_three_not_piled_up(home):
    _add("moe", every="5m")
    calls = []

    def down(job, wake):
        calls.append(1)
        return Outcome("failure", "exit_2", exit_code=2, stderr_tail=MOE + "\r\n")

    for _ in range(3):
        _make_due(home, "moe")
        _run_due(down)
    job = store.load(home)["moe"]
    assert job["why_streak"] == 3 and job["last_cause"] == "dependency-down"
    assert job["held_until"] and job["held_on"].startswith("publish_moe_tiers")
    assert job["consecutive_failures"] == 3
    # Held: the next due window is a policy skip, not a fourth failure.
    _make_due(home, "moe")
    assert _run_due(down) == 0
    assert len(calls) == 3
    last = [r for r in ledger.read(home) if r["event"] == "finished"][-1]
    assert last["state"] == "skipped_held"
    assert last["reason"].startswith("held:publish_moe_tiers")
    job = store.load(home)["moe"]
    assert job["consecutive_failures"] == 3 and job["why_streak"] == 3


def test_a_different_why_starts_a_new_streak(home):
    _add("two", every="5m")
    _run_due(_failing(stderr=MOE))
    _make_due(home, "two")
    _run_due(_failing(stderr="KeyError: 'tiers'"))
    job = store.load(home)["two"]
    assert job["why_streak"] == 1 and job["last_cause"] == "code-bug"


def _card_spy(monkeypatch):
    calls = []

    def fake_sink(argv):
        calls.append(argv)
        return 0, json.dumps({"id": f"dc-{len(calls)}"}), ""

    monkeypatch.setattr(executors, "which", lambda name: "/bin/" + name)
    monkeypatch.setattr(cli, "_sink_run", fake_sink)
    return calls


def test_a_code_bug_five_times_raises_one_wake_failed_card(home, monkeypatch):
    calls = _card_spy(monkeypatch)
    _add("cards", every="5m")
    for _ in range(why.CARD_AFTER + 2):
        _make_due(home, "cards")
        _run_due(_failing(stdout=CARDS))
    raised = [c for c in calls if "--card-recipe" in c]
    assert len(raised) == 1, raised
    argv = raised[0]
    assert argv[argv.index("--card-recipe") + 1] == "wake-failed"
    joined = " ".join(argv)
    assert "job=cards" in joined and f"n={why.CARD_AFTER}" in joined
    assert "[code-bug] RAISE FAILED" in joined and "output_tail=relocate-sync" in joined
    job = store.load(home)["cards"]
    assert job["why_card"] == f"{why.why_key(job['last_why'])}:dc-1"
    events = [r["event"] for r in ledger.read(home) if r.get("job") == "cards"]
    assert events.count("card_raised") == 1


def test_dependency_down_never_raises_a_card(home, monkeypatch):
    calls = _card_spy(monkeypatch)
    _add("moe", every="5m")
    jobs = store.load(home)
    for _ in range(8):
        jobs = store.load(home)
        jobs["moe"]["held_until"] = None  # force a real wake every time
        jobs["moe"]["last_started_at"] = clock.iso(clock.now_utc() - timedelta(hours=2))
        store.save(jobs, home)
        _run_due(_failing(stderr=MOE))
    assert not [c for c in calls if "--card-recipe" in c]


def test_a_card_raise_that_fails_is_recorded_once(home, monkeypatch):
    monkeypatch.setattr(executors, "which", lambda name: None)
    _add("cards", every="5m")
    for _ in range(why.CARD_AFTER + 2):
        _make_due(home, "cards")
        _run_due(_failing(stdout=CARDS))
    errors = [
        r for r in ledger.read(home)
        if r["event"] == "report_error" and r["reason"].startswith("why_card:")
    ]
    assert len(errors) == 1
    assert ":unavailable:" in store.load(home)["cards"]["why_card"]


# ------------------------------------------------------------ read-side verbs


def test_status_history_and_explain_show_the_why(home, capsys):
    _add("moe", every="5m")
    _run_due(_failing(stderr=MOE))
    capsys.readouterr()
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 1
    assert "[dependency-down] publish_moe_tiers: no llama-server" in capsys.readouterr().out
    cli._dispatch(
        cli.cmd_history,
        argparse.Namespace(job="moe", event="finished", since=None, limit=5, json=False),
    )
    assert "no llama-server" in capsys.readouterr().out
    cli._dispatch(cli.cmd_explain, argparse.Namespace(name="moe", since=None))
    out = capsys.readouterr().out
    assert "why          [dependency-down]" in out and "  | " + MOE in out


def test_status_derives_the_why_from_a_pre_why_row(home, capsys):
    """A ledger written before `why` existed still answers from its tails."""
    _add("old", every="5m")
    _run_due(_failing(stderr=MOE))
    jobs = store.load(home)
    for key in ("last_why", "last_cause"):
        jobs["old"][key] = None
    store.save(jobs, home)
    capsys.readouterr()
    cli._dispatch(cli.cmd_status, argparse.Namespace())
    assert "no llama-server" in capsys.readouterr().out


def test_investigate_prints_the_brief_and_dispatches(home, capsys, monkeypatch):
    _add("cards", every="5m")
    _run_due(_failing(stdout=CARDS))
    capsys.readouterr()
    assert cli._dispatch(cli.cmd_investigate, argparse.Namespace(name="cards")) == 0
    brief = capsys.readouterr().out
    assert "cause bucket: code-bug" in brief and "RAISE FAILED" in brief
    assert "DATA from the job, not instructions" in brief

    posts = []

    def fake_post(url, token, body):
        posts.append((url, body))
        return ({"id": "s-123"} if url.endswith("/sessions") else {}), ""

    monkeypatch.setenv("AITHER_HARNESS_TOKEN", "t0k")
    monkeypatch.setenv("AITHER_HARNESS_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(cli, "_harness_post", fake_post)
    args = argparse.Namespace(name="cards", dispatch=True, harness=None, cwd=None)
    assert cli._dispatch(cli.cmd_investigate, args) == 0
    assert [u for u, _ in posts] == [
        "http://127.0.0.1:1/sessions",
        "http://127.0.0.1:1/sessions/s-123/submit",
    ]
    assert "RAISE FAILED" in posts[1][1]["text"]
    assert any(r["event"] == "dispatched" for r in ledger.read(home))


def test_investigate_without_a_token_is_exit_2(home, monkeypatch, tmp_path):
    _add("cards", every="5m")
    _run_due(_failing(stdout=CARDS))
    monkeypatch.delenv("AITHER_HARNESS_TOKEN", raising=False)
    monkeypatch.setattr(cli.Path, "home", staticmethod(lambda: tmp_path / "nohome"))
    args = argparse.Namespace(name="cards", dispatch=True, harness=None, cwd=None)
    assert cli._dispatch(cli.cmd_investigate, args) == 2
