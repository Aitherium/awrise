"""Telling someone: the relay line and the ONE card per streak.

Both sinks run after the ledger row and the store are already written, so the
property under test is never "did the message arrive" alone -- it is "the pass
still finished, the store is still right, and a sink that could not run left a
row saying so". A scheduler that stops scheduling because a chat server is
down would be a worse product than one that says nothing.
"""

import argparse
import json
import os
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import cli, clock, ledger, store

from .test_executors import write_fake_tool

# A fake awrelay: appends its argv to $AWRISE_TEST_LOG and exits with
# $AWRISE_TEST_RC (0 unless the test says otherwise).
FAKE_AWRELAY = """
import json, os, sys
with open(os.environ["AWRISE_TEST_LOG"], "a", encoding="utf-8") as fh:
    fh.write(json.dumps(sys.argv[1:]) + "\\n")
sys.exit(int(os.environ.get("AWRISE_TEST_RC") or 0))
"""

# A fake awask: `ask` mints a card id and logs the argv; `show <id> --json`
# reports the answer sitting in $AWRISE_TEST_ANSWER (absent = still open).
FAKE_AWASK = """
import json, os, sys
argv = sys.argv[1:]
with open(os.environ["AWRISE_TEST_LOG"], "a", encoding="utf-8") as fh:
    fh.write(json.dumps(argv) + "\\n")
if argv and argv[0] == "ask":
    print(json.dumps({"id": "d-card01", "status": "open"}))
    sys.exit(0)
if argv and argv[0] == "show":
    answer = os.environ.get("AWRISE_TEST_ANSWER") or ""
    print(json.dumps({"id": argv[1], "status": "answered" if answer else "open",
                      "answer": answer or None}))
    sys.exit(0)
sys.exit(2)
"""


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv(cli.RELAY_CHANNEL_ENV, raising=False)
    monkeypatch.delenv(cli.RELAY_NICK_ENV, raising=False)
    return store.home()


@pytest.fixture
def sinkbin(tmp_path, monkeypatch) -> Path:
    """Fake awrelay + awask first on PATH, and a log they both append to."""
    path = tmp_path / "bin"
    log = tmp_path / "sink.log"
    write_fake_tool(path, "awrelay", FAKE_AWRELAY)
    write_fake_tool(path, "awask", FAKE_AWASK)
    monkeypatch.setenv("PATH", str(path) + os.pathsep + os.environ.get("PATH", ""))
    monkeypatch.setenv("AWRISE_TEST_LOG", str(log))
    monkeypatch.delenv("AWRISE_TEST_RC", raising=False)
    monkeypatch.delenv("AWRISE_TEST_ANSWER", raising=False)
    return log


@pytest.fixture
def nobin(tmp_path, monkeypatch) -> Path:
    """Neither tool is installed -- the ordinary state of a stranger's host."""
    path = tmp_path / "emptybin"
    path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("PATH", str(path))
    return path


def _calls(log: Path) -> list:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]


def _add(name="j", every="1h", run="exit 1", **extra) -> int:
    args = argparse.Namespace(
        name=name,
        every=every,
        run=run,
        timeout=None,
        cwd=None,
        at=None,
        allow_overrun=False,
        disabled=False,
        detach=False,
        missed=None,
        executor=None,
        bearer_file=None,
        permission_mode=None,
        report_relay=None,
        card_after=None,
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _failing(name="j", **extra) -> int:
    """A job whose every wake fails, without relying on a shell builtin."""
    return _add(name, run="import sys; sys.exit(3)", executor="python", **extra)


def _run_due() -> int:
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=None), argparse.Namespace(quiet=True, invoker="test")
    )


def _later(monkeypatch, hours: int = 2) -> None:
    """Move the whole process's clock forward so the NEXT pass finds the job
    due again. A test that waited for a real hour is a test nobody runs."""
    monkeypatch.setenv(clock.NOW_ENV, f"+{hours * 3600}")


def _rows(home: Path, event: str) -> list:
    return [row for row in ledger.read(home) if row.get("event") == event]


def _job(home: Path, name="j") -> dict:
    return store.load(home)[name]


# ----------------------------------------------------------------- the relay


def test_a_failing_wake_posts_one_enveloped_relay_line(home, sinkbin, monkeypatch):
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#awrise")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing() == 0
    assert _run_due() == 1
    sent = [c for c in _calls(sinkbin) if c and c[0] == "send"]
    assert len(sent) == 1, sent
    assert sent[0][1] == "#awrise"
    assert sent[0][2].startswith("[awrise] j: failure ("), sent[0]
    assert sent[0][3:] == ["--kind", "finding"]
    assert not _rows(home, "report_error")


def test_a_successful_wake_posts_nothing(home, sinkbin, monkeypatch):
    """The negative twin. `on` lists the bad states only, so a green pass is
    silent -- a scheduler that messages a channel every minute gets muted."""
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#awrise")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _add(run="pass", executor="python") == 0
    assert _run_due() == 0
    assert [c for c in _calls(sinkbin) if c and c[0] == "send"] == []


def test_no_channel_means_no_sink_at_all(home, sinkbin):
    assert _failing() == 0
    assert _run_due() == 1
    assert _calls(sinkbin) == []
    assert not _rows(home, "report_error")


def test_a_missing_nick_is_a_report_error_row_never_a_crash(home, sinkbin, monkeypatch):
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#awrise")
    assert _failing() == 0
    assert _run_due() == 1, "the pass still reports the job's own verdict"
    rows = _rows(home, "report_error")
    assert len(rows) == 1 and rows[0]["reason"].startswith("awrelay_nick_missing:"), rows
    assert _calls(sinkbin) == [], "awrelay was run without the nick it needs"
    assert _job(home)["last_state"] == "failure", "the wake's own record survived"


def test_an_absent_awrelay_is_a_report_error_row(home, nobin, monkeypatch):
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#awrise")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing() == 0
    assert _run_due() == 1
    rows = _rows(home, "report_error")
    assert len(rows) == 1 and rows[0]["reason"] == "awrelay_not_installed", rows


def test_a_relay_that_exits_non_zero_is_a_report_error_row(home, sinkbin, monkeypatch):
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#awrise")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    monkeypatch.setenv("AWRISE_TEST_RC", "4")
    assert _failing() == 0
    assert _run_due() == 1
    rows = _rows(home, "report_error")
    assert len(rows) == 1 and rows[0]["reason"].startswith("awrelay_exit_4:"), rows


def test_the_job_spec_channel_beats_the_environment(home, sinkbin, monkeypatch):
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#fallback")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing(report_relay="#mine") == 0
    assert _run_due() == 1
    sent = [c for c in _calls(sinkbin) if c and c[0] == "send"]
    assert sent and sent[0][1] == "#mine"


def test_a_channel_without_a_leading_hash_is_refused_at_add(home, sinkbin):
    assert _failing(report_relay="awrise") == 1
    assert store.load(home) == {}


# ------------------------------------------------------------------ the card


def test_one_card_per_streak_and_the_id_is_stored(home, sinkbin):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    raised = _rows(home, "card_raised")
    assert len(raised) == 1 and raised[0]["card_id"] == "d-card01", raised
    assert _job(home)["report"]["card_id"] == "d-card01"
    asks = [c for c in _calls(sinkbin) if c and c[0] == "ask"]
    assert len(asks) == 1
    assert "--default" in asks[0] and asks[0][asks[0].index("--default") + 1] == "keep"
    options = [asks[0][i + 1] for i, part in enumerate(asks[0]) if part == "--option"]
    assert [o.split(":", 1)[0] for o in options] == ["disable", "keep"]


def test_a_second_failure_in_the_same_streak_raises_no_second_card(home, sinkbin):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    assert cli._dispatch(cli.cmd_run, argparse.Namespace(name="j", force=True)) == 1
    assert len(_rows(home, "card_raised")) == 1
    assert len([c for c in _calls(sinkbin) if c and c[0] == "ask"]) == 1


def test_below_the_threshold_no_card_is_raised(home, sinkbin):
    """The negative twin for the threshold itself."""
    assert _failing(card_after=3) == 0
    assert _run_due() == 1
    assert _rows(home, "card_raised") == []
    assert [c for c in _calls(sinkbin) if c and c[0] == "ask"] == []
    assert _job(home)["report"]["card_id"] is None


def test_card_after_zero_never_raises(home, sinkbin):
    assert _failing(card_after=0) == 0
    assert _run_due() == 1
    assert _rows(home, "card_raised") == []


def test_the_next_pass_reads_disable_and_applies_it(home, sinkbin, monkeypatch):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    assert _job(home)["enabled"] is True
    monkeypatch.setenv("AWRISE_TEST_ANSWER", "disable")
    _later(monkeypatch)
    assert _run_due() == 0, "a disabled job is not a failing pass"
    answered = _rows(home, "card_answered")
    assert len(answered) == 1 and answered[0]["reason"] == "answer_disable", answered
    job = _job(home)
    assert job["enabled"] is False
    assert job["report"]["card_id"] == "kept:d-card01"
    states = [r["state"] for r in _rows(home, "finished") if r.get("job") == "j"]
    assert states[-1] == "skipped_disabled", states


def test_keep_leaves_the_job_alone_and_spends_the_card(home, sinkbin, monkeypatch):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    monkeypatch.setenv("AWRISE_TEST_ANSWER", "keep")
    _later(monkeypatch)
    assert _run_due() == 1
    job = _job(home)
    assert job["enabled"] is True
    assert job["report"]["card_id"] == "kept:d-card01"
    assert len(_rows(home, "card_raised")) == 1, "the spent card raised a second one"


def test_an_unanswered_card_changes_nothing(home, sinkbin, monkeypatch):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    before = (store.store_path(home)).read_bytes()
    _later(monkeypatch)
    assert _run_due() == 1
    job = _job(home)
    assert job["enabled"] is True and job["report"]["card_id"] == "d-card01"
    assert _rows(home, "card_answered") == []
    assert before != b"", "the store was written at least once"


def test_a_recovered_job_clears_the_card_and_can_raise_another(home, sinkbin, monkeypatch):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    assert _job(home)["report"]["card_id"] == "d-card01"
    monkeypatch.setenv("AWRISE_TEST_ANSWER", "keep")
    _later(monkeypatch)
    assert _run_due() == 1
    assert (
        cli._dispatch(
            cli.cmd_set, argparse.Namespace(name="j", assignments=["run=pass"], allow_overrun=False)
        )
        == 0
    )
    assert cli._dispatch(cli.cmd_run, argparse.Namespace(name="j", force=True)) == 0
    job = _job(home)
    assert job["consecutive_failures"] == 0
    assert job["report"]["card_id"] is None, "a spent card outlived its streak"


def test_an_absent_awask_writes_exactly_one_report_error_per_streak(home, nobin, monkeypatch):
    assert _failing(card_after=1) == 0
    assert _run_due() == 1
    _later(monkeypatch)
    assert _run_due() == 1
    rows = _rows(home, "report_error")
    assert len(rows) == 1 and rows[0]["reason"] == "awask_not_installed", rows
    assert _job(home)["report"]["card_id"].startswith("unavailable:")
    assert _rows(home, "card_raised") == []


# ------------------------------------------------- the channel from the env


def test_an_env_channel_that_is_not_a_channel_is_refused_and_says_so(home, sinkbin, monkeypatch):
    """The environment is the OTHER lane into this argument, and it has no
    add-time gate. A value shaped like a flag is read by awrelay's own parser,
    which exits 0 having posted nothing -- a configured channel that is
    silently dead, with nothing in the ledger saying so."""
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "--kind")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing() == 0
    assert _run_due() == 1
    assert _calls(sinkbin) == [], "awrelay was handed a value that is not a channel"
    rows = _rows(home, "report_error")
    assert len(rows) == 1, rows
    assert rows[0]["reason"].startswith("relay_channel_refused:"), rows
    assert _job(home)["last_state"] == "failure", "the wake's own record survived"


def test_a_good_env_channel_still_posts(home, sinkbin, monkeypatch):
    """The negative twin: the judgement refuses a non-channel and nothing else."""
    monkeypatch.setenv(cli.RELAY_CHANNEL_ENV, "#ops")
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing() == 0
    assert _run_due() == 1
    sent = [c for c in _calls(sinkbin) if c and c[0] == "send"]
    assert len(sent) == 1 and sent[0][1] == "#ops", sent
    assert not _rows(home, "report_error")


# ------------------------------------------------------ the orphaned wake


def _orphan(home: Path, wake_id: str, name: str, minutes: int = 180) -> str:
    """A `started` row old enough to be past any bound, with a pid that is
    gone -- what a host that rebooted mid-wake leaves behind."""
    ts = clock.iso(clock.now_utc() - timedelta(minutes=minutes))
    row = {
        "schema": 1,
        "wake_id": wake_id,
        "pass_id": "p-dead0000",
        "ts": ts,
        "invoker": "t",
        "host": "h",
        "pid": 1,
        "interpreter": "x",
        "job": name,
        "event": "started",
        "state": None,
        "reason": "due",
        "timeout_s": 60,
    }
    path = ledger.day_file(home, clock.parse_ts(ts))
    path.parent.mkdir(exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")
    return ts


def _reconcile() -> int:
    return cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=False, reset=False))


def test_an_orphaned_wake_reaches_the_relay(home, sinkbin, monkeypatch):
    """`orphaned` is in the default `report.on`, and it is the ONE outcome a
    wake reaches without a pass of its own: the host died holding it. A sink
    that is silent for it is silent for the failure nobody is watching."""
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing("o", report_relay="#ops") == 0
    _orphan(home, "w-orphan10", "o")
    assert _reconcile() == 1
    sent = [c for c in _calls(sinkbin) if c and c[0] == "send"]
    assert len(sent) == 1, sent
    assert sent[0][1] == "#ops"
    assert sent[0][2].startswith("[awrise] o: orphaned ("), sent[0]
    assert _job(home, "o")["last_state"] == "orphaned"


def test_a_job_that_only_orphans_still_raises_its_card(home, sinkbin, monkeypatch):
    """Consecutive orphans walk past `card_after` exactly like failures do;
    before this they walked past it in silence."""
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _failing("o", card_after=2) == 0
    for index in range(4):
        _orphan(home, f"w-orphan2{index}", "o")
        assert _reconcile() == 1
    job = _job(home, "o")
    assert job["consecutive_failures"] == 4
    asked = [c for c in _calls(sinkbin) if c and c[0] == "ask"]
    assert len(asked) == 1, asked
    assert job["report"]["card_id"] == "d-card01"
    assert len(_rows(home, "card_raised")) == 1


def test_a_successful_wake_after_an_orphan_is_not_reported(home, sinkbin, monkeypatch):
    """The negative twin: the orphan path uses the same `on` list as every
    other outcome, so a state that is not in it stays silent."""
    monkeypatch.setenv(cli.RELAY_NICK_ENV, "tester")
    assert _add("o", run="pass", executor="python", report_relay="#ops") == 0
    assert _run_due() == 0
    assert [c for c in _calls(sinkbin) if c and c[0] == "send"] == []


# -------------------------------------------- `report` is OFF until it is set
#
# Review finding (2026-09-19): `_report_block` merged store.REPORT_DEFAULTS
# (`card_after: 3`) onto a job whose stored `report` is None -- a job that
# configured no reporting at all -- so the card sink fired after three bad
# wakes for EVERY job, raised a real owner-facing decision card wherever awask
# is on PATH, and rewrote the spec with a `report` block the operator never
# authored. README:81 says `report | off`, and store.py says "every field is
# off or inert by default". The self-test case named
# `a_job_with_no_sinks_configured_writes_no_report_rows` does not cover it: it
# adds the job with an explicit `card_after=0`, which CREATES a report block.


def test_a_job_that_configured_no_reporting_raises_no_card(home, sinkbin, monkeypatch):
    assert _failing("q") == 0
    assert _job(home, "q")["report"] is None, "the fixture must start with no report block"
    for _pass in range(4):
        assert _run_due() == 1
        _later(monkeypatch, 2 * (_pass + 1))
    assert _job(home, "q")["consecutive_failures"] == 4
    assert [c for c in _calls(sinkbin) if c and c[0] == "ask"] == [], "no card was authorised"
    assert not _rows(home, "card_raised")
    assert _job(home, "q")["report"] is None, "an unconfigured spec must not be rewritten"


def test_a_job_that_configured_no_reporting_writes_no_report_error_row(home, nobin, monkeypatch):
    """The awask-absent twin: the sink must not run at all, so it cannot fail."""
    assert _failing("q") == 0
    for _pass in range(4):
        assert _run_due() == 1
        _later(monkeypatch, 2 * (_pass + 1))
    assert not _rows(home, "report_error"), _rows(home, "report_error")
    assert _job(home, "q")["report"] is None


def test_card_after_still_fires_once_the_operator_asks_for_it(home, sinkbin, monkeypatch):
    """The negative twin: the guard is about what was CONFIGURED, not about
    turning the card sink off."""
    assert _failing("q", card_after=2) == 0
    assert _job(home, "q")["report"]["card_after"] == 2
    for _pass in range(2):
        assert _run_due() == 1
        _later(monkeypatch, 2 * (_pass + 1))
    assert [c for c in _calls(sinkbin) if c and c[0] == "ask"], "a configured card must be raised"
    assert _rows(home, "card_raised")
