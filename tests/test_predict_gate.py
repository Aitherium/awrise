"""``predict: off|warn|skip`` -- the optional awpredict-backed predict gate.

Every property here reduces to one of three halves: below the row threshold
(or on any engine failure) the verdict is UNJUDGED and the job fires exactly
as if predict were off; a `skip` policy refuses to fire on a bad verdict
UNLESS the job's own last row was already `skipped_predicted` (no
starvation); and a broken or slow awpredict never blocks or fails the wake
-- it costs one `report_error` row and nothing else. Every case runs against
a TEMP `AWRISE_HOME` (the `home` fixture below); none of this ever opens
``~/.aither/awrise``.
"""

from __future__ import annotations

import argparse
import contextlib
import io
from pathlib import Path

import pytest
from awrise import cli, ledger, store
from awrise.executors import Outcome


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("AWRISE_NOW", raising=False)
    yield store.home()


def _add(name: str = "p", every: str = "1h", run: str = "pass", **extra) -> int:
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
        executor="python",
        bearer_file=None,
        permission_mode=None,
        report_relay=None,
        card_after=0,
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return cli._dispatch(cli.cmd_add, args)


def _set(name: str, key: str, raw: str) -> int:
    return cli._dispatch(
        cli.cmd_set,
        argparse.Namespace(name=name, assignments=[f"{key}={raw}"], allow_overrun=False),
    )


def _run_due(executor=None) -> int:
    return cli._dispatch(
        lambda a: cli.cmd_run_due(a, executor=executor),
        argparse.Namespace(quiet=True, invoker="test"),
    )


def _later(monkeypatch, seconds: int) -> None:
    monkeypatch.setenv("AWRISE_NOW", f"+{seconds}")


def _finished(base: Path, name: str = "p") -> list:
    return [r for r in ledger.read(base) if r.get("event") == "finished" and r.get("job") == name]


def _started(base: Path, name: str = "p") -> list:
    return [r for r in ledger.read(base) if r.get("event") == "started" and r.get("job") == name]


def _report_errors(base: Path) -> list:
    return [r for r in ledger.read(base) if r.get("event") == "report_error"]


def _build_history(monkeypatch, base: Path, state: str, count: int) -> None:
    """*count* judged finished rows for job "p", each on its own due window,
    under `predict: off` so building the history never itself consults the
    gate."""
    for i in range(count):
        if i:
            _later(monkeypatch, 3700 * i)
        assert _run_due(lambda job, wake, s=state: Outcome(s, f"fake_{s}")) == (
            1 if state in ledger.BAD_STATES else 0
        )


# ------------------------------------------------------------------- off


def test_predict_off_by_default_writes_no_prediction_and_builds_no_engine(home):
    assert _add() == 0
    assert _run_due() == 0
    assert "prediction" not in _started(home)[-1]
    assert str(home) not in cli._PREDICT_ENGINES


def test_predict_must_be_one_of_off_warn_skip(home):
    assert _add() == 0
    assert _set("p", "predict", "sideways") == 1


# -------------------------------------------------------- under threshold


def test_under_threshold_is_unjudged_and_still_fires(home, monkeypatch):
    assert _add() == 0
    assert _set("p", "predict", "warn") == 0
    assert _run_due() == 0
    started = _started(home)[-1]
    assert started["prediction"]["verdict"] == "UNJUDGED", started
    assert "historical rows" in started["prediction"]["reason"], started
    assert _finished(home)[-1]["state"] == "success", _finished(home)[-1]
    assert not _report_errors(home)


# ------------------------------------------------------------- engine cache


def test_engine_is_built_exactly_once_across_repeated_due_checks(home, monkeypatch):
    import awpredict.core.mlp as mlp

    assert _add() == 0
    assert _set("p", "predict", "warn") == 0
    _build_history(monkeypatch, home, "success", cli.PREDICT_MIN_ROWS)

    builds = []
    real_init = mlp.MLPWorldModel.__init__

    def counting_init(self, *a, **k):
        builds.append(1)
        return real_init(self, *a, **k)

    monkeypatch.setattr(mlp.MLPWorldModel, "__init__", counting_init)
    # Three MORE due-checks, every one past the row threshold, so every one
    # of them reaches `_predict_engine` -- the engine must be constructed on
    # the FIRST of these and reused by the other two.
    for i in range(3):
        _later(monkeypatch, 3700 * (cli.PREDICT_MIN_ROWS + i))
        assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
    assert builds == [1], builds


# ------------------------------------------------------------------ timeout


def test_predict_timeout_fails_open_and_writes_a_report_error_row(home, monkeypatch):
    assert _add() == 0
    assert _set("p", "predict", "warn") == 0
    _build_history(monkeypatch, home, "success", cli.PREDICT_MIN_ROWS)

    def _hang(fn, timeout_s):
        raise TimeoutError(f"timed out after {timeout_s:g}s")

    monkeypatch.setattr(cli, "_predict_call_with_timeout", _hang)
    _later(monkeypatch, 3700 * cli.PREDICT_MIN_ROWS)
    assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
    started = _started(home)[-1]
    assert started["prediction"]["verdict"] == "UNJUDGED", started
    assert "timed out" in started["prediction"]["reason"], started
    reasons = " ".join(r.get("reason") or "" for r in _report_errors(home))
    assert "timed out" in reasons, reasons


def test_predict_exception_fails_open_and_writes_a_report_error_row(home, monkeypatch):
    assert _add() == 0
    assert _set("p", "predict", "warn") == 0
    _build_history(monkeypatch, home, "success", cli.PREDICT_MIN_ROWS)

    def _boom(base):
        raise RuntimeError("engine exploded")

    monkeypatch.setattr(cli, "_predict_engine", _boom)
    _later(monkeypatch, 3700 * cli.PREDICT_MIN_ROWS)
    assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
    started = _started(home)[-1]
    assert started["prediction"]["verdict"] == "UNJUDGED", started
    assert "engine exploded" in started["prediction"]["reason"], started
    reasons = " ".join(r.get("reason") or "" for r in _report_errors(home))
    assert "engine exploded" in reasons, reasons


# --------------------------------------------------------------------- skip


def test_skip_holds_on_a_bad_verdict_and_never_skips_twice_running(home, monkeypatch):
    assert _add() == 0
    _build_history(monkeypatch, home, "timeout", cli.PREDICT_MIN_ROWS)
    assert _set("p", "predict", "skip") == 0

    fired = []
    _later(monkeypatch, 3700 * cli.PREDICT_MIN_ROWS)
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake")) == 0
    assert fired == [], "a bad-verdict skip must never call the executor"
    assert _finished(home)[-1]["state"] == "skipped_predicted"

    # No-starvation: the row right after `skipped_predicted` is forced,
    # regardless of what the model still predicts.
    _later(monkeypatch, 3700 * (cli.PREDICT_MIN_ROWS + 1))
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake")) == 1
    assert fired == [1], "the row after skipped_predicted must be a real attempt"
    assert _finished(home)[-1]["state"] == "timeout"
    started = _started(home)[-1]
    assert started["prediction"]["verdict"] == "bad", started


def test_a_fired_through_prediction_lands_on_the_started_row_not_silence(home, monkeypatch):
    """UNJUDGED and a good verdict both fire through under `warn` -- and
    both are recorded on the started row, exactly like a forced attempt
    under `skip` is (the previous case)."""
    assert _add() == 0
    assert _set("p", "predict", "warn") == 0
    _build_history(monkeypatch, home, "success", cli.PREDICT_MIN_ROWS)
    _later(monkeypatch, 3700 * cli.PREDICT_MIN_ROWS)
    assert _run_due(lambda job, wake: Outcome("success", "ok")) == 0
    started = _started(home)[-1]
    assert started["prediction"]["verdict"] == "good", started
    assert started["prediction"]["mode"] is not None, started


# --------------------------------------------------------------- dry run


def test_dry_run_predict_skip_agrees_with_the_real_pass(home, monkeypatch):
    assert _add() == 0
    _build_history(monkeypatch, home, "timeout", cli.PREDICT_MIN_ROWS)
    assert _set("p", "predict", "skip") == 0

    _later(monkeypatch, 3700 * cli.PREDICT_MIN_ROWS)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = cli._dispatch(
            cli.cmd_run_due,
            argparse.Namespace(quiet=False, invoker="test", dry_run=True, drain=False, prune=None),
        )
    assert rc == 0
    assert "hold" in out.getvalue()
    assert not [r for r in ledger.read(home) if r.get("event") == "finished" and r.get("dry_run")]

    fired = []
    assert _run_due(lambda job, wake: fired.append(1) or Outcome("timeout", "fake")) == 0
    assert fired == [], "the real pass must agree with the dry run and hold too"
    assert _finished(home)[-1]["state"] == "skipped_predicted"
