"""`prewarm`: one job per unit, and never two units sharing one job name.

Review finding (2026-09-19): `_slug` strips the `aither-`/`aitheros-` prefix as
well as slugging, so `foo.service` and `aither-foo.service` collapse to the one
job name `prewarm-foo`. `cmd_prewarm --apply` then did `jobs[proposal["job"]] =
job` for each proposal in turn -- silently overwriting the earlier record while
appending `{"state": "added"}` for BOTH, and printing "2 job(s) added". One unit
was scheduled, the other was woken by nothing, and the verb reported success for
both: the exact silence this brick exists to remove.
"""

import argparse
import json
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import cli, clock, store


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    return store.home()


@pytest.fixture
def ledger_dir(tmp_path) -> Path:
    path = tmp_path / "usage"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _yesterday_at(hour: int = 7) -> str:
    day = (clock.now_utc() - timedelta(days=1)).date()
    return f"{day.isoformat()}T{hour:02d}:30:00+00:00"


def _record(directory: Path, stem: str, unit: str, hour: int = 7) -> None:
    (directory / f"{stem}.json").write_text(
        json.dumps({"service": stem, "unit": unit, "last_request_at": _yesterday_at(hour)}),
        encoding="utf-8",
    )


def _args(ledger_dir: Path, **extra) -> argparse.Namespace:
    args = argparse.Namespace(
        ledger_dir=str(ledger_dir),
        days_ago=1,
        every=cli.PREWARM_EVERY,
        run=None,
        exclude=None,
        park_after=False,
        apply=False,
        allow_derived=False,
        json=False,
    )
    for key, value in extra.items():
        setattr(args, key, value)
    return args


def test_two_units_that_slug_to_one_job_name_do_not_silently_overwrite(
    home, ledger_dir, capsys
):
    _record(ledger_dir, "alpha", "foo.service")
    _record(ledger_dir, "beta", "aither-foo.service")
    rc = cli._dispatch(cli.cmd_prewarm, _args(ledger_dir, apply=True))
    out = capsys.readouterr().out
    jobs = store.load(home)
    # Exactly one unit is scheduled, and the other is NAMED as not scheduled.
    assert len(jobs) == 1, jobs
    [(name, job)] = jobs.items()
    assert name == "prewarm-foo"
    dropped = [unit for unit in ("foo.service", "aither-foo.service") if unit != job["wake"]]
    assert dropped, (job["wake"], out)
    assert "1 job(s) added" in out, out
    assert "REFUSE" in out and dropped[0] in out, out
    assert "NOT scheduled" in out, out
    assert rc != 0, "a unit that could not be scheduled is not a clean pass"


def test_the_collision_is_reported_before_apply_too(home, ledger_dir, capsys):
    _record(ledger_dir, "alpha", "foo.service")
    _record(ledger_dir, "beta", "aither-foo.service")
    cli._dispatch(cli.cmd_prewarm, _args(ledger_dir, json=True))
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["proposals"]) == 1, payload["proposals"]
    assert len(payload["skipped"]) == 1, payload["skipped"]
    assert "prewarm-foo" in payload["skipped"][0]["why"], payload["skipped"]
    assert store.load(home) == {}, "a plan without --apply writes nothing"


def test_two_distinct_units_each_get_their_own_job(home, ledger_dir):
    """The negative twin: the guard is about collisions, not about refusing."""
    _record(ledger_dir, "alpha", "foo.service")
    _record(ledger_dir, "beta", "bar.service", hour=9)
    assert cli._dispatch(cli.cmd_prewarm, _args(ledger_dir, apply=True)) == 0
    jobs = store.load(home)
    assert sorted(jobs) == ["prewarm-bar", "prewarm-foo"], jobs
    assert {job["wake"] for job in jobs.values()} == {"foo.service", "bar.service"}
    assert jobs["prewarm-foo"]["at"] == "07:00" and jobs["prewarm-bar"]["at"] == "09:00"


def test_an_applied_prewarm_job_waits_for_its_anchor(home, ledger_dir):
    """`--apply` creates every job with an `at:` anchor and no start stamp. It
    must not be due on the very next pass -- that woke every proposed unit at
    once."""
    _record(ledger_dir, "alpha", "foo.service", hour=3)
    assert cli._dispatch(cli.cmd_prewarm, _args(ledger_dir, apply=True)) == 0
    job = store.load(home)["prewarm-foo"]
    assert job["last_started_at"] is None and job["at"] == "03:00"
    now = clock.parse_ts(job["created_at"]) + timedelta(minutes=1)
    assert clock.is_due(job, now) is False, clock.next_due(job, now)
