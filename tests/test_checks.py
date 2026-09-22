"""WL001-WL005: each rule fired by a record that breaks it, quiet on one that does not.

The rules read the ledger, the store and the lock directory and never run
anything, so every case here builds the evidence by hand -- which is also the
only way to prove a rule can still FAIL.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from awrise import checks, cli, clock, ledger, lock, store

DEAD_PID = 0x7FFFFFFE  # past every pid range this runs on: never alive


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


def _row(home: Path, event: str, ts: datetime, **extra) -> None:
    row = {
        "schema": 1,
        "wake_id": extra.pop("wake_id", "w-aaaaaaaa"),
        "pass_id": "p-aaaaaaaa",
        "ts": clock.iso(ts),
        "invoker": "test",
        "host": "h",
        "pid": extra.pop("pid", 1),
        "interpreter": "x",
        "job": extra.pop("job", None),
        "event": event,
        "state": extra.pop("state", None),
        "reason": extra.pop("reason", "test"),
    }
    row.update(extra)
    path = ledger.day_file(home, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row) + "\n")


def _job(home: Path, name="j", every="1m", last_started=None, **extra):
    jobs = store.load(home)
    job = store.new_job(every=every, run="echo hi")
    job["timeout_s"] = 30
    if last_started is not None:
        job["last_started_at"] = clock.iso(last_started)
        job["last_state"] = "success"
        job["last_reason"] = "exit_0"
        job["last_wake_id"] = "w-earlier1"
    job.update(extra)
    jobs[name] = job
    store.save(jobs, home)
    return job


def _wide_window(home: Path, now: datetime, hours: int = 3) -> None:
    """Enough ledger for a cadence verdict to mean something."""
    _row(home, "tick", now - timedelta(hours=hours), reason="pass_start")
    _row(home, "tick", now, reason="pass_start")


def _find(home: Path, rule: str):
    for finding in checks.run(base=home):
        if finding.rule == rule:
            return finding
    raise AssertionError(f"{rule} was not judged at all")


# ------------------------------------------------------------------- WL001


def test_wl001_fires_on_a_wake_that_never_finished(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    _row(
        home,
        "started",
        now - timedelta(hours=2),
        job="j",
        wake_id="w-open0001",
        pid=DEAD_PID,
        timeout_s=30,
    )
    finding = _find(home, "WL001")
    assert finding.code == checks.VIOLATION
    assert "w-open0001" in " ".join(finding.details)


def test_wl001_is_quiet_when_the_wake_was_closed(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    _row(
        home,
        "started",
        now - timedelta(hours=2),
        job="j",
        wake_id="w-shut0001",
        pid=DEAD_PID,
        timeout_s=30,
    )
    _row(
        home,
        "finished",
        now - timedelta(hours=2),
        job="j",
        wake_id="w-shut0001",
        state="success",
        reason="exit_0",
    )
    assert _find(home, "WL001").code == checks.OK


def test_wl001_leaves_a_wake_that_could_still_be_running_alone(home):
    """A live pass is not an orphan: its own pid is alive and inside the bound."""
    import os

    now = clock.now_utc()
    _job(home, last_started=now)
    _row(home, "started", now, job="j", wake_id="w-live0001", pid=os.getpid(), timeout_s=300)
    assert _find(home, "WL001").code == checks.OK


# ------------------------------------------------------------------- WL002


def test_wl002_fires_on_a_line_awrise_could_not_have_written(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    _row(home, "tick", now, reason="pass_start")
    with open(ledger.day_file(home, now), "a", encoding="utf-8", newline="\n") as fh:
        fh.write("{ not json at all\n")
    finding = _find(home, "WL002")
    assert finding.code == checks.VIOLATION and "unreadable" in " ".join(finding.details)


def test_wl002_fires_on_a_state_outside_the_closed_set(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    _row(
        home, "finished", now, job="j", wake_id="w-weird001", state="probably_fine", reason="exit_0"
    )
    finding = _find(home, "WL002")
    assert finding.code == checks.VIOLATION
    assert "probably_fine" in " ".join(finding.details)


def test_wl002_is_quiet_on_a_ledger_awrise_wrote(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    ledger.append(home, {"event": "tick", "reason": "pass_start", "pass_id": "p-x"})
    ledger.append(
        home,
        {
            "event": "finished",
            "job": "j",
            "state": "success",
            "reason": "exit_0",
            "wake_id": "w-ok000001",
        },
    )
    assert _find(home, "WL002").code == checks.OK


# ------------------------------------------------------------------- WL003


def test_wl003_fires_on_an_enabled_job_the_clock_stopped_reaching(home):
    now = clock.now_utc()
    _job(home, every="1m", last_started=now - timedelta(hours=2))
    _wide_window(home, now)
    finding = _find(home, "WL003")
    assert finding.code == checks.VIOLATION
    assert "j:" in " ".join(finding.details) and "window(s)" in " ".join(finding.details)


def test_wl003_is_quiet_for_a_job_woken_inside_its_window(home):
    now = clock.now_utc()
    _job(home, every="1m", last_started=now)
    _wide_window(home, now)
    assert _find(home, "WL003").code == checks.OK


def test_wl003_is_unjudged_on_a_ledger_younger_than_two_windows(home):
    """The slice's own check: a young ledger is exit 2, never a green 0."""
    now = clock.now_utc()
    _job(home, every="1h", last_started=now - timedelta(hours=5))
    _row(home, "tick", now - timedelta(seconds=30), reason="pass_start")
    _row(home, "tick", now, reason="pass_start")
    finding = _find(home, "WL003")
    assert finding.code == checks.UNJUDGED
    assert "less than 2x the window of any enabled job" in finding.summary
    assert checks.verdict(checks.run(base=home)) == checks.UNJUDGED
    # negative twin: widen the window and the SAME store is judged, and red
    _row(home, "tick", now - timedelta(hours=5), reason="pass_start")
    assert _find(home, "WL003").code == checks.VIOLATION


def test_wl003_judges_a_starved_job_beside_a_long_cadence_one(home):
    """The guard is per job. A weekly job must not blind the rule to a
    1-minute job the clock stopped reaching hours ago -- keyed on the longest
    cadence in the store, one monthly job makes this gate UNJUDGED forever."""
    now = clock.now_utc()
    _job(home, name="fast", every="1m", last_started=now - timedelta(hours=2))
    _wide_window(home, now)
    assert _find(home, "WL003").code == checks.VIOLATION
    _job(home, name="monthly", every="30d")  # healthy, never run, changes nothing
    finding = _find(home, "WL003")
    assert finding.code == checks.VIOLATION, finding.summary
    assert any("fast:" in detail for detail in finding.details), finding.details
    assert any("monthly" in detail and "not judged" in detail for detail in finding.details), (
        "the skipped job is named, not implied"
    )
    # negative twin: wake the fast job and the same two-job store is OK, not red
    _job(home, name="fast", every="1m", last_started=now)
    ok = _find(home, "WL003")
    assert ok.code == checks.OK and "1 of 2" in ok.summary, ok.summary


def test_wl003_is_unjudged_when_every_job_is_too_young(home):
    """Per job does not mean lenient: with nothing judgeable it is still 2."""
    now = clock.now_utc()
    _job(home, name="slow", every="1h", last_started=now - timedelta(hours=5))
    _row(home, "tick", now - timedelta(seconds=30), reason="pass_start")
    _row(home, "tick", now, reason="pass_start")
    finding = _find(home, "WL003")
    assert finding.code == checks.UNJUDGED
    assert checks.verdict(checks.run(base=home)) == checks.UNJUDGED


def test_one_unreadable_line_cannot_lend_the_ledger_a_history(home):
    """A truncated final line is the normal outcome of a crash mid-append. It
    is reported as a row, and it must not stretch the span the window guard
    is measured against -- stamped at the day file's midnight it turned a
    seconds-old ledger into up to 24h of record and a green verdict."""
    now = clock.now_utc()
    _job(home, name="j", every="10m", last_started=now - timedelta(hours=4))
    _row(home, "tick", now, reason="pass_start")
    assert _find(home, "WL003").code == checks.UNJUDGED, "seconds of ledger: unjudged"
    day = ledger.day_file(home, now)
    with open(day, "a", encoding="utf-8", newline="\n") as fh:
        fh.write("{truncated\n")
    assert [r for r in ledger.read(home) if r.get("event") == "unreadable"], (
        "the line is still reported as a row"
    )
    assert checks.span_s(ledger.read(home)) is None, "it lends the record no span"
    assert _find(home, "WL003").code == checks.UNJUDGED, (
        "one unreadable line must not buy a verdict"
    )
    # negative twin: real rows from awrise DO buy the span, and the verdict
    _row(home, "tick", now - timedelta(hours=4), reason="pass_start")
    assert _find(home, "WL003").code == checks.VIOLATION


def test_wl003_is_suppressed_while_a_wake_is_in_flight(home):
    """A scheduler that will not start a second copy looks exactly like a dead
    clock; a live lock is what tells them apart."""
    import os

    now = clock.now_utc()
    _job(home, every="1m", last_started=now - timedelta(hours=2))
    _wide_window(home, now)
    assert _find(home, "WL003").code == checks.VIOLATION
    path = lock.lock_path(home, "j")
    path.mkdir(parents=True)
    with open(path / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {
                "wake_id": "w-live0001",
                "pass_pid": os.getpid(),
                "child_pid": 0,
                "started_at": clock.iso(now),
                "timeout_s": 300,
            },
            fh,
        )
    assert _find(home, "WL003").code == checks.OK


def test_wl003_does_not_judge_a_disabled_job(home):
    now = clock.now_utc()
    _job(home, every="1m", last_started=now - timedelta(hours=2), enabled=False)
    _wide_window(home, now)
    assert _find(home, "WL003").code == checks.UNJUDGED


# ------------------------------------------------------------------- WL004


def test_wl004_fires_on_a_lock_whose_holder_is_gone(home):
    now = clock.now_utc()
    _job(home, last_started=now)
    path = lock.lock_path(home, "j")
    path.mkdir(parents=True)
    with open(path / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {
                "wake_id": "w-dead0001",
                "pass_pid": DEAD_PID,
                "child_pid": 0,
                "started_at": clock.iso(now),
                "timeout_s": 30,
            },
            fh,
        )
    finding = _find(home, "WL004")
    assert finding.code == checks.VIOLATION and "pid_gone" in " ".join(finding.details)


def test_wl004_fires_on_a_lock_past_its_age_bound(home):
    now = clock.now_utc()
    import os

    _job(home, last_started=now)
    path = lock.lock_path(home, "j")
    path.mkdir(parents=True)
    with open(path / "wake.json", "w", encoding="utf-8", newline="\n") as fh:
        json.dump(
            {
                "wake_id": "w-old00001",
                "pass_pid": os.getpid(),
                "child_pid": 0,
                "started_at": clock.iso(now - timedelta(hours=4)),
                "timeout_s": 30,
            },
            fh,
        )
    finding = _find(home, "WL004")
    assert finding.code == checks.VIOLATION
    assert "age_exceeded" in " ".join(finding.details)


def test_wl004_is_quiet_with_no_lock_at_all(home):
    _job(home, last_started=clock.now_utc())
    assert _find(home, "WL004").code == checks.OK


# ------------------------------------------------------------- the verdict


def test_an_empty_home_is_unjudged_not_clean(home):
    assert checks.verdict(checks.run(base=home)) == checks.UNJUDGED


def test_a_violation_outranks_an_unjudged_rule(home):
    now = clock.now_utc()
    _job(home, every="1h", last_started=now)  # too young for WL003
    _row(
        home,
        "started",
        now - timedelta(hours=2),
        job="j",
        wake_id="w-open0002",
        pid=DEAD_PID,
        timeout_s=30,
    )
    findings = checks.run(base=home)
    assert _find(home, "WL003").code == checks.UNJUDGED
    assert checks.verdict(findings) == checks.VIOLATION


def test_the_module_entry_point_exits_with_the_verdict(home, capsys):
    now = clock.now_utc()
    _job(home, every="1m", last_started=now - timedelta(hours=2))
    _wide_window(home, now)
    assert checks.main([]) == checks.VIOLATION
    assert "WL003" in capsys.readouterr().out
    assert checks.main(["--json"]) == checks.VIOLATION
    payload = json.loads(capsys.readouterr().out)
    assert payload["verdict"] == "NOT OK"
    assert [f["rule"] for f in payload["findings"]] == list(checks.RULES)


def test_an_unparseable_window_is_unjudged(home, capsys):
    assert checks.main(["--since", "soon"]) == checks.UNJUDGED
    assert "UNJUDGED" in capsys.readouterr().err


def test_a_corrupt_store_is_unjudged_never_clean(home, capsys):
    (home / "jobs.json").write_text("{ not json", encoding="utf-8")
    assert checks.main([]) == checks.UNJUDGED
    assert "UNJUDGED" in capsys.readouterr().err


def test_the_checks_self_test_passes_and_covers_every_rule(capsys):
    assert checks.self_test() == checks.OK
    out = capsys.readouterr().out
    for rule in checks.RULES:
        assert rule in out, f"{rule} has no self-test case"
    assert "FAIL" not in out


def test_the_checks_verb_is_wired_into_the_cli(home, capsys):
    now = clock.now_utc()
    _job(home, every="1m", last_started=now - timedelta(hours=2))
    _wide_window(home, now)
    assert cli.main(["checks"]) == checks.VIOLATION
    assert "WL003" in capsys.readouterr().out


def test_the_checks_self_test_flag_is_not_swallowed_by_the_global_one(home, capsys):
    """`awrise checks --self-test` must run the GATE's cases, not the brick's."""
    assert cli.main(["checks", "--self-test"]) == checks.OK
    out = capsys.readouterr().out
    assert "awrise.checks self-test" in out and "WL001 fires" in out
    assert "interval_parse_accepts" not in out, "the global suite ran instead"
    # negative twin: with no verb in front of it, the flag IS the global one
    assert cli.main(["--self-test", "--list"]) == 0
    listed = capsys.readouterr().out
    assert "interval_parse_accepts_s_m_h_d_w_and_compounds" in listed
    assert "WL001 fires" not in listed


# ------------------------------------------------------------------- WL005


def test_wl005_fires_on_an_attached_job_that_outran_a_tick(home):
    """The starvation measured 2026-09-19: one long attached run holds the pass."""
    now = clock.now_utc()
    _job(home, last_started=now)
    _row(home, "finished", now, job="j", state="success", duration_s=900.0)
    finding = _find(home, "WL005")
    assert finding.code == checks.VIOLATION
    assert "detach" in " ".join(finding.details)


def test_wl005_fires_on_an_attached_job_that_timed_out(home):
    now = clock.now_utc()
    _job(home, last_started=now, timeout_s=3300)
    _row(home, "finished", now, job="j", state="timeout", reason="killed after 3300s")
    finding = _find(home, "WL005")
    assert finding.code == checks.VIOLATION
    assert "timed out" in " ".join(finding.details)


def test_wl005_is_quiet_on_a_detached_job(home):
    """detach: true closes the pass at spawn -- the whole point of the rule."""
    now = clock.now_utc()
    _job(home, last_started=now, detach=True, timeout_s=3300)
    _row(home, "finished", now, job="j", state="timeout", reason="killed after 3300s")
    assert _find(home, "WL005").code == checks.OK


def test_wl005_does_not_judge_a_ceiling(home):
    """A big timeout_s is not evidence: only an observed run is."""
    now = clock.now_utc()
    _job(home, last_started=now, timeout_s=3300)
    _row(home, "finished", now, job="j", state="success", duration_s=0.08)
    assert _find(home, "WL005").code == checks.OK


# ------------------------------------------------------------------- WL006


def test_wl006_is_unjudged_while_a_detached_wake_has_no_outcome(home):
    """Measured 2026-09-20: status said OK while awmine had exited 1 three
    times and fleet-gates sat at consecutive_failures=3."""
    now = clock.now_utc()
    _job(home, last_started=now, detach=True)
    jobs = store.load(home)
    jobs["j"]["last_state"] = "detached"
    jobs["j"]["consecutive_failures"] = 3
    store.save(jobs, home)
    finding = _find(home, "WL006")
    assert finding.code == checks.UNJUDGED
    assert "consecutive_failures=3" in " ".join(finding.details)


def test_wl006_is_quiet_once_the_outcome_is_known(home):
    now = clock.now_utc()
    _job(home, last_started=now, detach=True)
    assert _find(home, "WL006").code == checks.OK


def test_wl006_ignores_an_attached_job(home):
    """An attached job's exit code IS in the row -- WL006 has nothing to say."""
    now = clock.now_utc()
    _job(home, last_started=now)
    jobs = store.load(home)
    jobs["j"]["last_state"] = "detached"
    store.save(jobs, home)
    assert _find(home, "WL006").code == checks.OK


def test_an_unjudged_detached_wake_is_never_a_pass(home):
    now = clock.now_utc()
    _job(home, last_started=now, detach=True)
    jobs = store.load(home)
    jobs["j"]["last_state"] = "detached"
    store.save(jobs, home)
    assert checks.verdict(checks.run(base=home)) != checks.OK


# ----------------------------------------------- WL006 reads the receipt


def _detached_with_receipt(home, tmp_path, payload, *, age_s: float = 0.0):
    """A detached job whose child left `payload` in a receipt `age_s` old."""
    import os
    now = clock.now_utc()
    rec = tmp_path / "receipt.json"
    rec.write_text(json.dumps(payload), encoding="utf-8")
    if age_s:
        stamp = rec.stat().st_mtime - age_s
        os.utime(rec, (stamp, stamp))
    _job(home, last_started=now, detach=True, receipt=str(rec))
    jobs = store.load(home)
    jobs["j"]["last_state"] = "detached"
    store.save(jobs, home)
    return rec


def test_wl006_closes_a_detached_wake_from_its_own_receipt(home, tmp_path):
    """The whole point of declaring a receipt: stop abstaining."""
    _detached_with_receipt(home, tmp_path, {"exit_code": 0})
    finding = _find(home, "WL006")
    assert finding.code == checks.OK
    assert "exit_code=0" in " ".join(finding.details)


def test_wl006_is_a_violation_when_the_receipt_reports_failure(home, tmp_path):
    """A job that declared a receipt gets to say it FAILED -- that outranks
    the abstention, or declaring one would only ever soften the verdict."""
    _detached_with_receipt(home, tmp_path, {"exit_code": 1})
    finding = _find(home, "WL006")
    assert finding.code == checks.VIOLATION
    assert "exit_code=1" in " ".join(finding.details)


def test_wl006_refuses_a_receipt_older_than_the_wake_it_would_judge(home, tmp_path):
    """The likeliest way this check could lie: last run's verdict wearing this
    run's name. An hour-old receipt for a wake that started now is not evidence."""
    _detached_with_receipt(home, tmp_path, {"exit_code": 0}, age_s=3600)
    finding = _find(home, "WL006")
    assert finding.code == checks.UNJUDGED
    assert "stale" in " ".join(finding.details)


def test_wl006_refuses_a_receipt_with_no_exit_code(home, tmp_path):
    """awmine's own receipt was exactly this shape: full of counts, silent on
    whether the run passed."""
    _detached_with_receipt(home, tmp_path, {"residual_hits": 41})
    assert _find(home, "WL006").code == checks.UNJUDGED


def test_wl006_says_how_to_declare_a_receipt_when_none_is_set(home):
    now = clock.now_utc()
    _job(home, last_started=now, detach=True)
    jobs = store.load(home)
    jobs["j"]["last_state"] = "detached"
    store.save(jobs, home)
    assert "receipt=" in " ".join(_find(home, "WL006").details)
