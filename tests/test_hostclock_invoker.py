"""Every adapter's OWN invoker must be one the freshness judge counts.

Review finding (2026-09-19): ``SCHEDULED_INVOKERS`` was ``frozenset(KINDS)``,
and ``check()`` / ``_doctor.report()`` judge clock liveness only from ticks
whose invoker is in that set. But ``_render_systemd`` builds its ExecStart with
``_run_due_argv(ctx, "systemd")`` -- one ``awrise.service`` is shared by both
systemd adapters -- so every real systemd tick is stamped ``systemd``, which was
NOT in the set. cron, launchd and schtasks matched; both systemd adapters did
not, and a Linux host whose timer fired correctly was reported as a measured NO.

The assertion below is derived from what the adapters WRITE rather than from a
list anyone maintains, so a new adapter (or a renamed invoker) cannot repeat it.
"""

import json
import os
import re
from pathlib import Path

import pytest
from awrise import clock, hostclock, ledger, store


@pytest.fixture
def home(tmp_path, monkeypatch) -> Path:
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path / "home"))
    return store.home()


def _ctx(kind: str) -> hostclock.Context:
    sep = "\\" if kind == "schtasks" else "/"
    return hostclock.Context(
        python="py",
        home=sep.join(("h", "awrise")),
        bin_dir=sep.join(("h", "awrise", "bin")),
        log_path=sep.join(("h", "awrise", "logs", "awrise.log")),
        every_s=60,
        user="someone",
        version="0.0.0",
        sep=sep,
    )


def _invokers_in(text: str) -> list:
    """Every ``--invoker <x>`` in a rendered artifact, whatever shape it has.

    The launchd plist puts each argv word in its own ``<string>`` element and
    the schtasks payload quotes them, so the tags and quotes come out first --
    a regex over the raw text answers "no invoker at all" for launchd, which
    would make this assertion vacuous exactly where it matters.
    """
    flat = re.sub(r"</?[A-Za-z][^>]*>", " ", text).replace('"', " ")
    return re.findall(r"--invoker\s+([A-Za-z0-9._-]+)", flat)


@pytest.mark.parametrize("kind", hostclock.KINDS)
def test_every_adapter_stamps_an_invoker_the_judge_counts_as_scheduled(kind):
    found = []
    for text in hostclock.render(kind, _ctx(kind)).values():
        found.extend(_invokers_in(text))
    assert found, f"the {kind} render names no --invoker at all"
    unknown = sorted(set(found) - hostclock.SCHEDULED_INVOKERS)
    assert not unknown, (
        f"{kind} stamps {unknown}, which hostclock.check() does not count as a "
        f"scheduled tick -- a correctly firing clock would be reported dead"
    )


def test_a_hand_run_pass_is_still_not_a_scheduled_tick():
    """The negative twin: the set is not simply everything."""
    assert "manual" not in hostclock.SCHEDULED_INVOKERS
    assert "selftest" not in hostclock.SCHEDULED_INVOKERS
    assert "foreign" not in hostclock.SCHEDULED_INVOKERS


@pytest.mark.parametrize("kind", ("systemd-user", "systemd-system"))
def test_a_ticking_systemd_host_is_judged_ok(home, monkeypatch, kind):
    """The whole path: a record, its payloads, a live probe and ticks stamped
    exactly as the rendered unit stamps them."""
    # `systemd-system` targets an ABSOLUTE path, so without this the test writes a
    # REAL unit file: on Windows it left C:\etc\systemd\system\awrise.service
    # behind (found 2026-09-20), and on a root Linux box it installs onto the host.
    units = home.parent / "etc-systemd-system"
    units.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(hostclock.SYSTEMD_SYSTEM_DIR_ENV, str(units))
    ctx = hostclock.context(kind, home)
    artifacts = hostclock.render(kind, ctx)
    hostclock._write_payloads(kind, ctx, artifacts)
    hostclock.write_record(kind, ctx, artifacts, base=home)
    monkeypatch.setattr(
        hostclock,
        "probe",
        lambda k, c: hostclock.Definition(True, True, "awrise.timer enabled, next in 55s"),
    )
    stamped = _invokers_in(artifacts["awrise.service"])
    assert stamped, artifacts["awrise.service"]
    for ago in (600, 300, 60):
        ledger.append(
            home,
            {
                "event": "tick",
                "reason": "pass_start",
                "invoker": stamped[0],
                "ts": clock.iso(clock.now_utc() - __import__("datetime").timedelta(seconds=ago)),
            },
        )
    code, lines = hostclock.check(base=home)
    assert code == 0, lines
    assert any("last tick" in line for line in lines), lines


def test_a_record_written_before_this_fix_is_still_judged_by_its_own_ticks(home, monkeypatch):
    """Compatibility: a unit installed by an earlier 0.2.0 stamps ``systemd``
    and must not go red the moment awrise is upgraded."""
    kind = "systemd-user"
    ctx = hostclock.context(kind, home)
    artifacts = hostclock.render(kind, ctx)
    hostclock._write_payloads(kind, ctx, artifacts)
    hostclock.write_record(kind, ctx, artifacts, base=home)
    monkeypatch.setattr(
        hostclock, "probe", lambda k, c: hostclock.Definition(True, True, "registered")
    )
    ledger.append(home, {"event": "tick", "reason": "pass_start", "invoker": "systemd"})
    assert hostclock.check(base=home)[0] == 0
    assert json.loads(hostclock.record_path(home).read_text(encoding="utf-8"))["kind"] == kind


def test_a_detached_child_does_not_excuse_a_stopped_clock(home, monkeypatch):
    """`check` excuses a missing tick when a PASS is in progress. A detached
    child holds its job's lock for as long as it runs and no pass is behind it,
    so counting it would let one long-lived child mask a dead host clock."""
    from awrise import lock

    kind = "systemd-user"
    ctx = hostclock.context(kind, home)
    artifacts = hostclock.render(kind, ctx)
    hostclock._write_payloads(kind, ctx, artifacts)
    record = hostclock.write_record(kind, ctx, artifacts, base=home)
    record["installed_at"] = "2020-01-01T00:00:00+00:00"
    hostclock.record_path(home).write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setattr(
        hostclock, "probe", lambda k, c: hostclock.Definition(True, True, "registered")
    )
    store.save({"d": store.new_job(every="1h", run="echo hi", interval_s=3600.0)}, home)
    handle = lock.acquire(home, "d", {"wake_id": "w-1", "pass_id": "p-1"}, 300)
    handle.note_child(os.getpid())
    handle.mark_detached()
    try:
        assert lock.inspect(home, "d", 300).detached is True
        assert hostclock.live_locks(home) == [], "a detached child is not a pass in progress"
        code, lines = hostclock.check(base=home)
        assert code == 1, lines
        assert any("not running" in line for line in lines), lines
        # The negative twin: an ordinary held lock still excuses the gap.
        handle.holder["detached"] = False
        handle._rewrite_if_ours()
        assert hostclock.live_locks(home) == ["d"]
        code, lines = hostclock.check(base=home)
        assert code == 0, lines
        assert any("a pass is in progress" in line for line in lines), lines
    finally:
        handle.holder["detached"] = False
        handle.release()
