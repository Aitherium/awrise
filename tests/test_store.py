"""The store never loses a job: atomic writes, loud corruption, v1 migration."""

import argparse
import json
import os
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from awrise import cli, clock, ledger, store


def _add(name, every="15m", run="echo hi", **extra):
    args = argparse.Namespace(
        name=name,
        every=every,
        run=run,
        timeout=None,
        cwd=None,
        at=None,
        allow_overrun=False,
        disabled=False,
        **extra,
    )
    return cli._dispatch(cli.cmd_add, args)


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    return tmp_path


def test_save_is_tmp_plus_replace_and_keeps_bak(home):
    assert _add("a") == 0
    # The first write has no PREVIOUS version, so it is its own backup: a
    # store with no .bak has nothing to restore, and the advertised recovery
    # would be a dead end for as long as it stayed that way.
    first = json.loads((home / "jobs.json.bak").read_text(encoding="utf-8"))
    assert set(first["jobs"]) == {"a"}
    assert _add("b") == 0
    bak = json.loads((home / "jobs.json.bak").read_text(encoding="utf-8"))
    assert set(bak["jobs"]) == {"a"}, "the .bak is the version before this one"
    current = json.loads((home / "jobs.json").read_text(encoding="utf-8"))
    assert set(current["jobs"]) == {"a", "b"} and current["schema"] == store.SCHEMA
    assert not list(home.glob("jobs.json.tmp-*"))


def test_a_store_corrupted_after_its_first_save_is_restorable(home):
    """The recovery the exit-2 message names has to work from the first job
    on. Without a .bak every verb exits 2 forever, pointing at a restore that
    exits 2 too, and the only way out is deleting a file by hand."""
    assert _add("a") == 0
    (home / "jobs.json").write_text('{"schema": 2, "jobs": {"a": ', encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)
    store.restore(home)
    assert set(store.load(home)) == {"a"}
    assert not list(home.glob("jobs.json.corrupt-*")), "the corrupt copy is parked"


def test_reset_is_the_last_resort_and_refuses_a_readable_store(home):
    """A store corrupted before it ever had a .bak (one written by an older
    awrise) still needs a way out that is not "delete this file yourself"."""
    assert _add("a") == 0
    (home / "jobs.json.bak").unlink()
    (home / "jobs.json").write_text('{"schema": 2, "jobs": {"a": ', encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)
    with pytest.raises(store.StoreError):
        store.restore(home)
    store.reset(home)
    assert store.load(home) == {}
    parked = list((home / "corrupt").glob("jobs.json.corrupt-*"))
    assert len(parked) == 1, parked
    # negative twin: it will not do that to a store it can read, with or
    # without a .bak beside it
    assert _add("b") == 0
    (home / "jobs.json.bak").unlink()
    with pytest.raises(store.StoreError, match="readable"):
        store.reset(home)
    assert set(store.load(home)) == {"b"}


def test_crash_mid_save_keeps_jobs(home, monkeypatch):
    assert _add("keep") == 0
    before = (home / "jobs.json").read_bytes()
    jobs = store.load(home)
    jobs["keep"]["run"] = "echo changed"

    def boom(src, dst):
        raise OSError("simulated crash")

    monkeypatch.setattr(store, "_replace", boom)
    with pytest.raises(store.StoreError):
        store.save(jobs, home)
    monkeypatch.undo()
    assert (home / "jobs.json").read_bytes() == before
    assert store.load(home)["keep"]["run"] == "echo hi"
    assert not list(home.glob("jobs.json.tmp-*")), "the failed tmp is removed"


def test_corrupt_store_exits_2_never_empty_dict(home):
    assert _add("a") == 0
    assert _add("b") == 0
    (home / "jobs.json").write_text("{definitely not json", encoding="utf-8")
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
    corrupt = list(home.glob("jobs.json.corrupt-*"))
    assert len(corrupt) == 1 and corrupt[0].read_text(encoding="utf-8").startswith("{definitely")
    # a second read is STILL refused: the corrupt store is not replaced by {}
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) == 2
    assert _add("c") == 2, "add cannot quietly start a fresh store over a corrupt one"


def test_reconcile_restore_returns_the_bak(home):
    assert _add("a") == 0
    assert _add("b") == 0
    (home / "jobs.json").write_text("{corrupt", encoding="utf-8")
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=True)) == 0
    assert set(store.load(home)) == {"a"}, "the .bak held the previous save"
    assert not list(home.glob("jobs.json.corrupt-*"))
    assert list((home / "corrupt").glob("jobs.json.corrupt-*")), "parked, not deleted"
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 0


def test_restore_without_bak_exits_2(home):
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=True)) == 2


def test_restore_refuses_a_corrupt_bak(home):
    assert _add("a") == 0
    (home / "jobs.json.bak").write_text("nope", encoding="utf-8")
    assert cli._dispatch(cli.cmd_reconcile, argparse.Namespace(restore=True)) == 2
    assert set(store.load(home)) == {"a"}, "the live store is untouched"


def test_unknown_schema_is_refused(home):
    (home / "jobs.json").write_text(json.dumps({"schema": 99, "jobs": {}}), encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)


def test_v1_store_migrates_in_memory_and_writes_back_once(home):
    v1 = {
        "old": {
            "interval": "3600.0",
            "command": "echo v1",
            "last_run": "2026-01-02T03:04:05.000001",
            "last_status": "failure",
        },
        "fresh": {"interval": "60", "command": "echo new", "last_run": None, "last_status": None},
    }
    (home / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
    jobs = store.load(home)
    assert jobs["old"]["every"] == "1h" and jobs["old"]["interval_s"] == 3600.0
    assert jobs["old"]["run"] == "echo v1"
    assert jobs["old"]["last_started_at"] == "2026-01-02T03:04:05.000001+00:00"
    assert jobs["old"]["last_state"] == "failure"
    assert jobs["fresh"]["every"] == "1m" and jobs["fresh"]["last_started_at"] is None
    assert jobs["fresh"]["timeout_s"] == 300 and jobs["fresh"]["enabled"] is True
    assert json.loads((home / "jobs.json.v1.bak").read_text(encoding="utf-8")) == v1
    written = json.loads((home / "jobs.json").read_text(encoding="utf-8"))
    assert written["schema"] == store.SCHEMA and set(written["jobs"]) == {"old", "fresh"}
    # a naive v1 stamp no longer raises against an aware now
    assert not clock.is_due(jobs["old"], clock.parse_ts("2026-01-02T03:30:00+00:00"))
    assert clock.is_due(jobs["old"], clock.parse_ts("2026-01-02T04:04:06+00:00"))


def _write_v2(home, jobs):
    (home / "jobs.json").write_text(json.dumps({"schema": 2, "jobs": jobs}), encoding="utf-8")


def _good():
    return {
        "every": "1h",
        "interval_s": 3600.0,
        "run": "echo hi",
        "timeout_s": 300,
        "enabled": True,
        "cwd": None,
        "at": None,
        "last_wake_id": None,
        "last_started_at": None,
        "last_finished_at": None,
        "last_state": None,
        "last_reason": None,
        "consecutive_failures": 0,
        "created_at": "2026-01-02T03:04:05.000000+00:00",
        "updated_at": "2026-01-02T03:04:05.000000+00:00",
    }


@pytest.mark.parametrize(
    "field,value",
    [
        (None, 5),  # record is not an object
        ("last_started_at", "garbage"),
        ("last_finished_at", 12345),
        ("updated_at", "2026-13-45"),
        ("timeout_s", "abc"),
        ("timeout_s", 0),
        ("timeout_s", True),
        ("interval_s", "abc"),
        ("interval_s", 60.0),  # disagrees with every=1h
        ("every", "5x"),
        ("every", 3600),
        ("run", None),
        ("enabled", "yes"),
        ("cwd", 7),
        ("at", "25:00"),
        ("last_state", "kinda_ok"),
        ("consecutive_failures", -1),
        ("consecutive_failures", "2"),
        ("last_reason", 3),
        ("colour", "red"),  # unknown key: a knob nobody reads
    ],
)
def test_load_refuses_a_malformed_record_with_exit_2(home, field, value):
    job = _good()
    if field is None:
        job = value
    else:
        job[field] = value
    _write_v2(home, {"x": job})
    before = (home / "jobs.json").read_bytes()
    with pytest.raises(store.StoreError) as info:
        store.load(home)
    assert "'x'" in str(info.value)
    if field:
        assert field in str(info.value), str(info.value)
    assert (home / "jobs.json").read_bytes() == before, "valid JSON is never moved aside"
    assert not list(home.glob("jobs.json.corrupt-*"))
    # every verb answers 2, not a traceback
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 2
    assert cli._dispatch(cli.cmd_run_due, argparse.Namespace(quiet=True, invoker="t")) == 2
    assert cli._dispatch(cli.cmd_status, argparse.Namespace()) == 2
    # The pass's own tick rows are written (a clock firing into a broken store
    # is not a stopped clock), but nothing claims the JOB did anything.
    events = [row.get("event") for row in ledger.read(home)]
    assert set(events) <= {"tick", "tick_end"}, (
        f"no row about a job that cannot load; got {sorted(set(events))}"
    )


@pytest.mark.parametrize("name", ["..", "sub/dir", "CON", "a b", "x" * 65, ""])
def test_load_refuses_a_record_whose_name_is_outside_the_grammar(home, name):
    _write_v2(home, {name: _good()})
    with pytest.raises(store.StoreError) as info:
        store.load(home)
    assert "job name" in str(info.value)


def test_load_fills_defaults_for_a_minimal_hand_written_record(home):
    _write_v2(home, {"min": {"every": "15m", "interval_s": 900, "run": "echo hi"}})
    job = store.load(home)["min"]
    assert job["timeout_s"] == 300 and job["enabled"] is True and job["interval_s"] == 900.0
    assert job["last_state"] is None and job["consecutive_failures"] == 0
    assert cli._dispatch(cli.cmd_list, argparse.Namespace(json=False)) == 0


@pytest.mark.parametrize("interval", ["5m", "abc", None, "0", 0, -60])
def test_v1_record_with_an_unusable_interval_is_refused(home, interval):
    v1 = {"x": {"interval": interval, "command": "echo", "last_run": None, "last_status": None}}
    (home / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
    with pytest.raises(store.StoreError) as info:
        store.load(home)
    assert "interval" in str(info.value)
    assert not (home / "jobs.json.v1.bak").exists(), "nothing migrated"


def test_v1_unknown_last_status_is_refused(home):
    v1 = {"x": {"interval": "60", "command": "echo", "last_run": None, "last_status": "meh"}}
    (home / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)


# ------------------------------------------------- readers vs os.replace


def test_save_succeeds_while_another_awrise_holds_the_store_open(home, monkeypatch):
    """One pass reading jobs.json must not make another pass's replace fail.
    Measured on Windows: a plain open() shares no DELETE, and the pass that
    lost the race had already run its job -- the class of double fire the
    pinned write order exists to prevent. The holder keeps ONE handle from
    awrise's own reader open for the whole test, so the retry cannot mask a
    reader that stopped sharing DELETE."""
    assert _add("a") == 0
    monkeypatch.setattr(store, "REPLACE_BUDGET_S", 0.3)
    script = (
        "import sys, time\n"
        "from awrise import store\n"
        "from pathlib import Path\n"
        "fh = store.open_shared_read(Path(sys.argv[1]) / 'jobs.json')\n"
        "print('ready', flush=True)\n"
        "time.sleep(15)\n"
        "fh.close()\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(home)],
        cwd=str(Path(store.__file__).resolve().parents[1]),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert holder.stdout.readline().strip() == b"ready"
        t0 = time.monotonic()
        for i in range(5):
            assert _add(f"b{i}") == 0, "the replace must succeed past a DELETE-sharing reader"
        assert time.monotonic() - t0 < 1.0, "and without waiting on the retry"
        with store.open_shared_read(home / "jobs.json") as mine:
            assert _add("c") == 0, "the same holds for a handle in this process"
            assert b'"a"' in mine.read(), "the reader keeps the bytes it opened"
    finally:
        holder.kill()
        holder.communicate()
    assert len(store.load(home)) == 7


def test_save_waits_out_a_transient_foreign_reader(home, monkeypatch):
    """An editor or an indexer holding jobs.json for a moment is retried past;
    a hold longer than the budget is a StoreError (exit 2), never a lost edit."""
    assert _add("a") == 0
    path = home / "jobs.json"
    released = threading.Event()

    def hold(seconds):
        with open(path, "r", encoding="utf-8") as fh:  # a foreign reader: no DELETE sharing
            fh.read()
            time.sleep(seconds)
        released.set()

    t = threading.Thread(target=hold, args=(0.4,))
    t.start()
    time.sleep(0.05)
    assert _add("b") == 0, "a 0.4s hold is inside the retry budget"
    t.join()
    assert set(store.load(home)) == {"a", "b"}
    calls = {"n": 0}

    def always_denied(src, dst):
        calls["n"] += 1
        raise PermissionError(13, "Access is denied")

    monkeypatch.setattr(store, "_replace", always_denied)
    t0 = time.monotonic()
    with pytest.raises(PermissionError):
        store._replace_retrying(path, path, budget_s=0.3)
    assert 0.25 < time.monotonic() - t0 < 3 and calls["n"] > 1
    # the tmp of a failed save is removed and the store is intact
    monkeypatch.setattr(store, "REPLACE_BUDGET_S", 0.2)
    jobs = store.load(home)
    jobs["a"]["run"] = "echo changed"
    with pytest.raises(store.StoreError):
        store.save(jobs, home)
    monkeypatch.undo()
    monkeypatch.setenv("AWRISE_HOME", str(home))
    assert not list(home.glob("jobs.json.tmp-*"))
    assert store.load(home)["a"]["run"] == "echo hi"


def test_reload_merge_carries_only_touched_state(home):
    assert _add("a") == 0
    assert _add("b") == 0
    mine = store.load(home)
    # a concurrent operator edit lands on disk after we loaded
    theirs = store.load(home)
    theirs["b"]["run"] = "echo edited"
    del theirs["a"]
    theirs["c"] = store.new_job("5m", "echo c")
    store.save(theirs, home)
    mine["b"]["last_state"] = "success"
    mine["b"]["last_started_at"] = clock.iso(clock.now_utc())
    mine["a"]["last_state"] = "success"
    merged = store.reload_merge(mine, ["a", "b"], home)
    assert "a" not in merged, "a removal on disk wins over stale memory"
    assert "c" in merged, "a concurrent add survives"
    assert merged["b"]["run"] == "echo edited", "their spec edit survives"
    assert merged["b"]["last_state"] == "success", "our state stamp lands"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX mode bits are inert on NTFS")
def test_group_writable_home_is_refused(home):
    assert _add("a") == 0
    os.chmod(home, stat.S_IRWXU | stat.S_IWGRP | stat.S_IXGRP | stat.S_IRGRP)
    with pytest.raises(store.StoreError):
        store.load(home)
    os.chmod(home, stat.S_IRWXU)
    assert set(store.load(home)) == {"a"}


@pytest.mark.skipif(sys.platform != "win32", reason="Windows owner check")
def test_windows_owner_probe_judges_own_files(home):
    assert _add("a") == 0
    assert store._windows_owned_by_me(home / "jobs.json") is True
    assert store.perms_problems(home) == []


# ------------------------------------------------- recovery from a wedged store
#
# Review findings (2026-09-19), both measured:
#
# 1. A 0.1.0 store holding a job name outside the 0.2.0 grammar was an
#    unrecoverable exit-2 brick. 0.1.0's `cmd_add` validated `--name` not at
#    all, so `"my backup"` is a realistic v1 key; the migration then refused it
#    and BOTH documented escapes refused too -- `--restore` because a v1 store
#    never wrote a `.bak`, and `--reset` because "jobs.json is readable" (the v1
#    file IS valid JSON). `awrise remove`, the fix the reset message named, also
#    exited 2 because it loads the store first. Every verb was dead forever and
#    the only way out was editing the file by hand.
# 2. With jobs.json AND jobs.json.bak both damaged -- the exact state `reset`
#    documents itself as the last way out of -- `restore` refused because the
#    .bak is unparseable and `reset` refused because the .bak EXISTS.


def test_a_v1_name_outside_the_grammar_is_migrated_not_bricked(home):
    v1 = {
        "my backup": {
            "interval": "3600.0",
            "command": "echo hi",
            "last_run": None,
            "last_status": None,
        },
        "keep": {"interval": "60", "command": "echo keep", "last_run": None, "last_status": None},
    }
    (home / "jobs.json").write_text(json.dumps(v1), encoding="utf-8")
    jobs = store.load(home)
    assert set(jobs) == {"my-backup", "keep"}, jobs
    assert jobs["my-backup"]["run"] == "echo hi"
    # The original is kept byte-for-byte and the rename is SAID, not silent.
    assert json.loads((home / "jobs.json.v1.bak").read_text(encoding="utf-8")) == v1
    assert any("my backup" in note and "my-backup" in note for note in store.MIGRATION_NOTES), (
        store.MIGRATION_NOTES
    )
    # And the store is usable: a second load is a clean v2 read.
    store.MIGRATION_NOTES.clear()
    assert set(store.load(home)) == {"my-backup", "keep"}
    assert store.MIGRATION_NOTES == []


@pytest.mark.parametrize(
    "bad,expected",
    [
        ("my backup", "my-backup"),
        ("trailing.", "trailing"),
        ("CON", "job-CON"),
        ("..weird..", "weird"),
        ("x" * 80, "x" * 64),
        ("sub/dir", "sub-dir"),
    ],
)
def test_every_v1_name_shape_becomes_a_name_this_version_accepts(home, bad, expected):
    (home / "jobs.json").write_text(
        json.dumps({bad: {"interval": "60", "command": "echo hi"}}), encoding="utf-8"
    )
    jobs = store.load(home)
    assert list(jobs) == [expected], jobs
    store.validate_name(expected)


def test_two_v1_names_that_sanitise_alike_stay_two_jobs(home):
    (home / "jobs.json").write_text(
        json.dumps(
            {
                "my backup": {"interval": "60", "command": "echo one"},
                "my/backup": {"interval": "60", "command": "echo two"},
            }
        ),
        encoding="utf-8",
    )
    jobs = store.load(home)
    assert len(jobs) == 2, jobs
    assert sorted(job["run"] for job in jobs.values()) == ["echo one", "echo two"]
    for name in jobs:
        store.validate_name(name)


def test_reset_is_the_way_out_of_a_store_that_is_json_but_not_a_store(home):
    """`reset` refused while the file merely PARSED. The guard is about not
    losing a working set of jobs, and a store no verb can load is not one."""
    (home / "jobs.json").write_text(
        json.dumps({"schema": 2, "jobs": {"my backup": _good()}}), encoding="utf-8"
    )
    with pytest.raises(store.StoreError):
        store.load(home)
    store.reset(home)
    assert store.load(home) == {}
    parked = sorted((home / "corrupt").glob("jobs.json.corrupt-*"))
    assert parked, "the bytes are parked, never removed"
    assert json.loads(parked[-1].read_text(encoding="utf-8"))["jobs"], parked[-1]


def test_reset_still_refuses_while_the_store_really_loads(home):
    """The negative twin: reset may never be the command that loses jobs."""
    assert _add("a") == 0
    with pytest.raises(store.StoreError) as info:
        store.reset(home)
    assert "readable" in str(info.value) or "loads" in str(info.value)
    assert set(store.load(home)) == {"a"}


def test_a_damaged_store_and_a_damaged_backup_are_not_a_deadlock(home):
    assert _add("j") == 0
    assert _add("k") == 0
    (home / "jobs.json.bak").write_text('{"schema": 2, "jobs": {"j', encoding="utf-8")
    (home / "jobs.json").write_text("not json at all", encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)  # parks jobs.json as jobs.json.corrupt-<ts>
    with pytest.raises(store.StoreError) as info:
        store.restore(home)
    assert "nothing safe to restore" in str(info.value)
    # `reset` is what the restore message points at, so it must WORK here.
    store.reset(home)
    assert store.load(home) == {}
    parked = sorted(path.name for path in (home / "corrupt").iterdir())
    assert any(name.startswith("jobs.json.bak") for name in parked), parked
    assert any("corrupt-" in name for name in parked), parked


def test_reset_still_refuses_while_the_backup_can_be_restored(home):
    """The negative twin: a GOOD .bak still sends the operator to --restore."""
    assert _add("j") == 0
    assert _add("k") == 0
    (home / "jobs.json").write_text("not json at all", encoding="utf-8")
    with pytest.raises(store.StoreError):
        store.load(home)
    with pytest.raises(store.StoreError) as info:
        store.reset(home)
    assert "restore" in str(info.value)
    store.restore(home)
    # The .bak is the copy made BEFORE the last save, so it holds the store as
    # it was one job ago -- that is what restore has always meant here.
    assert set(store.load(home)) == {"j"}


def test_the_backup_copy_is_atomic(home):
    """A truncate-and-write `_copy` left a half-written .bak whenever it failed
    mid-way, which is how the deadlock above is REACHED."""
    assert _add("j") == 0
    bak = home / "jobs.json.bak"
    good = bak.read_bytes()
    calls = []
    real_replace = store.replace_file

    def boom(src, dst):
        calls.append((src, dst))
        if dst == bak:
            raise OSError("no space left on device")
        return real_replace(src, dst)

    store.replace_file = boom
    try:
        with pytest.raises(store.StoreError):
            store.save({**store.load(home), "k": _good()}, home)
    finally:
        store.replace_file = real_replace
    assert bak.read_bytes() == good, "a failed backup copy must not truncate the .bak"
