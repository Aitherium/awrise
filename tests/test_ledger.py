"""The writer only says what the code measured; the reader shows every row."""

import json
import subprocess
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest
from awrise import ledger

PKG = Path(__file__).resolve().parent.parent


def test_concurrent_appends_from_processes_lose_nothing(tmp_path):
    """Two passes append at the same instant. Measured before the fix on
    Windows: 4 processes x 40 rows lost 10-22 rows per trial with every
    writer reporting success (the CRT's O_APPEND seeks, then writes)."""
    procs, per_proc = 4, 40
    script = (
        "import sys, time\n"
        "from pathlib import Path\n"
        "from awrise import ledger\n"
        "base, start, n, tag = Path(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3]), "
        "sys.argv[4]\n"
        "while time.time() < start:\n"
        "    pass\n"
        "for i in range(n):\n"
        "    ledger.append(base, {'event': 'finished', 'state': 'success', 'job': 'j',\n"
        "                         'reason': f'{tag}-{i}'})\n"
    )
    start = time.time() + 1.5
    children = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(tmp_path), str(start), str(per_proc), f"p{i}"],
            cwd=str(PKG),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for i in range(procs)
    ]
    for child in children:
        _out, err = child.communicate(timeout=120)
        assert child.returncode == 0, err.decode("utf-8", "replace")
    files = list((tmp_path / "ledger").glob("*.jsonl"))
    lines = [ln for f in files for ln in f.read_bytes().split(b"\n") if ln]
    assert len(lines) == procs * per_proc, f"lost {procs * per_proc - len(lines)} rows"
    reasons = {json.loads(ln)["reason"] for ln in lines}
    assert len(reasons) == procs * per_proc, "every row intact, none interleaved"
    assert not [r for r in ledger.read(tmp_path) if r["event"] == "unreadable"]


def test_concurrent_appends_from_threads_lose_nothing(tmp_path):
    threads, per_thread = 8, 50
    errors = []

    def writer(tag):
        try:
            for i in range(per_thread):
                ledger.append(
                    tmp_path,
                    {"event": "finished", "state": "success", "job": "j", "reason": f"{tag}-{i}"},
                )
        except Exception as exc:  # noqa: BLE001 - reported below
            errors.append(exc)

    pool = [threading.Thread(target=writer, args=(f"t{i}",)) for i in range(threads)]
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    assert not errors
    rows = ledger.read(tmp_path)
    assert len(rows) == threads * per_thread
    assert len({r["reason"] for r in rows}) == threads * per_thread


def test_writer_refuses_unknown_event(tmp_path):
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(tmp_path, {"event": "exploded", "job": "j", "reason": "x"})
    assert not list(tmp_path.glob("ledger/*.jsonl"))


def test_writer_refuses_unknown_state(tmp_path):
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(
            tmp_path, {"event": "finished", "state": "mostly_fine", "job": "j", "reason": "x"}
        )


def test_writer_refuses_empty_reason_and_finished_without_state(tmp_path):
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(
            tmp_path, {"event": "finished", "state": "success", "job": "j", "reason": "   "}
        )
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(tmp_path, {"event": "finished", "job": "j", "reason": "x"})
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(tmp_path, {"event": "started", "reason": "no job named"})


def test_writer_accepts_every_closed_state_and_fills_the_envelope(tmp_path):
    for state in sorted(ledger.STATES):
        wake = ledger.append(
            tmp_path, {"event": "finished", "state": state, "job": "j", "reason": "ok"}
        )
        assert wake.startswith("w-") and len(wake) == 10
        assert all(ch in ledger.ALPHABET for ch in wake[2:])
    rows = ledger.read(tmp_path)
    assert sorted(r["state"] for r in rows) == sorted(ledger.STATES)
    for row in rows:
        assert row["schema"] == ledger.SCHEMA
        assert row["ts"].endswith("+00:00")
        assert row["pid"] and row["host"] and row["interpreter"]
        assert row["invoker"] == "manual"


def test_rows_are_one_json_object_per_line_in_a_daily_file(tmp_path):
    ledger.append(tmp_path, {"event": "finished", "state": "success", "job": "j", "reason": "a"})
    ledger.append(tmp_path, {"event": "finished", "state": "failure", "job": "j", "reason": "b"})
    files = list((tmp_path / "ledger").glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_bytes().split(b"\n")
    assert lines[-1] == b"" and len(lines) == 3, "LF-terminated, no CRLF"
    assert [json.loads(line)["reason"] for line in lines[:2]] == ["a", "b"]


def test_output_tails_are_redacted_before_the_append(tmp_path):
    ledger.append(
        tmp_path,
        {
            "event": "finished",
            "state": "failure",
            "job": "j",
            "reason": "x",
            "stdout_tail": "key sk-abcdefghijklmnop end",
            "stderr_tail": "Authorization: Bearer abcdefghijklmnop",
            "note": "AKIAIOSFODNN7EXAMPLE xoxb-1234567890-abc ghp_abcdefghijk",
        },
    )
    raw = next((tmp_path / "ledger").glob("*.jsonl")).read_text(encoding="utf-8")
    for secret in (
        "sk-abcdefghijklmnop",
        "Bearer abcdefghijklmnop",
        "AKIAIOSFODNN7EXAMPLE",
        "xoxb-1234567890-abc",
        "ghp_abcdefghijk",
    ):
        assert secret not in raw
    assert raw.count("<redacted:") == 5


def test_tail_keeps_the_last_chars_and_decodes_leniently():
    data = b"x" * 3000 + b"\xff tail aither_sk_live_0123456789"
    out = ledger.tail(data, limit=64)
    assert len(out) <= 64 and out.endswith("<redacted:aither_sk>")
    assert ledger.tail(None) == "" and ledger.tail(b"") == ""


def test_reader_reports_a_malformed_line_instead_of_dropping_it(tmp_path):
    ledger.append(tmp_path, {"event": "finished", "state": "success", "job": "j", "reason": "ok"})
    path = next((tmp_path / "ledger").glob("*.jsonl"))
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write("garbage line\n")
    rows = ledger.read(tmp_path)
    assert len(rows) == 2 and rows[1]["event"] == "unreadable"
    assert "is not JSON" in rows[1]["reason"]


def test_open_wakes_pairs_started_with_finished(tmp_path):
    a = ledger.append(tmp_path, {"event": "started", "job": "j", "reason": "due"})
    b = ledger.append(tmp_path, {"event": "started", "job": "k", "reason": "due"})
    ledger.append(
        tmp_path,
        {"wake_id": a, "event": "finished", "state": "success", "job": "j", "reason": "exit_0"},
    )
    opens = ledger.open_wakes(ledger.read(tmp_path))
    assert set(opens) == {b}


def test_read_filters_by_job_event_and_since(tmp_path):
    ledger.append(tmp_path, {"event": "started", "job": "j", "reason": "due"})
    ledger.append(tmp_path, {"event": "finished", "state": "success", "job": "j", "reason": "ok"})
    ledger.append(tmp_path, {"event": "removed", "job": "k", "reason": "operator_remove"})
    assert len(ledger.read(tmp_path, job="j")) == 2
    assert [r["job"] for r in ledger.read(tmp_path, event="removed")] == ["k"]
    assert len(ledger.read(tmp_path, since=timedelta(days=1))) == 3
    assert ledger.read(tmp_path / "nowhere") == []


def test_reader_reports_a_valid_json_non_object_line_instead_of_crashing(tmp_path):
    ledger.append(
        tmp_path, {"event": "started", "job": "j", "reason": "due", "wake_id": "w-obj00001"}
    )
    path = next(ledger.ledger_dir(tmp_path).glob("*.jsonl"))
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write('null\n42\n[1, 2, 3]\n"str"\n')
    rows = ledger.read(tmp_path)
    assert len(rows) == 5
    assert all(isinstance(r, dict) for r in rows)
    bad = [r for r in rows if r["event"] == "unreadable"]
    assert len(bad) == 4 and all("is not a JSON object" in r["reason"] for r in bad)
    assert list(ledger.open_wakes(rows)) == ["w-obj00001"], "open_wakes survives the lines"


def test_open_wakes_pairs_by_wake_id_not_by_file_order(tmp_path):
    """A started row that lands in a LATER day file than its finished row (a
    clock stepped back between the two writes) is still closed."""
    early = "2026-01-01T10:00:00+00:00"
    late = "2026-01-02T10:00:00+00:00"
    ledger.append(
        tmp_path,
        {
            "event": "finished",
            "state": "success",
            "job": "j",
            "reason": "exit_0",
            "wake_id": "w-order001",
            "ts": early,
        },
    )
    ledger.append(
        tmp_path,
        {"event": "started", "job": "j", "reason": "due", "wake_id": "w-order001", "ts": late},
    )
    ledger.append(
        tmp_path,
        {"event": "started", "job": "j", "reason": "due", "wake_id": "w-order002", "ts": late},
    )
    files = sorted(p.name for p in ledger.ledger_dir(tmp_path).glob("*.jsonl"))
    assert files == ["2026-01-01.jsonl", "2026-01-02.jsonl"]
    assert list(ledger.open_wakes(ledger.read(tmp_path))) == ["w-order002"]


def test_writer_honours_a_supplied_ts_and_refuses_a_bad_one(tmp_path):
    wake = ledger.append(
        tmp_path,
        {
            "event": "finished",
            "state": "success",
            "job": "j",
            "reason": "x",
            "ts": "2026-03-04T05:06:07.000008Z",
        },
    )
    row = [r for r in ledger.read(tmp_path) if r["wake_id"] == wake][0]
    assert row["ts"] == "2026-03-04T05:06:07.000008+00:00"
    assert (ledger.ledger_dir(tmp_path) / "2026-03-04.jsonl").exists()
    with pytest.raises(ledger.LedgerRefusedError):
        ledger.append(
            tmp_path,
            {
                "event": "finished",
                "state": "success",
                "job": "j",
                "reason": "x",
                "ts": "yesterday-ish",
            },
        )
