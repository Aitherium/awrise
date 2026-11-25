"""Tests for split AWRISE_HOME detection and `awrise reset-failures`."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from awrise import _doctor, checks, cli, clock, doctor_local, ledger, store


@contextlib.contextmanager
def _temp_home(name: str = "awrise"):
    prev = os.environ.get("AWRISE_HOME")
    with tempfile.TemporaryDirectory(prefix="awrise-split-test-") as root:
        base = Path(root) / name
        base.mkdir(parents=True, exist_ok=True)
        os.environ["AWRISE_HOME"] = str(base)
        doctor_local._LAST = None
        try:
            yield Path(root), base
        finally:
            doctor_local._LAST = None
            if prev is None:
                os.environ.pop("AWRISE_HOME", None)
            else:
                os.environ["AWRISE_HOME"] = prev


def _write_scheduled_tick(base: Path, invoker: str = "schtasks") -> None:
    ledger.append(
        base,
        {
            "event": "tick",
            "reason": "pass_start",
            "pass_id": ledger.new_id("p-"),
            "invoker": invoker,
            "ts": clock.iso(clock.now_utc()),
        },
    )


class SplitHomeDoctorTests(unittest.TestCase):
    def test_detects_wrapper_pointing_to_different_home(self) -> None:
        with _temp_home("awrise") as (root, base):
            other = root / "awrise-wzns"
            other.mkdir(parents=True)
            _write_scheduled_tick(other)

            bin_dir = base / "bin"
            bin_dir.mkdir(parents=True)
            (bin_dir / "run-due.cmd").write_text(
                f'@echo off\nset "AWRISE_HOME={other}"\npython -m awrise run-due\n',
                encoding="utf-8",
            )

            line, problems, unjudged = doctor_local._split_home_line(base)
            self.assertIsNotNone(line)
            self.assertIn("SPLIT HOME", line or "")
            self.assertIn("awrise-wzns", line or "")
            self.assertEqual(len(problems), 1)
            self.assertEqual(unjudged, [])

            # Doctor report must exit 1 (measured problem).
            doctor_local._LAST = None
            out = io.StringIO()
            code = _doctor.report(out=out)
            self.assertEqual(code, 1, out.getvalue())
            self.assertIn("SPLIT HOME", out.getvalue())

    def test_wrapper_matching_base_is_clean(self) -> None:
        with _temp_home("awrise") as (_root, base):
            _write_scheduled_tick(base)
            bin_dir = base / "bin"
            bin_dir.mkdir(parents=True)
            (bin_dir / "run-due.cmd").write_text(
                f'@echo off\nset "AWRISE_HOME={base}"\npython -m awrise run-due\n',
                encoding="utf-8",
            )
            line, problems, unjudged = doctor_local._split_home_line(base)
            self.assertIsNone(line)
            self.assertEqual(problems, [])
            self.assertEqual(unjudged, [])

    def test_cross_platform_windows_and_wsl_path_normalization(self) -> None:
        self.assertEqual(
            doctor_local._normalize_home_cmp(r"C:\Users\wzns\.aither\awrise"),
            doctor_local._normalize_home_cmp("/var/mnt/c/Users/wzns/.aither/awrise"),
        )
        self.assertNotEqual(
            doctor_local._normalize_home_cmp(r"C:\Users\wzns\.aither\awrise-wzns"),
            doctor_local._normalize_home_cmp("/var/mnt/c/Users/wzns/.aither/awrise"),
        )

    def test_detects_active_sibling_when_base_is_stale(self) -> None:
        with _temp_home("awrise") as (root, base):
            sibling = root / "awrise-wzns"
            sibling.mkdir(parents=True)
            _write_scheduled_tick(sibling)

            line, problems, unjudged = doctor_local._split_home_line(base)
            self.assertIsNotNone(line)
            self.assertIn("sibling store", line or "")
            self.assertIn("awrise-wzns", line or "")
            self.assertEqual(len(problems), 1)
            self.assertEqual(unjudged, [])

            # Once base itself ticks, sibling warning disappears.
            _write_scheduled_tick(base)
            line2, problems2, _ = doctor_local._split_home_line(base)
            self.assertIsNone(line2)
            self.assertEqual(problems2, [])

    def test_detached_stale_line_surfaces_frozen_receiptless_jobs(self) -> None:
        with _temp_home("awrise") as (_root, base):
            jobs = {
                "frozen-a": {
                    **store.SPEC_DEFAULTS,
                    **store.STATE_DEFAULTS,
                    "every": "5m",
                    "interval_s": 300,
                    "run": "echo a",
                    "detach": True,
                    "receipt": None,
                    "consecutive_failures": 1843,
                    "last_state": "detached",
                },
                "with-receipt": {
                    **store.SPEC_DEFAULTS,
                    **store.STATE_DEFAULTS,
                    "every": "5m",
                    "interval_s": 300,
                    "run": "echo b",
                    "detach": True,
                    "receipt": "/tmp/r.json",
                    "consecutive_failures": 12,
                    "last_state": "detached",
                },
            }
            store.save(jobs, base)
            line, problems, unjudged = doctor_local._detached_stale_line(base)
            self.assertIsNotNone(line)
            self.assertIn("frozen-a (1843x)", line or "")
            self.assertNotIn("with-receipt", line or "")
            self.assertEqual(problems, [])
            self.assertEqual(unjudged, [])


class ResetFailuresTests(unittest.TestCase):
    def _seed_store(self, base: Path) -> None:
        jobs = {
            "detached-blind": {
                **store.SPEC_DEFAULTS,
                **store.STATE_DEFAULTS,
                "every": "5m",
                "interval_s": 300,
                "run": "echo blind",
                "detach": True,
                "receipt": None,
                "consecutive_failures": 1843,
                "why_streak": 10,
                "last_why": "old timeout",
                "last_cause": "timeout",
                "last_state": "detached",
            },
            "detached-failed-latest": {
                **store.SPEC_DEFAULTS,
                **store.STATE_DEFAULTS,
                "every": "5m",
                "interval_s": 300,
                "run": "echo fail-latest",
                "detach": True,
                "receipt": None,
                "consecutive_failures": 5,
                "last_state": "failure",
            },
            "detached-receipt": {
                **store.SPEC_DEFAULTS,
                **store.STATE_DEFAULTS,
                "every": "5m",
                "interval_s": 300,
                "run": "echo receipt",
                "detach": True,
                "receipt": "/tmp/real.receipt.json",
                "consecutive_failures": 42,
                "last_state": "failure",
            },
            "attached-failing": {
                **store.SPEC_DEFAULTS,
                **store.STATE_DEFAULTS,
                "every": "5m",
                "interval_s": 300,
                "run": "echo fail",
                "detach": False,
                "consecutive_failures": 7,
                "last_state": "failure",
            },
            "healthy-job": {
                **store.SPEC_DEFAULTS,
                **store.STATE_DEFAULTS,
                "every": "5m",
                "interval_s": 300,
                "run": "echo ok",
                "detach": True,
                "consecutive_failures": 0,
                "last_state": "detached",
            },
        }
        store.save(jobs, base)

    def test_reset_detached_no_receipt_dry_run_and_apply(self) -> None:
        with _temp_home("awrise") as (_root, base):
            self._seed_store(base)

            # Dry run reports only detached-blind and touches nothing.
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cli.main(["reset-failures", "--detached-no-receipt", "--dry-run", "--json"])
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertTrue(payload["dry_run"])
            self.assertEqual([j["name"] for j in payload["jobs"]], ["detached-blind"])
            self.assertEqual(store.load(base)["detached-blind"]["consecutive_failures"], 1843)
            self.assertEqual(ledger.read(base), [])

            # Real apply clears detached-blind ONLY and records an audited reconciled row.
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cli.main(
                    [
                        "reset-failures",
                        "--detached-no-receipt",
                        "--reason",
                        "post-detach cleanup",
                        "--json",
                    ]
                )
            self.assertEqual(rc, 0)
            payload = json.loads(buf.getvalue())
            self.assertFalse(payload["dry_run"])
            self.assertEqual([j["name"] for j in payload["reset"]], ["detached-blind"])

            after = store.load(base)
            self.assertEqual(after["detached-blind"]["consecutive_failures"], 0)
            self.assertEqual(after["detached-blind"]["why_streak"], 0)
            self.assertIsNone(after["detached-blind"]["last_why"])
            self.assertIsNone(after["detached-blind"]["last_cause"])
            # Real failing jobs (including detached jobs whose latest run failed) MUST remain untouched.
            self.assertEqual(after["detached-failed-latest"]["consecutive_failures"], 5)
            self.assertEqual(after["detached-receipt"]["consecutive_failures"], 42)
            self.assertEqual(after["attached-failing"]["consecutive_failures"], 7)

            rows = ledger.read(base)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event"], "reconciled")
            self.assertEqual(rows[0]["job"], "detached-blind")
            self.assertIn("was 1843x", rows[0]["reason"])
            self.assertIn("post-detach cleanup", rows[0]["reason"])

            # WL002 closed-vocabulary check must stay green.
            finding = checks.wl002_the_ledger_vocabulary_is_closed(rows)
            self.assertEqual(finding.code, checks.OK, finding.details)

    def test_reset_name_and_all_and_missing(self) -> None:
        with _temp_home("awrise") as (_root, base):
            self._seed_store(base)

            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                rc = cli.main(["reset-failures", "--name", "does-not-exist"])
            self.assertEqual(rc, 1)
            self.assertIn("Not found", err.getvalue())

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cli.main(["reset-failures", "--name", "attached-failing"])
            self.assertEqual(rc, 0)
            self.assertEqual(store.load(base)["attached-failing"]["consecutive_failures"], 0)
            self.assertEqual(store.load(base)["detached-receipt"]["consecutive_failures"], 42)

            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = cli.main(["reset-failures", "--all"])
            self.assertEqual(rc, 0)
            after = store.load(base)
            for name in after:
                self.assertEqual(after[name]["consecutive_failures"], 0)

    def test_set_detach_warns_when_failures_exist_without_receipt(self) -> None:
        with _temp_home("awrise") as (_root, base):
            self._seed_store(base)
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                rc = cli.main(["set", "--name", "attached-failing", "detach=true"])
            self.assertEqual(rc, 0)
            self.assertIn("NOTE: attached-failing is detached without a receipt", err.getvalue())
            self.assertIn("reset-failures --name attached-failing", err.getvalue())


if __name__ == "__main__":
    unittest.main()
