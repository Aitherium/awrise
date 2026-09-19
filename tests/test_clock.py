"""Intervals, dueness and the two clock guards (upper bound, skew)."""

from datetime import timedelta

import pytest
from awrise import clock


def test_interval_upper_bound_is_a_value_error_not_an_overflow():
    for bad in ("500000w", "99999999999999w", "9" * 400 + "s"):
        with pytest.raises(ValueError):
            clock.parse_interval(bad)
    assert clock.parse_interval("5200w") == timedelta(weeks=5200)


def test_next_due_never_leaves_the_calendar_for_an_accepted_interval():
    job = {"last_started_at": "2026-09-18T00:00:00+00:00", "interval_s": clock.MAX_INTERVAL_S}
    assert clock.next_due(job, clock.now_utc()).year == 2126


def test_skew_is_positive_only_past_the_tolerance():
    now = clock.parse_ts("2026-09-18T12:00:00+00:00")
    assert clock.skew_s({"last_started_at": "2026-09-18T11:59:59+00:00"}, now) is None
    assert clock.skew_s({"last_started_at": "2026-09-18T12:00:00.900000+00:00"}, now) is None
    assert clock.skew_s({"last_started_at": "2026-09-18T12:00:01.500000+00:00"}, now) == 1.5
    assert clock.skew_s({"last_started_at": "2026-09-18T13:00:00+00:00"}, now) == 3600.0
    assert clock.skew_s({}, now) is None


# ---------------------------------------------------- the unstamped `at` anchor
#
# Review finding (2026-09-19): a job with an `at:` anchor and no `last_started_at`
# took the anchor branch and then returned `now`, so a daily 03:00 job added at
# 14:00 was due IMMEDIATELY and ran the wrong work at the wrong hour. README:73
# promises "due is the next anchor after the last start", and `prewarm --apply`
# creates every proposed job with an anchor and no stamp -- so one --apply woke
# every proposed unit at once. The anchor is honoured from the first pass now.


def test_an_unstamped_anchored_job_waits_for_the_anchor_not_the_next_pass():
    now = clock.parse_ts("2026-09-18T14:00:00+00:00")
    job = {
        "at": "03:00",
        "interval_s": 86400.0,
        "last_started_at": None,
        "created_at": "2026-09-18T14:00:00+00:00",
    }
    assert clock.next_due(job, now) == clock.parse_ts("2026-09-19T03:00:00+00:00")
    assert clock.is_due(job, now) is False


def test_an_unstamped_anchored_job_is_due_at_todays_anchor_when_it_is_still_ahead():
    now = clock.parse_ts("2026-09-18T02:00:00+00:00")
    job = {
        "at": "03:00",
        "interval_s": 86400.0,
        "last_started_at": None,
        "created_at": "2026-09-18T02:00:00+00:00",
    }
    assert clock.next_due(job, now) == clock.parse_ts("2026-09-18T03:00:00+00:00")
    assert clock.is_due(job, now) is False


def test_an_unstamped_anchored_job_fires_once_the_anchor_has_come_round():
    job = {
        "at": "03:00",
        "interval_s": 86400.0,
        "last_started_at": None,
        "created_at": "2026-09-18T02:00:00+00:00",
    }
    assert clock.is_due(job, clock.parse_ts("2026-09-18T02:59:00+00:00")) is False
    assert clock.is_due(job, clock.parse_ts("2026-09-18T03:04:00+00:00")) is True


def test_an_anchored_job_with_no_stamps_at_all_still_waits_for_its_anchor():
    """A hand-written record with neither stamp: anchored on `now`, never `now`."""
    now = clock.parse_ts("2026-09-18T14:00:00+00:00")
    job = {"at": "03:00", "interval_s": 86400.0}
    assert clock.next_due(job, now) == clock.parse_ts("2026-09-19T03:00:00+00:00")
    assert clock.is_due(job, now) is False


def test_an_unstamped_anchored_job_reports_no_missed_windows():
    now = clock.parse_ts("2026-09-18T14:00:00+00:00")
    job = {
        "at": "03:00",
        "interval_s": 86400.0,
        "last_started_at": None,
        "created_at": "2026-09-18T14:00:00+00:00",
    }
    assert clock.missed_windows(job, now) == 0
