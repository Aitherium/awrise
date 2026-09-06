import json
import os
import pytest
from datetime import timedelta
from pathlib import Path
from awrise.cli import parse_interval, cmd_add, cmd_list, cmd_run_due, load_jobs, save_jobs
import argparse
import tempfile

def test_parse_interval_valid():
    assert parse_interval("15m") == timedelta(minutes=15)
    assert parse_interval("2h") == timedelta(hours=2)
    assert parse_interval("1d") == timedelta(days=1)
    assert parse_interval("0.5h") == timedelta(hours=0.5)
    assert parse_interval("30m") == timedelta(minutes=30)

def test_parse_interval_invalid():
    with pytest.raises(ValueError):
        parse_interval("")
    with pytest.raises(ValueError):
        parse_interval("abc")
    with pytest.raises(ValueError):
        parse_interval("15x")
    with pytest.raises(ValueError):
        parse_interval("-5m")
    with pytest.raises(ValueError):
        parse_interval("0m")

def test_parse_interval_case_insensitive():
    assert parse_interval("15M") == timedelta(minutes=15)
    assert parse_interval("2H") == timedelta(hours=2)

def test_add_job(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    args = argparse.Namespace(name="test", every="15m", run="echo test")
    result = cmd_add(args)
    assert result == 0
    jobs = load_jobs()
    assert "test" in jobs
    assert jobs["test"]["command"] == "echo test"
    assert jobs["test"]["interval"] == "900.0"

def test_add_duplicate_job(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    args = argparse.Namespace(name="test", every="15m", run="echo 1")
    cmd_add(args)
    result = cmd_add(args)
    assert result == 1

def test_list_jobs(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    args = argparse.Namespace(name="j1", every="15m", run="cmd1")
    cmd_add(args)
    list_args = argparse.Namespace()
    result = cmd_list(list_args)
    assert result == 0

def test_remove_job(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    add_args = argparse.Namespace(name="test", every="15m", run="echo test")
    cmd_add(add_args)
    rm_args = argparse.Namespace(name="test")
    from awrise.cli import cmd_remove
    result = cmd_remove(rm_args)
    assert result == 0
    jobs = load_jobs()
    assert "test" not in jobs

def test_idempotency(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    add_args = argparse.Namespace(name="t", every="300m", run="echo hi")
    cmd_add(add_args)
    run_args = argparse.Namespace(quiet=True)
    cmd_run_due(run_args)
    with open(tmp_path / "jobs.json") as f:
        jobs1 = json.load(f)
    last_run1 = jobs1["t"]["last_run"]
    cmd_run_due(run_args)
    with open(tmp_path / "jobs.json") as f:
        jobs2 = json.load(f)
    last_run2 = jobs2["t"]["last_run"]
    assert last_run1 == last_run2, "Running twice should not execute the same job twice"

def test_empty_command_skip(tmp_path, monkeypatch):
    monkeypatch.setenv("AWRISE_HOME", str(tmp_path))
    jobs_file = tmp_path / "jobs.json"
    test_jobs = {"empty": {"interval": "300", "command": "", "last_run": None, "last_status": None}}
    with open(jobs_file, "w") as f:
        json.dump(test_jobs, f)
    run_args = argparse.Namespace(quiet=True)
    result = cmd_run_due(run_args)
    assert result == 0

