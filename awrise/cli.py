"""awrise CLI - Schedule and run jobs."""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

def get_awrise_home() -> Path:
    home = os.environ.get("AWRISE_HOME")
    path = Path(home) if home else Path.home() / ".aither" / "awrise"
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_jobs_file() -> Path:
    return get_awrise_home() / "jobs.json"

def load_jobs() -> dict:
    jobs_file = get_jobs_file()
    if not jobs_file.exists():
        return {}
    try:
        with open(jobs_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return {}

def save_jobs(jobs: dict) -> None:
    with open(get_jobs_file(), "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2)

def parse_interval(interval_str: str) -> timedelta:
    import re
    interval_str = interval_str.strip().lower()
    if not interval_str:
        raise ValueError("Interval empty")
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([mhd])$", interval_str)
    if not match:
        raise ValueError(f"Invalid: {interval_str}")
    value = float(match.group(1))
    if value <= 0:
        raise ValueError(f"Interval must be positive: {interval_str}")
    unit = match.group(2)
    return {"m": timedelta(minutes=value), "h": timedelta(hours=value), "d": timedelta(days=value)}[unit]

def cmd_add(args) -> int:
    if not all([args.name, args.every, args.run]):
        print("--name, --every, --run required", file=sys.stderr)
        return 1
    try:
        interval = parse_interval(args.every)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    jobs = load_jobs()
    if args.name in jobs:
        print("Job exists", file=sys.stderr)
        return 1
    jobs[args.name] = {
        "interval": str(interval.total_seconds()),
        "command": args.run.strip(),
        "last_run": None,
        "last_status": None
    }
    save_jobs(jobs)
    print(f"Added {args.name}")
    return 0

def cmd_remove(args) -> int:
    jobs = load_jobs()
    if args.name not in jobs:
        print("Not found", file=sys.stderr)
        return 1
    del jobs[args.name]
    save_jobs(jobs)
    print(f"Removed {args.name}")
    return 0

def cmd_list(args) -> int:
    jobs = load_jobs()
    if not jobs:
        print("No jobs")
        return 0
    print(f"{'Name':<20} {'Interval':<12} {'Command':<40} {'Last Run':<20} {'Status':<10}")
    print("-" * 102)
    for name, job in sorted(jobs.items()):
        sec = float(job.get("interval", 0))
        istr = f"{sec / 86400:.0f}d" if sec >= 86400 else (f"{sec / 3600:.0f}h" if sec >= 3600 else f"{sec / 60:.0f}m")
        cmd_str = job.get("command", "")[:40]
        last_run = job.get("last_run") or "never"
        status = job.get("last_status") or "pending"
        print(f"{name:<20} {istr:<12} {cmd_str:<40} {last_run:<20} {status:<10}")
    return 0

def cmd_run_due(args) -> int:
    jobs = load_jobs()
    if not jobs:
        if not args.quiet:
            print("No jobs")
        return 0
    now = datetime.utcnow()
    for name, job in jobs.items():
        cmd = job.get("command", "").strip()
        if not cmd:
            print(f"Skip {name}: empty", file=sys.stderr)
            continue
        interval_sec = float(job.get("interval", 0))
        last_run_str = job.get("last_run")
        is_due = False
        if last_run_str is None:
            is_due = True
        else:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                if (now - last_run).total_seconds() >= interval_sec:
                    is_due = True
            except ValueError:
                is_due = True
        if is_due:
            if not args.quiet:
                print(f"Running {name}...")
            try:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=300)
                job["last_run"] = now.isoformat()
                if result.returncode == 0:
                    job["last_status"] = "success"
                    if not args.quiet:
                        print(f"  OK {name}")
                else:
                    job["last_status"] = "failure"
                    print(f"  FAIL {name} exit {result.returncode}", file=sys.stderr)
            except subprocess.TimeoutExpired:
                job["last_run"] = now.isoformat()
                job["last_status"] = "timeout"
                print(f"  FAIL {name} timeout", file=sys.stderr)
            except Exception as e:
                job["last_run"] = now.isoformat()
                job["last_status"] = "error"
                print(f"  FAIL {name}: {e}", file=sys.stderr)
    save_jobs(jobs)
    return 0

def cmd_self_test(args) -> int:
    print("Running self-tests...")
    all_pass = True
    print("  Test: parse_interval with valid inputs...")
    for i_str, expected in [("15m", timedelta(minutes=15)), ("2h", timedelta(hours=2)), ("1d", timedelta(days=1))]:
        try:
            if parse_interval(i_str) == expected:
                print(f"    PASS: {i_str}")
            else:
                print(f"    FAIL: {i_str}")
                all_pass = False
        except Exception:
            print(f"    FAIL: {i_str} raised")
            all_pass = False
    print("  Test: parse_interval rejects invalid...")
    for i_str in ["", "abc", "15x", "0m"]:
        try:
            parse_interval(i_str)
            print(f"    FAIL: {i_str} should reject")
            all_pass = False
        except ValueError:
            print(f"    PASS: {i_str} rejected")
    return 0 if all_pass else 1

def main() -> int:
    if "--self-test" in sys.argv:
        sys.argv = ["awrise"]
        args = argparse.Namespace()
        return cmd_self_test(args)

    parser = argparse.ArgumentParser(prog="awrise", description="Wake something on a schedule")
    subs = parser.add_subparsers(dest="command")

    add_p = subs.add_parser("add")
    add_p.add_argument("--name", required=True)
    add_p.add_argument("--every", required=True)
    add_p.add_argument("--run", required=True)
    add_p.set_defaults(func=cmd_add)

    rm_p = subs.add_parser("remove")
    rm_p.add_argument("--name", required=True)
    rm_p.set_defaults(func=cmd_remove)

    list_p = subs.add_parser("list")
    list_p.set_defaults(func=cmd_list)

    run_p = subs.add_parser("run-due")
    run_p.add_argument("--quiet", action="store_true")
    run_p.set_defaults(func=cmd_run_due)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 0
    func = getattr(args, "func", None)
    return func(args) if func else 0

if __name__ == "__main__":
    sys.exit(main())
