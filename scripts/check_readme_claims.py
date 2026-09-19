#!/usr/bin/env python3
"""AWR001 -- the README's "Self-test verifies" list IS the self-test.

WHY
---
0.1.0's README listed four things the self-test "verifies". The self-test
exercised one of them. Nothing compared the two, so the README was a claim about
a gate rather than a description of it -- the same shape as a routine whose
report says a step ran when nothing measured it.

The list is not documentation ABOUT the cases; it is the cases. This checker
reads the bullets under `### Self-test verifies` and compares them, in order,
with the output of `awrise --self-test --list`. A case added, removed or renamed
turns this red until the README is regenerated.

    python scripts/check_readme_claims.py            # judge the README
    python scripts/check_readme_claims.py --self-test # prove the rule can fail

Exit: 0 clean, 1 violation, 2 could not judge -- never 0 on silence.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List

PKG_ROOT = Path(__file__).resolve().parent.parent
HEADING = "### Self-test verifies"
_BULLET = re.compile(r"^-\s+`([^`]+)`\s*$")


class CouldNotJudgeError(Exception):
    """Exit 2. A rule that cannot run is dead, not passing."""


def readme_cases(text: str) -> List[str]:
    """The case names claimed by the README, in the order it claims them."""
    lines = text.splitlines()
    try:
        start = next(i for i, ln in enumerate(lines) if ln.strip() == HEADING)
    except StopIteration:
        raise CouldNotJudgeError(
            f"README has no {HEADING!r} section -- nothing to compare, and an absent "
            f"claim must not read as a kept one"
        ) from None
    out: List[str] = []
    for ln in lines[start + 1 :]:
        if ln.startswith("#"):
            break
        m = _BULLET.match(ln.strip())
        if m:
            out.append(m.group(1).strip())
    return out


def selftest_cases(root: Path) -> List[str]:
    """The case names the CLI itself lists."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "awrise", "--self-test", "--list"],
            cwd=str(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CouldNotJudgeError(f"could not run the self-test lister: {exc}") from exc
    if proc.returncode != 0:
        raise CouldNotJudgeError(
            f"`--self-test --list` exited {proc.returncode}: {proc.stderr.strip()[-300:]}"
        )
    names = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if not names:
        raise CouldNotJudgeError("`--self-test --list` printed nothing")
    return names


def compare(claimed: List[str], actual: List[str]) -> List[str]:
    """AWR001 findings. Order matters: the README is generated FROM the list."""
    findings: List[str] = []
    missing = [c for c in actual if c not in claimed]
    invented = [c for c in claimed if c not in actual]
    for c in missing:
        findings.append(f"AWR001 the self-test case `{c}` is not listed in the README")
    for c in invented:
        findings.append(
            f"AWR001 the README lists `{c}`, which is not a self-test case -- "
            f"a claim about a gate that does not exist"
        )
    if not findings and claimed != actual:
        findings.append(
            "AWR001 the README lists every case but in a different order than "
            "`--self-test --list`; regenerate it rather than reordering by hand"
        )
    return findings


def judge(root: Path) -> List[str]:
    readme = root / "README.md"
    try:
        text = readme.read_text(encoding="utf-8")
    except OSError as exc:
        raise CouldNotJudgeError(f"cannot read {readme}: {exc}") from exc
    return compare(readme_cases(text), selftest_cases(root))


def self_test() -> int:
    """Prove AWR001 can still fail, and that an honest README passes."""
    bad = 0

    def check(label: str, cond: bool) -> None:
        nonlocal bad
        print(f"  {'ok  ' if cond else 'FAIL'} {label}")
        if not cond:
            bad += 1

    actual = ["a", "b", "c"]
    check("an exact list passes", compare(["a", "b", "c"], actual) == [])
    check(
        "a missing case fails", any("`c` is not listed" in f for f in compare(["a", "b"], actual))
    )
    check(
        "an invented case fails",
        any("not a self-test case" in f for f in compare(["a", "b", "c", "d"], actual)),
    )
    check(
        "a reordered list fails",
        any("different order" in f for f in compare(["c", "b", "a"], actual)),
    )
    body = f"{HEADING}\n\n- `a`\n- `b`\n\n## Design\n\n- `not_a_case`\n"
    check("only the bullets under the heading are read", readme_cases(body) == ["a", "b"])
    try:
        readme_cases("# awrise\n\nno such section\n")
        check("a README with no section is UNJUDGED, not clean", False)
    except CouldNotJudgeError:
        check("a README with no section is UNJUDGED, not clean", True)
    print("self-test ok" if not bad else "self-test FAILED")
    return 1 if bad else 0


def main(argv: List[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--self-test", action="store_true", help="prove the rule can still fail")
    ap.add_argument(
        "--root",
        default=str(PKG_ROOT),
        help="package root holding README.md (default: this package)",
    )
    args = ap.parse_args(argv)
    if args.self_test:
        return self_test()
    try:
        findings = judge(Path(args.root))
    except CouldNotJudgeError as exc:
        print(f"UNJUDGED: {exc}")
        return 2
    if findings:
        for f in findings:
            print(f)
        print(
            f"\n[FAIL] {len(findings)} AWR001 finding(s). The list is generated: "
            f"`awrise --self-test --list` is the source, the README is the copy."
        )
        return 1
    print("[ok] AWR001 -- the README's self-test list equals `--self-test --list`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
