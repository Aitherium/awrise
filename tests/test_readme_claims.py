"""AWR001 in the package suite: the README's claims about the self-test are the self-test.

The checker itself lives in `scripts/check_readme_claims.py` so it can run standalone in a
static lane; these tests make the package's own `pytest tests/` red when the two drift, which
is the lane that actually runs before a publish.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

PKG_ROOT = Path(__file__).resolve().parent.parent


def _load_checker():
    path = PKG_ROOT / "scripts" / "check_readme_claims.py"
    spec = importlib.util.spec_from_file_location("awrise_readme_claims", path)
    assert spec and spec.loader, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


CHECKER = _load_checker()


def test_readme_lists_exactly_the_self_test_cases():
    findings = CHECKER.judge(PKG_ROOT)
    assert findings == [], "\n".join(findings)


def test_the_readme_list_is_not_empty():
    cases = CHECKER.readme_cases((PKG_ROOT / "README.md").read_text(encoding="utf-8"))
    assert len(cases) > 10, f"only {len(cases)} case(s) claimed -- a list nobody regenerated"


def test_the_rule_can_still_fail():
    """The negative twin: drop one case and AWR001 must go red."""
    actual = CHECKER.selftest_cases(PKG_ROOT)
    assert CHECKER.compare(actual[:-1], actual), "a README missing a case passed AWR001"
    assert CHECKER.compare(actual + ["invented_case"], actual), "an invented case passed"


def test_a_readme_without_the_section_is_unjudged_not_clean():
    with pytest.raises(CHECKER.CouldNotJudgeError):
        CHECKER.readme_cases("# awrise\n\nnothing here\n")


def test_the_checker_self_test_passes():
    proc = subprocess.run(
        [sys.executable, str(PKG_ROOT / "scripts" / "check_readme_claims.py"), "--self-test"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_the_version_is_one_number_everywhere():
    """PVR004's local twin: `__version__` and pyproject must not disagree."""
    import re

    pyproject = (PKG_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    declared = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.M)
    assert declared, "pyproject declares no static version"
    init = (PKG_ROOT / "awrise" / "__init__.py").read_text(encoding="utf-8")
    module = re.search(r'^__version__\s*=\s*"([^"]+)"', init, re.M)
    assert module, "awrise/__init__.py declares no __version__"
    assert declared.group(1) == module.group(1), (
        f"pyproject says {declared.group(1)}, __init__ says {module.group(1)}"
    )
