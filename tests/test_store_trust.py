"""The store trust boundary, on the brick's primary measured host.

store.py's own docstring states the invariant: "a home or store another user
can write is refused (that is the trust boundary: whoever writes jobs.json runs
shell commands as you)". Review finding (2026-09-19), CRITICAL, verified live:
on win32 that was not implemented. `_windows_owned_by_me` requested only
OWNER_SECURITY_INFORMATION (the DACL pointer was passed as None) and compared
SIDs, so write access was never judged -- a jobs.json owned by me with an ACE
granting another principal Write returned `perms_problems() == []` and loaded.
It also failed OPEN: the helper returns None whenever the security information
cannot be read, and only `mine is False` became a problem, so a check that
COULD NOT JUDGE was silently a pass, against this repo's 0/1/2 contract.
"""

import subprocess
import sys
from pathlib import Path

import pytest
from awrise import store

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="the Windows trust boundary")

#: A real, resolvable, non-owner principal. `Users` is a local group every
#: interactive account is in, so an ACE granting it write really does mean
#: "any local user may rewrite the commands awrise runs".
FOREIGN = "*S-1-5-32-545"  # BUILTIN\Users


def _icacls(*argv) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["icacls", *argv], capture_output=True, text=True, encoding="utf-8", errors="replace"
    )


def _grant(path: Path, rights: str) -> None:
    """Add an ACE for FOREIGN, or fail loudly. Never a body skip: a filesystem
    that cannot carry an ACE is a filesystem this whole module is meaningless
    on, and `pytestmark` already scopes it to win32."""
    # The inheritance flags are meaningful on a directory and silently ignored
    # on a file, so the two forms are spelled separately: `(OI)(CI)` on a file
    # makes icacls exit 0 having granted nothing, which would make a test pass
    # for the wrong reason.
    prefix = "(OI)(CI)" if path.is_dir() else ""
    done = _icacls(str(path), "/grant", f"{FOREIGN}:{prefix}{rights}")
    assert done.returncode == 0, f"could not set an ACE on {path}: {done.stdout}{done.stderr}"


def _grant_write(path: Path) -> None:
    _grant(path, "(M)")


@pytest.fixture
def base(tmp_path, monkeypatch) -> Path:
    path = tmp_path / "home"
    path.mkdir()
    monkeypatch.setenv("AWRISE_HOME", str(path))
    return path


def test_a_clean_home_is_judged_trustworthy(base):
    """The negative twin, first: this check must be able to PASS."""
    (base / store.STORE_NAME).write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    assert store._windows_foreign_writers(base) == []
    assert store.perms_problems(base) == []
    assert store.load(base) == {}


def test_a_home_another_principal_may_write_is_refused(base):
    (base / store.STORE_NAME).write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    assert store.perms_problems(base) == [], "precondition: it starts clean"
    _grant_write(base)
    assert store._windows_foreign_writers(base), "the ACE must be visible to the probe"
    problems = store.perms_problems(base)
    assert problems, "a home a non-owner may write is not a trustworthy home"
    assert any("writable by" in problem for problem in problems), problems
    with pytest.raises(store.StoreError) as info:
        store.load(base)
    assert "untrusted" in str(info.value)


def test_a_jobs_file_another_principal_may_write_is_refused(base):
    """The FILE is judged too, not only the directory it sits in."""
    jobs = base / store.STORE_NAME
    jobs.write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    assert store.perms_problems(base) == []
    _grant_write(jobs)
    problems = store.perms_problems(base)
    assert any(str(jobs) in problem for problem in problems), problems
    with pytest.raises(store.StoreError):
        store.load(base)


def test_a_store_that_cannot_be_judged_is_refused_not_trusted(base, monkeypatch):
    """Fail-closed: `None` from the probe is a problem, never silence."""
    (base / store.STORE_NAME).write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    monkeypatch.setattr(store, "_windows_foreign_writers", lambda path: None)
    problems = store.perms_problems(base)
    assert problems and all("cannot be judged" in problem for problem in problems), problems
    with pytest.raises(store.StoreError):
        store.load(base)
    monkeypatch.setattr(store, "_windows_foreign_writers", lambda path: [])
    monkeypatch.setattr(store, "_windows_owned_by_me", lambda path: None)
    assert store.perms_problems(base), "an unreadable owner is unjudged, not owner-only"


def test_a_null_access_list_is_a_measured_problem_not_an_unjudged_one(base, monkeypatch):
    monkeypatch.setattr(
        store, "_windows_foreign_writers", lambda path: ["everyone (the file has no access list)"]
    )
    problems = store.perms_problems(base)
    assert problems and "everyone" in problems[0], problems


def test_home_tightens_a_directory_it_inherited_write_access_on(base):
    """awrise owns its home, so it drops what the parent granted -- the
    Windows twin of `mkdir(mode=0o700)`, which is inert on NTFS."""
    _grant_write(base)
    assert store._windows_foreign_writers(base), "precondition: it starts open"
    assert store.home() == base
    assert store._windows_foreign_writers(base) == [], _icacls(str(base)).stdout
    assert store.perms_problems(base) == []
    # And a file created afterwards inherits only the three trusted principals.
    (base / store.STORE_NAME).write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    assert store._windows_foreign_writers(base / store.STORE_NAME) == []


def test_the_probe_ignores_an_inherit_only_entry(base):
    """An (IO) ACE is a template for children, not access to this path -- and
    reporting it would make every home under a normal profile look hostile."""
    jobs = base / store.STORE_NAME
    jobs.write_text('{"schema": 2, "jobs": {}}', encoding="utf-8")
    _grant(base, "(IO)(M)")
    assert store._windows_foreign_writers(base) == [], _icacls(str(base)).stdout


def test_a_read_only_entry_for_another_principal_is_not_a_write(base):
    """The boundary is WRITE. A reader cannot change what awrise runs."""
    _grant(base, "(RX)")
    assert store._windows_foreign_writers(base) == [], _icacls(str(base)).stdout
    assert store.perms_problems(base) == []
