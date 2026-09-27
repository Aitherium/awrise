"""A small, self-contained floor against catastrophic shell commands.

``import-routine`` (see ``cli.py``) turns a schedule + a shell command straight
from someone else's config file into a real ``run`` string on THIS job store.
That is exactly the shape a classifier exists to sit in front of, and the
platform this package ships alongside already has one -- a fail-CLOSED,
always-applied floor of built-in block patterns that a configurable rule set
can only EXTEND, never disarm.

awrise cannot import that classifier: it is standalone by contract (see
``scripts/check_moat_boundary.py``), so nothing under ``lib.`` or
``services.`` may appear in a shipped file, guarded or not -- that exception
exists for optional PACKAGES (``awm``, ``awpredict``), not for modules that
live inside the monorepo itself.

So this module PORTS the pattern list as a versioned constant instead of
importing it. That is a one-time copy that would silently rot the moment the
source list changes, which is why a separate, unshipped tool compares this
list against the live one and fails the moment they diverge -- see that
tool's own docstring for the mechanism. This module only has to stay
INTERNALLY consistent: the same patterns applied the same way, regardless of
whether the source ever changes under it.

Patterns adapted from professorpalmer/marionette's harness/command_policy.py
(MIT) -- see the platform's own NOTICE.md for the upstream attribution this
list was ported from before it reached this file.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

#: (pattern, reason) pairs. Bump ``BLOCK_PATTERNS_VERSION`` whenever this list
#: changes shape (added, removed or reworded), so a diff against the parity
#: tool's own record is a one-line change, not a full re-read.
BLOCK_PATTERNS_VERSION = 1

BLOCK_PATTERNS: List[Tuple[str, str]] = [
    # Recursive delete at a catastrophic target. Flag-order-agnostic: matches
    # -rf, -fr, -r -f, --recursive --force.
    (
        r"\brm\s+(?:-{1,2}[\w-]+\s+)*(?:-[\w]*r[\w]*|--recursive)\b"
        r"(?:\s+-{1,2}[\w-]+)*\s+"
        r"(?:/|~|~/|\.|\./|\*|/\*|\$\{?\w+)\s*(?:$|[;&|])",
        "Recursive delete at a catastrophic target (root / home / cwd / wildcard / env-var)",
    ),
    # Recursive delete of a critical system path.
    (
        r"\brm\s+(?:-{1,2}[\w-]+\s+)*(?:-[\w]*r[\w]*|--recursive)\b[^\n]*?"
        r"\s/(?:boot|etc|bin|sbin|usr|lib|var|opt|root|home)(?:/\S*)?\s*(?:$|[;&|])",
        "Recursive delete of a critical system path",
    ),
    # Windows: recursive force delete at a drive root or system directory.
    (
        r"(?:Remove-Item|ri|rd|rmdir)\b[^\n]*?"
        r"(?:[A-Za-z]:\\?\s*(?:$|[;&|])|[A-Za-z]:\\(?:Windows|Program Files|Users)\b)",
        "Windows recursive delete at a drive root or system directory",
    ),
    # Filesystem / partition / raw-device destruction.
    (r"\bmkfs\.", "Filesystem format"),
    (r"\bdd\s+[^\n]*of=/dev/(?:sd|nvme|vd|xvd|hd|mmcblk)", "Raw write to a storage device"),
    (r"\b(?:wipefs|sgdisk\s+--zap|parted\s+[^\n]*mklabel)\b", "Partition-table destruction"),
    # Remote code execution: download piped into an interpreter.
    (
        r"\b(?:curl|wget)\b[^\n]*\|[^\n]*\b(?:bash|sh|zsh|python[\d.]*|perl|ruby)\b",
        "Remote code execution via pipe-to-shell",
    ),
    (
        r"\b(?:iwr|irm|Invoke-WebRequest|Invoke-RestMethod)\b[^\n]*\|[^\n]*"
        r"(?:iex|Invoke-Expression)",
        "PowerShell remote code execution via pipe-to-iex",
    ),
    # Secret exfiltration: a credential artifact AND a network egress tool in
    # the same command, in either order.
    (
        r"(?=[^\n]*(?:id_rsa|id_ed25519|id_ecdsa|\.ssh\b|\.aws/credentials|"
        r"\.env\b|\.pem\b|\.git-credentials|\.kube/config|\.docker/config\.json|"
        r"\.npmrc|credentials\.json|service-account[^\n]*\.json))"
        r"(?=[^\n]*\b(?:curl|wget|scp|rsync|nc|ncat|netcat|ssh|ftp|"
        r"Invoke-WebRequest|Invoke-RestMethod)\b)",
        "Secret exfiltration: credential artifact piped/copied to a network egress tool",
    ),
    # Dynamic code execution from an opaque blob.
    (
        r"\bbase64\s+(?:-d|-D|--decode)\b[^\n]*\|[^\n]*\b(?:bash|sh|python[\d.]*|perl)\b",
        "Dynamic code execution from a base64 blob",
    ),
    # Catastrophic process kills.
    (r":\(\)\{\s*:\|:&\s*\};:", "Fork bomb"),
    (r"\bkill\s+-9\s+-1\b", "Kill all processes"),
]

_COMPILED: List[Tuple[re.Pattern, str, str]] = [
    (re.compile(pattern, re.IGNORECASE), pattern, reason) for pattern, reason in BLOCK_PATTERNS
]


def classify(command_line: str) -> Optional[Tuple[str, str]]:
    """``(pattern, reason)`` of the first built-in rule this command trips, or
    ``None``. Case-insensitive, same as the floor this was ported from."""
    if not command_line:
        return None
    for compiled, pattern, reason in _COMPILED:
        if compiled.search(command_line):
            return pattern, reason
    return None


#: A translated routine command is concatenated and run through a real shell
#: (``<shell> -c <command> <args...>``), so anything here reaches the shell's
#: own parser verbatim. These are ADDITIVE to the block-pattern floor above,
#: not a replacement for it: a command can be metacharacter-free and still be
#: a BLOCK, and vice versa.
SHELL_METACHARACTERS: Tuple[str, ...] = (";", "|", "&", "`", "$(", "${", ">", "<", "&&", "||")

#: Longest tokens first, so ``&&``/``||`` are reported as themselves rather
#: than as two hits of ``&``/``|``.
_META_RE = re.compile(
    "|".join(re.escape(tok) for tok in sorted(SHELL_METACHARACTERS, key=len, reverse=True))
)


def find_metacharacter(command_line: str) -> Optional[str]:
    """The first shell metacharacter/expansion token in *command_line*, or
    ``None``. Never evaluates the string -- this is a scan, not a shell."""
    if not command_line:
        return None
    match = _META_RE.search(command_line)
    return match.group(0) if match else None
