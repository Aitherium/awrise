"""Process priority for the lanes (Windows; a no-op everywhere else).

WHY. Task Scheduler starts a task at priority 7: below-normal CPU AND *Low* I/O
priority. Measured 2026-11-16 on the Windows host: the awrise pass (`nice` 16384,
`ionice` 1) read its own ledger at 57 KB/s -- 7 reads a second -- while the same file
cat'd from a normal-priority shell read at 90 MB/s. A pass that needs the ledger
(`tick`'s gap, reconcile, history windows) therefore sat for tens of minutes behind
the host's other I/O, and its wakes inherited the same Low I/O. Lanes fix WHICH pass
a pager waits behind; this fixes how fast the pager's own pass can run.

The critical lane raises itself to normal CPU and normal I/O: it is a few seconds of
work a minute and its wakes are the pagers. The normal lane keeps what the scheduler
gave it, so the owner's interactive desktop sees no new load. Raising to NORMAL needs
no privilege (only HIGH does); every call is best effort and never raises.
"""

from __future__ import annotations

import sys
from typing import Optional

#: CPU classes (kernel32 SetPriorityClass).
NORMAL_PRIORITY_CLASS = 0x00000020
BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
#: I/O priorities (ntdll ProcessIoPriority = 33).
IO_VERY_LOW, IO_LOW, IO_NORMAL = 0, 1, 2
_PROCESS_IO_PRIORITY = 33


def _win():
    import ctypes  # noqa: PLC0415 - Windows only
    from ctypes import wintypes  # noqa: PLC0415

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    nt = ctypes.WinDLL("ntdll")
    k32.GetCurrentProcess.restype = wintypes.HANDLE
    k32.SetPriorityClass.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    k32.GetPriorityClass.argtypes = [wintypes.HANDLE]
    # Without argtypes ctypes passes the -1 pseudo-handle as a 32-bit int and the call
    # answers STATUS_INVALID_HANDLE on 64-bit Windows.
    nt.NtQueryInformationProcess.argtypes = [
        wintypes.HANDLE, ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong, ctypes.c_void_p,
    ]
    nt.NtQueryInformationProcess.restype = ctypes.c_long
    nt.NtSetInformationProcess.argtypes = [
        wintypes.HANDLE, ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong,
    ]
    nt.NtSetInformationProcess.restype = ctypes.c_long
    return ctypes, k32, nt


def io_priority() -> Optional[int]:
    """This process's I/O priority (0 very low, 1 low, 2 normal), or None."""
    if sys.platform != "win32":
        return None
    try:
        ctypes, k32, nt = _win()
        value = ctypes.c_ulong(0)
        rc = nt.NtQueryInformationProcess(
            k32.GetCurrentProcess(), _PROCESS_IO_PRIORITY, ctypes.byref(value), 4, None
        )
        return int(value.value) if rc == 0 else None
    except Exception:  # noqa: BLE001 - diagnostics must never stop a pass
        return None


def cpu_class() -> Optional[int]:
    if sys.platform != "win32":
        return None
    try:
        _ctypes, k32, _nt = _win()
        value = int(k32.GetPriorityClass(k32.GetCurrentProcess()))
        return value or None
    except Exception:  # noqa: BLE001
        return None


def _set(cpu: int, io: int) -> bool:
    try:
        ctypes, k32, nt = _win()
        handle = k32.GetCurrentProcess()
        ok = bool(k32.SetPriorityClass(handle, cpu))
        value = ctypes.c_ulong(io)
        rc = nt.NtSetInformationProcess(handle, _PROCESS_IO_PRIORITY, ctypes.byref(value), 4)
        return ok and rc == 0
    except Exception:  # noqa: BLE001
        return False


def apply(lane: str) -> bool:
    """Put this process where its lane belongs. False when nothing was changed.

    ``critical`` -> normal CPU, normal I/O. ``normal`` -> below-normal CPU, low I/O,
    which is what Task Scheduler started it with (and what a normal pass spawned by a
    critical firing must go back to). Any other lane, and any non-Windows host: no-op.
    """
    if sys.platform != "win32":
        return False
    if lane == "critical":
        return _set(NORMAL_PRIORITY_CLASS, IO_NORMAL)
    if lane == "normal":
        return _set(BELOW_NORMAL_PRIORITY_CLASS, IO_LOW)
    return False
