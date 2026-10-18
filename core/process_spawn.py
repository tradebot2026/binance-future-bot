"""Sibling-process spawn that survives the parent exit on Windows and POSIX.

main.py must not adopt watchdog.py as a child of a crashing loop. The launcher
and the watchdog auto-restart path share this helper so stdio is always
redirected and Windows never uses start_new_session / DETACHED_PROCESS.
"""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Optional


def spawn_detached_python(script: str, *, cwd: Optional[str] = None) -> subprocess.Popen:
    """Start `script` in a new process group with stdin/stdout/stderr redirected."""
    workdir = cwd or os.getcwd()
    cmd = [sys.executable, "-u", script]
    kwargs: dict = {
        "cwd": workdir,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if sys.platform == "win32":
        flags = 0
        flags |= int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) or 0)
        if flags:
            kwargs["creationflags"] = flags
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(cmd, **kwargs)
