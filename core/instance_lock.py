"""Single-instance lock for main.py — prevents two trading processes."""

from __future__ import annotations

import os
import sys
from typing import Optional

from config import Config
from logger import error_logger, system_logger

_LOCK_FD: Optional[int] = None


def instance_lock_path() -> str:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    return os.path.join(
        Config.DATA_DIR, os.getenv("INSTANCE_LOCK_FILE", "main_instance.lock")
    )


def pid_is_alive(pid: int, *, unknown_means_alive: bool = True) -> bool:
    """Return whether *pid* currently exists.

    Instance-lock callers keep unknown_means_alive=True so a probe failure
    cannot start a second main.py. Watchdog process checks pass False so a
    probe failure cannot look like a live bot and skip auto-restart.
    """
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes

            process = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if process:
                ctypes.windll.kernel32.CloseHandle(process)
                return True
            # Access denied usually means the process exists.
            if ctypes.GetLastError() in (5, 0x5):
                return True
            return False
        except Exception:
            return unknown_means_alive
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return unknown_means_alive


def read_lock_pid(path: Optional[str] = None) -> Optional[int]:
    lock_path = path or instance_lock_path()
    if not os.path.isfile(lock_path):
        return None
    try:
        with open(lock_path, encoding="utf-8") as handle:
            return int((handle.read() or "0").strip() or "0")
    except (OSError, ValueError):
        return None


def another_main_is_running() -> bool:
    pid = read_lock_pid()
    if pid is None or pid == os.getpid():
        return False
    return pid_is_alive(pid)


def _try_exclusive_lock(fd: int) -> bool:
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


def _unlock_fd(fd: int) -> None:
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass


def acquire_main_lock() -> bool:
    """Exclusive lock. Returns False if another live main.py holds the file."""
    global _LOCK_FD
    if _LOCK_FD is not None:
        return True
    path = instance_lock_path()
    existing = read_lock_pid(path)
    if existing is not None and existing != os.getpid() and pid_is_alive(existing):
        error_logger.critical(
            "Instance lock held by live pid %s — refusing to start a second main.py.",
            existing,
        )
        return False
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        error_logger.critical("Failed to open instance lock: %s", exc)
        return False
    if not _try_exclusive_lock(fd):
        os.close(fd)
        error_logger.critical(
            "Instance lock flock held — refusing to start a second main.py."
        )
        return False
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("ascii"))
    except OSError as exc:
        _unlock_fd(fd)
        os.close(fd)
        error_logger.critical("Failed to write instance lock: %s", exc)
        return False
    _LOCK_FD = fd
    system_logger.info("Instance lock acquired (pid=%s).", os.getpid())
    return True


def release_main_lock() -> None:
    global _LOCK_FD
    path = instance_lock_path()
    pid = read_lock_pid(path)
    if _LOCK_FD is not None:
        _unlock_fd(_LOCK_FD)
        try:
            os.close(_LOCK_FD)
        except OSError:
            pass
        _LOCK_FD = None
    if pid is not None and pid != os.getpid():
        return
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError as exc:
        error_logger.debug("Instance lock release failed: %s", exc)
