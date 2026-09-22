"""Single-instance lock for main.py — prevents two trading processes."""

from __future__ import annotations

import os
import sys
from typing import Optional

from config import Config
from logger import error_logger, system_logger


def instance_lock_path() -> str:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    return os.path.join(
        Config.DATA_DIR, os.getenv("INSTANCE_LOCK_FILE", "main_instance.lock")
    )


def _pid_is_alive(pid: int) -> bool:
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
            return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


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
    return _pid_is_alive(pid)


def acquire_main_lock() -> bool:
    """Exclusive lock. Returns False if another live main.py holds the file."""
    path = instance_lock_path()
    existing = read_lock_pid(path)
    if existing is not None and existing != os.getpid() and _pid_is_alive(existing):
        error_logger.critical(
            "Instance lock held by live pid %s — refusing to start a second main.py.",
            existing,
        )
        return False
    try:
        flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        fd = os.open(path, flags)
        try:
            os.write(fd, str(os.getpid()).encode("ascii"))
        finally:
            os.close(fd)
    except OSError as exc:
        error_logger.critical("Failed to write instance lock: %s", exc)
        return False
    system_logger.info("Instance lock acquired (pid=%s).", os.getpid())
    return True


def release_main_lock() -> None:
    path = instance_lock_path()
    pid = read_lock_pid(path)
    if pid is not None and pid != os.getpid():
        return
    try:
        if os.path.isfile(path):
            os.remove(path)
    except OSError as exc:
        error_logger.debug("Instance lock release failed: %s", exc)
