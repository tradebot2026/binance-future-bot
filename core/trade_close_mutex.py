"""In-process mutex for trade close — prevents double-close races in one process."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

_registry_lock = threading.Lock()
_trade_locks: dict[str, threading.Lock] = {}


def _lock_for(trade_id: str) -> threading.Lock:
    with _registry_lock:
        lock = _trade_locks.get(trade_id)
        if lock is None:
            lock = threading.Lock()
            _trade_locks[trade_id] = lock
        return lock


@contextmanager
def trade_close_mutex(trade_id: str, *, blocking: bool = True) -> Iterator[bool]:
    """
    Serialize close handling for a trade within this process.
    Yields True when the lock was acquired, False when non-blocking acquire fails.
    """
    tid = str(trade_id or "").strip()
    if not tid:
        yield True
        return

    lock = _lock_for(tid)
    acquired = lock.acquire(blocking=blocking)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()
