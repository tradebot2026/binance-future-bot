"""In-process mutex for symbol entry — prevents duplicate order placement races."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Iterator

from config import Config

_registry_lock = threading.Lock()
_symbol_locks: dict[str, threading.Lock] = {}
_active_entries: dict[str, float] = {}


def _lock_for(symbol: str) -> threading.Lock:
    sym = symbol.upper()
    with _registry_lock:
        lock = _symbol_locks.get(sym)
        if lock is None:
            lock = threading.Lock()
            _symbol_locks[sym] = lock
        return lock


def _ttl_seconds() -> float:
    return max(float(Config.ENTRY_IN_FLIGHT_TTL_SECONDS), 1.0)


def is_symbol_entry_in_flight(symbol: str) -> bool:
    """True when an entry for this symbol is active within the in-flight TTL."""
    sym = symbol.upper()
    with _registry_lock:
        started = _active_entries.get(sym)
        if started is None:
            return False
        if (time.monotonic() - started) > _ttl_seconds():
            _active_entries.pop(sym, None)
            return False
        return True


def _purge_stale(sym: str) -> None:
    started = _active_entries.get(sym)
    if started is not None and (time.monotonic() - started) > _ttl_seconds():
        _active_entries.pop(sym, None)


@contextmanager
def entry_in_flight_mutex(symbol: str, *, blocking: bool = False) -> Iterator[bool]:
    """
    Serialize entry attempts for one symbol within this process.
    Yields True when the in-flight claim was acquired, False when busy.
    """
    sym = symbol.upper()
    if not sym:
        yield True
        return

    lock = _lock_for(sym)
    acquired = lock.acquire(blocking=blocking)
    claimed = False
    try:
        if not acquired:
            yield False
            return

        with _registry_lock:
            _purge_stale(sym)
            if sym in _active_entries:
                yield False
                return
            _active_entries[sym] = time.monotonic()
            claimed = True
        yield True
    finally:
        if acquired:
            if claimed:
                with _registry_lock:
                    _active_entries.pop(sym, None)
            lock.release()
