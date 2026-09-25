"""Tiered scan priority — instant HOT queue vs rotating background batches."""

from __future__ import annotations

import time
from typing import Optional

from config import Config
from core.symbol_rotation_manager import SymbolRotationManager


class ScanPriorityQueue:
    """
    Priority (hot): HOT / OPPORTUNITY / spikes — scanned frequently, skip evaluated memory.
    Rotating (background): remaining scannable symbols in 15–20 coin batches.
    """

    def __init__(self) -> None:
        self._hot: list[str] = []
        self._background: list[str] = []
        self._background_index: int = 0
        self._background_bootstrap_index: int = 0
        self._last_hot_scan_at: float = 0.0
        self._last_background_batch_at: float = 0.0
        self.rotation = SymbolRotationManager()
        self._last_batch: list[str] = []
        self._pending_fast_track: list[str] = []

    @property
    def hot_symbols(self) -> list[str]:
        return list(self._hot)

    @property
    def background_symbols(self) -> list[str]:
        return list(self._background)

    @property
    def full_universe(self) -> list[str]:
        return self._hot + self._background

    def is_priority(self, symbol: str) -> bool:
        return symbol.upper() in {s.upper() for s in self._hot}

    def update(
        self,
        ranked_symbols: list[str],
        *,
        extended_symbols: Optional[list[str]] = None,
        trigger_scores: Optional[dict[str, float]] = None,
        lifecycle: Optional[dict[str, str]] = None,
        fast_track: Optional[list[str]] = None,
    ) -> None:
        """Split ranked universe into priority and rotating queues."""
        pending = list(self._pending_fast_track)
        self._pending_fast_track = []
        merged_fast = []
        seen: set[str] = set()
        for sym in list(fast_track or []) + pending:
            key = sym.upper()
            if key in seen:
                continue
            seen.add(key)
            merged_fast.append(key)

        self._hot, self._background = self.rotation.build_scan_slices(
            ranked_symbols,
            extended_symbols or [],
            trigger_scores,
            lifecycle=lifecycle,
            fast_track=merged_fast,
        )
        if self._background_index >= len(self._background):
            self._background_index = 0

    def fast_track(self, symbols: list[str]) -> list[str]:
        """Instantly promote spike symbols onto the priority queue."""
        if not symbols:
            return []
        added: list[str] = []
        hot_set = {s.upper() for s in self._hot}
        for raw in symbols:
            sym = raw.upper()
            if not sym or sym in hot_set:
                continue
            self.rotation.clear_evaluated([sym])
            self._background = [s for s in self._background if s.upper() != sym]
            self._hot.insert(0, sym)
            hot_set.add(sym)
            added.append(sym)
        cap = max(Config.HOT_SCAN_SIZE, 1)
        overflow = self._hot[cap:]
        self._hot = self._hot[:cap]
        if overflow:
            existing = {s.upper() for s in self._background}
            for sym in overflow:
                if sym.upper() not in existing:
                    self._background.insert(0, sym)
                    existing.add(sym.upper())
        if added:
            self._last_hot_scan_at = 0.0
            self._pending_fast_track.extend(added)
        return added

    def should_run_hot_scan(self) -> bool:
        if not self._hot:
            return False
        if Config.SCAN_ALIGN_TO_MINUTE:
            return True
        interval = max(Config.HOT_SCAN_INTERVAL_SECONDS, 5.0)
        return (time.monotonic() - self._last_hot_scan_at) >= interval

    def mark_hot_scan_complete(self) -> None:
        self._last_hot_scan_at = time.monotonic()

    def should_run_background_batch(self) -> bool:
        if not self._background:
            return False
        if Config.SCAN_ALIGN_TO_MINUTE:
            return True
        delay = max(Config.BACKGROUND_SCAN_BATCH_DELAY_SECONDS, 0.5)
        return (time.monotonic() - self._last_background_batch_at) >= delay

    def next_background_batch(self) -> list[str]:
        """Return next rotating batch of background symbols (deduplicated)."""
        if not self._background:
            return []

        batch_size = max(
            Config.ROTATING_SCAN_BATCH_SIZE,
            Config.BACKGROUND_SCAN_BATCH_SIZE,
            1,
        )
        seen: set[str] = set()
        batch: list[str] = []
        pool_len = len(self._background)

        for _ in range(min(batch_size, pool_len)):
            sym = self._background[self._background_index % pool_len]
            self._background_index = (self._background_index + 1) % pool_len
            if sym in seen:
                continue
            seen.add(sym)
            batch.append(sym)

        self._last_background_batch_at = time.monotonic()
        self._last_batch = list(batch)
        return batch

    def mark_last_background_batch_evaluated(self) -> None:
        """Move the last rotating batch into evaluated memory (skip unless fast-tracked)."""
        if self._last_batch:
            remaining = [
                sym for sym in self._last_batch if not self.is_priority(sym)
            ]
            self.rotation.mark_evaluated(remaining)
            self._last_batch = []

    def next_background_bootstrap_symbols(self, count: int | None = None) -> list[str]:
        """Rotate through background symbols for paced REST kline seeding."""
        if not self._background:
            return []
        n = count if count is not None else max(Config.BACKGROUND_SCAN_BATCH_SIZE, 1)
        n = min(n, len(self._background))
        batch: list[str] = []
        for _ in range(n):
            sym = self._background[self._background_bootstrap_index % len(self._background)]
            self._background_bootstrap_index = (
                self._background_bootstrap_index + 1
            ) % len(self._background)
            if sym not in batch:
                batch.append(sym)
        return batch
