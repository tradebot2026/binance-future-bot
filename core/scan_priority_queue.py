"""Tiered scan priority — hot watchlist vs background rotation queue."""

from __future__ import annotations

import time

from config import Config


class ScanPriorityQueue:
    """
    Tier 1 (hot): top N high-activity symbols scanned frequently (WS-only).
    Tier 2 (background): remaining symbols rotated in small batches.
    """

    def __init__(self) -> None:
        self._hot: list[str] = []
        self._background: list[str] = []
        self._background_index: int = 0
        self._background_bootstrap_index: int = 0
        self._last_hot_scan_at: float = 0.0
        self._last_background_batch_at: float = 0.0

    @property
    def hot_symbols(self) -> list[str]:
        return list(self._hot)

    @property
    def background_symbols(self) -> list[str]:
        return list(self._background)

    @property
    def full_universe(self) -> list[str]:
        return self._hot + self._background

    def update(self, ranked_symbols: list[str]) -> None:
        """Split ranked universe into hot watchlist and background queue."""
        ranked = [s.upper() for s in ranked_symbols if s]
        hot_size = max(Config.HOT_SCAN_SIZE, 1)
        self._hot = ranked[:hot_size]
        self._background = ranked[hot_size:]
        if self._background_index >= len(self._background):
            self._background_index = 0

    def should_run_hot_scan(self) -> bool:
        if not self._hot:
            return False
        interval = max(Config.HOT_SCAN_INTERVAL_SECONDS, 5.0)
        return (time.monotonic() - self._last_hot_scan_at) >= interval

    def mark_hot_scan_complete(self) -> None:
        self._last_hot_scan_at = time.monotonic()

    def should_run_background_batch(self) -> bool:
        if not self._background:
            return False
        delay = max(Config.BACKGROUND_SCAN_BATCH_DELAY_SECONDS, 0.5)
        return (time.monotonic() - self._last_background_batch_at) >= delay

    def next_background_batch(self) -> list[str]:
        """Return next rotating batch of background symbols (deduplicated)."""
        if not self._background:
            return []

        batch_size = max(Config.BACKGROUND_SCAN_BATCH_SIZE, 1)
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
        return batch

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
