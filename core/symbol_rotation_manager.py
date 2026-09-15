"""Dynamic symbol rotation — active watch, evaluated memory, and pool refresh."""

from __future__ import annotations

import random
import time
from typing import Optional

from config import Config
from logger import scanner_logger


class SymbolRotationManager:
    """
    Manages Top-60 pool rotation:
    - active_watch: top N symbols closest to trigger (hot scan)
    - background: next batch excluding evaluated memory
    - evaluated memory: recently scanned non-trigger symbols (45–60 min TTL)
    - full purge every ROTATION_MEMORY_PURGE_HOURS
    """

    def __init__(self) -> None:
        self._evaluated: dict[str, float] = {}
        self._last_full_purge_at: float = time.monotonic()
        self._rotation_cycle: int = 0

    @property
    def evaluated_count(self) -> int:
        self.cleanup_expired()
        return len(self._evaluated)

    @property
    def rotation_cycle(self) -> int:
        return self._rotation_cycle

    def cleanup_expired(self) -> int:
        now = time.monotonic()
        expired = [sym for sym, until in self._evaluated.items() if until <= now]
        for sym in expired:
            del self._evaluated[sym]
        return len(expired)

    def maybe_full_purge(self) -> bool:
        """Purge evaluated memory and reset rotation every 3–4 hours."""
        if not Config.ENABLE_DYNAMIC_SYMBOL_ROTATION:
            return False
        purge_seconds = max(Config.ROTATION_MEMORY_PURGE_HOURS, 0.5) * 3600.0
        now = time.monotonic()
        if (now - self._last_full_purge_at) < purge_seconds:
            return False
        cleared = len(self._evaluated)
        self._evaluated.clear()
        self._last_full_purge_at = now
        self._rotation_cycle += 1
        scanner_logger.info(
            "Symbol rotation full purge — cleared %s evaluated symbols (cycle=%s).",
            cleared,
            self._rotation_cycle,
        )
        return True

    def is_in_evaluated_memory(self, symbol: str) -> bool:
        self.cleanup_expired()
        return symbol.upper() in self._evaluated

    def mark_evaluated(self, symbols: list[str]) -> None:
        """Add symbols to evaluated memory after a background scan batch."""
        if not Config.ENABLE_DYNAMIC_SYMBOL_ROTATION or not symbols:
            return
        self.cleanup_expired()
        min_m = max(Config.ROTATION_EVALUATED_MEMORY_MIN_MINUTES, 1)
        max_m = max(Config.ROTATION_EVALUATED_MEMORY_MAX_MINUTES, min_m)
        now = time.monotonic()
        for symbol in symbols:
            sym = symbol.upper()
            ttl_min = random.randint(min_m, max_m)
            self._evaluated[sym] = now + ttl_min * 60.0
        scanner_logger.debug(
            "Evaluated memory +%s symbols (total=%s, ttl=%s-%s min).",
            len(symbols),
            len(self._evaluated),
            min_m,
            max_m,
        )

    def build_scan_slices(
        self,
        primary_pool: list[str],
        extended_pool: list[str],
        trigger_scores: Optional[dict[str, float]] = None,
    ) -> tuple[list[str], list[str]]:
        """
        Split pools into active_watch (hot) and background scan queues.
        Returns (active_watch, background).
        """
        if not Config.ENABLE_DYNAMIC_SYMBOL_ROTATION:
            ranked = [s.upper() for s in primary_pool if s]
            hot_size = max(Config.HOT_SCAN_SIZE, 1)
            return ranked[:hot_size], ranked[hot_size:]

        self.maybe_full_purge()
        self.cleanup_expired()

        scores = {k.upper(): float(v) for k, v in (trigger_scores or {}).items()}
        hot_size = max(Config.HOT_SCAN_SIZE, 1)
        bg_target = max(Config.TOP_UNIVERSE_POOL_SIZE - hot_size, 1)

        primary = [s.upper() for s in primary_pool if s]
        extended = [s.upper() for s in extended_pool if s and s.upper() not in primary]

        def rank_key(sym: str) -> tuple[float, float]:
            return (scores.get(sym, 0.0), -primary.index(sym) if sym in primary else 0.0)

        ranked_primary = sorted(primary, key=rank_key, reverse=True)
        active_watch = ranked_primary[:hot_size]

        blocked = set(self._evaluated.keys()) | set(active_watch)
        background: list[str] = []

        for sym in ranked_primary[hot_size:]:
            if sym in blocked:
                continue
            background.append(sym)
            if len(background) >= bg_target:
                break

        if len(background) < bg_target:
            for sym in extended:
                if sym in blocked or sym in background:
                    continue
                background.append(sym)
                if len(background) >= bg_target:
                    break

        if background:
            scanner_logger.debug(
                "Rotation slices — active_watch=%s background=%s evaluated=%s cycle=%s.",
                len(active_watch),
                len(background),
                len(self._evaluated),
                self._rotation_cycle,
            )
        return active_watch, background
