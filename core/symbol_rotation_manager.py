"""Dynamic symbol rotation — lifecycle-aware dual queues and evaluated memory."""

from __future__ import annotations

import random
import time
from typing import Optional

from config import Config
from core.opportunity_tracker import PRIORITY_STATES, SCANNABLE_STATES
from core.types import CoinLifecycle
from logger import scanner_logger


class SymbolRotationManager:
    """
    Dual queue:
    - priority (hot): HOT / OPPORTUNITY plus fast-tracked spikes
    - rotating: ACTIVE / WATCH / CANDIDATE in batches; DORMANT excluded
    Evaluated memory skips rotating symbols only (never blocks priority).
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
        """Add symbols to evaluated memory after a rotating-batch scan."""
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

    def clear_evaluated(self, symbols: list[str]) -> None:
        """Allow fast-tracked coins to be scanned immediately."""
        for symbol in symbols:
            self._evaluated.pop(symbol.upper(), None)

    def build_scan_slices(
        self,
        primary_pool: list[str],
        extended_pool: list[str],
        trigger_scores: Optional[dict[str, float]] = None,
        *,
        lifecycle: Optional[dict[str, str]] = None,
        fast_track: Optional[list[str]] = None,
    ) -> tuple[list[str], list[str]]:
        """
        Split pools into priority (hot) and rotating background queues.
        Returns (priority, rotating).
        """
        scores = {k.upper(): float(v) for k, v in (trigger_scores or {}).items()}
        life = {
            str(k).upper(): str(v).upper()
            for k, v in (lifecycle or {}).items()
        }
        hot_size = max(Config.HOT_SCAN_SIZE, 1)

        if not Config.ENABLE_DYNAMIC_SYMBOL_ROTATION:
            ranked = [s.upper() for s in primary_pool if s]
            ranked.sort(key=lambda sym: scores.get(sym, 0.0), reverse=True)
            return ranked[:hot_size], ranked[hot_size:]

        self.maybe_full_purge()
        self.cleanup_expired()

        primary = [s.upper() for s in primary_pool if s]
        extended = [s.upper() for s in extended_pool if s and s.upper() not in primary]
        universe = primary + [s for s in extended if s not in primary]
        primary_set = set(primary)

        def rank_key(sym: str) -> tuple[float, float]:
            return (scores.get(sym, 0.0), -primary.index(sym) if sym in primary else 0.0)

        fast = [s.upper() for s in (fast_track or []) if s]
        self.clear_evaluated(fast)

        priority: list[str] = []
        seen: set[str] = set()

        def _push(sym: str) -> None:
            if not sym or sym in seen:
                return
            state = life.get(sym, "")
            if state == CoinLifecycle.DORMANT.value:
                return
            seen.add(sym)
            priority.append(sym)

        for sym in fast:
            _push(sym)
        for sym in sorted(universe, key=rank_key, reverse=True):
            state = life.get(sym, "")
            if state in {item.value for item in PRIORITY_STATES} or (
                scores.get(sym, 0.0) >= Config.OPPORTUNITY_HOT_MIN and sym in primary_set
            ):
                _push(sym)
            if len(priority) >= hot_size:
                break

        if len(priority) < hot_size:
            for sym in sorted(primary, key=rank_key, reverse=True):
                _push(sym)
                if len(priority) >= hot_size:
                    break

        priority = priority[:hot_size]
        blocked = set(self._evaluated.keys()) | set(priority)
        rotating: list[str] = []
        scannable = {item.value for item in SCANNABLE_STATES}

        def _accept_rotating(sym: str) -> bool:
            if sym in blocked or sym in rotating:
                return False
            state = life.get(sym, CoinLifecycle.ACTIVE.value)
            if state == CoinLifecycle.DORMANT.value:
                return False
            if life and state not in scannable:
                return False
            return True

        weakened: list[str] = []
        bg_target = max(Config.TOP_UNIVERSE_POOL_SIZE - len(priority), 1)
        for pool in (primary[len(priority) :], extended, primary):
            for sym in pool:
                if not _accept_rotating(sym):
                    continue
                if life.get(sym) == CoinLifecycle.WEAKENED.value:
                    weakened.append(sym)
                    continue
                rotating.append(sym)
                if len(rotating) >= bg_target:
                    break
            if len(rotating) >= bg_target:
                break

        if len(rotating) < bg_target:
            for sym in weakened:
                if _accept_rotating(sym):
                    rotating.append(sym)
                if len(rotating) >= bg_target:
                    break

        if rotating or priority:
            scanner_logger.debug(
                "Rotation slices — priority=%s rotating=%s evaluated=%s cycle=%s.",
                len(priority),
                len(rotating),
                len(self._evaluated),
                self._rotation_cycle,
            )
        return priority, rotating
