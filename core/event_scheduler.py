"""Candle-close event queue with stagger, dedup, and catch-up tracking."""

from __future__ import annotations

import heapq
import threading
import time
from typing import Callable, Optional

from config import Config
from core.types import CandleCloseEvent
from logger import scanner_logger


class EventScheduler:
    """
    Queue candle-close evaluations triggered by WS kline closes.
    Staggers work to avoid thundering herd at bar boundaries.
    """

    def __init__(
        self,
        trigger_timeframes: Optional[list[str]] = None,
        stagger_ms: Optional[float] = None,
    ) -> None:
        self._trigger_tfs = set(
            trigger_timeframes or Config.get_scan_trigger_timeframes()
        )
        self._stagger_sec = max(stagger_ms or Config.EVENT_EVAL_STAGGER_MS, 0.0) / 1000.0
        self._lock = threading.RLock()
        self._heap: list[tuple[float, int, CandleCloseEvent]] = []
        self._seq = 0
        self._seen: dict[tuple[str, str, int], float] = {}
        self._last_evaluated: dict[tuple[str, str], int] = {}
        self._seen_ttl = 3600.0

    @property
    def trigger_timeframes(self) -> set[str]:
        return set(self._trigger_tfs)

    def on_candle_close(
        self,
        symbol: str,
        timeframe: str,
        bar_open_ms: int,
        *,
        source: str = "ws",
    ) -> None:
        symbol = symbol.upper()
        if timeframe not in self._trigger_tfs or bar_open_ms <= 0:
            return

        dedup_key = (symbol, timeframe, bar_open_ms)
        now = time.monotonic()
        with self._lock:
            if dedup_key in self._seen:
                return
            self._seen[dedup_key] = now
            self._purge_seen(now)

            stagger = (hash(symbol) % 1000) * self._stagger_sec
            due_at = now + stagger
            self._seq += 1
            event = CandleCloseEvent(
                symbol=symbol,
                timeframe=timeframe,
                bar_open_ms=bar_open_ms,
                source=source,  # type: ignore[arg-type]
                due_at=due_at,
            )
            heapq.heappush(self._heap, (due_at, self._seq, event))

    def drain_due(self, limit: int = 50) -> list[CandleCloseEvent]:
        """Return events whose stagger delay has elapsed."""
        now = time.monotonic()
        due: list[CandleCloseEvent] = []
        with self._lock:
            while self._heap and len(due) < limit:
                due_at, _, event = self._heap[0]
                if due_at > now:
                    break
                heapq.heappop(self._heap)
                due.append(event)
        return due

    def pending_count(self) -> int:
        with self._lock:
            return len(self._heap)

    def mark_evaluated(self, symbol: str, timeframe: str, bar_open_ms: int) -> None:
        symbol = symbol.upper()
        with self._lock:
            prev = self._last_evaluated.get((symbol, timeframe), 0)
            if bar_open_ms > prev:
                self._last_evaluated[(symbol, timeframe)] = bar_open_ms

    def last_evaluated(self, symbol: str, timeframe: str) -> int:
        return self._last_evaluated.get((symbol.upper(), timeframe), 0)

    def run_catchup(
        self,
        symbols: list[str],
        bar_open_ms_fn: Callable[[str, str], Optional[int]],
    ) -> int:
        """
        Enqueue missed closes for symbols where last closed bar > last_evaluated.
        Returns count of catch-up events scheduled.
        """
        scheduled = 0
        for symbol in symbols:
            sym = symbol.upper()
            for tf in self._trigger_tfs:
                closed_open_ms = bar_open_ms_fn(sym, tf)
                if closed_open_ms is None or closed_open_ms <= 0:
                    continue
                if closed_open_ms <= self.last_evaluated(sym, tf):
                    continue
                before = self.pending_count()
                self.on_candle_close(sym, tf, closed_open_ms, source="catchup")
                if self.pending_count() > before:
                    scheduled += 1
        if scheduled:
            scanner_logger.info(
                "Catch-up scheduled %s missed candle-close evaluation(s).", scheduled
            )
        return scheduled

    def _purge_seen(self, now: float) -> None:
        cutoff = now - self._seen_ttl
        stale = [key for key, ts in self._seen.items() if ts < cutoff]
        for key in stale:
            del self._seen[key]
