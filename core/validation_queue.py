"""Async 1-coin-per-minute backtest validation queue (Stage 2)."""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from config import Config
from core.candle_backtest import backtest_min_bars, run_15m_backtest
from logger import error_logger, scanner_logger, trade_logger
from utils import safe_float


@dataclass
class _QueuedCandidate:
    payload: dict[str, Any]
    queued_at: float = field(default_factory=time.monotonic)


class AsyncBacktestValidator:
    """
    Lightweight producer (main scan) / single-consumer validator.
    Fetches 500x15m bars and applies dynamic sample WR rules (3/4/5+).
    """

    def __init__(self, exchange: Any) -> None:
        self.exchange = exchange
        self._inbound: queue.Queue[_QueuedCandidate] = queue.Queue(
            maxsize=max(int(Config.BACKTEST_QUEUE_MAX), 1)
        )
        self._approved: queue.Queue[dict[str, Any]] = queue.Queue()
        self._pending: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._last_process_at = 0.0
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="async-backtest-validator",
            daemon=True,
        )
        self._thread.start()
        scanner_logger.info(
            "Async backtest validator started | interval=%.0fs | bars=%s %s | min_wr=%.1f%%",
            Config.BACKTEST_VALIDATION_INTERVAL_SECONDS,
            Config.BACKTEST_CANDLE_LIMIT,
            Config.BACKTEST_TIMEFRAME,
            Config.BACKTEST_MIN_WIN_RATE,
        )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def enqueue_many(self, candidates: list[dict[str, Any]]) -> int:
        added = 0
        for candidate in candidates or []:
            if self.enqueue(candidate):
                added += 1
        return added

    def enqueue(self, candidate: dict[str, Any]) -> bool:
        symbol = str(candidate.get("symbol", "")).upper()
        if not symbol:
            return False
        with self._lock:
            if symbol in self._pending:
                return False
            if self._inbound.full():
                try:
                    dropped = self._inbound.get_nowait()
                    self._pending.discard(
                        str(dropped.payload.get("symbol", "")).upper()
                    )
                    scanner_logger.info(
                        "[BACKTEST_QUEUE] dropped oldest %s — queue full",
                        dropped.payload.get("symbol"),
                    )
                except queue.Empty:
                    pass
            self._pending.add(symbol)
        try:
            self._inbound.put_nowait(_QueuedCandidate(payload=dict(candidate)))
        except queue.Full:
            with self._lock:
                self._pending.discard(symbol)
            return False
        scanner_logger.info(
            "[BACKTEST_QUEUE] queued %s %s strategy=%s score=%.1f",
            symbol,
            candidate.get("action"),
            candidate.get("strategy"),
            safe_float(candidate.get("score")),
        )
        return True

    def drain_approved(self, max_n: int = 3) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while len(out) < max(max_n, 1):
            try:
                out.append(self._approved.get_nowait())
            except queue.Empty:
                break
        return out

    def _loop(self) -> None:
        interval = max(float(Config.BACKTEST_VALIDATION_INTERVAL_SECONDS), 60.0)
        ttl = max(float(Config.BACKTEST_QUEUE_TTL_SECONDS), 30.0)
        while not self._stop.is_set():
            try:
                item = self._inbound.get(timeout=0.5)
            except queue.Empty:
                continue
            symbol = str(item.payload.get("symbol", "")).upper()
            try:
                if (time.monotonic() - item.queued_at) > ttl:
                    trade_logger.warning(
                        "[BACKTEST_REJECTED] %s — queue TTL expired", symbol
                    )
                    continue
                self._wait_rate_limit(interval)
                if self._stop.is_set():
                    break
                approved = self._validate(item.payload)
                if approved is not None:
                    self._approved.put(approved)
            except Exception as exc:
                error_logger.error("Backtest validator failed for %s: %s", symbol, exc)
            finally:
                with self._lock:
                    self._pending.discard(symbol)
                self._last_process_at = time.monotonic()

    def _wait_rate_limit(self, interval: float) -> None:
        if self._last_process_at <= 0:
            return
        remaining = interval - (time.monotonic() - self._last_process_at)
        while remaining > 0 and not self._stop.is_set():
            time.sleep(min(0.25, remaining))
            remaining = interval - (time.monotonic() - self._last_process_at)

    def _validate(self, candidate: dict[str, Any]) -> Optional[dict[str, Any]]:
        symbol = str(candidate.get("symbol", "")).upper()
        blocked, reason = self.exchange.is_rest_blocked()
        if blocked:
            trade_logger.warning(
                "[BACKTEST_REJECTED] %s — REST blocked (%s)", symbol, reason
            )
            return None

        df = self._fetch_15m_history(symbol)
        result = run_15m_backtest(df)
        if not result.passed:
            if result.reason == "Insufficient historical trade samples":
                trade_logger.warning(
                    "[BACKTEST_REJECTED] %s — Insufficient historical trade samples "
                    "(closed=%s wr=%.1f%%)",
                    symbol,
                    result.trades,
                    result.win_rate,
                )
            else:
                trade_logger.warning(
                    "[BACKTEST_REJECTED] %s wr=%.1f%% trades=%s reason=%s",
                    symbol,
                    result.win_rate,
                    result.trades,
                    result.reason,
                )
            return None

        approved = dict(candidate)
        meta = dict(approved.get("structure_metadata") or {})
        meta.update(
            {
                "backtest_validated": True,
                "backtest_win_rate": result.win_rate,
                "backtest_trades": result.trades,
                "backtest_wins": result.wins,
                "backtest_expectancy_r": result.expectancy_r,
                "backtest_profit_r": result.profit_r,
                "backtest_bars": result.bars_used,
            }
        )
        approved["structure_metadata"] = meta
        approved["backtest_validated"] = True
        approved["backtest_win_rate"] = result.win_rate
        approved["backtest_wins"] = result.wins
        approved["backtest_trades"] = result.trades
        if result.last_atr > 0:
            approved["atr"] = result.last_atr
        trade_logger.info(
            "[BACKTEST_PASSED] %s wr=%.1f%% trades=%s exp_r=%.2f atr=%.6f",
            symbol,
            result.win_rate,
            result.trades,
            result.expectancy_r,
            approved.get("atr") or 0.0,
        )
        return approved

    def _fetch_15m_history(self, symbol: str) -> Optional[Any]:
        timeframe = str(Config.BACKTEST_TIMEFRAME or "15m")
        fetch_limit = max(int(Config.BACKTEST_CANDLE_LIMIT), 200)
        min_bars = backtest_min_bars()
        hub = getattr(self.exchange, "_market_data", None)
        cached = None
        try:
            cached = self.exchange.fetch_historical_candles(
                symbol, timeframe, limit=fetch_limit, allow_rest=False
            )
        except Exception:
            cached = None
        if cached is not None and not cached.empty and len(cached) >= min_bars:
            return cached

        can_boot = getattr(self.exchange, "can_bootstrap_klines_rest", None)
        if callable(can_boot) and not can_boot():
            return cached
        try:
            with self.exchange.bootstrap_context():
                df = self.exchange.fetch_bootstrap_klines_df(
                    symbol, timeframe, fetch_limit
                )
        except Exception as exc:
            error_logger.warning(
                "Backtest 15m fetch failed for %s: %s", symbol, exc
            )
            return cached
        if df is not None and not df.empty and hub is not None:
            seeder = getattr(hub, "seed_klines_from_dataframe", None)
            if callable(seeder):
                try:
                    seeder(symbol, timeframe, df)
                except Exception:
                    pass
        return df
