"""
Market scanner module.
Facade over the event-driven orchestrator.
Universe ranking lives in pipeline.universe_builder; strategy evaluation
lives in strategies/ + engines/.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from config import Config
from database import DatabaseManager
from exchange import BinanceExchangeManager
from indicators.market_analyzer import MIN_ANALYZER_BARS, MarketAnalyzer
from logger import scanner_logger
from pipeline.event_scan_orchestrator import EventScanOrchestrator
from pipeline.scanner_pipeline import StrategyScannerPipeline
from pipeline.universe_builder import UniverseBuilder, UniverseFilterStats

# Backward-compatible re-export
__all__ = ["MarketAnalyzer", "MarketScanner", "MIN_ANALYZER_BARS", "UniverseFilterStats"]

# Removed from the live scanner facade. Ranking is UniverseBuilder;
# evaluation is EventScanOrchestrator + strategy modules.
_QUARANTINED_SCANNER_METHODS = (
    "evaluate_single_symbol",
    "evaluate_range_symbol",
    "_build_universe_candidates",
    "scan_market",
    "scan_unified",
    "scan_range_market",
)


class MarketScanner:
    """Scans Binance Futures via the modular pipeline / event orchestrator."""

    def __init__(self, exchange: BinanceExchangeManager, db: DatabaseManager) -> None:
        self.exchange = exchange
        self.db = db
        self.analyzer = MarketAnalyzer()
        self.entry_tf = Config.ENTRY_TIMEFRAME
        self.confirm_tf = Config.CONFIRM_TIMEFRAME
        self.trend_tf = Config.TREND_TIMEFRAME
        self.candle_limit = Config.CANDLE_FETCH_LIMIT
        self._hub = getattr(exchange, "_market_data", None)
        self._scan_priority: list[str] = []
        self._pipeline = StrategyScannerPipeline(exchange, db)
        self._universe_builder = UniverseBuilder(exchange, db)
        self.orchestrator = EventScanOrchestrator(exchange, db)
        self._total_flush_count: int = 0

    @property
    def _last_universe_symbols(self) -> list[str]:
        return self._pipeline.last_universe_symbols

    @_last_universe_symbols.setter
    def _last_universe_symbols(self, value: list[str]) -> None:
        self._pipeline._last_universe_symbols = value

    def get_tradable_symbols(self) -> tuple[list[str], dict[str, float]]:
        """
        Build dynamic scan universe from WS ticker cache.
        On Testnet (or when strict filters yield too few symbols), filters relax
        automatically to keep at least UNIVERSE_RELAXED_MIN_SYMBOLS liquid pairs.
        """
        if self._hub is not None:
            self._hub.ensure_ticker_cache_ready()
        result = self._universe_builder.build(priority_symbols=self._scan_priority)
        self._pipeline._last_universe_symbols = result.symbols
        if (
            result.symbols
            and result.stats.filter_profile != "strict"
            and Config.USE_TESTNET
        ):
            scanner_logger.debug(
                "Testnet universe built with profile=%s (%s symbols).",
                result.stats.filter_profile,
                len(result.symbols),
            )
        return result.symbols, result.price_map

    def ensure_scan_klines_ready(self, symbols: Optional[list[str]] = None) -> int:
        """
        One-time REST kline seed for scan universe — runs outside scan_context.
        Skips pairs already bootstrapped; safe to call before each scan batch.
        """
        if not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP or not self._hub:
            return 0
        if self.exchange.in_scan_mode:
            return 0
        if symbols is None:
            symbols, _ = self.get_tradable_symbols()
        if not symbols:
            return 0

        timeframes = Config.get_scan_kline_intervals()
        with self.exchange.bootstrap_context():
            return self._hub.subscribe_and_bootstrap_klines(
                symbols,
                timeframes,
                self.exchange.fetch_bootstrap_klines_df,
            )

    def refresh_event_universe(self) -> list[str]:
        """Refresh Tier-1 watchlist for event-driven scanning."""
        if self.orchestrator is None:
            return []
        return self.orchestrator.bootstrap_watchlist_once()

    def subscribe_watchlist_ws_only(self) -> list[str]:
        """Subscribe kline streams from the current ticker cache — no REST poll."""
        if self.orchestrator is None:
            return []
        if self._hub is not None and not self._hub.is_ticker_cache_usable():
            self._hub.wait_quietly_for_ticker_cache(timeout_seconds=5.0)
        if self._hub is not None and not self.orchestrator.tier1_symbols:
            return self.orchestrator.bootstrap_watchlist_once()
        return self.orchestrator.refresh_tier1_universe(force=True, allow_rest=False)

    def bootstrap_scan_universe(self) -> list[str]:
        """One-shot REST universe seed + Tier-1 watchlist at startup."""
        if self.orchestrator is None:
            return []
        return self.orchestrator.bootstrap_watchlist_once()

    def process_event_scan_cycle(self) -> List[Dict[str, Any]]:
        """Drain candle-close queue and return execution candidates."""
        if self.orchestrator is None:
            return []
        return self.orchestrator.process_due_events()

    def process_priority_scan_cycle(self) -> List[Dict[str, Any]]:
        """Tiered hot/background/event scan cycle (WS-only evaluation)."""
        if self.orchestrator is None:
            return []
        candidates = self.orchestrator.process_priority_scan_cycle()
        if candidates:
            scanner_logger.info(
                "Scanner forwarding %s pick_best winner(s) to execute_trade().",
                len(candidates),
            )
        return candidates

    def bootstrap_hot_symbols_at_startup(self) -> int:
        """Paced REST bootstrap for Tier-1 hot watchlist only."""
        if not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP or not self._hub:
            return 0
        if self.orchestrator is None:
            return self.ensure_scan_klines_ready()
        symbols = self.orchestrator.priority_queue.hot_symbols
        if not symbols:
            symbols = self.orchestrator.tier1_symbols[: Config.scan_watchlist_size()]
        return self.ensure_scan_klines_ready(symbols)

    def bootstrap_background_klines(self) -> int:
        """Paced REST bootstrap for queued scan misses / one background symbol."""
        if (
            not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP
            or not self._hub
            or self.orchestrator is None
        ):
            return 0
        symbols = self.orchestrator.take_kline_cache_misses(8)
        if not symbols:
            symbols = self.orchestrator.priority_queue.next_background_bootstrap_symbols(1)
        if not symbols:
            return 0
        return self.orchestrator._bootstrap_missing_scan_klines(symbols)

    def populate_warmup_kline_cache(self) -> int:
        """Paced REST kline populate during WARMUP_MODE (one symbol per tick)."""
        if self._hub is not None and not self._hub.is_ticker_cache_usable():
            self._hub.wait_quietly_for_ticker_cache(timeout_seconds=5.0)
        if (
            not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP
            or not self._hub
            or self.orchestrator is None
        ):
            return 0
        if not self._hub.is_ticker_cache_usable():
            return 0
        return self.orchestrator.populate_warmup_klines()

    def warmup_and_evaluate_kline_misses(self) -> List[Dict[str, Any]]:
        """Bootstrap one not-ready miss (missing TFs only), then evaluate if complete."""
        if self.orchestrator is None:
            return []
        return self.orchestrator.warmup_and_evaluate_kline_misses()

    def get_tier2_summary(self) -> list[tuple[str, str, float]]:
        """Return Tier-2 active coins: (symbol, strategy, score)."""
        if self.orchestrator is None:
            return []
        return self.orchestrator.tier2_summary()

    def gc_flushed_klines(self, symbols: list[str]) -> int:
        """Drop flushed Normal kline streams/buffers in one hub rebuild."""
        hub = self._hub
        if hub is None or not symbols:
            return 0
        batch = getattr(hub, "demote_symbols_klines", None)
        if callable(batch):
            return int(batch(symbols) or 0)
        drop = getattr(hub, "demote_symbol_klines", None)
        if not callable(drop):
            return 0
        for symbol in symbols:
            drop(symbol)
        return len(symbols)

    @property
    def total_flush_count(self) -> int:
        funnel = getattr(self.orchestrator, "funnel", None) if self.orchestrator else None
        if funnel is not None:
            self._total_flush_count = int(getattr(funnel, "flush_count", 0) or 0)
        return int(self._total_flush_count)

    def get_watchlist_tiers(self) -> dict[str, Any]:
        """Return live Normal ingest progress plus Hot/Super snapshots."""
        empty = {
            "tier1_hot": [],
            "tier1_background": [],
            "tier1_full": [],
            "tier2": [],
            "tier2_near_miss": [],
            "pass_number": 1,
            "passes_total": 3,
            "current_index": 0,
            "universe_size": 120,
            "ingested_count": 0,
            "currently_scanning": "",
            "recently_scanned": [],
            "normal_scores": {},
            "flush_minutes": 180,
            "flush_count": 0,
        }
        if self.orchestrator is None:
            return empty
        orchestrator = self.orchestrator
        funnel = getattr(orchestrator, "funnel", None)
        snap = funnel.watchlist_snapshot() if funnel is not None else {}
        recently = list(snap.get("recently_scanned") or [])
        ingested = list(getattr(funnel, "normal_symbols", None) or [])
        flush_count = int(snap.get("flush_count") or 0)
        self._total_flush_count = flush_count
        return {
            "tier1_hot": list(getattr(funnel, "hot_symbols", None) or orchestrator.priority_queue.hot_symbols),
            "tier1_background": orchestrator.priority_queue.background_symbols,
            "tier1_full": recently or ingested,
            "rotation_evaluated": orchestrator.priority_queue.rotation.evaluated_count,
            "rotation_cycle": int(
                getattr(funnel, "lock_cycle", None)
                or orchestrator.priority_queue.rotation.rotation_cycle
            ),
            "lock_cycle": int(getattr(funnel, "lock_cycle", 0) or 0),
            "tier2": orchestrator.tier2_summary(),
            "super": funnel.super_summary() if funnel is not None else [],
            "tier2_near_miss": orchestrator.assignment_manager.near_miss_summary(),
            "pass_number": int(snap.get("pass_number") or 1),
            "passes_total": int(snap.get("passes_total") or 3),
            "current_index": int(snap.get("current_index") or 0),
            "universe_size": int(snap.get("universe_size") or 120),
            "ingested_count": int(snap.get("ingested_count") or len(ingested)),
            "currently_scanning": str(snap.get("currently_scanning") or ""),
            "recently_scanned": recently,
            "normal_scores": dict(snap.get("normal_scores") or {}),
            "flush_minutes": int(snap.get("flush_minutes") or 0),
            "flush_count": flush_count,
        }
