"""Tests for fail-closed DB gates and WS-only opportunity ranking."""

from __future__ import annotations

import sqlite3
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from core.opportunity_tracker import (
    OpportunityTracker,
    TickerFeatures,
    score_channels,
    build_score_context,
    composite_opportunity_score,
)
from core.minute_clock import MinuteScanClock
from core.scan_priority_queue import ScanPriorityQueue
from core.types import CandleCloseEvent, CoinLifecycle, SignalCandidate
from database import DatabaseManager
from exceptions import DatabaseError
from pipeline.event_scan_orchestrator import EventScanOrchestrator
from pipeline.universe_builder import UniverseBuilder
from risk_manager import RiskManager
from scanner import MarketScanner, _QUARANTINED_SCANNER_METHODS


def _feat(
    symbol: str,
    *,
    last: float = 100.0,
    open_: float = 98.0,
    high: float = 104.0,
    low: float = 97.0,
    volume: float = 20_000_000.0,
    spread: float = 0.02,
    range_pct: float = 4.0,
    change_pct: float = 2.0,
    **kwargs: float,
) -> TickerFeatures:
    return TickerFeatures(
        symbol=symbol,
        last_price=last,
        open_price=open_,
        high_price=high,
        low_price=low,
        volume_24h=volume,
        spread_pct=spread,
        range_pct=range_pct,
        change_pct=change_pct,
        **kwargs,
    )


class TestFailClosedDatabase(unittest.TestCase):
    def test_get_active_trades_count_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(Config, "DB_PATH", f"{tmp}/test.db"):
                db = DatabaseManager()
                with patch.object(db, "connection") as cm:
                    inner = MagicMock()
                    inner.execute.side_effect = sqlite3.Error("locked")
                    cm.return_value.__enter__.return_value = inner
                    with self.assertRaises(DatabaseError):
                        db.get_active_trades_count()

    def test_count_by_strategy_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(Config, "DB_PATH", f"{tmp}/test.db"):
                db = DatabaseManager()
                with patch.object(db, "connection") as cm:
                    inner = MagicMock()
                    inner.execute.side_effect = sqlite3.Error("locked")
                    cm.return_value.__enter__.return_value = inner
                    with self.assertRaises(DatabaseError):
                        db.count_active_trades_by_strategy("SMC_TREND")

    def test_risk_snapshot_blocks_entries_on_db_error(self) -> None:
        db = MagicMock()
        db.get_active_trades_count.side_effect = DatabaseError("locked")
        exchange = MagicMock()
        exchange.get_open_positions_count.return_value = 0
        exchange.get_futures_balance.return_value = 1000.0
        risk = RiskManager(exchange, db)
        snapshot = risk.get_risk_snapshot()
        self.assertFalse(snapshot.entries_allowed)
        self.assertIn("Database unavailable", snapshot.block_reason)


class TestOpportunityScoring(unittest.TestCase):
    def test_channel_scores_are_bounded(self) -> None:
        rows = [
            _feat("AAAUSDT", volume=50_000_000.0, range_pct=6.0, change_pct=5.0),
            _feat("BBBUSDT", volume=5_000_000.0, range_pct=0.4, change_pct=0.1),
        ]
        ctx = build_score_context(rows)
        for row in rows:
            channels = score_channels(row, ctx)
            self.assertGreaterEqual(channels.momentum, 0.0)
            self.assertLessEqual(channels.momentum, 100.0)
            self.assertGreaterEqual(channels.reversal, 0.0)
            self.assertLessEqual(channels.reversal, 100.0)
            self.assertGreaterEqual(channels.structure, 0.0)
            self.assertLessEqual(channels.structure, 100.0)
            composite = composite_opportunity_score(channels)
            self.assertGreaterEqual(composite, 0.0)
            self.assertLessEqual(composite, 100.0)
        strong = score_channels(rows[0], ctx)
        weak = score_channels(rows[1], ctx)
        self.assertGreater(
            composite_opportunity_score(strong),
            composite_opportunity_score(weak),
        )

    def test_momentum_dominates_breakout_features(self) -> None:
        breakout = _feat(
            "AAAUSDT",
            volume=80_000_000.0,
            range_pct=7.5,
            change_pct=5.5,
            bar_range_ratio=90.0,
        )
        squeeze = _feat(
            "BBBUSDT",
            volume=8_000_000.0,
            range_pct=0.35,
            change_pct=0.15,
            compression=85.0,
        )
        ctx = build_score_context([breakout, squeeze])
        breakout_channels = score_channels(breakout, ctx)
        squeeze_channels = score_channels(squeeze, ctx)
        self.assertEqual(breakout_channels.dominant, "momentum")
        self.assertGreater(breakout_channels.momentum, squeeze_channels.momentum)
        self.assertGreaterEqual(squeeze_channels.structure, squeeze_channels.momentum)

    def test_relative_rank_orders_by_opportunity_score(self) -> None:
        tracker = OpportunityTracker()
        tracker.ingest(
            [
                _feat("WEAKUSDT", volume=2_000_000.0, range_pct=0.3, change_pct=0.1),
                _feat("STRONGUSDT", volume=80_000_000.0, range_pct=7.0, change_pct=5.0),
                _feat("MIDUSDT", volume=20_000_000.0, range_pct=2.0, change_pct=1.5),
            ],
            now=1.0,
        )
        self.assertEqual(tracker.get("STRONGUSDT").relative_rank, 1)
        self.assertEqual(tracker.get("MIDUSDT").relative_rank, 2)
        self.assertEqual(tracker.get("WEAKUSDT").relative_rank, 3)
        self.assertGreater(
            tracker.get("STRONGUSDT").score,
            tracker.get("MIDUSDT").score,
        )

    def test_velocity_rank_and_hot_fast_track(self) -> None:
        tracker = OpportunityTracker()
        first = [_feat("AAAUSDT", last=100.0, volume=10_000_000.0)]
        tracker.ingest(first, now=100.0)
        rec = tracker.get("AAAUSDT")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.relative_rank, 1)
        self.assertNotEqual(rec.lifecycle, CoinLifecycle.UNSEEN)

        spiked = [
            _feat(
                "AAAUSDT",
                last=102.0,
                volume=14_000_000.0,
                range_pct=7.5,
                change_pct=4.5,
            )
        ]
        hot = tracker.ingest(spiked, now=110.0)
        rec = tracker.get("AAAUSDT")
        self.assertGreater(rec.velocity, 0.0)
        self.assertIn(rec.lifecycle, (CoinLifecycle.HOT, CoinLifecycle.OPPORTUNITY, CoinLifecycle.ACTIVE, CoinLifecycle.WATCH, CoinLifecycle.CANDIDATE))
        self.assertTrue(isinstance(hot, list))

    def test_stale_decay_moves_to_dormant(self) -> None:
        tracker = OpportunityTracker()
        tracker.ingest([_feat("AAAUSDT", range_pct=5.0, change_pct=3.0)], now=1.0)
        rec = tracker.get("AAAUSDT")
        self.assertIsNotNone(rec)
        self.assertGreater(rec.score, 0.0)
        tracker.ingest([], now=1.0 + Config.OPPORTUNITY_STALE_SECONDS + 10.0)
        rec = tracker.get("AAAUSDT")
        if rec is not None:
            self.assertEqual(rec.lifecycle, CoinLifecycle.DORMANT)
            self.assertLess(rec.score, rec.peak_score)


class TestDualQueueFastTrack(unittest.TestCase):
    def test_fast_track_jumps_rotating_symbol(self) -> None:
        queue = ScanPriorityQueue()
        queue.update(
            ["AAAUSDT", "BBBUSDT", "CCCUSDT"],
            extended_symbols=["DDDUSDT"],
            trigger_scores={"AAAUSDT": 40.0, "BBBUSDT": 38.0, "CCCUSDT": 36.0},
            lifecycle={
                "AAAUSDT": CoinLifecycle.ACTIVE.value,
                "BBBUSDT": CoinLifecycle.WATCH.value,
                "CCCUSDT": CoinLifecycle.CANDIDATE.value,
                "DDDUSDT": CoinLifecycle.WATCH.value,
            },
        )
        self.assertTrue(queue.full_universe)
        added = queue.fast_track(["DDDUSDT"])
        self.assertEqual(added, ["DDDUSDT"])
        self.assertTrue(queue.is_priority("DDDUSDT"))
        self.assertNotIn("DDDUSDT", queue.background_symbols)

    def test_rotating_batch_size(self) -> None:
        queue = ScanPriorityQueue()
        symbols = [f"S{i:02d}USDT" for i in range(40)]
        queue.update(symbols, trigger_scores={s: 50.0 - i for i, s in enumerate(symbols)})
        batch = queue.next_background_batch()
        self.assertLessEqual(len(batch), max(Config.ROTATING_SCAN_BATCH_SIZE, 1))
        self.assertGreaterEqual(len(batch), 1)


class TestUniverseBuilderWsOnly(unittest.TestCase):
    def test_build_uses_hub_ticker_cache_not_rest(self) -> None:
        exchange = MagicMock()
        exchange.get_futures_ticker_map.side_effect = AssertionError("REST ticker map")
        exchange.get_book_ticker_map.side_effect = AssertionError("REST book map")
        hub = MagicMock()
        hub.get_ticker_map.return_value = {
            "AAAUSDT": {
                "lastPrice": 10.0,
                "openPrice": 9.5,
                "highPrice": 10.4,
                "lowPrice": 9.4,
                "quoteVolume": 25_000_000.0,
            }
        }
        hub.has_ws_book_data.return_value = False
        hub.get_candles_cached_only.return_value = MagicMock(empty=True, __len__=lambda _self: 0)
        hub.is_ticker_cache_usable.return_value = True
        exchange._market_data = hub
        db = MagicMock()
        db.is_blacklisted.return_value = False
        db.cleanup_expired_blacklist.return_value = None
        db.get_active_blacklist_symbols.return_value = set()
        builder = UniverseBuilder(exchange, db)
        result = builder.build()
        exchange.get_futures_ticker_map.assert_not_called()
        self.assertIn("AAAUSDT", result.opportunity_scores or result.symbols or ["AAAUSDT"])
        self.assertTrue(result.opportunity_scores or result.symbols)


def _candidate(symbol: str, score: float, strategy: str = "SMC_TREND") -> SignalCandidate:
    return SignalCandidate(
        symbol=symbol,
        action="LONG",
        strategy=strategy,
        score=score,
        price=1.0,
        atr=1.0,
        timeframe="5m",
    )


class TestCandidateDedupe(unittest.TestCase):
    def test_hot_and_background_same_symbol_keep_higher_score(self) -> None:
        hot = _candidate("ETHUSDT", 82.0)
        background = _candidate("ETHUSDT", 70.0)
        other = _candidate("SOLUSDT", 75.0)
        out = EventScanOrchestrator._dedupe_symbol_candidates(
            [hot, background, other]
        )
        self.assertEqual(len(out), 2)
        by_symbol = {row.symbol: row for row in out}
        self.assertEqual(by_symbol["ETHUSDT"].score, 82.0)
        self.assertEqual(by_symbol["SOLUSDT"].score, 75.0)

    def test_event_replaces_hot_when_score_higher(self) -> None:
        hot = _candidate("BTCUSDT", 71.0, "SMC_TREND")
        event = _candidate("BTCUSDT", 88.0, "BREAKOUT_RETEST")
        out = EventScanOrchestrator._dedupe_symbol_candidates([hot, event])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].score, 88.0)
        self.assertEqual(out[0].strategy, "BREAKOUT_RETEST")


class TestScannerQuarantine(unittest.TestCase):
    def test_legacy_single_symbol_evaluators_removed(self) -> None:
        for name in _QUARANTINED_SCANNER_METHODS:
            self.assertFalse(
                hasattr(MarketScanner, name),
                msg=f"{name} should be quarantined from MarketScanner",
            )
        self.assertFalse(hasattr(MarketScanner, "scan_market"))
        self.assertFalse(hasattr(MarketScanner, "scan_unified"))
        self.assertFalse(hasattr(MarketScanner, "scan_range_market"))
        self.assertTrue(hasattr(MarketScanner, "process_priority_scan_cycle"))


class TestPortfolioAllocatorOnEventPath(unittest.TestCase):
    def test_allocator_rejects_when_enabled(self) -> None:
        from core.types import AllocationResult

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch.portfolio_allocator = MagicMock()
        orch.conflict_guard = MagicMock()
        orch.portfolio_allocator.approve.return_value = AllocationResult(
            False, reason="no_risk_headroom"
        )
        signal = _candidate("ETHUSDT", 80.0)
        with patch.object(Config, "ENABLE_PORTFOLIO_ALLOCATOR", True):
            out = orch._apply_portfolio_allocator(signal)
        self.assertIsNone(out)
        orch.conflict_guard.reject_with_log.assert_called_once()

    def test_allocator_skipped_when_disabled(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch.portfolio_allocator = MagicMock()
        orch.conflict_guard = MagicMock()
        signal = _candidate("ETHUSDT", 80.0)
        with patch.object(Config, "ENABLE_PORTFOLIO_ALLOCATOR", False):
            out = orch._apply_portfolio_allocator(signal)
        self.assertIs(out, signal)
        orch.portfolio_allocator.approve.assert_not_called()

    def test_context_strategy_slot_keys_exist(self) -> None:
        from core.portfolio_allocator import PortfolioAllocator

        for tag in ("MTF_ALIGNMENT", "VP_KEYLEVEL", "ORDER_FLOW", "OI_FUNDING"):
            self.assertIn(tag, PortfolioAllocator.STRATEGY_SLOT_KEYS)
            self.assertIn(tag, PortfolioAllocator.STRATEGY_BUDGET_KEYS)


class TestObserveLiveTickersThrottle(unittest.TestCase):
    def test_observe_skips_full_ticker_map_and_per_symbol_sql(self) -> None:
        exchange = MagicMock()
        hub = MagicMock()
        tickers = {
            "AAAUSDT": {
                "lastPrice": 10.0,
                "openPrice": 9.5,
                "highPrice": 10.4,
                "lowPrice": 9.4,
                "quoteVolume": 25_000_000.0,
            },
            "ZZZUSDT": {
                "lastPrice": 2.0,
                "openPrice": 2.0,
                "highPrice": 2.1,
                "lowPrice": 1.9,
                "quoteVolume": 1_000_000.0,
            },
            "BLKUSDT": {
                "lastPrice": 5.0,
                "openPrice": 4.8,
                "highPrice": 5.2,
                "lowPrice": 4.7,
                "quoteVolume": 8_000_000.0,
            },
        }
        hub.get_ticker_map.return_value = tickers
        hub.has_ws_book_data.return_value = False
        hub.get_candles_cached_only.return_value = MagicMock(
            empty=True, __len__=lambda _self: 0
        )
        exchange._market_data = hub
        db = MagicMock()
        db.get_active_blacklist_symbols.return_value = {"BLKUSDT"}
        db.cleanup_expired_blacklist.return_value = None
        builder = UniverseBuilder(exchange, db)
        builder._last_primary = {"AAAUSDT"}
        builder._last_extended = set()
        builder.observe_live_tickers()
        db.is_blacklisted.assert_not_called()
        db.get_active_blacklist_symbols.assert_called()
        self.assertIsNone(builder.tracker.get("ZZZUSDT"))
        self.assertIsNone(builder.tracker.get("BLKUSDT"))
        self.assertIsNotNone(builder.tracker.get("AAAUSDT"))


class TestVwapMrUnregistered(unittest.TestCase):
    def test_registry_does_not_include_vwap_mr(self) -> None:
        from strategies import build_strategy_registry
        from strategies.strategy_extensions import EXTENSION_STRATEGIES

        self.assertEqual(EXTENSION_STRATEGIES, ())
        registry = build_strategy_registry()
        self.assertNotIn("VWAP_MEAN_REVERSION", registry.tags())


class TestRegimeRouterMapping(unittest.TestCase):
    def test_strong_trend_excludes_range(self) -> None:
        from core.regime_router import RegimeRouter
        from core.types import RegimeLabel

        tags = RegimeRouter.strategies_for_regime(RegimeLabel.STRONG_TREND)
        self.assertIn("SMC_TREND", tags)
        self.assertNotIn("RANGE_REVERSION", tags)
        self.assertNotIn("VWAP_MEAN_REVERSION", tags)

    def test_range_chop_includes_range_excludes_smc(self) -> None:
        from core.regime_router import RegimeRouter
        from core.types import RegimeLabel

        tags = RegimeRouter.strategies_for_regime(RegimeLabel.RANGE_CHOP)
        self.assertIn("RANGE_REVERSION", tags)
        self.assertNotIn("SMC_TREND", tags)

    def test_unclear_covers_live_strategies(self) -> None:
        from core.regime_router import RegimeRouter
        from core.types import RegimeLabel

        tags = RegimeRouter.strategies_for_regime(RegimeLabel.UNCLEAR)
        self.assertGreaterEqual(len(tags), 12)
        self.assertIn("SMC_TREND", tags)
        self.assertIn("RANGE_REVERSION", tags)
        self.assertNotIn("VWAP_MEAN_REVERSION", tags)


class TestMinuteScanClock(unittest.TestCase):
    def test_fires_once_per_minute_after_offset(self) -> None:
        clock = MinuteScanClock(offset_seconds=1.0, enabled=True)
        minute = 1_700_000_000 // 60 * 60
        self.assertFalse(clock.due(minute + 0.2))
        self.assertTrue(clock.due(minute + 1.05))
        self.assertFalse(clock.due(minute + 15.0))
        self.assertTrue(clock.due(minute + 61.1))

    def test_disabled_clock_is_always_due(self) -> None:
        clock = MinuteScanClock(offset_seconds=1.0, enabled=False)
        self.assertTrue(clock.due(100.0))
        self.assertTrue(clock.due(100.0))

    def test_skip_current_minute_waits_for_next_close(self) -> None:
        clock = MinuteScanClock(offset_seconds=1.0, enabled=True)
        minute = 1_700_000_060
        clock.skip_current_minute(minute + 10.0)
        self.assertFalse(clock.due(minute + 15.0))
        self.assertTrue(clock.due(minute + 61.1))

    def test_aligned_queue_honors_hot_scan_interval(self) -> None:
        queue = ScanPriorityQueue()
        queue._hot = ["ETHUSDT"]
        queue._last_hot_scan_at = time.monotonic()
        with patch.object(Config, "SCAN_ALIGN_TO_MINUTE", True), patch.object(
            Config, "HOT_SCAN_INTERVAL_SECONDS", 120.0
        ):
            self.assertFalse(queue.should_run_hot_scan())
        queue._last_hot_scan_at = 0.0
        self.assertTrue(queue.should_run_hot_scan())
        with patch.object(Config, "SCAN_ALIGN_TO_MINUTE", False), patch.object(
            Config, "HOT_SCAN_INTERVAL_SECONDS", 20.0
        ):
            queue._last_hot_scan_at = time.monotonic()
            self.assertFalse(queue.should_run_hot_scan())


class TestScanWarmupGate(unittest.TestCase):
    def test_legacy_warmup_module_removed(self) -> None:
        from pathlib import Path

        self.assertFalse((Path(__file__).resolve().parents[1] / "core" / "scan_warmup.py").exists())

    def test_config_bypasses_warmup_window(self) -> None:
        with patch.object(Config, "SCAN_WARMUP_SECONDS", 60.0):
            self.assertEqual(Config.scan_warmup_seconds(), 0.0)
        with patch.object(Config, "SCAN_WARMUP_SECONDS", 400.0):
            self.assertEqual(Config.scan_warmup_seconds(), 0.0)
        with patch.object(Config, "SCAN_WARMUP_SECONDS", 0.0):
            self.assertEqual(Config.scan_warmup_seconds(), 0.0)
        with patch.object(Config, "SCAN_WARMUP_SECONDS", 300.0):
            self.assertEqual(Config.scan_warmup_seconds(), 0.0)
        self.assertGreaterEqual(Config.scan_symbol_delay_seconds(), 0.0)

    def test_warmup_kline_rest_guards_are_clamped(self) -> None:
        with patch.object(Config, "WARMUP_KLINE_FETCH_LIMIT", 20):
            self.assertEqual(Config.warmup_kline_fetch_limit(), 50)
        with patch.object(Config, "WARMUP_KLINE_FETCH_LIMIT", 500):
            self.assertEqual(Config.warmup_kline_fetch_limit(), 100)
        with patch.object(Config, "WARMUP_KLINE_DELAY_SECONDS", 0.1):
            self.assertEqual(Config.warmup_kline_delay_seconds(), 0.5)
        with patch.object(Config, "WARMUP_KLINE_DELAY_SECONDS", 3.0):
            self.assertEqual(Config.warmup_kline_delay_seconds(), 1.0)
        with patch.object(Config, "WARMUP_REST_WEIGHT_PAUSE", 900):
            self.assertEqual(Config.warmup_rest_weight_pause(), 500)
        with patch.object(Config, "WARMUP_REST_PAUSE_SECONDS", 1.0):
            self.assertEqual(Config.warmup_rest_pause_seconds(), 5.0)
        with patch.object(Config, "WARMUP_REST_PAUSE_SECONDS", 30.0):
            self.assertEqual(Config.warmup_rest_pause_seconds(), 10.0)
        with patch.object(Config, "WS_KLINE_BOOTSTRAP_CONCURRENCY", 8):
            self.assertEqual(Config.ws_kline_bootstrap_concurrency(), 1)

    def test_evaluate_symbols_ws_sleeps_five_seconds_between_symbols(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch.exchange = MagicMock()
        orch.exchange.scan_context.return_value.__enter__ = MagicMock()
        orch.exchange.scan_context.return_value.__exit__ = MagicMock(return_value=False)
        orch._hub = None
        orch._evaluate_symbol = MagicMock(return_value=None)
        with patch.object(Config, "ENABLE_THREE_TIER_FUNNEL", False), patch.object(
            Config, "SCAN_SYMBOL_DELAY_SECONDS", 5.0
        ), patch(
            "pipeline.event_scan_orchestrator.time.sleep"
        ) as sleeper:
            orch._evaluate_symbols_ws(
                ["AAAUSDT", "BBBUSDT"],
                timeframe="5m",
                open_symbols=set(),
                ticker_map={},
                book_map={},
            )
        sleeper.assert_called_once_with(5.0)
        self.assertEqual(orch._evaluate_symbol.call_count, 2)


class TestKlineWarmupScan(unittest.TestCase):
    def test_has_complete_klines_requires_analyzer_bars(self) -> None:
        from pipeline.snapshot_factory import SnapshotFactory

        exchange = MagicMock()
        factory = SnapshotFactory(exchange)
        exchange.fetch_historical_candles.return_value = pd.DataFrame(
            {"close": [1.0] * 40}
        )
        self.assertFalse(factory.has_complete_klines("ENAUSDT"))
        exchange.fetch_historical_candles.return_value = pd.DataFrame(
            {"close": [1.0] * 250}
        )
        self.assertTrue(factory.has_complete_klines("ENAUSDT"))

    def test_cache_miss_is_queued_without_scan_rejected(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.build.return_value = None
        orch._kline_cache_misses = []
        orch._kline_pending_log_at = {}
        orch._price_map = {"ENAUSDT": 1.0}
        orch._volume_ranks = {}
        orch._hub = None
        orch.priority_queue = MagicMock()
        orch.priority_queue.rotation.is_in_evaluated_memory.return_value = False
        orch.priority_queue.is_priority.return_value = True
        orch.assignment_manager = MagicMock()
        orch.event_scheduler = MagicMock()
        event = CandleCloseEvent(
            symbol="ENAUSDT", timeframe="5m", bar_open_ms=1
        )
        with patch(
            "pipeline.event_scan_orchestrator.log_scan_rejected"
        ) as rejected:
            out = orch._evaluate_symbol(
                "ENAUSDT",
                bar_open_ms=1,
                timeframe="5m",
                open_symbols=set(),
                ticker_map={"ENAUSDT": {"lastPrice": 1.0, "quoteVolume": 1.0}},
                book_map={},
                mark_event=event,
            )
        self.assertIsNone(out)
        rejected.assert_not_called()
        orch.event_scheduler.mark_evaluated.assert_not_called()
        orch.event_scheduler.requeue.assert_called_once()
        self.assertEqual(orch.take_kline_cache_misses(1), ["ENAUSDT"])

    def test_warmup_skips_rest_when_governor_blocks(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.exchange.in_scan_mode = False
        orch.exchange._ws_reconnect_or_warmup.return_value = False
        orch.exchange.can_bootstrap_klines_rest.return_value = False
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.has_complete_klines.return_value = False
        seeded = orch._bootstrap_missing_scan_klines(["ENAUSDT"])
        self.assertEqual(seeded, 0)
        orch.exchange.bootstrap_context.assert_not_called()
        orch.exchange.fetch_bootstrap_klines_df.assert_not_called()

    def test_warmup_evaluates_only_symbols_that_filled(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.exchange.in_scan_mode = False
        orch.db = MagicMock()
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.has_complete_klines.return_value = False
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["ENAUSDT"]
        orch._kline_cache_misses = ["ENAUSDT", "BBUSDT"]
        orch._kline_pending_log_at = {}
        orch._scan_gate_open = MagicMock(return_value=(False, ""))
        orch._bootstrap_missing_scan_klines = MagicMock(return_value=3)
        orch._partition_kline_ready = MagicMock(return_value=(["ENAUSDT"], []))
        orch._open_symbols = MagicMock(return_value=set())
        orch._ws_ticker_map = MagicMock(return_value={})
        orch._ws_book_map = MagicMock(return_value={})
        orch._evaluate_symbols_ws = MagicMock(
            return_value=[_candidate("ENAUSDT", 80.0)]
        )
        out = orch.warmup_and_evaluate_kline_misses()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["symbol"], "ENAUSDT")
        orch._bootstrap_missing_scan_klines.assert_called_once_with(["ENAUSDT"])
        orch._hub.subscribe_kline_streams.assert_called()
        self.assertEqual(orch._kline_cache_misses, ["BBUSDT"])
        orch._evaluate_symbols_ws.assert_called_once()

    def test_bootstrap_caps_one_incomplete_symbol(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch._hub.bootstrap_klines_for_symbols.return_value = 2
        orch.exchange = MagicMock()
        orch.exchange.in_scan_mode = False
        orch.exchange._ws_reconnect_or_warmup.return_value = False
        orch.exchange.can_bootstrap_klines_rest.return_value = True
        orch.exchange.can_make_background_rest_call.return_value = True
        orch.exchange.bootstrap_context.return_value.__enter__ = MagicMock()
        orch.exchange.bootstrap_context.return_value.__exit__ = MagicMock(
            return_value=False
        )
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.has_complete_klines.side_effect = (
            lambda symbol: symbol == "BBUSDT"
        )
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["ENAUSDT"]
        seeded = orch._bootstrap_missing_scan_klines(["ENAUSDT", "BBUSDT", "CCUSDT"])
        self.assertEqual(seeded, 2)
        orch._hub.bootstrap_klines_for_symbols.assert_called_once()
        args, kwargs = orch._hub.bootstrap_klines_for_symbols.call_args
        self.assertEqual(args[0], ["ENAUSDT"])
        self.assertEqual(kwargs.get("max_pairs"), 3)

    def test_warmup_skips_bootstrap_when_target_already_complete(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.db = MagicMock()
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.has_complete_klines.return_value = True
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["ENAUSDT"]
        orch._kline_cache_misses = ["ENAUSDT"]
        orch._kline_pending_log_at = {}
        orch._scan_gate_open = MagicMock(return_value=(False, ""))
        orch._bootstrap_missing_scan_klines = MagicMock(return_value=0)
        orch._partition_kline_ready = MagicMock(return_value=(["ENAUSDT"], []))
        orch._open_symbols = MagicMock(return_value=set())
        orch._ws_ticker_map = MagicMock(return_value={})
        orch._ws_book_map = MagicMock(return_value={})
        orch._evaluate_symbols_ws = MagicMock(return_value=[])
        orch.warmup_and_evaluate_kline_misses()
        orch._bootstrap_missing_scan_klines.assert_not_called()
        orch._evaluate_symbols_ws.assert_called_once()

    def test_warmup_failed_bootstrap_does_not_crash(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.db = MagicMock()
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.has_complete_klines.return_value = False
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["ENAUSDT"]
        orch._kline_cache_misses = ["ENAUSDT"]
        orch._kline_pending_log_at = {}
        orch._scan_gate_open = MagicMock(return_value=(False, ""))
        orch._bootstrap_missing_scan_klines = MagicMock(
            side_effect=RuntimeError("rest down")
        )
        orch._partition_kline_ready = MagicMock(return_value=([], ["ENAUSDT"]))
        orch._open_symbols = MagicMock(return_value=set())
        orch._evaluate_symbols_ws = MagicMock()
        out = orch.warmup_and_evaluate_kline_misses()
        self.assertEqual(out, [])
        self.assertEqual(orch._kline_cache_misses, ["ENAUSDT"])
        orch._evaluate_symbols_ws.assert_not_called()


class TestIndicatorHistoryFloor(unittest.TestCase):
    def _ohlcv(self, n: int) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": [1.0] * n,
                "high": [1.1] * n,
                "low": [0.9] * n,
                "close": [1.0] * n,
                "volume": [100.0] * n,
            }
        )

    def test_prepare_df_rejects_thin_5m_history(self) -> None:
        from core.candle_prep import prepare_df
        from indicators.market_analyzer import MarketAnalyzer

        analyzer = MarketAnalyzer()
        self.assertIsNone(prepare_df(self._ohlcv(40), analyzer))
        self.assertIsNone(prepare_df(self._ohlcv(150), analyzer))
        self.assertIsNone(prepare_df(self._ohlcv(200), analyzer))

    def test_prepare_df_accepts_200_closed_5m_bars(self) -> None:
        from core.candle_prep import prepare_df
        from indicators.market_analyzer import MarketAnalyzer

        analyzer = MarketAnalyzer()
        out = prepare_df(self._ohlcv(201), analyzer)
        self.assertIsNotNone(out)
        self.assertGreaterEqual(len(out), 150)

    def test_snapshot_requires_200_closed_entry_bars(self) -> None:
        from pipeline.snapshot_factory import SnapshotFactory

        exchange = MagicMock()

        def _candles(_symbol, timeframe, **_kwargs):
            n = 201 if timeframe in ("5m", "15m") else 250
            return pd.DataFrame({"close": [1.0] * n})

        factory = SnapshotFactory(exchange)
        exchange.fetch_historical_candles.side_effect = _candles
        self.assertTrue(factory.has_complete_klines("ENAUSDT"))

        def _thin_5m(_symbol, timeframe, **_kwargs):
            n = 150 if timeframe == "5m" else 250
            return pd.DataFrame({"close": [1.0] * n})

        exchange.fetch_historical_candles.side_effect = _thin_5m
        self.assertFalse(factory.has_complete_klines("ENAUSDT"))


class TestPrematureCloseGuards(unittest.TestCase):
    def test_monitor_timeouts_are_raised(self) -> None:
        self.assertGreaterEqual(Config.MONITOR_LOOP_STALL_SECONDS, 45.0)
        self.assertGreaterEqual(Config.POSITION_GRACE_PERIOD_SECONDS, 120.0)
        self.assertGreaterEqual(Config.POSITION_RECONCILE_MISS_THRESHOLD, 8)
        self.assertGreaterEqual(Config.WS_STALE_SECONDS, 90)
        self.assertGreaterEqual(Config.WATCHDOG_MAIN_STALE_SECONDS, 180)
        self.assertFalse(Config.ENABLE_RANGE_AUXILIARY_EXITS)
        self.assertGreaterEqual(Config.INDICATOR_MIN_BARS, 200)

    def test_reconcile_skips_without_rest_flat_confirm(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = None
        mgr._cancel_all_native_orders = MagicMock()
        mgr._mark_trade_closed = MagicMock()
        trade = {"trade_id": "t1", "symbol": "QNTUSDT", "side": "LONG"}
        with patch("manager.confirm_external_close_allowed", return_value=False):
            ok = mgr._reconcile_exchange_flat(trade, "cache_qty_zero")
        self.assertFalse(ok)
        mgr._mark_trade_closed.assert_not_called()

    def test_reconcile_skips_exchange_reject_until_rest_confirms_flat(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = None
        mgr._cancel_all_native_orders = MagicMock()
        mgr._mark_trade_closed = MagicMock()
        trade = {"trade_id": "t1", "symbol": "QNTUSDT", "side": "LONG"}
        with patch("manager.rest_position_quantity", return_value=1.25):
            ok = mgr._reconcile_exchange_flat(
                trade, "-2022", confirmed_by_exchange=True
            )
        self.assertFalse(ok)
        mgr._mark_trade_closed.assert_not_called()

    def test_range_time_and_adx_exits_are_disabled(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        trade = {"trade_id": "t1", "symbol": "QNTUSDT", "side": "LONG"}
        with patch.object(Config, "ENABLE_RANGE_AUXILIARY_EXITS", False), patch.object(
            mgr, "_check_range_boundary_breakout", return_value=False
        ), patch.object(mgr, "_range_bars_elapsed", return_value=99), patch.object(
            mgr, "_fetch_confirm_adx", return_value=80.0
        ), patch.object(
            mgr, "_close_position"
        ) as close:
            self.assertFalse(mgr._check_range_hard_exits(trade, 100.0))
            close.assert_not_called()

    def test_range_boundary_reversal_still_closes(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        trade = {"trade_id": "t1", "symbol": "QNTUSDT", "side": "LONG"}
        with patch.object(
            mgr, "_check_range_boundary_breakout", return_value=True
        ), patch.object(mgr, "_close_position") as close:
            self.assertTrue(mgr._check_range_hard_exits(trade, 100.0))
            close.assert_not_called()


class TestStopLossMarketCloseLoop(unittest.TestCase):
    def _manager(self):
        import threading

        from exchange import SymbolRules
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = MagicMock()
        mgr._close_lock = threading.Lock()
        mgr._close_inflight = set()
        mgr._close_queue = MagicMock()
        mgr._market_close_attempts = {}
        mgr._market_close_exhausted = set()
        mgr._cancel_all_native_orders = MagicMock()
        mgr._mark_trade_closed = MagicMock()
        mgr.exchange.get_symbol_rules.return_value = SymbolRules(
            2, 3, 0.01, 0.001, 0.001, 5.0
        )
        mgr.exchange.get_position_quantity_cached.return_value = 0.0
        mgr.exchange.get_position_quantity.return_value = 0.0
        mgr.exchange.get_market_price.return_value = 99.0
        return mgr

    def test_market_close_max_attempts_in_range(self) -> None:
        self.assertGreaterEqual(Config.MARKET_CLOSE_MAX_ATTEMPTS, 3)
        self.assertLessEqual(Config.MARKET_CLOSE_MAX_ATTEMPTS, 5)

    def test_stop_loss_rest_flat_closes_without_miss_threshold(self) -> None:
        from exchange import ClosedPositionPnl

        mgr = self._manager()
        trade = {"trade_id": "t-sl", "symbol": "BTCUSDT", "side": "LONG"}
        with patch.object(mgr, "_inspect_exchange_position_qty", return_value=0.0), patch(
            "manager.confirm_external_close_allowed", return_value=False
        ), patch(
            "manager.resolve_exchange_close_pnl",
            return_value=ClosedPositionPnl(
                realized_pnl=-1.25, exit_price=99.0, source="income", fill_count=1
            ),
        ):
            ok = mgr._close_position(trade, quantity=1.0, reason="STOP_LOSS")
        self.assertTrue(ok)
        mgr._mark_trade_closed.assert_called_once()
        self.assertEqual(mgr._mark_trade_closed.call_args.kwargs["reason"], "STOP_LOSS")
        self.assertEqual(mgr._mark_trade_closed.call_args.kwargs["pnl"], -1.25)
        mgr._close_queue.put.assert_not_called()

    def test_reduce_only_reject_rest_flat_closes_db(self) -> None:
        from exceptions import PositionAlreadyClosedError
        from exchange import ClosedPositionPnl

        mgr = self._manager()
        trade = {
            "trade_id": "t-sl",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "status": "OPEN",
        }
        mgr.db.get_trade.return_value = trade
        mgr.exchange.get_position_quantity_cached.return_value = 1.0
        mgr.exchange.get_position_quantity.return_value = 1.0
        mgr.exchange.close_position_quantity.side_effect = PositionAlreadyClosedError(
            "ReduceOnly order is rejected"
        )
        with patch.object(mgr, "_inspect_exchange_position_qty", return_value=0.0), patch(
            "manager.resolve_exchange_close_pnl",
            return_value=ClosedPositionPnl(
                realized_pnl=-2.0, exit_price=98.5, source="income", fill_count=1
            ),
        ):
            ok = mgr._execute_close_order(trade, 1.0, "STOP_LOSS")
        self.assertTrue(ok)
        mgr._mark_trade_closed.assert_called_once()
        self.assertEqual(mgr._mark_trade_closed.call_args.kwargs["reason"], "STOP_LOSS")
        self.assertEqual(mgr._mark_trade_closed.call_args.kwargs["pnl"], -2.0)

    def test_exhausted_market_close_does_not_retry_when_rest_still_open(self) -> None:
        mgr = self._manager()
        trade = {"trade_id": "t-sl", "symbol": "BTCUSDT", "side": "LONG"}
        mgr.exchange.get_position_quantity_cached.return_value = 1.0
        mgr.exchange.get_position_quantity.return_value = 1.0
        with patch("manager.claim_exit", return_value=True), patch(
            "manager.exit_claim_active", return_value=False
        ):
            for _ in range(Config.MARKET_CLOSE_MAX_ATTEMPTS):
                mgr._close_inflight.clear()
                self.assertTrue(
                    mgr._close_position(trade, quantity=1.0, reason="STOP_LOSS")
                )
        self.assertEqual(
            mgr._close_queue.put.call_count, Config.MARKET_CLOSE_MAX_ATTEMPTS
        )
        self.assertTrue(mgr._market_close_is_exhausted("t-sl", "STOP_LOSS"))
        mgr._close_queue.put.reset_mock()
        with patch.object(mgr, "_inspect_exchange_position_qty", return_value=1.25):
            ok = mgr._close_position(trade, quantity=1.0, reason="STOP_LOSS")
        self.assertFalse(ok)
        mgr._close_queue.put.assert_not_called()
        mgr._mark_trade_closed.assert_not_called()

    def test_manage_long_skips_stop_loss_after_exhausted_retries(self) -> None:
        mgr = self._manager()
        trade = {
            "trade_id": "t-sl",
            "symbol": "BTCUSDT",
            "side": "LONG",
            "status": "OPEN",
            "entry_price": 100.0,
            "stop_loss": 95.0,
            "take_profit_1": 0,
            "take_profit_2": 0,
            "take_profit_3": 0,
        }
        mgr.db.get_trade.return_value = trade
        mgr.db.parse_trade_metadata.return_value = {}
        mgr._market_close_exhausted.add("t-sl:STOP_LOSS")
        with patch.object(mgr, "_advance_profit_stop_ladder"), patch.object(
            mgr, "_apply_trailing_stop"
        ), patch.object(mgr, "_trigger_virtual_sl") as sl, patch.object(
            mgr, "_close_position"
        ) as close:
            mgr._manage_long_trade(trade, 90.0)
        sl.assert_not_called()
        close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
