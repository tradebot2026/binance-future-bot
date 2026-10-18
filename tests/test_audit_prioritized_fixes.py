"""Deterministic tests for audit-prioritized candle, score, order, and watchdog fixes."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from constants import ALL_STRATEGY_TAGS
from core.bot_health import (
    get_incomplete_kline_count,
    get_scan_history_ready,
    touch_scan_cycle,
    watchdog_sibling_status,
)
from core.strategy_score_ranges import execution_min_score, score_range_for
from pipeline.event_scan_orchestrator import EventScanOrchestrator
from pipeline.snapshot_factory import SnapshotFactory
from telegram_bot import classify_exchange_order_status


_FUNNEL_SCORE_ENV = {
    "SMC_TREND_HOT_SCORE": "",
    "SMC_TREND_SUPER_SCORE": "",
    "RANGE_REVERSION_HOT_SCORE": "",
    "RANGE_REVERSION_SUPER_SCORE": "",
}


class TestSuperVsExecutionMin(unittest.TestCase):
    def test_live_smc_super_is_not_below_execution_min(self) -> None:
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False):
            band = score_range_for("SMC_TREND")
        self.assertGreaterEqual(band.super_score, band.execution_min)
        self.assertEqual(band.execution_min, Config.STRATEGY_MIN_SCORE)
        self.assertEqual(band.classify(68.0), "HOT")
        self.assertEqual(band.classify(70.0), "SUPER")

    def test_testnet_relax_raises_super_to_relaxed_execution_min(self) -> None:
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", True
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True):
            band = score_range_for("SMC_TREND")
            exec_min = execution_min_score("SMC_TREND")
        self.assertEqual(exec_min, 60.0)
        self.assertGreaterEqual(band.super_score, exec_min)
        self.assertEqual(band.classify(58.0), "HOT")
        self.assertEqual(band.classify(60.0), "SUPER")

    def test_every_strategy_super_meets_its_execution_min(self) -> None:
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False
        ):
            for tag in ALL_STRATEGY_TAGS:
                band = score_range_for(tag)
                self.assertGreaterEqual(
                    band.super_score,
                    band.execution_min,
                    msg=f"{tag} Super {band.super_score} < exec {band.execution_min}",
                )
                self.assertLess(band.hot_score, band.super_score, msg=tag)


class TestKlineHistorySkip(unittest.TestCase):
    def test_history_status_reports_have_vs_need(self) -> None:
        exchange = MagicMock()
        factory = SnapshotFactory(exchange)
        exchange.fetch_historical_candles.return_value = pd.DataFrame(
            {"close": [1.0] * 40}
        )
        status = factory.kline_history_status("ENAUSDT")
        self.assertFalse(status["ready"])
        self.assertIn("have=40", status["reason"])
        self.assertIn("need=", status["reason"])
        self.assertFalse(factory.has_complete_klines("ENAUSDT"))

    def test_complete_history_is_ready(self) -> None:
        exchange = MagicMock()
        factory = SnapshotFactory(exchange)
        exchange.fetch_historical_candles.return_value = pd.DataFrame(
            {"close": [1.0] * 250}
        )
        self.assertTrue(factory.has_complete_klines("ENAUSDT"))
        self.assertTrue(factory.kline_history_status("ENAUSDT")["ready"])

    def test_super_eval_does_not_run_on_incomplete_history(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch.funnel = MagicMock()
        orch.funnel._super = {}
        orch.snapshot_factory = MagicMock()
        orch.snapshot_factory.kline_history_status.return_value = {
            "ready": False,
            "reason": "Insufficient kline history 5m have=40 need=201",
        }
        orch._build_eval_snapshot = MagicMock(return_value=None)
        out = orch._evaluate_super_symbol(
            "ENAUSDT",
            timeframe="5m",
            open_symbols=set(),
            ticker_map={},
            book_map={},
        )
        self.assertIsNone(out)
        self.assertIn("have=40", orch._last_super_skip_reason)
        orch._build_eval_snapshot.assert_called_once()

    def test_incomplete_scan_is_not_history_ready(self) -> None:
        touch_scan_cycle(scanned=4, universe_total=10, incomplete_klines=3)
        self.assertFalse(get_scan_history_ready())
        self.assertEqual(get_incomplete_kline_count(), 3)
        touch_scan_cycle(scanned=4, universe_total=10, incomplete_klines=0)
        self.assertTrue(get_scan_history_ready())

    def test_bounded_topup_allowed_when_scan_ws_only(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.exchange.in_scan_mode = False
        orch.exchange._ws_reconnect_or_warmup.return_value = False
        orch.exchange.can_bootstrap_klines_rest.return_value = True
        orch.exchange.can_make_background_rest_call.return_value = True
        orch.exchange.bootstrap_context.return_value.__enter__ = MagicMock()
        orch.exchange.bootstrap_context.return_value.__exit__ = MagicMock(
            return_value=False
        )
        orch.exchange.fetch_bootstrap_klines_df.return_value = pd.DataFrame(
            {"close": [1.0, 1.1]}
        )
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["ENAUSDT"]
        orch._hub.get_candles_cached_only.return_value = pd.DataFrame(
            {"close": [1.0] * 200}
        )
        with patch.object(Config, "SCAN_WS_ONLY", True), patch.object(
            Config, "ENABLE_PACED_KLINE_BOOTSTRAP", True
        ), patch.object(Config, "get_scan_kline_intervals", return_value=["5m"]):
            orch._maybe_fetch_single_closed_kline(["ENAUSDT"])
        orch.exchange.bootstrap_context.assert_called_once()
        orch.exchange.fetch_bootstrap_klines_df.assert_called_once()
        args = orch.exchange.fetch_bootstrap_klines_df.call_args[0]
        self.assertEqual(args[0], "ENAUSDT")
        self.assertEqual(args[1], "5m")
        self.assertGreaterEqual(int(args[2]), 201)

    def test_super_low_score_reason_includes_execution_min(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        rec = MagicMock()
        rec.score = 68.0
        rec.strategy = "SMC_TREND"
        rec.candidate = {}
        rec.backup_strategies = []
        orch.funnel = MagicMock()
        orch.funnel._super = {"ENAUSDT": rec}
        orch.snapshot_factory = MagicMock()
        snapshot = MagicMock()
        snapshot.spread_pct = 0.01
        snapshot.candles = {}
        orch._build_eval_snapshot = MagicMock(return_value=snapshot)
        signal = MagicMock()
        signal.strategy = "SMC_TREND"
        signal.score = 68.0
        signal.structure_metadata = {}
        orch._evaluate_symbol = MagicMock(return_value=signal)
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False), patch.object(
            Config, "MAX_SPREAD_PERCENT", 0.5
        ):
            out = orch._evaluate_super_symbol(
                "ENAUSDT",
                timeframe="5m",
                open_symbols=set(),
                ticker_map={},
                book_map={},
                live_score_only=True,
            )
        self.assertIsNone(out)
        self.assertIn("execution min", orch._last_super_skip_reason)
        self.assertIn("70.0", orch._last_super_skip_reason)


class TestOrderStatusClassification(unittest.TestCase):
    def test_filled_requires_executed_qty(self) -> None:
        filled = classify_exchange_order_status(
            {"status": "FILLED", "executedQty": "4", "avgPrice": "1.2"}
        )
        self.assertEqual(filled["status"], "FILLED")
        self.assertTrue(filled["confirmed_fill"])
        ack = classify_exchange_order_status(
            {"status": "NEW", "executedQty": "4", "avgPrice": "1.2"}
        )
        self.assertEqual(ack["status"], "SUBMITTED")
        self.assertFalse(ack["confirmed_fill"])
        empty_fill = classify_exchange_order_status(
            {"status": "FILLED", "executedQty": "0"}
        )
        self.assertEqual(empty_fill["status"], "UNKNOWN")
        self.assertFalse(empty_fill["confirmed_fill"])
        partial = classify_exchange_order_status(
            {"status": "PARTIALLY_FILLED", "executedQty": "1"}
        )
        self.assertEqual(partial["status"], "PARTIALLY_FILLED")
        rejected = classify_exchange_order_status({"status": "REJECTED"})
        self.assertEqual(rejected["status"], "REJECTED")
        unknown = classify_exchange_order_status(None)
        self.assertEqual(unknown["status"], "UNKNOWN")


class TestWatchdogAndRestartBounds(unittest.TestCase):
    def test_zero_auto_restarts_uses_hard_cap(self) -> None:
        with patch.object(Config, "MAX_AUTO_RESTARTS", 0), patch.object(
            Config, "MAX_AUTO_RESTARTS_HARD_CAP", 5
        ):
            self.assertEqual(Config.max_crash_auto_restarts(), 5)
        with patch.object(Config, "MAX_AUTO_RESTARTS", 9), patch.object(
            Config, "MAX_AUTO_RESTARTS_HARD_CAP", 5
        ):
            self.assertEqual(Config.max_crash_auto_restarts(), 5)
        with patch.object(Config, "MAX_AUTO_RESTARTS", 3), patch.object(
            Config, "MAX_AUTO_RESTARTS_HARD_CAP", 5
        ):
            self.assertEqual(Config.max_crash_auto_restarts(), 3)

    def test_watchdog_sibling_status_from_state_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "watchdog_state.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"saved_at": 1_000.0, "pid": 4242}, handle)
            with patch.object(Config, "WATCHDOG_STATE_FILE", path), patch.object(
                Config, "WATCHDOG_INTERVAL_SECONDS", 30
            ):
                fresh = watchdog_sibling_status(now=1_010.0)
                stale = watchdog_sibling_status(now=1_000.0 + 400.0)
        self.assertTrue(fresh["fresh"])
        self.assertEqual(fresh["pid"], 4242)
        self.assertFalse(stale["fresh"])


if __name__ == "__main__":
    unittest.main()
