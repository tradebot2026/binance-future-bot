"""Offline tests for SL/TP, DCA fills, scanner overlap, and CPU cache."""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from core.dca_plan import (
    last_resort_sl_hit,
    sl_is_valid_vs_entry,
    update_weighted_average,
    weighted_average_entry,
)
from core.execution_ledger import ExecutionLedger
from core.instance_lock import acquire_main_lock, release_main_lock
from exchange import SymbolRules
from executor import TradeExecutor
from indicators.market_analyzer import (
    MIN_ANALYZER_BARS,
    MarketAnalyzer,
    clear_indicator_cache,
)
from pipeline.event_scan_orchestrator import (
    EventScanOrchestrator,
    _snapshot_kline_fingerprint,
)
from utils import round_protective_price, round_step_size


def _rules() -> SymbolRules:
    return SymbolRules(2, 3, 0.01, 0.001, 0.001, 5.0)


def _ohlcv(n: int = 260, close: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min"),
            "open": [close] * n,
            "high": [close + 1.0] * n,
            "low": [close - 1.0] * n,
            "close": [close] * n,
            "volume": [10.0] * n,
        }
    )


class TestStopLossSides(unittest.TestCase):
    def test_long_and_short_validation(self) -> None:
        executor = TradeExecutor(MagicMock(), MagicMock())
        ok, _ = executor._validate_stop_loss("LONG", 100.0, 99.0)
        self.assertTrue(ok)
        ok, reason = executor._validate_stop_loss("LONG", 100.0, 100.01)
        self.assertFalse(ok)
        self.assertIn("below", reason)
        ok, _ = executor._validate_stop_loss("SHORT", 100.0, 101.0)
        self.assertTrue(ok)
        ok, reason = executor._validate_stop_loss("SHORT", 100.0, 99.99)
        self.assertFalse(ok)
        self.assertIn("above", reason)

    def test_immediate_stop_helpers(self) -> None:
        self.assertTrue(last_resort_sl_hit("LONG", 99.0, 100.0))
        self.assertFalse(last_resort_sl_hit("LONG", 101.0, 100.0))
        self.assertTrue(last_resort_sl_hit("SHORT", 101.0, 100.0))
        self.assertFalse(last_resort_sl_hit("SHORT", 99.0, 100.0))
        self.assertFalse(sl_is_valid_vs_entry("SHORT", 100.0, 99.99))
        self.assertTrue(sl_is_valid_vs_entry("SHORT", 100.0, 100.01))

    def test_floor_rounding_puts_short_sl_on_wrong_side(self) -> None:
        entry = 100.0
        raw_sl = 100.004
        floored = round_step_size(raw_sl, 0.01, 2)
        protected = round_protective_price(
            raw_sl, 0.01, 2, side="SHORT", kind="SL"
        )
        self.assertEqual(floored, 100.0)
        self.assertFalse(sl_is_valid_vs_entry("SHORT", entry, floored))
        self.assertGreater(protected, entry)
        self.assertTrue(sl_is_valid_vs_entry("SHORT", entry, protected))

    def test_live_price_through_sl_rejects_before_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            exchange = MagicMock()
            exchange.get_execution_safety.return_value = ("EXECUTION_SAFE", "")
            exchange.get_symbol_rules.return_value = _rules()
            exchange.is_rest_blocked.return_value = (False, "")
            db = MagicMock()
            db.is_symbol_on_cooldown.return_value = (False, "")
            executor = TradeExecutor(exchange, db)
            with patch.object(Config, "DRY_RUN", False), patch(
                "executor.get_execution_ledger", return_value=ledger
            ), patch.object(
                executor, "_resolve_live_execution_price", return_value=100.0
            ), patch.object(
                executor,
                "_resolve_execution_levels",
                return_value=(99.5, 99.0, 98.5, 98.0),
            ), patch(
                "executor.symbol_blocked_for_new_entry", return_value=(False, "")
            ):
                result = executor.execute_trade(
                    "STRKUSDT", "SHORT", 1.0, 100.0, "SMC_TREND", 80.0
                )
            self.assertIsNone(result)
            exchange.execute_futures_order.assert_not_called()


class TestDcaFills(unittest.TestCase):
    def test_weighted_average(self) -> None:
        avg = weighted_average_entry([(100.0, 1.0), (90.0, 1.0)])
        self.assertAlmostEqual(avg, 95.0)
        meta: dict = {}
        out = update_weighted_average(
            meta, prior_avg=100.0, prior_qty=2.0, fill_price=80.0, add_qty=2.0
        )
        self.assertAlmostEqual(out, 90.0)
        self.assertAlmostEqual(meta["avg_entry_price"], 90.0)

    def test_uncertain_dca_does_not_record_qty(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.return_value = {
            "status": "NEW",
            "orderId": 44,
            "executedQty": "0",
        }
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_entries": 2,
            "dca_entry2_px": 98.0,
        }
        executor = TradeExecutor(exchange, db)
        trade = {
            "trade_id": "t-dca",
            "symbol": "ONUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
            "entry_stage": 2,
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "ENABLE_DCA_LADDER", True
        ):
            result = executor.execute_dca_add(trade, 2, 97.0)
        self.assertIsNone(result)
        db.update_trade.assert_called()
        exchange.seed_position_after_fill.assert_not_called()

    def test_dca_timeout_keeps_claim(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.side_effect = RuntimeError("timeout")
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_entries": 2,
            "dca_entry2_px": 98.0,
        }
        executor = TradeExecutor(exchange, db)
        trade = {
            "trade_id": "t-dca",
            "symbol": "ONUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
            "entry_stage": 2,
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "ENABLE_DCA_LADDER", True
        ):
            result = executor.execute_dca_add(trade, 2, 97.0)
        self.assertIsNone(result)
        db.revert_entry_stage.assert_not_called()
        db.update_trade.assert_called()
        meta = db.update_trade.call_args[0][1]["metadata"]
        self.assertTrue(meta.get("dca_uncertain_client_id"))

    def test_dca_reject_reverts_stage(self) -> None:
        from exceptions import OrderExecutionError

        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.side_effect = OrderExecutionError(
            "ORDER_REJECTED: insufficient margin"
        )
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_entries": 2,
            "dca_entry2_px": 98.0,
        }
        executor = TradeExecutor(exchange, db)
        trade = {
            "trade_id": "t-dca",
            "symbol": "ONUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
            "entry_stage": 2,
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "ENABLE_DCA_LADDER", True
        ):
            result = executor.execute_dca_add(trade, 2, 97.0)
        self.assertIsNone(result)
        db.revert_entry_stage.assert_called_with("t-dca", 2)

    def test_partial_dca_uses_executed_qty(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.get_market_price.return_value = 97.0
        exchange.execute_futures_order.return_value = {
            "status": "PARTIALLY_FILLED",
            "orderId": 9,
            "executedQty": "0.4",
            "avgPrice": "97.5",
        }
        meta = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_entries": 2,
            "dca_entry2_px": 98.0,
            "avg_entry_price": 100.0,
        }
        db = MagicMock()
        db.parse_trade_metadata.return_value = meta
        executor = TradeExecutor(exchange, db)
        trade = {
            "trade_id": "t-dca",
            "symbol": "ONUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
            "entry_stage": 2,
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "ENABLE_DCA_LADDER", True
        ):
            result = executor.execute_dca_add(trade, 2, 97.0)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["add_qty"], 0.4)
        self.assertAlmostEqual(result["total_qty"], 1.4)
        self.assertAlmostEqual(meta["avg_entry_price"], (100.0 * 1.0 + 97.5 * 0.4) / 1.4)


class TestScannerAndCache(unittest.TestCase):
    def test_scan_overlap_returns_empty(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._scan_cycle_lock = threading.Lock()
        orch._last_super_dispatch = [{"symbol": "AAAUSDT"}]
        self.assertTrue(orch._scan_cycle_lock.acquire(blocking=False))
        try:
            with patch.object(
                orch,
                "_process_priority_scan_cycle_inner",
                side_effect=AssertionError("must not run"),
            ):
                out = orch.process_priority_scan_cycle()
        finally:
            orch._scan_cycle_lock.release()
        self.assertEqual(out, [])

    def test_super_fingerprint_changes_with_new_bar(self) -> None:
        snap1 = SimpleNamespace(candles={"5m": _ohlcv(10, 1.0)})
        snap2 = SimpleNamespace(candles={"5m": _ohlcv(11, 1.0)})
        self.assertNotEqual(
            _snapshot_kline_fingerprint(snap1),
            _snapshot_kline_fingerprint(snap2),
        )

    def test_indicator_cache_reuses_identical_frames(self) -> None:
        clear_indicator_cache()
        raw = _ohlcv(MIN_ANALYZER_BARS + 10, 50.0)
        first = MarketAnalyzer.apply_all_indicators(raw)
        second = MarketAnalyzer.apply_all_indicators(raw.copy())
        self.assertIs(first, second)
        self.assertIn("ema_200", first.columns)
        self.assertIn("bos_bullish", first.columns)
        clear_indicator_cache()

    def test_super_eval_batch_keeps_book_cap(self) -> None:
        self.assertGreaterEqual(Config.super_eval_batch_size(), Config.MAX_POSITIONS)


class TestRestartLockAndDailyRisk(unittest.TestCase):
    def test_same_process_lock_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch("core.instance_lock.Config.DATA_DIR", tmp), patch.dict(
                os.environ, {"INSTANCE_LOCK_FILE": "t.lock"}
            ):
                self.assertTrue(acquire_main_lock())
                self.assertTrue(acquire_main_lock())
                release_main_lock()

    def test_daily_limit_helpers_still_present(self) -> None:
        from scheduler import DailyScheduler
        from core.risk_engine import RiskEngine
        from risk_manager import RiskManager

        self.assertTrue(hasattr(DailyScheduler, "check_daily_limits"))
        self.assertTrue(hasattr(DailyScheduler, "is_entry_paused"))
        self.assertTrue(hasattr(RiskEngine, "approve_entry"))
        self.assertTrue(hasattr(RiskManager, "get_risk_snapshot"))


class TestStaleDataAndProtection(unittest.TestCase):
    def test_missing_live_price_does_not_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            exchange = MagicMock()
            exchange.get_execution_safety.return_value = ("EXECUTION_SAFE", "")
            db = MagicMock()
            executor = TradeExecutor(exchange, db)
            with patch.object(Config, "DRY_RUN", False), patch(
                "executor.get_execution_ledger", return_value=ledger
            ), patch.object(
                executor, "_resolve_live_execution_price", return_value=0.0
            ):
                result = executor.execute_trade(
                    "AAAUSDT", "LONG", 1.0, 100.0, "SMC_TREND", 80.0
                )
            self.assertIsNone(result)
            exchange.execute_futures_order.assert_not_called()

    def test_native_zero_sl_is_not_sent(self) -> None:
        from exchange import BinanceExchangeManager

        client = MagicMock()
        mgr = BinanceExchangeManager.__new__(BinanceExchangeManager)
        mgr.client = client
        mgr._critical_alerts = None
        with patch.object(mgr, "place_conditional_order") as place:
            placed = BinanceExchangeManager.place_native_exit_bracket(
                mgr, "ETHUSDT", "SHORT", 0.0, []
            )
        self.assertEqual(placed, {})
        place.assert_not_called()


if __name__ == "__main__":
    unittest.main()
