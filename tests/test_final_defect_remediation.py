"""Offline tests for final defect remediation (native verify, health, DRY_RUN)."""

from __future__ import annotations

import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import Config, _env_dry_run
from core.exchange_protection import (
    ensure_native_stop,
    protection_is_verified,
)
from core.system_health import NOT_READY, UNPROTECTED, system_health_snapshot
from core.strategy_score_ranges import execution_min_score, score_range_for
from executor import TradeExecutor
from utils import round_protective_price


class TestDryRunFailClosed(unittest.TestCase):
    def test_unset_and_invalid_dry_run_block_orders(self) -> None:
        with patch.dict(os.environ, {"USE_TESTNET": "true"}, clear=False):
            os.environ.pop("DRY_RUN", None)
            self.assertTrue(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "nope"}, clear=False):
            self.assertTrue(_env_dry_run())
        with patch.object(Config, "DRY_RUN", True):
            self.assertFalse(Config.orders_allowed())
        with patch.object(Config, "DRY_RUN", False):
            self.assertTrue(Config.orders_allowed())

    def test_exchange_order_blocked_when_dry_run(self) -> None:
        from exchange import BinanceExchangeManager
        from exceptions import OrderExecutionError

        mgr = BinanceExchangeManager.__new__(BinanceExchangeManager)
        with patch.object(Config, "DRY_RUN", True):
            with self.assertRaises(OrderExecutionError):
                mgr.execute_futures_order("ETHUSDT", "BUY", "LONG", 1.0)


class TestFillQtyNoPlannedFallback(unittest.TestCase):
    def test_adopt_fill_ignores_missing_executed_qty(self) -> None:
        executor = TradeExecutor(MagicMock(), MagicMock())
        record = SimpleNamespace(
            symbol="ETHUSDT",
            quantity=4.0,
            entry=100.0,
            atr=1.0,
            strategy="SMC_TREND",
            score=80.0,
            exec_id="e1",
            binance_order_id="9",
            is_opened=lambda: False,
            is_unresolved=lambda: True,
        )
        ledger = MagicMock()
        ledger.find_by_ids.return_value = record
        with patch("executor.get_execution_ledger", return_value=ledger):
            out = executor.adopt_confirmed_fill(
                {
                    "status": "FILLED",
                    "orderId": "9",
                    "executedQty": "0",
                    "avgPrice": "100",
                    "symbol": "ETHUSDT",
                }
            )
        self.assertIsNone(out)


class TestSystemHealthDimensions(unittest.TestCase):
    def test_open_position_with_native_off_is_unprotected(self) -> None:
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False), patch(
            "core.system_health.read_heartbeat_payload",
            return_value={"main_wall": 1.0, "monitor_wall": 1.0, "reconcile_wall": 1.0},
        ), patch("core.system_health.get_scan_history_ready", return_value=True), patch(
            "core.system_health.watchdog_sibling_status",
            return_value={"fresh": True},
        ), patch(
            "core.system_health.get_main_stall_seconds", return_value=1.0
        ), patch(
            "core.system_health.get_monitor_stall_seconds", return_value=1.0
        ), patch(
            "core.system_health.get_reconcile_stall_seconds", return_value=1.0
        ):
            snap = system_health_snapshot(open_trades=2, db_ok=True, ws_state="HEALTHY")
        self.assertEqual(snap["overall"], UNPROTECTED)
        self.assertEqual(snap["dimensions"]["exchange_protection"], UNPROTECTED)

    def test_incomplete_history_is_not_ready(self) -> None:
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False), patch(
            "core.system_health.read_heartbeat_payload",
            return_value={"main_wall": 1.0, "monitor_wall": 1.0, "reconcile_wall": 1.0},
        ), patch("core.system_health.get_scan_history_ready", return_value=False), patch(
            "core.system_health.get_incomplete_kline_count", return_value=3
        ), patch(
            "core.system_health.watchdog_sibling_status",
            return_value={"fresh": True},
        ), patch(
            "core.system_health.get_main_stall_seconds", return_value=1.0
        ), patch(
            "core.system_health.get_monitor_stall_seconds", return_value=1.0
        ), patch(
            "core.system_health.get_reconcile_stall_seconds", return_value=1.0
        ):
            snap = system_health_snapshot(open_trades=0, db_ok=True, ws_state="HEALTHY")
        self.assertEqual(snap["overall"], NOT_READY)
        self.assertEqual(snap["dimensions"]["scanner"], NOT_READY)
        self.assertEqual(snap["dimensions"]["candle_history"], NOT_READY)


class TestNativeVerifyAndRounding(unittest.TestCase):
    def test_long_and_short_protective_rounding(self) -> None:
        long_sl = round_protective_price(99.14, 0.1, 1, side="LONG", kind="SL")
        short_sl = round_protective_price(100.86, 0.1, 1, side="SHORT", kind="SL")
        self.assertLessEqual(long_sl, 99.14)
        self.assertGreaterEqual(short_sl, 100.86)

    def test_ack_without_lookup_is_not_verified(self) -> None:
        from exchange import BinanceExchangeManager

        mgr = BinanceExchangeManager.__new__(BinanceExchangeManager)
        mgr.place_conditional_order = MagicMock(return_value={"orderId": 7})
        mgr.lookup_order_by_id = MagicMock(return_value=None)
        mgr.get_open_orders = MagicMock(return_value=[])
        placed = BinanceExchangeManager.place_native_exit_bracket(
            mgr, "ETHUSDT", "LONG", 90.0, []
        )
        self.assertNotIn("SL", placed)

    def test_kept_existing_is_uncertain_not_verified(self) -> None:
        from core.exchange_protection import protection_status, UNCERTAIN

        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            self.assertFalse(protection_is_verified("new_rejected_kept_existing", "1"))
            self.assertEqual(
                protection_status("new_rejected_kept_existing", "1"), UNCERTAIN
            )
            self.assertTrue(protection_is_verified("placed", "1"))
            self.assertTrue(protection_is_verified("existing_stop_ok", "1"))
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            self.assertFalse(protection_is_verified("placed", "1"))

    def test_ensure_stop_verifies_before_cancel(self) -> None:
        exchange = MagicMock()
        exchange.get_open_orders.return_value = [
            {
                "orderId": 1,
                "type": "STOP_MARKET",
                "positionSide": "SHORT",
                "stopPrice": "110.0",
            }
        ]
        exchange.get_symbol_rules.return_value = SimpleNamespace(tick_size=0.1)
        exchange.place_conditional_order.return_value = {"orderId": 2}
        exchange.lookup_order_by_id.return_value = {"orderId": 2, "status": "NEW"}
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            ok, reason, oid = ensure_native_stop(
                exchange,
                symbol="ETHUSDT",
                position_side="SHORT",
                sl_price=110.5,
                entry_price=100.0,
                old_sl_order_id="1",
            )
        self.assertTrue(ok)
        self.assertEqual(reason, "placed")
        self.assertEqual(oid, "2")
        exchange.cancel_order_by_id.assert_called()


class TestSuperAndMultiConfluence(unittest.TestCase):
    def test_smc_boundaries_hot_vs_super_vs_exec(self) -> None:
        env = {
            "SMC_TREND_HOT_SCORE": "",
            "SMC_TREND_SUPER_SCORE": "",
        }
        with patch.dict("os.environ", env, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False), patch.object(
            Config, "STRATEGY_MIN_SCORE", 70.0
        ):
            band = score_range_for("SMC_TREND")
            exec_min = execution_min_score("SMC_TREND")
        self.assertEqual(exec_min, 70.0)
        self.assertEqual(band.classify(69.9), "HOT")
        self.assertEqual(band.classify(70.0), "SUPER")

    def test_validate_candidate_rejects_multi_confluence_below_exec_min(self) -> None:
        import main as main_mod

        candidate = {
            "symbol": "ETHUSDT",
            "action": "LONG",
            "atr": 1.0,
            "price": 100.0,
            "score": 65.0,
            "strategy": "SMC_TREND",
            "structure_metadata": {"confluence_type": "BOS+FVG", "macro_trend": "BULLISH"},
        }
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False
        ):
            ok, reason, _ = main_mod._validate_candidate(candidate)
        self.assertFalse(ok)
        self.assertIn("execution minimum", reason)


class TestRestartExhaustion(unittest.TestCase):
    def test_exhausted_state_roundtrip(self) -> None:
        from core import restart_state

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "crash_restart_state.json")
            with patch.object(restart_state, "restart_state_path", return_value=path):
                restart_state.mark_restart_exhausted(6, 5)
                data = restart_state.read_restart_state()
                self.assertTrue(data.get("exhausted"))
                prior = restart_state.acknowledge_manual_start()
                self.assertTrue(prior.get("exhausted"))
                self.assertFalse(restart_state.read_restart_state().get("exhausted"))


class TestLauncherExists(unittest.TestCase):
    def test_sibling_launcher_does_not_import_exchange(self) -> None:
        root = os.path.join(os.path.dirname(__file__), "..")
        path = os.path.normpath(os.path.join(root, "start_hybrid_sniper.py"))
        self.assertTrue(os.path.isfile(path))
        text = open(path, encoding="utf-8").read()
        self.assertNotIn("BinanceExchangeManager", text)
        self.assertIn("watchdog.py", text)
        self.assertIn("main.py", text)


if __name__ == "__main__":
    unittest.main()
