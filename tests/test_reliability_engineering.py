"""Offline regression tests for reliability-engineering repairs."""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from core.dca_plan import weighted_average_entry
from core.exchange_protection import (
    MISSING,
    NATIVE_DISABLED,
    UNCERTAIN,
    VERIFIED,
    apply_protection_metadata,
    ensure_native_stop,
    protection_is_verified,
    protection_status,
)
from core.restart_state import mark_restart_exhausted, read_restart_state
from core.strategy_score_ranges import execution_min_score, score_range_for
from core.system_health import DEGRADED, UNPROTECTED, system_health_snapshot
from engines.smc_engine import effective_smc_min_score
from kline_bootstrap import merge_ohlcv_frames
from pipeline.event_scan_orchestrator import EventScanOrchestrator


class TestProtectionVerification(unittest.TestCase):
    def test_only_matched_or_lookup_new_is_verified(self) -> None:
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            self.assertTrue(protection_is_verified("placed", "9"))
            self.assertTrue(protection_is_verified("existing_stop_ok", "9"))
            self.assertFalse(protection_is_verified("placed_unverified", "9"))
            self.assertFalse(protection_is_verified("new_rejected_kept_existing", "1"))
            self.assertEqual(protection_status("placed", "9"), VERIFIED)
            self.assertEqual(
                protection_status("placed_unverified_kept_existing", "1"), UNCERTAIN
            )
            self.assertEqual(protection_status("native_disabled", None), MISSING)
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            self.assertEqual(protection_status("placed", "9"), NATIVE_DISABLED)
            meta = apply_protection_metadata({}, reason="placed", order_id="9")
            self.assertFalse(meta["native_protection_verified"])

    def test_native_on_without_inventory_is_degraded_not_healthy(self) -> None:
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True), patch(
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
            snap = system_health_snapshot(open_trades=1, db_ok=True, ws_state="HEALTHY")
        self.assertEqual(snap["dimensions"]["exchange_protection"], DEGRADED)
        self.assertNotEqual(snap["overall"], "HEALTHY")

    def test_native_on_unverified_open_is_unprotected(self) -> None:
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True), patch(
            "core.system_health.read_heartbeat_payload",
            return_value={
                "main_wall": 1.0,
                "monitor_wall": 1.0,
                "reconcile_wall": 1.0,
                "native_unprotected_open": 1,
                "native_verified": 0,
            },
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
            snap = system_health_snapshot(open_trades=1, db_ok=True, ws_state="HEALTHY")
        self.assertEqual(snap["overall"], UNPROTECTED)

    def test_ensure_stop_keeps_existing_on_unverified_new(self) -> None:
        exchange = MagicMock()
        exchange.get_open_orders.return_value = [
            {
                "orderId": 1,
                "type": "STOP_MARKET",
                "positionSide": "LONG",
                "stopPrice": "90.0",
            }
        ]
        exchange.get_symbol_rules.return_value = SimpleNamespace(tick_size=0.1)
        exchange.place_conditional_order.return_value = {"orderId": 2}
        exchange.lookup_order_by_id.return_value = None
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            ok, reason, oid = ensure_native_stop(
                exchange,
                symbol="ETHUSDT",
                position_side="LONG",
                sl_price=95.0,
                entry_price=100.0,
                old_sl_order_id="1",
            )
        self.assertTrue(ok)
        self.assertEqual(oid, "1")
        self.assertFalse(protection_is_verified(reason, oid))
        exchange.cancel_order_by_id.assert_not_called()


class TestScoreAndSuperConsistency(unittest.TestCase):
    def test_smc_engine_floor_matches_execution_min_live(self) -> None:
        env = {"SMC_TREND_HOT_SCORE": "", "SMC_TREND_SUPER_SCORE": ""}
        with patch.dict("os.environ", env, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False), patch.object(
            Config, "STRATEGY_MIN_SCORE", 70.0
        ), patch.object(Config, "MULTI_CONFLUENCE_MIN_SCORE", 65.0):
            exec_min = execution_min_score("SMC_TREND")
            engine_min = effective_smc_min_score("SWEEP_RETEST+OB_RETEST", "LONG")
            band = score_range_for("SMC_TREND")
            self.assertEqual(exec_min, 70.0)
            self.assertEqual(engine_min, 70.0)
            self.assertGreaterEqual(band.super_score, exec_min)
            self.assertEqual(band.classify(69.9), "HOT")
            self.assertEqual(band.classify(70.0), "SUPER")

    def test_testnet_relax_lowers_engine_floor_only_on_testnet(self) -> None:
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
        ), patch.object(Config, "LSC_MIN_SCORE", 68.0):
            live_floor = Config.effective_min_score(Config.LSC_MIN_SCORE)
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
        ), patch.object(Config, "TESTNET_MIN_SCORE_RELAX", 10.0), patch.object(
            Config, "LSC_MIN_SCORE", 68.0
        ):
            testnet_floor = Config.effective_min_score(Config.LSC_MIN_SCORE)
        self.assertEqual(live_floor, 68.0)
        self.assertEqual(testnet_floor, 58.0)


class TestCandleBootstrap(unittest.TestCase):
    def test_merge_dedupes_and_orders_timestamps(self) -> None:
        older = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-01-01 00:00", "2026-01-01 00:05"]),
                "open": [1.0, 1.1],
                "high": [1.2, 1.3],
                "low": [0.9, 1.0],
                "close": [1.1, 1.2],
                "volume": [1.0, 1.0],
            }
        )
        newer = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-01-01 00:05", "2026-01-01 00:10"]),
                "open": [1.15, 1.2],
                "high": [1.4, 1.5],
                "low": [1.0, 1.1],
                "close": [1.25, 1.3],
                "volume": [2.0, 2.0],
            }
        )
        merged = merge_ohlcv_frames(older, newer)
        self.assertEqual(len(merged), 3)
        self.assertTrue(merged["timestamp"].is_monotonic_increasing)
        mid = merged.iloc[1]
        self.assertEqual(float(mid["close"]), 1.25)

    def test_incomplete_history_fetches_required_depth_not_two_bars(self) -> None:
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
            {"close": [1.0] * 10}
        )
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = []
        orch._hub.get_candles_cached_only.return_value = pd.DataFrame({"close": [1.0] * 40})
        with patch.object(Config, "SCAN_WS_ONLY", True), patch.object(
            Config, "ENABLE_PACED_KLINE_BOOTSTRAP", True
        ), patch.object(Config, "get_scan_kline_intervals", return_value=["5m"]):
            orch._maybe_fetch_single_closed_kline(["AAAUSDT"])
        args = orch.exchange.fetch_bootstrap_klines_df.call_args[0]
        self.assertEqual(args[0], "AAAUSDT")
        self.assertGreaterEqual(int(args[2]), 201)

    def test_duplicate_inflight_skips_second_fetch(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch.exchange = MagicMock()
        orch.exchange.in_scan_mode = False
        orch.exchange._ws_reconnect_or_warmup.return_value = False
        orch.exchange.can_bootstrap_klines_rest.return_value = True
        orch.exchange.can_make_background_rest_call.return_value = True
        orch._kline_rest_inflight = {("AAAUSDT", "5m")}
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["AAAUSDT"]
        orch._hub.get_candles_cached_only.return_value = pd.DataFrame()
        with patch.object(Config, "SCAN_WS_ONLY", True), patch.object(
            Config, "ENABLE_PACED_KLINE_BOOTSTRAP", True
        ), patch.object(Config, "get_scan_kline_intervals", return_value=["5m"]):
            orch._maybe_fetch_single_closed_kline(["AAAUSDT"])
        orch.exchange.fetch_bootstrap_klines_df.assert_not_called()


class TestWebsocketEntryGate(unittest.TestCase):
    def test_degraded_ws_blocks_entries_not_when_healthy(self) -> None:
        import main as main_mod

        exchange = MagicMock()
        exchange._market_data.ws_state_is_degraded.return_value = True
        blocked, reason = main_mod._market_data_blocks_new_entries(exchange)
        self.assertTrue(blocked)
        self.assertIn("websocket", reason)

        exchange._market_data.ws_state_is_degraded.return_value = False
        blocked, reason = main_mod._market_data_blocks_new_entries(exchange)
        self.assertFalse(blocked)

        mock_risk = MagicMock()
        mock_risk.exchange._market_data.ws_state_is_degraded.return_value = True
        scheduler = MagicMock()
        scheduler.is_entry_paused.return_value = (False, "")
        mock_risk.get_exchange_open_positions_count.return_value = 0
        mock_risk.can_open_trade.return_value = (True, "")
        with patch("main.state_is_unreliable", return_value=(False, "")), patch(
            "main.entries_blocked_by_unresolved_orphans", return_value=(False, "")
        ):
            allowed, gate = main_mod._entries_allowed(scheduler, mock_risk, MagicMock())
        self.assertFalse(allowed)
        self.assertIn("websocket", gate)


class TestDcaAccounting(unittest.TestCase):
    def test_weighted_average_ignores_planned_zero_qty(self) -> None:
        avg = weighted_average_entry([(100.0, 2.0), (90.0, 0.0), (80.0, 1.0)])
        self.assertAlmostEqual(avg, (200.0 + 80.0) / 3.0)

    def test_dca_uncertain_does_not_resubmit(self) -> None:
        from executor import TradeExecutor

        executor = TradeExecutor(MagicMock(), MagicMock())
        executor.db.parse_trade_metadata.return_value = {
            "dca_uncertain_order_id": "9",
            "base_entry_price": 100.0,
        }
        trade = {"symbol": "ETHUSDT", "side": "LONG", "entry_stage": 2, "trade_id": "t1"}
        with patch("executor.dca_enabled", return_value=True), patch.object(
            Config, "DRY_RUN", False
        ), patch("executor.entry_stage_of", return_value=2):
            out = executor.execute_dca_add(trade, 2, 95.0)
        self.assertIsNone(out)
        executor.exchange.execute_futures_order.assert_not_called()


class TestRestartAndEnv(unittest.TestCase):
    def test_restart_exhaustion_persists(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(Config, "DATA_DIR", tmp):
                mark_restart_exhausted(6, 5)
                state = read_restart_state()
        self.assertTrue(state.get("exhausted"))
        self.assertEqual(int(state.get("crash_limit")), 5)

    def test_testtrade_disabled_by_default(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.exchange = MagicMock()
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "DRY_RUN", False
        ), patch.object(Config, "ENABLE_TESTTRADE_COMMAND", False):
            msg = tg._place_testnet_test_trade("ETHUSDT")
        self.assertIn("disabled", msg.lower())
        tg.exchange.execute_futures_order.assert_not_called()

    def test_telegram_does_not_call_ack_confirmed_without_verify(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.send_message = MagicMock()
        tg._actual_display_price = MagicMock(return_value=100.0)
        TelegramManager.send_trade_alert(
            tg,
            action="LONG",
            symbol="ETHUSDT",
            price=100.0,
            tp1=110.0,
            sl=90.0,
            quantity=1.0,
            fill_status="FILLED",
            native_enabled=True,
            native_sl_order_id="99",
            native_protection_verified=False,
        )
        msg = tg.send_message.call_args[0][0]
        self.assertIn("UNCERTAIN", msg)
        self.assertNotIn("verified id=", msg)

    def test_max_positions_still_twelve(self) -> None:
        self.assertEqual(Config.MAX_POSITIONS, 12)


if __name__ == "__main__":
    unittest.main()
