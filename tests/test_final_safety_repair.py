"""Deterministic offline tests for the final HYBRID_SNIPER safety repair.

Never connects to Binance, Telegram, or a VPS.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import Config
from core.dca_plan import sl_is_valid_vs_entry
from core.exchange_protection import ensure_native_stop, native_protection_enabled
from core.ops_heartbeat import evaluate_heartbeat_health
from exceptions import OrderExecutionError
from executor import TradeExecutor
from reconciliation import (
    _set_unresolved_orphans,
    clear_state_unreliable,
    entries_blocked_by_unresolved_orphans,
    mark_state_unreliable,
    reconcile_uncertain_dca,
    state_is_unreliable,
)
from utils import round_protective_price


def _rules():
    from exchange import SymbolRules

    return SymbolRules(2, 3, 0.01, 0.001, 0.001, 5.0)


class TestHeartbeatAndHungProcess(unittest.TestCase):
    def test_stale_monitor_unhealthy_even_if_pid_alive(self) -> None:
        now = 1_700_000_000.0
        payload = {
            "timestamp": now,
            "monitor_wall": now - 200.0,
            "scan_wall": now,
            "reconcile_wall": now,
            "main_wall": now,
        }
        verdict = evaluate_heartbeat_health(
            payload, now=now, stale_seconds=180.0, process_running=True
        )
        self.assertFalse(verdict["protection_healthy"])
        self.assertFalse(verdict["allow_restart"])
        self.assertIn("hung_process", verdict["reason"])
        self.assertIn("monitor", verdict["failing"])

    def test_fresh_monitor_healthy_even_if_scanner_quiet(self) -> None:
        now = 1_700_000_000.0
        payload = {
            "timestamp": now - 10.0,
            "monitor_wall": now - 5.0,
            "scan_wall": 0.0,
            "main_wall": now - 8.0,
        }
        verdict = evaluate_heartbeat_health(
            payload, now=now, stale_seconds=180.0, process_running=True
        )
        self.assertTrue(verdict["protection_healthy"])
        self.assertEqual(verdict["reason"], "ok")

    def test_dead_process_allows_restart(self) -> None:
        now = 1_700_000_000.0
        payload = {"monitor_wall": now - 400.0, "timestamp": now - 400.0}
        verdict = evaluate_heartbeat_health(
            payload, now=now, stale_seconds=180.0, process_running=False
        )
        self.assertFalse(verdict["protection_healthy"])
        self.assertTrue(verdict["allow_restart"])

    def test_scanner_refresh_does_not_mask_stale_monitor(self) -> None:
        now = 1_700_000_000.0
        payload = {
            "timestamp": now,
            "monitor_wall": now - 500.0,
            "scan_wall": now,
        }
        verdict = evaluate_heartbeat_health(
            payload, now=now, stale_seconds=180.0, process_running=True
        )
        self.assertFalse(verdict["protection_healthy"])

    def test_watchdog_hung_pid_no_restart(self) -> None:
        from watchdog import PositionWatchdog

        wd = PositionWatchdog.__new__(PositionWatchdog)
        wd.state = MagicMock()
        wd._alert_main_down = MagicMock()
        wd._maybe_restart_main = MagicMock()
        payload = {"monitor_wall": time.time() - 400.0, "timestamp": time.time()}
        with patch(
            "core.ops_heartbeat.read_heartbeat_payload", return_value=payload
        ), patch.object(
            PositionWatchdog, "is_main_process_running", return_value=True
        ), patch.object(Config, "WATCHDOG_MAIN_STALE_SECONDS", 180):
            healthy = PositionWatchdog.check_main_bot_health(wd)
        self.assertFalse(healthy)
        wd._maybe_restart_main.assert_not_called()


class TestNativeProtectiveOrders(unittest.TestCase):
    def test_native_default_is_on(self) -> None:
        self.assertTrue(Config.ENABLE_NATIVE_TP_SL)
        self.assertTrue(native_protection_enabled())

    def test_ensure_is_noop_when_native_off(self) -> None:
        exchange = MagicMock()
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            ok, reason, oid = ensure_native_stop(
                exchange, symbol="BTCUSDT", position_side="LONG", sl_price=99.0
            )
        self.assertFalse(ok)
        self.assertEqual(reason, "native_disabled")
        self.assertIsNone(oid)
        exchange.place_conditional_order.assert_not_called()

    def test_place_then_cancel_old_stop(self) -> None:
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
        exchange.lookup_order_by_id.return_value = {"orderId": 2, "status": "NEW"}
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
        self.assertEqual(oid, "2")
        self.assertEqual(reason, "placed")
        exchange.place_conditional_order.assert_called_once()
        exchange.cancel_order_by_id.assert_called_once_with("ETHUSDT", "1")
        place_order = exchange.place_conditional_order.call_args
        cancel_order = exchange.cancel_order_by_id.call_args
        self.assertEqual(place_order[0][3], "STOP_MARKET")
        self.assertTrue(place_order[1].get("close_position") or place_order[0][3])
        self.assertEqual(cancel_order[0][1], "1")

    def test_keep_existing_if_new_place_fails(self) -> None:
        exchange = MagicMock()
        exchange.get_open_orders.return_value = [
            {
                "orderId": 9,
                "type": "STOP_MARKET",
                "positionSide": "SHORT",
                "stopPrice": "101.0",
            }
        ]
        exchange.get_symbol_rules.return_value = SimpleNamespace(tick_size=0.01)
        exchange.place_conditional_order.side_effect = RuntimeError("timeout")
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            ok, reason, oid = ensure_native_stop(
                exchange,
                symbol="ONUSDT",
                position_side="SHORT",
                sl_price=102.0,
                entry_price=100.0,
                old_sl_order_id="9",
            )
        self.assertTrue(ok)
        self.assertEqual(oid, "9")
        self.assertEqual(reason, "placement_failed_kept_existing")
        exchange.cancel_order_by_id.assert_not_called()

    def test_invalid_sl_rejected(self) -> None:
        exchange = MagicMock()
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            ok, reason, oid = ensure_native_stop(
                exchange,
                symbol="BTCUSDT",
                position_side="SHORT",
                sl_price=99.0,
                entry_price=100.0,
            )
        self.assertFalse(ok)
        self.assertEqual(reason, "invalid_sl_side")
        self.assertIsNone(oid)
        exchange.place_conditional_order.assert_not_called()

    def test_short_and_long_trigger_rounding(self) -> None:
        short = round_protective_price(100.004, 0.01, 2, side="SHORT", kind="SL")
        long = round_protective_price(99.996, 0.01, 2, side="LONG", kind="SL")
        self.assertGreaterEqual(short, 100.01)
        self.assertLessEqual(long, 99.99)
        self.assertTrue(sl_is_valid_vs_entry("SHORT", 100.0, short))
        self.assertTrue(sl_is_valid_vs_entry("LONG", 100.0, long))

    def test_existing_matching_stop_reused(self) -> None:
        exchange = MagicMock()
        exchange.get_open_orders.return_value = [
            {
                "orderId": 44,
                "type": "STOP_MARKET",
                "positionSide": "LONG",
                "stopPrice": "95.00",
            }
        ]
        exchange.get_symbol_rules.return_value = SimpleNamespace(tick_size=0.01)
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            ok, reason, oid = ensure_native_stop(
                exchange,
                symbol="ETHUSDT",
                position_side="LONG",
                sl_price=95.0,
                entry_price=100.0,
            )
        self.assertTrue(ok)
        self.assertEqual(reason, "existing_stop_ok")
        self.assertEqual(oid, "44")
        exchange.place_conditional_order.assert_not_called()


class TestInvalidSlFailsafe(unittest.TestCase):
    def test_manager_closes_invalid_long_sl(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.failsafe_close_trade = MagicMock()
        mgr._advance_profit_stop_ladder = MagicMock()
        mgr._apply_trailing_stop = MagicMock()
        mgr.db = MagicMock()
        mgr.db.get_trade.return_value = {
            "trade_id": "t1",
            "symbol": "AAAUSDT",
            "side": "LONG",
            "status": "OPEN",
            "entry_price": 100.0,
            "stop_loss": 100.5,
            "quantity": 1.0,
        }
        mgr.db.parse_trade_metadata.return_value = {}
        with patch("manager.dca_plan_active", return_value=False), patch(
            "manager.resolved_final_sl", return_value=0.0
        ), patch("manager.sl_is_armed", return_value=True):
            TradeManager._manage_long_trade(mgr, mgr.db.get_trade.return_value, 99.0)
        mgr.failsafe_close_trade.assert_called()
        self.assertEqual(mgr.failsafe_close_trade.call_args[0][1], "INVALID_SL_FAILSAFE")


class TestUncertainDca(unittest.TestCase):
    def test_timeout_does_not_resubmit_or_revert(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.side_effect = TimeoutError("read timed out")
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_entries": 2,
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
        exchange.execute_futures_order.assert_called_once()

    def test_uncertain_blocks_second_add(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
            "dca_uncertain_client_id": "dca2abc",
            "dca_uncertain_step": 2,
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
        exchange.execute_futures_order.assert_not_called()

    def test_ack_new_is_not_a_fill(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.return_value = {
            "status": "NEW",
            "orderId": 77,
            "executedQty": "0",
        }
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
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
        exchange.seed_position_after_fill.assert_not_called()

    def test_hard_reject_reverts(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.get_futures_balance.return_value = 1000.0
        exchange.get_symbol_rules.return_value = _rules()
        exchange.format_quantity.return_value = 0.5
        exchange.execute_futures_order.side_effect = OrderExecutionError(
            "ORDER_REJECTED: -2019"
        )
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
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

    def test_reconcile_applies_late_dca_fill(self) -> None:
        exchange = MagicMock()
        exchange.lookup_order_by_id.return_value = None
        exchange.lookup_order_by_client_id.return_value = {
            "status": "FILLED",
            "orderId": 8,
            "executedQty": "0.5",
            "avgPrice": "97.0",
        }
        db = MagicMock()
        trade = {
            "trade_id": "t-dca",
            "symbol": "ONUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
        }
        db.get_open_trades.return_value = [trade]
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "dca_uncertain_client_id": "dca2abc",
            "dca_uncertain_step": 2,
            "avg_entry_price": 100.0,
        }
        executor = MagicMock()
        executor.db = db
        executor.adopt_confirmed_dca_fill.return_value = {"ok": True}
        n = reconcile_uncertain_dca(exchange, executor)
        self.assertEqual(n, 1)
        executor.adopt_confirmed_dca_fill.assert_called_once()


class TestRestartRecovery(unittest.TestCase):
    def tearDown(self) -> None:
        _set_unresolved_orphans([])
        clear_state_unreliable()

    def test_orphan_exchange_blocks_new_entries(self) -> None:
        _set_unresolved_orphans(
            [{"symbol": "BTCUSDT", "positionSide": "LONG", "quantity": 0.1}]
        )
        blocked, reason = entries_blocked_by_unresolved_orphans()
        self.assertTrue(blocked)
        self.assertIn("orphan", reason.lower())

    def test_cleared_orphans_allow_entries(self) -> None:
        _set_unresolved_orphans([])
        blocked, reason = entries_blocked_by_unresolved_orphans()
        self.assertFalse(blocked)
        self.assertEqual(reason, "")

    def test_unreliable_state_blocks_entries(self) -> None:
        mark_state_unreliable("startup_reconcile_failed")
        blocked, reason = state_is_unreliable()
        self.assertTrue(blocked)
        self.assertIn("unreliable", reason.lower())
        clear_state_unreliable()
        blocked, _ = state_is_unreliable()
        self.assertFalse(blocked)

    def test_entries_allowed_honors_orphan_gate(self) -> None:
        import main as main_mod

        _set_unresolved_orphans(
            [{"symbol": "ETHUSDT", "positionSide": "SHORT", "quantity": 2}]
        )
        scheduler = MagicMock()
        scheduler.is_entry_paused.return_value = (False, "")
        risk = MagicMock()
        allowed, reason = main_mod._entries_allowed(scheduler, risk, MagicMock())
        self.assertFalse(allowed)
        self.assertIn("orphan", reason.lower())
        scheduler.is_entry_paused.assert_not_called()


class TestScannerAndCapacity(unittest.TestCase):
    def test_live_score_skips_super_cache(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        rec = SimpleNamespace(
            strategy="SMC_TREND",
            score=90.0,
            candidate={"structure_metadata": {}},
            backup_strategies=[],
        )
        snapshot = SimpleNamespace(spread_pct=0.01, candles={})
        orch.funnel = SimpleNamespace(_super={"AAAUSDT": rec})
        orch._build_eval_snapshot = MagicMock(return_value=snapshot)
        orch._evaluate_symbol = MagicMock(return_value=None)
        orch._super_eval_cache = {
            "AAAUSDT": (("fp",), rec, "cached", False, time.monotonic())
        }
        with patch(
            "pipeline.event_scan_orchestrator._snapshot_kline_fingerprint",
            return_value=("fp",),
        ):
            out = EventScanOrchestrator._evaluate_super_symbol(
                orch,
                "AAAUSDT",
                timeframe="5m",
                open_symbols=set(),
                ticker_map={},
                book_map={},
                live_score_only=True,
            )
        self.assertIsNone(out)
        orch._evaluate_symbol.assert_called()

    def test_missing_market_data_skips_without_close(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr._close_position = MagicMock()
        mgr.failsafe_close_trade = MagicMock()
        mgr.db = MagicMock()
        trade = {
            "trade_id": "t1",
            "symbol": "AAAUSDT",
            "side": "LONG",
            "status": "OPEN",
            "quantity": 1.0,
        }
        mgr.db.get_trade.return_value = trade
        mgr._ensure_local_exit_memory = lambda t: t
        mgr._maybe_execute_dca = MagicMock()
        mgr._manage_long_trade = MagicMock()
        mgr._ws_position_mark = MagicMock(return_value=0.0)
        mgr._resolve_monitor_price = MagicMock(return_value=0.0)
        mgr._position_qty_map = MagicMock()
        with patch("manager.position_reconcile_guard") as guard, patch(
            "manager.is_within_position_grace_period", return_value=False
        ), patch("manager.is_range_strategy", return_value=False):
            guard.note_present = MagicMock()
            TradeManager._monitor_single_trade(
                mgr, trade, {("AAAUSDT", "LONG"): 1.0}, ws_only=True
            )
        mgr._manage_long_trade.assert_not_called()
        mgr.failsafe_close_trade.assert_not_called()

    def test_position_cap_constant(self) -> None:
        self.assertEqual(Config.MAX_POSITIONS, 12)


class TestShutdownAndDb(unittest.TestCase):
    def test_manager_stop_joins_workers(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr._monitor_stop = threading.Event()
        mgr._tick_event = threading.Event()
        mgr._close_queue = MagicMock()
        alive = MagicMock()
        alive.is_alive.return_value = True
        alive.join = MagicMock()
        mgr._tick_worker = alive
        mgr._fast_monitor = MagicMock()
        mgr._fast_monitor.is_alive.return_value = False
        mgr._close_worker = MagicMock()
        mgr._close_worker.is_alive.return_value = False
        TradeManager.stop(mgr)
        alive.join.assert_called()
        self.assertGreaterEqual(alive.join.call_args.kwargs.get("timeout") or 0, 8.0)

    def test_db_failure_blocks_entries(self) -> None:
        import main as main_mod

        scheduler = MagicMock()
        scheduler.is_entry_paused.side_effect = RuntimeError("db locked")
        allowed, reason = main_mod._entries_allowed(
            scheduler, MagicMock(), MagicMock()
        )
        self.assertFalse(allowed)
        self.assertIn("entry_gate_error", reason)


class TestCrashProtectionUnavailable(unittest.TestCase):
    def test_virtual_sl_does_not_claim_crash_safety(self) -> None:
        self.assertTrue(Config.ENABLE_NATIVE_TP_SL)
        self.assertTrue(Config.ENABLE_SOFT_TP_SL)
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            from core.exchange_protection import native_protection_enabled

            self.assertFalse(native_protection_enabled())

    def test_native_disabled_is_not_verified_protection(self) -> None:
        from core.exchange_protection import protection_is_verified

        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            self.assertFalse(protection_is_verified("native_disabled", None))
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            self.assertTrue(protection_is_verified("placed", "99"))
            self.assertFalse(protection_is_verified("placed_unverified", "99"))


class TestNativeVerifyBeforeCancel(unittest.TestCase):
    def test_rejected_new_stop_keeps_existing(self) -> None:
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
        exchange.lookup_order_by_id.return_value = {"orderId": 2, "status": "REJECTED"}
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
        self.assertEqual(reason, "new_rejected_kept_existing")
        exchange.cancel_order_by_id.assert_not_called()

    def test_unverified_lookup_keeps_existing(self) -> None:
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
        self.assertEqual(reason, "placed_unverified_kept_existing")
        exchange.cancel_order_by_id.assert_not_called()


class TestDatabaseFailClosed(unittest.TestCase):
    def test_get_open_trades_raises_on_sqlite_error(self) -> None:
        from exceptions import DatabaseError
        from database import DatabaseManager

        db = DatabaseManager.__new__(DatabaseManager)
        db.connection = MagicMock()
        db.connection.return_value.__enter__.side_effect = __import__("sqlite3").Error(
            "disk image malformed"
        )
        db.connection.return_value.__exit__.return_value = False
        with self.assertRaises(DatabaseError):
            DatabaseManager.get_open_trades(db)


class TestTelegramExecutionState(unittest.TestCase):
    def test_trade_alert_says_fill_not_ack_and_virtual_protection(self) -> None:
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
            native_enabled=False,
        )
        msg = tg.send_message.call_args[0][0]
        self.assertIn("NEW TRADE EXECUTED", msg)
        self.assertIn("Fill confirmed (FILLED)", msg)
        self.assertIn("not an ACK", msg)
        self.assertIn("Exchange SL:</b> OFF", msg)

    def test_uncertain_alert_does_not_claim_fill(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.send_message = MagicMock()
        TelegramManager.send_uncertain_order_alert(
            tg,
            symbol="ETHUSDT",
            side="LONG",
            status="NEW",
            client_order_id="dca2x",
            context="DCA",
        )
        msg = tg.send_message.call_args[0][0]
        self.assertIn("UNCERTAIN ORDER", msg)
        self.assertNotIn("NEW TRADE EXECUTED", msg)


class TestSlotReleaseAndMetrics(unittest.TestCase):
    def test_note_filled_releases_super_slot(self) -> None:
        from core.tier_funnel import TierFunnel

        funnel = TierFunnel.__new__(TierFunnel)
        funnel._super = {"AAAUSDT": object()}
        funnel._super_order = ["AAAUSDT"]
        funnel._super_index = 0
        funnel._hot = {}
        funnel._hot_order = []
        funnel._pending_hot_rest = set()
        funnel._promoted = {"AAAUSDT"}
        TierFunnel.note_filled(funnel, "AAAUSDT")
        self.assertNotIn("AAAUSDT", funnel._super)
        self.assertEqual(funnel._super_order, [])

    def test_scan_duration_is_recorded(self) -> None:
        from core.ops_heartbeat import record_scan_duration_ms
        from core import ops_heartbeat as hb

        record_scan_duration_ms(42.5)
        self.assertAlmostEqual(hb._last_scan_duration_ms, 42.5)


if __name__ == "__main__":
    unittest.main()
