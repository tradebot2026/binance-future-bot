"""Regression tests for remaining audit wire-ups (Telegram, heartbeat, daily baseline)."""

from __future__ import annotations

import inspect
import json
import os
import queue
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import Config
from core.daily_balance_manager import (
    DailyBalanceState,
    _save_state,
    on_utc_day_rollover,
)
from core.exchange_protection import alert_unprotected_position
from core.ops_heartbeat import (
    get_last_api_error,
    get_ws_health_state,
    note_api_error,
    set_ws_health_state,
)
from executor import TradeExecutor
from risk_manager import DailyPnLMetrics, daily_max_loss_reached
from scheduler import DailyScheduler


def _metrics(
    *,
    start_balance: float,
    current_wallet: float,
    equity_pct: float,
) -> DailyPnLMetrics:
    return DailyPnLMetrics(
        start_balance=start_balance,
        current_wallet=current_wallet,
        equity_day_pnl=current_wallet - start_balance if start_balance else 0.0,
        equity_day_pnl_percent=equity_pct,
        realized_pnl=0.0,
        unrealized_pnl=0.0,
        total_pnl=0.0,
        realized_pnl_percent=0.0,
        total_pnl_percent=equity_pct,
    )


class TestNativeEnableGate(unittest.TestCase):
    def test_native_attaches_only_when_config_flag_is_true(self) -> None:
        executor = TradeExecutor(MagicMock(), MagicMock())
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
            self.assertFalse(executor._should_attach_native_exits(True, {}, {}))
        with patch.object(Config, "ENABLE_NATIVE_TP_SL", True):
            self.assertTrue(executor._should_attach_native_exits(False, {}, {}))

    def test_verified_native_path_sends_protection_alert(self) -> None:
        source = inspect.getsource(TradeExecutor._place_native_exit_orders)
        self.assertIn("_notify_protection", source)
        self.assertIn("verified=True", source)

    def test_unprotected_uses_protection_alert_hook(self) -> None:
        tg = MagicMock()
        alert_unprotected_position(
            tg, symbol="ETHUSDT", position_side="LONG", reason="placed_unverified"
        )
        tg.send_protection_alert.assert_called_once()
        tg.send_message.assert_not_called()


class TestTelegramUncertainAndLateFill(unittest.TestCase):
    def test_dca_uncertain_sends_telegram(self) -> None:
        executor = TradeExecutor(MagicMock(), MagicMock())
        executor.telegram = MagicMock()
        trade = {"trade_id": "t1", "symbol": "ETHUSDT", "side": "LONG"}
        executor._mark_dca_uncertain(
            trade,
            {},
            2,
            client_order_id="dca2x",
            order_id="9",
            status="NEW",
        )
        executor.telegram.send_uncertain_order_alert.assert_called_once()
        kwargs = executor.telegram.send_uncertain_order_alert.call_args.kwargs
        self.assertEqual(kwargs["symbol"], "ETHUSDT")
        self.assertEqual(kwargs["context"], "DCA2")
        self.assertEqual(kwargs["status"], "NEW")

    def test_main_attaches_executor_telegram(self) -> None:
        import main as main_mod

        source = inspect.getsource(main_mod.main)
        self.assertIn("executor.telegram = tg", source)

    def test_late_fill_alert_includes_native_verification(self) -> None:
        from main import _adopt_pending_user_fills

        market_data = MagicMock()
        market_data.drain_pending_order_fills.return_value = [{"orderId": "1"}]
        executor = MagicMock()
        executor.adopt_confirmed_fill.return_value = {
            "action": "LONG",
            "symbol": "ETHUSDT",
            "entry_price": 100.0,
            "take_profit_1": 110.0,
            "stop_loss": 90.0,
            "take_profit_2": 0.0,
            "take_profit_3": 0.0,
            "score": 75.0,
            "strategy": "SMC_TREND",
            "quantity": 1.0,
            "metadata": {
                "native_sl_order_id": "99",
                "native_protection_verified": True,
                "structure": {
                    "funnel_score": 80.0,
                    "execution_min": 70.0,
                    "live_score": 75.0,
                },
            },
        }
        tg = MagicMock()
        _adopt_pending_user_fills(
            market_data, executor, tg, MagicMock(), MagicMock(), None
        )
        kwargs = tg.send_trade_alert.call_args.kwargs
        self.assertTrue(kwargs["native_protection_verified"])
        self.assertEqual(kwargs["native_sl_order_id"], "99")
        self.assertEqual(kwargs["funnel_score"], 80.0)
        self.assertEqual(kwargs["execution_min"], 70.0)

    def test_reconcile_close_without_manager_sends_telegram(self) -> None:
        from reconciliation import finalize_reconciled_trade_close

        exchange = MagicMock()
        exchange.refresh_wallet_after_trade.return_value = 1000.0
        db = MagicMock()
        db.close_trade_and_sync_stats.return_value = -1.5
        tg = MagicMock()
        trade = {
            "symbol": "ETHUSDT",
            "side": "LONG",
            "trade_id": "t1",
            "strategy": "SMC_TREND",
        }
        resolved = SimpleNamespace(
            exit_price=90.0, realized_pnl=-1.5, source="income"
        )
        with patch(
            "reconciliation.resolve_exchange_close_pnl", return_value=resolved
        ):
            pnl = finalize_reconciled_trade_close(
                exchange,
                db,
                trade,
                "RECONCILED_MANUAL_CLOSE",
                telegram=tg,
            )
        self.assertEqual(pnl, -1.5)
        tg.send_close_alert.assert_called_once()
        kwargs = tg.send_close_alert.call_args.kwargs
        self.assertEqual(kwargs["reason"], "RECONCILED_MANUAL_CLOSE")
        self.assertEqual(kwargs["symbol"], "ETHUSDT")
        self.assertEqual(kwargs["pnl_source"], "income")
        self.assertEqual(kwargs["pnl"], -1.5)


class TestHeartbeatInstrumentation(unittest.TestCase):
    def test_ws_and_api_error_roundtrip(self) -> None:
        note_api_error("audit-api-error-xyz")
        msg, stamp = get_last_api_error()
        self.assertIn("audit-api-error-xyz", msg)
        self.assertGreater(stamp, 0)
        set_ws_health_state("degraded")
        self.assertEqual(get_ws_health_state(), "DEGRADED")

    def test_exchange_records_api_error_on_heartbeat(self) -> None:
        from exchange import BinanceExchangeManager

        BinanceExchangeManager._note_heartbeat_api_error("rest-fail-abc")
        msg, _ = get_last_api_error()
        self.assertIn("rest-fail-abc", msg)

    def test_ws_snapshot_records_heartbeat_state(self) -> None:
        from market_data_hub import MarketDataHub

        hub = MarketDataHub(MagicMock())
        hub._ws_running = True
        hub._ws_started_at = time.monotonic() - 120.0
        hub._tickers = {"ETHUSDT": {"symbol": "ETHUSDT", "lastPrice": 1.0}}
        now = time.monotonic()
        hub._last_ticker_event_at = now
        hub._last_book_event_at = now
        with patch.object(Config, "ENABLE_WS_BOOK_STREAM", False), patch(
            "core.ops_heartbeat.set_ws_health_state"
        ) as setter:
            snap = hub.get_ws_health_snapshot()
        setter.assert_called()
        self.assertEqual(setter.call_args[0][0], snap["state"])

    def test_main_heartbeat_passes_market_data(self) -> None:
        import main as main_mod

        source = inspect.getsource(main_mod._write_bot_heartbeat)
        self.assertIn("set_ws_health_state", source)
        loop_src = inspect.getsource(main_mod.main)
        self.assertIn("_write_bot_heartbeat(cycle, market_data)", loop_src)

    def test_diagnostics_include_last_api_error(self) -> None:
        from reporter import format_bot_health_message

        note_api_error("diag-api-zzz")
        exchange = MagicMock()
        exchange.rest_usage_snapshot.return_value = {
            "state": "HEALTHY",
            "requests_1m": 1,
            "used_weight_1m": 1,
            "weight_limit": 1500,
            "weight_order_1m": 0,
            "weight_hot_1m": 0,
            "weight_normal_1m": 1,
            "weight_throttle_remaining_seconds": 0,
            "safety_remaining_seconds": 0,
        }
        exchange._rest_token_bucket.snapshot.return_value = {
            "tokens": 10.0,
            "capacity": 10.0,
            "hard_stop_remaining": 0.0,
        }
        hub = MagicMock()
        hub.get_ws_health_snapshot.return_value = {
            "state": "HEALTHY",
            "active_feeds": 1,
            "ticker_symbols": 1,
            "kline_streams": 1,
            "ticker_age_seconds": 0.1,
            "book_age_seconds": 0.1,
            "user_stream_age_seconds": 1.0,
            "user_stream_ok": True,
            "reconnect_in_progress": False,
        }
        db = MagicMock()
        db.get_active_trades_count.return_value = 0
        db.get_recent_critical_errors.return_value = []
        conn = MagicMock()

        def _execute(sql, *args):
            result = MagicMock()
            if "journal_mode" in str(sql).lower():
                result.fetchone.return_value = ("wal",)
            return result

        conn.execute.side_effect = _execute
        db.connection.return_value.__enter__.return_value = conn
        db.connection.return_value.__exit__.return_value = False
        msg = format_bot_health_message(exchange=exchange, market_data=hub, db=db)
        self.assertIn("diag-api-zzz", msg)


class TestDailyBaselineRobustness(unittest.TestCase):
    def _scheduler(self) -> DailyScheduler:
        scheduler = DailyScheduler.__new__(DailyScheduler)
        scheduler.controller = None
        scheduler.telegram = None
        scheduler.exchange = MagicMock()
        scheduler.db = MagicMock()
        scheduler.today_str = "2026-10-18"
        scheduler._last_limit_check_at = 0.0
        scheduler._limit_check_interval = 0.0
        scheduler._startup_initialized = False
        scheduler._on_day_rollover = None
        return scheduler

    def test_missing_baseline_blocks_new_entries(self) -> None:
        scheduler = self._scheduler()
        scheduler.db.get_daily_stats.return_value = None
        with patch("scheduler.utc_today_str", return_value="2026-10-18"), patch(
            "scheduler.ensure_daily_baseline", return_value=0.0
        ):
            paused, reason = scheduler.check_daily_limits()
        self.assertTrue(paused)
        self.assertIn("unavailable", reason.lower())
        scheduler.db.set_daily_status.assert_not_called()

    def test_zero_wallet_does_not_trip_max_loss(self) -> None:
        metrics = _metrics(
            start_balance=1000.0, current_wallet=0.0, equity_pct=-100.0
        )
        self.assertFalse(daily_max_loss_reached(metrics))

    def test_real_max_loss_still_pauses(self) -> None:
        scheduler = self._scheduler()
        scheduler.db.get_daily_stats.return_value = {
            "start_balance": 1000.0,
            "status": "ACTIVE",
        }
        metrics = _metrics(
            start_balance=1000.0, current_wallet=850.0, equity_pct=-15.0
        )
        with patch("scheduler.utc_today_str", return_value="2026-10-18"), patch(
            "scheduler.compute_daily_pnl_metrics", return_value=metrics
        ), patch.object(scheduler, "_get_balance", return_value=850.0), patch.object(
            Config, "DAILY_STOP_PERCENT", 10.0
        ):
            paused, reason = scheduler.check_daily_limits()
        self.assertTrue(paused)
        self.assertIn("max loss", reason.lower())

    def test_rollover_does_not_reuse_yesterday_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "daily_balance_state.json")
            with patch("core.daily_balance_manager._STATE_PATH", path):
                _save_state(DailyBalanceState("2026-10-17", 999.0))
                exchange = MagicMock()
                exchange.fetch_live_account_snapshot.return_value = SimpleNamespace(
                    wallet_balance=0.0
                )
                exchange.get_futures_balance.return_value = 0.0
                db = MagicMock()
                out = on_utc_day_rollover(exchange, db, "2026-10-18")
                self.assertEqual(out, 0.0)
                db.initialize_daily_stats.assert_not_called()

    def test_rollover_resets_startup_flag(self) -> None:
        scheduler = self._scheduler()
        scheduler._startup_initialized = True
        scheduler.today_str = "2026-10-17"
        with patch("scheduler.utc_today_str", return_value="2026-10-18"), patch.object(
            scheduler, "_initialize_trading_day"
        ) as init_day:
            scheduler._handle_day_rollover()
        self.assertEqual(scheduler.today_str, "2026-10-18")
        self.assertFalse(scheduler._startup_initialized)
        init_day.assert_called_once()

    def test_paused_day_stays_paused_when_wallet_recovers(self) -> None:
        from constants import DAILY_STATUS_PAUSED

        scheduler = self._scheduler()
        scheduler.db.get_daily_stats.return_value = {
            "start_balance": 1000.0,
            "status": DAILY_STATUS_PAUSED,
        }
        metrics = _metrics(
            start_balance=1000.0, current_wallet=1010.0, equity_pct=1.0
        )
        with patch("scheduler.utc_today_str", return_value="2026-10-18"), patch(
            "scheduler.compute_daily_pnl_metrics", return_value=metrics
        ), patch.object(scheduler, "_get_balance", return_value=1010.0), patch(
            "scheduler.ensure_daily_baseline", return_value=1000.0
        ):
            paused, reason = scheduler.check_daily_limits()
        self.assertTrue(paused)
        self.assertIn("paused", reason.lower())
        scheduler.db.set_daily_status.assert_not_called()

    def test_peek_entry_pause_does_not_mutate(self) -> None:
        from constants import DAILY_STATUS_PAUSED

        scheduler = self._scheduler()
        scheduler.check_daily_limits = MagicMock(return_value=(False, ""))
        scheduler.db.get_daily_stats.return_value = {
            "status": DAILY_STATUS_PAUSED
        }
        paused, reason = scheduler.peek_entry_pause()
        self.assertTrue(paused)
        self.assertIn("paused", reason.lower())
        scheduler.check_daily_limits.assert_not_called()
        scheduler.db.set_daily_status.assert_not_called()


class TestNativeDefaultAndGuards(unittest.TestCase):
    def test_config_source_defaults_native_on(self) -> None:
        self.assertIn(
            '_env_bool("ENABLE_NATIVE_TP_SL", True)',
            inspect.getsource(Config),
        )
        self.assertTrue(Config.ENABLE_NATIVE_TP_SL)

    def test_zero_wallet_snapshot_keeps_pause(self) -> None:
        from constants import DAILY_STATUS_PAUSED
        from risk_manager import RiskManager

        rm = RiskManager.__new__(RiskManager)
        rm.exchange = MagicMock()
        rm.db = MagicMock()
        rm.controller = None
        rm._peak_realized_pnl = 0.0
        rm._reference_balance = 1000.0
        rm.exchange.get_open_positions_count.return_value = 0
        rm.db.get_active_trades_count.return_value = 0
        rm.db.get_daily_stats.return_value = {
            "status": DAILY_STATUS_PAUSED,
            "start_balance": 1000.0,
            "entries_count": 1,
        }
        rm.db.get_daily_trade_analytics.return_value = {"closes": 1, "total_pnl": -50.0}
        rm.exchange.get_futures_balance.return_value = 0.0
        metrics = _metrics(
            start_balance=1000.0, current_wallet=0.0, equity_pct=-100.0
        )
        with patch(
            "risk_manager.compute_daily_pnl_metrics", return_value=metrics
        ), patch.object(rm, "_count_consecutive_losses", return_value=0), patch.object(
            rm, "_calculate_realized_drawdown_percent", return_value=0.0
        ), patch.object(rm, "_persist_runtime_state"):
            snap = rm.get_risk_snapshot()
        self.assertFalse(snap.entries_allowed)
        self.assertIn("paused", snap.block_reason.lower())
        rm.db.set_daily_status.assert_not_called()

    def test_twelve_cap_uses_max_of_db_and_exchange(self) -> None:
        import main as main_mod

        scheduler = MagicMock()
        scheduler.is_entry_paused.return_value = (False, "")
        risk = MagicMock()
        risk.get_exchange_open_positions_count.return_value = 3
        db = MagicMock()
        db.get_active_trades_count.return_value = 12
        with patch("main.state_is_unreliable", return_value=(False, "")), patch(
            "main.entries_blocked_by_unresolved_orphans", return_value=(False, "")
        ), patch(
            "main._market_data_blocks_new_entries", return_value=(False, "")
        ), patch.object(Config, "MAX_POSITIONS", 12):
            allowed, reason = main_mod._entries_allowed(scheduler, risk, db)
        self.assertFalse(allowed)
        self.assertIn("12/12", reason)
        risk.can_open_trade.assert_not_called()


class TestLedgerSeqAndSpawn(unittest.TestCase):
    def test_ledger_restores_seq_and_unresolved_beyond_deque(self) -> None:
        from datetime import datetime, timezone

        from core.execution_ledger import ExecutionLedger, ExecutionPhase

        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "uncertain.json")
            payload = [
                {
                    "exec_id": f"EXEC-{today}-00042",
                    "symbol": "ETHUSDT",
                    "action": "LONG",
                    "strategy": "SMC_TREND",
                    "score": 80.0,
                    "phase": ExecutionPhase.ORDER_STATUS_UNCERTAIN.value,
                    "client_order_id": "bfb42",
                    "binance_order_id": "9",
                    "quantity": 1.0,
                    "entry": 100.0,
                    "sl": 90.0,
                    "tp": 110.0,
                    "atr": 1.0,
                    "created_at": time.time(),
                    "updated_at": time.time(),
                }
            ]
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            ledger = ExecutionLedger(maxlen=2, persist_path=path)
            self.assertEqual(len(ledger.unresolved()), 1)
            nxt = ledger.new_record(
                symbol="BTCUSDT", action="LONG", strategy="SMC_TREND", score=70.0
            )
            self.assertEqual(nxt.exec_id, f"EXEC-{today}-00043")

    def test_windows_spawn_has_no_start_new_session(self) -> None:
        import subprocess
        import sys

        from core.process_spawn import spawn_detached_python

        with patch("core.process_spawn.subprocess.Popen") as popen:
            popen.return_value = MagicMock()
            spawn_detached_python("watchdog.py", cwd=".")
        kwargs = popen.call_args.kwargs
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertIs(kwargs["stdout"], subprocess.DEVNULL)
        self.assertIs(kwargs["stderr"], subprocess.DEVNULL)
        if sys.platform == "win32":
            self.assertNotIn("start_new_session", kwargs)
            self.assertIn("creationflags", kwargs)
        else:
            self.assertTrue(kwargs.get("start_new_session"))


class TestTelegramPendingPnlAndSuperSkip(unittest.TestCase):
    def test_pending_pnl_does_not_print_zero(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.send_message = MagicMock()
        tg.exchange = None
        tg.send_close_alert(
            symbol="ETHUSDT",
            reason="RECONCILED_MANUAL_CLOSE",
            pnl=0.0,
            pnl_source="pending",
            exit_price=101.0,
        )
        msg = tg.send_message.call_args[0][0]
        self.assertIn("pending", msg.lower())
        self.assertNotIn("$0.0000", msg)
        tg.send_close_alert(
            symbol="ETHUSDT",
            reason="RECONCILED_MANUAL_CLOSE",
            pnl=0.0,
            pnl_source="userTrades",
            exit_price=101.0,
        )
        confirmed = tg.send_message.call_args[0][0]
        self.assertIn("$0.0000", confirmed)

    def test_super_missing_klines_skip_not_demote(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        src = inspect.getsource(EventScanOrchestrator._run_super_day_validation)
        none_idx = src.find("if snapshot is None:")
        self.assertGreater(none_idx, 0)
        none_block = src[none_idx : src.find("if spread > max_spread")]
        self.assertIn("note_super_skip", none_block)
        self.assertNotIn("demote_super_to_normal", none_block)

    def test_dca_reconcile_honors_failsafe_close(self) -> None:
        from reconciliation import reconcile_uncertain_dca

        src = inspect.getsource(reconcile_uncertain_dca)
        self.assertIn('result.get("failsafe_close")', src)
        self.assertIn("failsafe_close_trade", src)

    def test_monitor_stall_unknown_before_first_tick(self) -> None:
        import core.ops_heartbeat as hb

        with patch.object(hb, "_monitor_last_tick_mono", 0.0):
            self.assertEqual(hb.get_monitor_stall_seconds(), float("inf"))
            self.assertFalse(hb.is_monitor_loop_stale(45.0))


class TestHighMediumResidualRepairs(unittest.TestCase):
    def test_virtual_sl_keeps_native_until_close_confirms(self) -> None:
        from manager import TradeManager

        long_src = inspect.getsource(TradeManager._manage_long_trade)
        short_src = inspect.getsource(TradeManager._manage_short_trade)
        failsafe_src = inspect.getsource(TradeManager.failsafe_close_trade)
        handle_tp_src = inspect.getsource(TradeManager._handle_take_profit)
        finalize_tp_src = inspect.getsource(TradeManager._finalize_take_profit)
        for src in (long_src, short_src, failsafe_src):
            self.assertIn("_close_position", src)
            self.assertNotIn("_cancel_all_native_orders", src)
        self.assertNotIn("_cancel_native_tp_order", handle_tp_src)
        self.assertIn("_cancel_native_tp_order", finalize_tp_src)
        self.assertLess(
            finalize_tp_src.find("TRADE_STATUS_CLOSED"),
            finalize_tp_src.find("_cancel_native_tp_order"),
        )

    def test_market_close_retry_after_cooldown(self) -> None:
        from manager import TradeManager

        tm = TradeManager.__new__(TradeManager)
        with patch.object(tm, "_market_close_max_attempts", return_value=1):
            tm._note_market_close_attempt("t1", "STOP_LOSS")
            self.assertFalse(tm._market_close_should_retry("t1", "STOP_LOSS"))
            tm._market_close_exhausted_at["t1:STOP_LOSS"] = time.monotonic() - 46.0
            self.assertTrue(tm._market_close_should_retry("t1", "STOP_LOSS"))

    def test_heartbeat_payload_includes_pid(self) -> None:
        import core.ops_heartbeat as hb

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "heartbeat.json")
            with patch.object(Config, "WATCHDOG_HEARTBEAT_FILE", path), patch.object(
                Config, "DATA_DIR", tmp
            ), patch.object(Config, "ENABLE_NATIVE_TP_SL", True), patch.object(
                hb, "_last_write_monotonic", 0.0
            ):
                hb.write_bot_heartbeat(source="manager_init")
            with open(path, encoding="utf-8") as handle:
                payload = json.load(handle)
        self.assertEqual(payload["pid"], os.getpid())
        self.assertEqual(payload["role"], "main")
        self.assertTrue(payload["native_tp_sl"])

    def test_watchdog_process_check_uses_heartbeat_pid(self) -> None:
        from watchdog import PositionWatchdog

        src = inspect.getsource(PositionWatchdog.is_main_process_running)
        self.assertIn("read_heartbeat_payload", src)
        self.assertIn("unknown_means_alive=False", src)
        self.assertIn("python(w)?", src)
        self.assertIn("return False", src)
        self.assertNotIn("Name='python.exe'", src)

        restart_src = inspect.getsource(PositionWatchdog._maybe_restart_main)
        self.assertIn("spawn_detached_python", restart_src)
        emergency_src = inspect.getsource(PositionWatchdog.check_open_positions)
        self.assertIn("native_unprotected_open", emergency_src)
        init_src = inspect.getsource(PositionWatchdog.__init__)
        self.assertIn("DatabaseManager()", init_src)
        self.assertNotIn("DatabaseManager(Config.DB_PATH)", init_src)

    def test_peak_resets_when_utc_day_changes(self) -> None:
        from risk_manager import RiskManager

        rm = RiskManager.__new__(RiskManager)
        rm.exchange = MagicMock()
        rm.db = MagicMock()
        rm.controller = None
        rm._peak_realized_pnl = 50.0
        rm._peak_utc_day = "2026-10-17"
        rm._reference_balance = 1000.0
        rm._consecutive_loss_reset_at = ""
        with patch("risk_manager.utc_today_str", return_value="2026-10-18"), patch.object(
            rm, "_persist_runtime_state"
        ) as persist:
            rm._maybe_reset_peak_for_new_day()
        self.assertEqual(rm._peak_realized_pnl, 0.0)
        self.assertEqual(rm._peak_utc_day, "2026-10-18")
        persist.assert_called()
        with patch("risk_manager.utc_today_str", return_value="2026-10-18"), patch.object(
            rm, "_persist_runtime_state"
        ) as persist_again:
            rm.reset_daily_drawdown_peak()
        persist_again.assert_called()
        persist_src = inspect.getsource(RiskManager._persist_runtime_state)
        self.assertIn("utc_day", persist_src)

    def test_peek_entry_pause_uses_utc_today(self) -> None:
        from constants import DAILY_STATUS_PAUSED

        scheduler = DailyScheduler.__new__(DailyScheduler)
        scheduler.controller = None
        scheduler.telegram = None
        scheduler.exchange = MagicMock()
        scheduler.db = MagicMock()
        scheduler.today_str = "2026-10-17"
        scheduler.db.get_daily_stats.return_value = {"status": DAILY_STATUS_PAUSED}
        with patch("scheduler.utc_today_str", return_value="2026-10-18"):
            paused, reason = scheduler.peek_entry_pause()
        self.assertTrue(paused)
        self.assertIn("paused", reason.lower())
        scheduler.db.get_daily_stats.assert_called_with("2026-10-18")

    def test_pending_close_does_not_book_daily_pnl(self) -> None:
        from reconciliation import finalize_reconciled_trade_close

        exchange = MagicMock()
        exchange.refresh_wallet_after_trade.return_value = 1000.0
        db = MagicMock()
        db.close_trade_and_sync_stats.return_value = 0.0
        tg = MagicMock()
        trade = {
            "symbol": "ETHUSDT",
            "side": "LONG",
            "trade_id": "t-pending",
            "strategy": "SMC_TREND",
        }
        resolved = SimpleNamespace(exit_price=90.0, realized_pnl=0.0, source="pending")
        with patch(
            "reconciliation.resolve_exchange_close_pnl", return_value=resolved
        ):
            finalize_reconciled_trade_close(
                exchange, db, trade, "RECONCILED_MANUAL_CLOSE", telegram=tg
            )
        kwargs = db.close_trade_and_sync_stats.call_args.kwargs
        self.assertFalse(kwargs["book_daily_pnl"])
        self.assertEqual(tg.send_close_alert.call_args.kwargs["pnl_source"], "pending")

    def test_super_empty_scores_skip_not_demote(self) -> None:
        from pipeline.event_scan_orchestrator import EventScanOrchestrator

        src = inspect.getsource(EventScanOrchestrator._run_super_day_validation)
        scores_idx = src.find("if not scores:")
        self.assertGreater(scores_idx, 0)
        scores_block = src[scores_idx : src.find("best = max(scores")]
        self.assertIn("note_super_skip", scores_block)
        self.assertNotIn("demote_super_to_normal", scores_block)

    def test_ws_health_exception_blocks_entries(self) -> None:
        import main as main_mod

        hub = MagicMock()
        hub.ws_state_is_degraded.side_effect = RuntimeError("boom")
        exchange = MagicMock()
        exchange._market_data = hub
        blocked, reason = main_mod._market_data_blocks_new_entries(exchange)
        self.assertTrue(blocked)
        self.assertIn("websocket health check failed", reason)

    def test_dca_rest_block_reverts_claimed_stage(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (True, "safety")
        exchange.get_futures_balance.return_value = 1000.0
        db = MagicMock()
        db.parse_trade_metadata.return_value = {
            "base_entry_price": 100.0,
            "entry_stage": 2,
        }
        executor = TradeExecutor(exchange, db)
        trade = {
            "trade_id": "t-dca",
            "symbol": "ETHUSDT",
            "side": "LONG",
            "entry_price": 100.0,
            "quantity": 1.0,
            "entry_stage": 2,
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "ENABLE_DCA_LADDER", True
        ):
            self.assertIsNone(executor.execute_dca_add(trade, 2, 97.0))
        db.revert_entry_stage.assert_called_with("t-dca", 2)
        exchange.execute_futures_order.assert_not_called()

    def test_wallet_for_dca_plan_falls_back_to_start_balance(self) -> None:
        exchange = MagicMock()
        exchange.get_futures_balance.return_value = 0.0
        db = MagicMock()
        db.get_daily_stats.return_value = {"start_balance": 1234.0}
        executor = TradeExecutor(exchange, db)
        self.assertAlmostEqual(executor._wallet_for_dca_plan(), 1234.0)
        refine_src = inspect.getsource(TradeExecutor)
        self.assertIn("_wallet_for_dca_plan()", refine_src)

    def test_live_orphans_are_monitored(self) -> None:
        from manager import TradeManager

        executor = TradeExecutor(MagicMock(), MagicMock())
        executor._register_live_orphan(
            {
                "trade_id": "orphan-1",
                "symbol": "ETHUSDT",
                "side": "LONG",
                "status": "OPEN",
            }
        )
        tm = TradeManager.__new__(TradeManager)
        tm.executor = executor
        tm.db = MagicMock()
        tm.db.get_open_trades.return_value = []
        trades = tm._open_trades_for_monitor()
        self.assertEqual(trades[0]["trade_id"], "orphan-1")

    def test_scan_path_uses_try_acquire_not_sleeping_bucket(self) -> None:
        from exchange import BinanceExchangeManager

        src = inspect.getsource(BinanceExchangeManager._throttled_call)
        self.assertIn("self._rest_token_bucket.try_acquire", src)
        self.assertIn("self._rest_token_bucket.acquire(call_weight)", src)
        self.assertLess(
            src.find("if lane != RestLane.EXECUTION"),
            src.find("self._rest_token_bucket.try_acquire"),
        )

    def test_main_rollover_hook_resets_peak(self) -> None:
        import main as main_mod

        src = inspect.getsource(main_mod.main)
        self.assertIn("reset_daily_drawdown_peak", src)
        self.assertIn("attach_day_rollover_hook(_on_utc_day_rollover)", src)

    def test_dca_memory_resyncs_after_executor_revert(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.executor = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = None
        mgr.risk_manager = None
        mgr._dca_stage = {"t-dca": 2}
        mgr._dca_inflight = set()
        meta = {
            "base_entry_price": 100.0,
            "entry_stage": 1,
            "dca_entries": 1,
            "dca_entry2_px": 97.0,
        }
        def _open_trade() -> dict:
            return {
                "trade_id": "t-dca",
                "symbol": "ETHUSDT",
                "side": "LONG",
                "entry_price": 100.0,
                "entry_stage": 1,
                "metadata": dict(meta),
                "status": "OPEN",
            }

        mgr.db.get_trade.side_effect = lambda _id: _open_trade()
        mgr.db.parse_trade_metadata.side_effect = lambda t: dict(
            t.get("metadata") or meta
        )
        mgr.db.claim_entry_stage.return_value = True
        mgr.executor.execute_dca_add.return_value = None
        with patch.object(Config, "ENABLE_DCA_LADDER", True), patch.object(
            Config, "DRY_RUN", False
        ):
            mgr._maybe_execute_dca(_open_trade(), 97.0)
        mgr.executor.execute_dca_add.assert_called_once()
        self.assertEqual(mgr._dca_stage["t-dca"], 1)

    def test_ws_only_zero_cache_keeps_virtual_sl(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = None
        mgr.executor = None
        mgr._maybe_execute_dca = MagicMock()
        mgr._manage_long_trade = MagicMock()
        mgr._ensure_local_exit_memory = lambda t: t
        mgr._trade_quantity_from_db = MagicMock(return_value=1.0)
        mgr._resolve_monitor_price = MagicMock(return_value=100.0)
        mgr._ws_position_mark = MagicMock(return_value=100.0)
        trade = {
            "trade_id": "t-ws",
            "symbol": "ETHUSDT",
            "side": "LONG",
            "status": "OPEN",
            "quantity": 1.0,
        }
        mgr.db.get_trade.return_value = trade
        with patch("manager.is_within_position_grace_period", return_value=False), patch(
            "manager.is_range_strategy", return_value=False
        ), patch("manager.position_reconcile_guard"):
            mgr._monitor_single_trade(
                trade, {("ETHUSDT", "LONG"): 0.0}, price=100.0, ws_only=True
            )
        mgr._manage_long_trade.assert_called_once()
        loop_src = inspect.getsource(TradeManager._fast_monitor_loop)
        self.assertIn("monitor_open_trades(ws_only=False)", loop_src)
        self.assertNotIn(
            "if rest_due and not user_ok and not self.exchange.is_rest_blocked()[0]:",
            loop_src,
        )

    def test_net_long_cap_fail_closed_on_missing_wallet_or_positions(self) -> None:
        from risk_manager import RiskManager

        rm = RiskManager.__new__(RiskManager)
        rm.exchange = MagicMock()
        rm.exchange.get_futures_balance.return_value = 0.0
        ok, reason = rm._net_long_exposure_allows_new()
        self.assertFalse(ok)
        self.assertIn("wallet", reason.lower())

        rm.exchange.get_futures_balance.return_value = 1000.0
        rm.exchange.get_all_open_positions.side_effect = RuntimeError("rest down")
        ok, reason = rm._net_long_exposure_allows_new()
        self.assertFalse(ok)
        self.assertIn("positions", reason.lower())

        rm.exchange.get_all_open_positions.side_effect = None
        rm.exchange.get_all_open_positions.return_value = []
        rm.exchange.position_snapshot_authoritative.return_value = True
        ok, reason = rm._net_long_exposure_allows_new()
        self.assertTrue(ok)
        self.assertEqual(reason, "")
        ok_pub, _ = rm.net_long_exposure_allows_new()
        self.assertTrue(ok_pub)

        rm.exchange.position_snapshot_authoritative.return_value = False
        ok, reason = rm._net_long_exposure_allows_new()
        self.assertFalse(ok)
        self.assertIn("authoritative", reason.lower())

    def test_zero_cache_rest_confirms_qty_for_virtual_sl(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr.telegram = None
        mgr.executor = None
        mgr._maybe_execute_dca = MagicMock()
        mgr._manage_long_trade = MagicMock()
        mgr._ensure_local_exit_memory = lambda t: t
        mgr._trade_quantity_from_db = MagicMock(return_value=1.0)
        mgr._resolve_monitor_price = MagicMock(return_value=100.0)
        mgr._ws_position_mark = MagicMock(return_value=100.0)
        mgr._qty_rest_confirm_at = {}
        mgr._force_rest_monitor = False
        mgr.exchange.rest_account_reads_blocked.return_value = False
        mgr.exchange.get_position_quantity_rest.return_value = 1.25
        trade = {
            "trade_id": "t-rest",
            "symbol": "ETHUSDT",
            "side": "LONG",
            "status": "OPEN",
            "quantity": 1.0,
        }
        mgr.db.get_trade.return_value = trade
        with patch("manager.is_within_position_grace_period", return_value=False), patch(
            "manager.is_range_strategy", return_value=False
        ), patch("manager.position_reconcile_guard"):
            mgr._monitor_single_trade(
                trade, {("ETHUSDT", "LONG"): 0.0}, price=100.0, ws_only=True
            )
        mgr.exchange.get_position_quantity_rest.assert_called()
        mgr._manage_long_trade.assert_called_once()
        self.assertTrue(mgr._force_rest_monitor)

    def test_monitor_includes_unresolved_exchange_orphans(self) -> None:
        from manager import TradeManager
        from reconciliation import _set_unresolved_orphans

        mgr = TradeManager.__new__(TradeManager)
        mgr.db = MagicMock()
        mgr.db.get_open_trades.return_value = []
        mgr.executor = None
        _set_unresolved_orphans(
            [
                {
                    "symbol": "ETHUSDT",
                    "positionSide": "LONG",
                    "quantity": 0.4,
                    "entry_price": 100.0,
                }
            ]
        )
        try:
            trades = mgr._open_trades_for_monitor()
        finally:
            _set_unresolved_orphans([])
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["trade_id"], "orphan-ex-ETHUSDT-LONG")
        self.assertTrue(trades[0]["metadata"]["orphan_exchange"])

    def test_watchdog_does_not_skip_empty_db_when_rest_has_positions(self) -> None:
        from watchdog import PositionWatchdog
        from reconciliation import _set_unresolved_orphans, unresolved_orphan_exchange_positions

        wd = PositionWatchdog.__new__(PositionWatchdog)
        wd.exchange = MagicMock()
        wd.db = MagicMock()
        wd.telegram = MagicMock()
        wd.state = MagicMock()
        wd.state.breach_first_seen = {}
        wd.state.note_breach.return_value = 0.0
        wd.state.should_alert.return_value = False
        wd.exchange.is_rest_blocked.return_value = (False, "")
        wd.exchange.fetch_all_open_positions_rest.return_value = [
            {
                "symbol": "ETHUSDT",
                "positionSide": "LONG",
                "quantity": 1.2,
                "mark_price": 100.0,
            }
        ]
        wd.db.get_open_trades.return_value = []
        wd._emergency_close = MagicMock()
        _set_unresolved_orphans([])
        try:
            with patch.object(Config, "WATCHDOG_BREACH_GRACE_SECONDS", 60):
                wd.check_open_positions(main_bot_healthy=True)
            wd._emergency_close.assert_not_called()
            self.assertTrue(unresolved_orphan_exchange_positions())

            wd.state.note_breach.return_value = 61.0
            with patch.object(Config, "WATCHDOG_BREACH_GRACE_SECONDS", 60):
                wd.check_open_positions(main_bot_healthy=True)
            wd._emergency_close.assert_called_once()
            trade = wd._emergency_close.call_args[0][0]
            self.assertEqual(trade["trade_id"], "orphan-ex-ETHUSDT-LONG")
            self.assertEqual(
                wd._emergency_close.call_args[0][2].reason, "UNPROTECTED_ORPHAN"
            )
        finally:
            _set_unresolved_orphans([])

    def test_watchdog_tracks_rest_when_db_raises(self) -> None:
        from watchdog import PositionWatchdog
        from reconciliation import _set_unresolved_orphans, unresolved_orphan_exchange_positions

        wd = PositionWatchdog.__new__(PositionWatchdog)
        wd.exchange = MagicMock()
        wd.db = MagicMock()
        wd.telegram = MagicMock()
        wd.state = MagicMock()
        wd.state.breach_first_seen = {}
        wd.state.note_breach.return_value = 61.0
        wd.state.should_alert.return_value = False
        wd.exchange.is_rest_blocked.return_value = (False, "")
        wd.exchange.fetch_all_open_positions_rest.return_value = [
            {
                "symbol": "ETHUSDT",
                "positionSide": "LONG",
                "quantity": 1.2,
                "mark_price": 100.0,
            }
        ]
        wd.db.get_open_trades.side_effect = RuntimeError("sqlite locked")
        wd._emergency_close = MagicMock()
        _set_unresolved_orphans([])
        try:
            with patch.object(Config, "WATCHDOG_BREACH_GRACE_SECONDS", 60):
                wd.check_open_positions(main_bot_healthy=True)
            self.assertTrue(unresolved_orphan_exchange_positions())
            wd._emergency_close.assert_not_called()
        finally:
            _set_unresolved_orphans([])

    def test_empty_ws_cache_is_not_authoritative_until_rest(self) -> None:
        from exchange import BinanceExchangeManager, PositionCache

        ex = BinanceExchangeManager.__new__(BinanceExchangeManager)
        ex._market_data = MagicMock()
        ex._market_data.user_stream_has_account_data.return_value = True
        ex._market_data.get_ws_positions.return_value = []
        ex._market_data.user_stream_is_stale.return_value = False
        ex._position_cache = PositionCache()
        ex._position_snapshot_authoritative = False
        ex._position_backoff_until = time.monotonic() + 999.0
        rows = ex._refresh_positions_cache()
        self.assertEqual(rows, [])
        self.assertFalse(ex.position_snapshot_authoritative())

    def test_soft_tp_sl_off_skips_virtual_monitor(self) -> None:
        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr._manage_long_trade = MagicMock()
        mgr._maybe_execute_dca = MagicMock()
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        trade = {
            "trade_id": "t-soft",
            "symbol": "ETHUSDT",
            "side": "LONG",
            "status": "OPEN",
            "quantity": 1.0,
        }
        with patch.object(Config, "ENABLE_SOFT_TP_SL", False):
            mgr._monitor_single_trade(
                trade, {("ETHUSDT", "LONG"): 1.0}, price=100.0, ws_only=True
            )
            mgr._process_price_tick("ETHUSDT", 100.0)
        mgr._manage_long_trade.assert_not_called()

    def test_duplicate_inflight_close_waits_for_worker(self) -> None:
        import threading

        from manager import TradeManager

        mgr = TradeManager.__new__(TradeManager)
        mgr.exchange = MagicMock()
        mgr.db = MagicMock()
        mgr._close_lock = threading.Lock()
        mgr._close_inflight = {"t1:STOP_LOSS"}
        done = threading.Event()
        done.set()
        mgr._close_done = {"t1:STOP_LOSS": done}
        mgr._close_ok = {"t1:STOP_LOSS": False}
        mgr._close_queue = MagicMock()
        trade = {"trade_id": "t1", "symbol": "ETHUSDT", "side": "LONG"}
        with patch.object(mgr, "_resolve_close_quantity", return_value=1.0), patch.object(
            mgr, "_market_close_is_exhausted", return_value=False
        ):
            ok = mgr._close_position(
                trade, quantity=1.0, reason="STOP_LOSS", blocking=True
            )
            queued = mgr._close_position(
                trade, quantity=1.0, reason="STOP_LOSS", blocking=False
            )
        self.assertFalse(ok)
        self.assertFalse(queued)
        mgr._close_queue.put.assert_not_called()

    def test_telegram_critical_alert_not_dropped_when_queue_full(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.enabled = True
        tg.bot = MagicMock()
        tg.chat_id = "1"
        tg._api_lock = threading.Lock()
        tg._outbound = queue.Queue(maxsize=1)
        tg._outbound.put_nowait("filler")
        tg.send_message("🚨 Watchdog Emergency Close ETHUSDT")
        tg.bot.send_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
