"""Safety guards: DRY_RUN, mainnet /forceresume lock, merged monitor API."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from config import Config, _env_dry_run
from executor import TradeExecutor
from manager import TradeManager
from scheduler import DailyScheduler


class TestDryRunGuard(unittest.TestCase):
    def test_execute_trade_does_not_place_order(self) -> None:
        exchange = MagicMock()
        db = MagicMock()
        executor = TradeExecutor(exchange, db)
        with patch.object(Config, "DRY_RUN", True):
            result = executor.execute_trade(
                symbol="ETHUSDT",
                action="LONG",
                atr=1.0,
                current_price=100.0,
                strategy="SMC_TREND",
                score=80.0,
            )
        self.assertIsNone(result)
        exchange.execution_context.assert_not_called()

    def test_missing_or_empty_dry_run_follows_network(self) -> None:
        with patch.dict(os.environ, {"USE_TESTNET": "true"}, clear=False):
            os.environ.pop("DRY_RUN", None)
            self.assertFalse(_env_dry_run())
        with patch.dict(
            os.environ,
            {"BINANCE_ENV": "MAINNET", "USE_TESTNET": "false", "DRY_RUN": "  "},
            clear=False,
        ):
            self.assertTrue(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "false"}, clear=False):
            self.assertFalse(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "true"}, clear=False):
            self.assertTrue(_env_dry_run())

    def test_execute_trade_places_order_when_dry_run_false(self) -> None:
        exchange = MagicMock()
        db = MagicMock()
        executor = TradeExecutor(exchange, db)
        with patch.object(Config, "DRY_RUN", False), patch.object(
            executor, "_execute_trade_inner", return_value={"ok": True}
        ) as inner:
            result = executor.execute_trade(
                symbol="ETHUSDT",
                action="LONG",
                atr=1.0,
                current_price=100.0,
                strategy="SMC_TREND",
                score=80.0,
            )
        self.assertEqual(result, {"ok": True})
        exchange.execution_context.assert_called_once()
        inner.assert_called_once()

    def test_warming_ws_uses_rest_ticker_for_execution_price(self) -> None:
        exchange = MagicMock()
        db = MagicMock()
        hub = MagicMock()
        hub._reconnect_in_progress = False
        hub.is_ws_warming_up.return_value = True
        hub.get_ws_health_snapshot.return_value = {"state": "WARMING"}
        hub.get_fresh_ticker_price.return_value = None
        exchange.get_market_data_hub.return_value = hub
        exchange.fetch_ticker.return_value = 123.45
        exchange.get_symbol_price.return_value = 123.45
        exchange.get_ticker.return_value = 123.45
        executor = TradeExecutor(exchange, db)
        with patch.object(Config, "DRY_RUN", False), patch.object(
            executor, "_execute_trade_inner", return_value={"ok": True}
        ) as inner:
            result = executor.execute_trade(
                symbol="ETHUSDT",
                action="LONG",
                atr=1.0,
                current_price=100.0,
                strategy="SMC_TREND",
                score=80.0,
            )
        self.assertEqual(result, {"ok": True})
        inner.assert_called_once()
        self.assertEqual(inner.call_args.args[3], 123.45)

    def test_stale_ws_uses_rest_ticker_and_still_places(self) -> None:
        exchange = MagicMock()
        db = MagicMock()
        hub = MagicMock()
        hub._reconnect_in_progress = False
        hub.execution_requires_rest_price.return_value = True
        hub.is_ws_warming_up.return_value = False
        hub.ws_is_stale.return_value = True
        hub.get_ws_health_snapshot.return_value = {"state": "STALE"}
        hub.get_fresh_ticker_price.return_value = None
        exchange.get_market_data_hub.return_value = hub
        exchange.fetch_ticker.return_value = 250.0
        exchange.get_symbol_price.return_value = 250.0
        exchange.get_ticker.return_value = 250.0
        executor = TradeExecutor(exchange, db)
        with patch.object(Config, "DRY_RUN", False), patch.object(
            executor, "_execute_trade_inner", return_value={"ok": True}
        ) as inner:
            result = executor.execute_trade(
                symbol="ETHUSDT",
                action="LONG",
                atr=1.0,
                current_price=100.0,
                strategy="SMC_TREND",
                score=80.0,
            )
        self.assertEqual(result, {"ok": True})
        inner.assert_called_once()
        self.assertEqual(inner.call_args.args[3], 250.0)

    def test_stale_rest_failure_still_places_with_signal_price(self) -> None:
        exchange = MagicMock()
        db = MagicMock()
        hub = MagicMock()
        hub._reconnect_in_progress = False
        hub.execution_requires_rest_price.return_value = True
        hub.get_price.return_value = 0.0
        hub.get_fresh_ticker_price.return_value = None
        exchange.get_market_data_hub.return_value = hub
        exchange.fetch_ticker.return_value = None
        exchange.get_symbol_price.return_value = None
        exchange.get_ticker.return_value = None
        exchange.get_live_mark_price.return_value = None
        executor = TradeExecutor(exchange, db)
        with patch.object(Config, "DRY_RUN", False), patch.object(
            executor, "_execute_trade_inner", return_value={"ok": True}
        ) as inner:
            result = executor.execute_trade(
                symbol="ETHUSDT",
                action="LONG",
                atr=1.0,
                current_price=100.0,
                strategy="SMC_TREND",
                score=80.0,
            )
        self.assertEqual(result, {"ok": True})
        inner.assert_called_once()
        self.assertEqual(inner.call_args.args[3], 100.0)


class TestForceResumeMainnetLock(unittest.TestCase):
    def test_force_resume_blocked_on_mainnet(self) -> None:
        scheduler = DailyScheduler.__new__(DailyScheduler)
        scheduler.controller = MagicMock()
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "ALLOW_MAINNET_FORCE_RESUME", False
        ):
            note = scheduler.force_resume_entries()
        self.assertTrue(note.startswith("BLOCKED"))
        scheduler.controller.force_resume_daily_limits.assert_not_called()

    def test_force_resume_allowed_on_testnet(self) -> None:
        scheduler = DailyScheduler.__new__(DailyScheduler)
        scheduler.controller = MagicMock()
        scheduler.telegram = None
        scheduler.exchange = MagicMock()
        scheduler.db = MagicMock()
        scheduler.today_str = "2026-09-20"
        scheduler._last_limit_check_at = 0.0
        scheduler.exchange.get_futures_balance.return_value = 1000.0
        scheduler.db.get_daily_stats.return_value = {"current_balance": 1000.0}
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            scheduler, "ensure_startup_initialized"
        ), patch.object(scheduler, "_get_balance", return_value=1000.0):
            note = scheduler.force_resume_entries()
        self.assertFalse(note.startswith("BLOCKED"))
        scheduler.controller.force_resume_daily_limits.assert_called_once()


class TestSchedulerPauseTuple(unittest.TestCase):
    def test_risk_engine_does_not_block_when_scheduler_unpaused(self) -> None:
        from core.risk_engine import RiskEngine
        from risk_manager import RiskSnapshot

        scheduler = MagicMock()
        scheduler.is_entry_paused.return_value = (False, "")
        risk = MagicMock()
        risk.get_risk_snapshot.return_value = RiskSnapshot(
            open_positions=0,
            exchange_open_positions=0,
            daily_entries=0,
            daily_trades=0,
            consecutive_losses=0,
            drawdown_percent=0.0,
            current_balance=1000.0,
            daily_realized_pnl=0.0,
            daily_realized_pnl_percent=0.0,
            unrealized_pnl=0.0,
            entries_allowed=True,
            block_reason="",
        )
        risk.can_open_trade.return_value = (True, "")
        engine = RiskEngine(risk, MagicMock(), MagicMock(), scheduler=scheduler)
        ok, reason = engine.approve_entry("BTCUSDT", "SMC_TREND", 50000.0)
        self.assertTrue(ok, reason)
        self.assertEqual(reason, "")

    def test_risk_engine_blocks_only_when_pause_flag_true(self) -> None:
        from core.risk_engine import RiskEngine

        scheduler = MagicMock()
        scheduler.is_entry_paused.return_value = (
            True,
            "Daily limit already reached — entries paused for today.",
        )
        engine = RiskEngine(MagicMock(), MagicMock(), MagicMock(), scheduler=scheduler)
        ok, reason = engine.approve_entry("BTCUSDT", "SMC_TREND", 50000.0)
        self.assertFalse(ok)
        self.assertIn("paused", reason.lower())

    def test_utc_rollover_reinitializes_active_day(self) -> None:
        scheduler = DailyScheduler.__new__(DailyScheduler)
        scheduler.controller = MagicMock()
        scheduler.telegram = None
        scheduler.exchange = MagicMock()
        scheduler.db = MagicMock()
        scheduler.today_str = "2026-09-19"
        scheduler._last_limit_check_at = 99.0
        with patch(
            "scheduler.utc_today_str", return_value="2026-09-20"
        ), patch.object(scheduler, "_initialize_trading_day") as init_day:
            scheduler._handle_day_rollover()
        self.assertEqual(scheduler.today_str, "2026-09-20")
        self.assertEqual(scheduler._last_limit_check_at, 0.0)
        scheduler.controller.clear_daily_limit_override.assert_called_once()
        init_day.assert_called_once_with(force_balance_refresh=True)


class TestMergedPositionMonitor(unittest.TestCase):
    def test_manager_exposes_single_stop_event(self) -> None:
        self.assertTrue(hasattr(TradeManager, "stop"))
        self.assertTrue(hasattr(TradeManager, "stop_event"))


class TestMainLoopIsEventDriven(unittest.TestCase):
    def test_main_loop_does_not_call_legacy_scan_branches(self) -> None:
        import inspect
        import main as main_mod

        source = inspect.getsource(main_mod.main)
        self.assertIn("process_priority_scan_cycle", source)
        self.assertIn("warmup_and_evaluate_kline_misses", source)
        self.assertIn("MinuteScanClock", source)
        self.assertIn("scan_clock.due", source)
        self.assertIn("ScanWarmupGate", source)
        self.assertIn("WARMUP_MODE", source)
        from core.scan_warmup import ScanWarmupGate

        self.assertEqual(ScanWarmupGate.MODE_WARMUP, "WARMUP_MODE")
        self.assertEqual(ScanWarmupGate.MODE_ACTIVE, "ACTIVE_SCANNING_MODE")
        self.assertNotIn("scan_unified()", source)
        self.assertNotIn("scan_range_market()", source)
        self.assertNotIn("scan_market()", source)


class TestEnginesPackage(unittest.TestCase):
    def test_signal_engines_import_from_package(self) -> None:
        from engines.smc_engine import effective_smc_min_score
        from engines.range_engine import evaluate_range_setup

        self.assertTrue(callable(effective_smc_min_score))
        self.assertTrue(callable(evaluate_range_setup))


class TestTelegramTestTrade(unittest.TestCase):
    def test_testtrade_blocked_on_mainnet(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.exchange = MagicMock()
        with patch.object(Config, "USE_TESTNET", False):
            msg = tg._place_testnet_test_trade("ONGUSDT")
        self.assertIn("MAINNET", msg)
        tg.exchange.fetch_ticker.assert_not_called()

    def test_testtrade_places_min_market_on_testnet(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.exchange = MagicMock()
        tg.exchange.fetch_ticker.return_value = 1.25
        rules = MagicMock()
        rules.min_qty = 1.0
        rules.min_notional = 5.0
        rules.step_size = 1.0
        rules.quantity_precision = 0
        tg.exchange.get_symbol_rules.return_value = rules
        tg.exchange.execute_futures_order.return_value = {
            "orderId": 99,
            "status": "FILLED",
            "avgPrice": "1.25",
            "executedQty": "4",
        }
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "DRY_RUN", False
        ), patch.object(Config, "QUOTE_ASSET", "USDT"):
            msg = tg._place_testnet_test_trade("ONGUSDT")
        self.assertIn("TEST TRADE SUBMITTED", msg)
        tg.exchange.fetch_ticker.assert_called_once_with("ONGUSDT")
        tg.exchange.execute_futures_order.assert_called_once()
        kwargs = tg.exchange.execute_futures_order.call_args.kwargs
        self.assertEqual(kwargs["side"], "BUY")
        self.assertEqual(kwargs["position_side"], "LONG")


class TestCandidateRestPrice(unittest.TestCase):
    def test_execute_candidates_fills_zero_price_from_rest(self) -> None:
        import main as main_mod

        executor = MagicMock()
        executor.exchange.fetch_ticker.return_value = 12.5
        executor.execute_trade.return_value = None
        candidate = {
            "symbol": "ONGUSDT",
            "action": "LONG",
            "atr": 0.1,
            "price": 0.0,
            "score": 90.0,
            "strategy": "VWAP_PULLBACK",
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "MAX_ENTRIES_PER_CYCLE", 3
        ), patch.object(
            main_mod, "_entries_allowed", return_value=(True, "")
        ), patch(
            "core.risk_engine.RiskEngine.approve_entry", return_value=(True, "")
        ), patch(
            "strategies.build_strategy_registry", return_value=MagicMock()
        ):
            main_mod._execute_candidates(
                [candidate],
                executor,
                MagicMock(),
                MagicMock(),
                MagicMock(),
                MagicMock(),
            )
        executor.exchange.fetch_ticker.assert_called()
        executor.execute_trade.assert_called()
        self.assertEqual(executor.execute_trade.call_args.kwargs["current_price"], 12.5)


if __name__ == "__main__":
    unittest.main()
