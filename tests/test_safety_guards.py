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

    def test_missing_or_empty_dry_run_defaults_true(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DRY_RUN", None)
            self.assertTrue(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "  "}, clear=False):
            self.assertTrue(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "false"}, clear=False):
            self.assertFalse(_env_dry_run())
        with patch.dict(os.environ, {"DRY_RUN": "true"}, clear=False):
            self.assertTrue(_env_dry_run())


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
        self.assertNotIn("scan_unified()", source)
        self.assertNotIn("scan_range_market()", source)
        self.assertNotIn("scan_market()", source)


class TestEnginesPackage(unittest.TestCase):
    def test_signal_engines_import_from_package(self) -> None:
        from engines.smc_engine import effective_smc_min_score
        from engines.range_engine import evaluate_range_setup

        self.assertTrue(callable(effective_smc_min_score))
        self.assertTrue(callable(evaluate_range_setup))


if __name__ == "__main__":
    unittest.main()
