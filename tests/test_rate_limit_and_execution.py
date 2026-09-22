"""Rate-limit safety mode, execution ledger, and Testnet env fail-closed."""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import Config, _env_use_testnet
from core.execution_ledger import ExecutionLedger, ExecutionPhase
from rest_rate_guard import RestUsageTracker, weight_for_call


class TestEnvFailClosed(unittest.TestCase):
    def test_binance_env_testnet_wins_over_use_testnet_false(self) -> None:
        with patch.dict(
            os.environ,
            {"BINANCE_ENV": "TESTNET", "USE_TESTNET": "false"},
            clear=False,
        ):
            self.assertTrue(_env_use_testnet())

    def test_mainnet_requires_explicit_env_and_flag(self) -> None:
        with patch.dict(
            os.environ,
            {"BINANCE_ENV": "MAINNET", "USE_TESTNET": "false"},
            clear=False,
        ):
            self.assertFalse(_env_use_testnet())


class TestRestUsageTracker(unittest.TestCase):
    def test_http_429_enters_safety_mode(self) -> None:
        tracker = RestUsageTracker()
        response = SimpleNamespace(
            status_code=429,
            headers={"Retry-After": "12", "X-MBX-USED-WEIGHT-1M": "2400"},
        )
        tracker.note_http_response(response)
        self.assertTrue(tracker.in_safety_mode())
        self.assertFalse(tracker.allows_background_rest())
        self.assertFalse(tracker.allows_new_entries())
        self.assertGreater(tracker.safety_remaining(), 0)

    def test_http_418_is_ip_banned(self) -> None:
        tracker = RestUsageTracker()
        response = SimpleNamespace(status_code=418, headers={})
        tracker.note_http_response(response)
        snap = tracker.snapshot()
        self.assertEqual(snap["state"], "IP_BANNED")
        self.assertGreaterEqual(tracker.safety_remaining(), 600)

    def test_minus_1003_halt_is_not_capped_at_120s(self) -> None:
        tracker = RestUsageTracker()
        tracker.note_binance_error(
            code=-1003,
            message="Too many requests; current limit of IP is 6000",
            halt_seconds=300,
            banned=False,
        )
        self.assertGreaterEqual(tracker.safety_remaining(), 299)
        self.assertTrue(tracker.in_safety_mode())

    def test_futures_ticker_all_symbols_weight(self) -> None:
        def futures_ticker(**kwargs):
            return []

        self.assertEqual(weight_for_call(futures_ticker), 40)
        self.assertEqual(weight_for_call(futures_ticker, symbol="BTCUSDT"), 1)


class TestExecutionLedger(unittest.TestCase):
    def test_approved_is_not_opened(self) -> None:
        ledger = ExecutionLedger()
        record = ledger.new_record(
            symbol="BTCUSDT", action="LONG", strategy="SMC_TREND", score=80.0
        )
        ledger.transition(record, ExecutionPhase.TRADE_APPROVED)
        self.assertFalse(record.is_opened())
        ledger.transition(record, ExecutionPhase.ORDER_SUBMITTING)
        self.assertFalse(record.is_opened())
        ledger.transition(record, ExecutionPhase.ORDER_ACCEPTED_NOT_FILLED)
        self.assertFalse(record.is_opened())
        ledger.transition(record, ExecutionPhase.ORDER_FILLED, binance_order_id="1")
        self.assertTrue(record.is_opened())
        self.assertTrue(record.exec_id.startswith("EXEC-"))
        self.assertTrue(record.client_order_id.startswith("bfb"))


class TestWatchdogNoDuplicateRestart(unittest.TestCase):
    def test_stale_heartbeat_does_not_restart_if_process_running(self) -> None:
        from watchdog import PositionWatchdog

        wd = PositionWatchdog.__new__(PositionWatchdog)
        wd.state = MagicMock()
        wd._alert_main_down = MagicMock()
        wd._maybe_restart_main = MagicMock()
        with patch.object(PositionWatchdog, "main_bot_stale_seconds", return_value=120.0):
            with patch.object(PositionWatchdog, "is_main_process_running", return_value=True):
                with patch.object(Config, "WATCHDOG_MAIN_STALE_SECONDS", 60):
                    healthy = PositionWatchdog.check_main_bot_health(wd)
        self.assertTrue(healthy)
        wd._maybe_restart_main.assert_not_called()


class TestExecutorSafetyGate(unittest.TestCase):
    def test_safety_pause_skips_order(self) -> None:
        from executor import TradeExecutor

        exchange = MagicMock()
        exchange.get_execution_safety.return_value = (
            "API_RATE_LIMITED",
            "REST halted",
        )
        executor = TradeExecutor(exchange, MagicMock())
        with patch.object(Config, "DRY_RUN", False):
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
        exchange.execute_futures_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
