"""Rate-limit safety mode, execution ledger, and Testnet env fail-closed."""

from __future__ import annotations

import os
import time
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

    def test_used_weight_4500_throttles_background_rest(self) -> None:
        tracker = RestUsageTracker()
        tracker.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "4500"},
            )
        )
        snap = tracker.snapshot()
        self.assertEqual(snap["state"], "RATE_LIMIT_WARNING")
        self.assertFalse(tracker.allows_background_rest())
        self.assertFalse(tracker.in_safety_mode())
        self.assertGreater(tracker.weight_throttle_remaining(), 0)
        self.assertEqual(snap["used_weight_1m"], 4500)

    def test_used_weight_probe_after_short_throttle(self) -> None:
        tracker = RestUsageTracker()
        tracker.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "4600"},
            )
        )
        with tracker._lock:
            tracker._weight_throttle_until = time.monotonic() - 0.1
        snap = tracker.snapshot()
        self.assertEqual(snap["state"], "HIGH_USAGE")
        self.assertTrue(tracker.allows_background_rest())
        self.assertFalse(tracker.in_safety_mode())

    def test_used_weight_decays_after_rolling_minute(self) -> None:
        tracker = RestUsageTracker()
        tracker.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "5000"},
            )
        )
        with tracker._lock:
            tracker._used_weight_updated_at = time.monotonic() - 61.0
            tracker._weight_throttle_until = 0.0
        snap = tracker.snapshot()
        self.assertEqual(snap["used_weight_1m"], 0)
        self.assertEqual(snap["state"], "HEALTHY")
        self.assertTrue(tracker.allows_background_rest())


class TestRestBlockedUsesSnapshotState(unittest.TestCase):
    def test_is_rest_blocked_does_not_use_missing_health_state(self) -> None:
        from exchange import BinanceExchangeManager

        exchange = BinanceExchangeManager.__new__(BinanceExchangeManager)
        exchange._rest_usage = RestUsageTracker()
        exchange._market_data = None
        exchange._rest_token_bucket = SimpleNamespace(
            is_hard_stopped=lambda: False,
            hard_stop_remaining=lambda: 0,
        )
        blocked, reason = exchange.is_rest_blocked()
        self.assertIsInstance(blocked, bool)
        self.assertIsInstance(reason, str)
        self.assertFalse(blocked)

        exchange._rest_usage.note_http_response(
            SimpleNamespace(status_code=429, headers={"Retry-After": "8"})
        )
        blocked, reason = exchange.is_rest_blocked()
        self.assertTrue(blocked)
        self.assertIn("API_RATE_LIMITED", reason)
        self.assertFalse(hasattr(exchange._rest_usage, "health_state"))

        exchange._rest_usage = RestUsageTracker()
        exchange._rest_usage.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "4500"},
            )
        )
        blocked, reason = exchange.is_rest_blocked()
        self.assertTrue(blocked)
        self.assertIn("weight", reason.lower())


class TestLastKnownBalanceOnRateLimit(unittest.TestCase):
    def test_fallback_keeps_expired_cache_on_rate_limit(self) -> None:
        from exchange import AccountRestCache, BalanceCache, BinanceExchangeManager

        exchange = BinanceExchangeManager.__new__(BinanceExchangeManager)
        exchange._balance_cache = BalanceCache()
        exchange._balance_cache.set(1234.56)
        exchange._balance_cache.invalidate()
        exchange._account_rest_cache = AccountRestCache()
        exchange._market_data = None
        self.assertEqual(exchange._fallback_quote_balance(), 1234.56)
        self.assertFalse(exchange._balance_cache.is_valid())
        self.assertEqual(exchange._balance_cache.last_known(), 1234.56)


class TestGovernorNoExecutionBypass(unittest.TestCase):
    def _exchange(self):
        from exchange import AccountRestCache, BalanceCache, BinanceExchangeManager

        exchange = BinanceExchangeManager.__new__(BinanceExchangeManager)
        exchange._rest_usage = RestUsageTracker()
        exchange._market_data = None
        exchange._rest_token_bucket = SimpleNamespace(
            is_hard_stopped=lambda: False,
            hard_stop_remaining=lambda: 0,
        )
        exchange._balance_cache = BalanceCache()
        exchange._account_rest_cache = AccountRestCache()
        exchange._throttled_call = MagicMock(
            side_effect=AssertionError("background REST must not run")
        )
        return exchange

    def test_refresh_wallet_uses_cache_when_weight_throttled(self) -> None:
        exchange = self._exchange()
        exchange._balance_cache.set(1618.0)
        exchange._rest_usage.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "4500"},
            )
        )
        wallet = exchange.refresh_wallet_after_trade()
        self.assertEqual(wallet, 1618.0)
        exchange._throttled_call.assert_not_called()

    def test_snapshot_does_not_force_live_when_throttled(self) -> None:
        exchange = self._exchange()
        exchange._balance_cache.set(1618.0)
        exchange._rest_usage.note_http_response(
            SimpleNamespace(
                status_code=200,
                headers={"X-MBX-USED-WEIGHT-1M": "5400"},
            )
        )
        snap = exchange.fetch_live_account_snapshot(
            include_today_income=True, force_refresh=True
        )
        self.assertGreater(snap.wallet_balance, 0)
        exchange._throttled_call.assert_not_called()


class TestScanRejectedLog(unittest.TestCase):
    def test_pick_best_loser_uses_scan_rejected_tag(self) -> None:
        from executor import log_scan_rejected

        with patch("executor.trade_logger") as log:
            log_scan_rejected(
                "ONGUSDT",
                "Lower score than winner TREND_MOMENTUM LONG raw=94.0",
                strategy="BREAKOUT_RETEST",
            )
        log.warning.assert_called()
        rendered = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        self.assertIn("[SCAN_REJECTED]", rendered)
        self.assertNotIn("[EXECUTION_REJECTED]", rendered)


class TestColdStartWalletFailClosed(unittest.TestCase):
    def test_allocator_skips_rest_when_wallet_not_hydrated(self) -> None:
        from core.portfolio_allocator import PortfolioAllocator
        from core.types import SignalCandidate

        exchange = MagicMock()
        exchange.wallet_is_hydrated.return_value = False
        allocator = PortfolioAllocator(exchange, MagicMock())
        result = allocator.approve(
            SignalCandidate(
                symbol="ONGUSDT",
                action="LONG",
                strategy="SMC_TREND",
                score=80.0,
                price=1.0,
                atr=0.1,
                timeframe="5m",
            )
        )
        self.assertFalse(result.approved)
        self.assertEqual(result.reason, "wallet_not_ready")
        exchange.get_futures_balance.assert_not_called()


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
