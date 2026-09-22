"""Hardening pass: entry gates, late fills, lock, arbitration, Testnet."""

from __future__ import annotations

import os
import tempfile
import time
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config, _env_use_testnet
from core.candidate_arbitrator import CandidateArbitrator
from core.candle_prep import drop_forming_bar
from core.execution_ledger import ExecutionLedger, ExecutionPhase, ExecutionRecord
from core.symbol_conflict_guard import SymbolConflictGuard
from core.types import MarketSnapshot, RegimeLabel, SignalCandidate
from exceptions import OrderExecutionError
from exchange import SymbolRules
from executor import TradeExecutor
from market_data_hub import MarketDataHub
from rest_rate_guard import RestUsageTracker


def _rules() -> SymbolRules:
    return SymbolRules(2, 3, 0.01, 0.001, 0.001, 5.0)


def _ready_hub(symbol: str = "ETHUSDT") -> MarketDataHub:
    hub = MarketDataHub(MagicMock())
    now = time.monotonic()
    hub._ws_running = True
    hub._reconnect_in_progress = False
    hub._ws_started_at = now - 120
    hub._last_ticker_event_at = now
    hub._last_book_event_at = now
    hub._last_real_ticker_at = now
    hub._tickers[symbol] = {
        "symbol": symbol,
        "lastPrice": 100.0,
        "updated_at": now,
    }
    return hub


def _exchange_with_hub(hub: MarketDataHub) -> SimpleNamespace:
    usage = RestUsageTracker()
    exchange = SimpleNamespace(
        _rest_usage=usage,
        _market_data=hub,
        _entry_ready_logged=False,
    )
    from exchange import BinanceExchangeManager

    exchange.get_execution_safety = BinanceExchangeManager.get_execution_safety.__get__(
        exchange, BinanceExchangeManager
    )
    exchange._log_entry_block = BinanceExchangeManager._log_entry_block.__get__(
        exchange, BinanceExchangeManager
    )
    return exchange


def _candidate(
    symbol: str = "ONGUSDT",
    score: float = 80.0,
    strategy: str = "SMC_TREND",
    action: str = "LONG",
) -> SignalCandidate:
    return SignalCandidate(
        symbol=symbol,
        action=action,  # type: ignore[arg-type]
        strategy=strategy,
        score=score,
        price=100.0,
        atr=1.0,
        timeframe="5m",
        regime_fit=1.0,
        priority_weight=1.0,
    )


class TestMarketDataEntryGate(unittest.TestCase):
    def test_ws_disconnected_blocks_entry(self) -> None:
        hub = _ready_hub()
        hub._ws_running = False
        ready, detail = hub.is_market_data_ready_for_entry("ETHUSDT")
        self.assertFalse(ready)
        self.assertEqual(detail, "WS_DISCONNECTED")
        state, _ = _exchange_with_hub(hub).get_execution_safety("ETHUSDT")
        self.assertEqual(state, "WS_DISCONNECTED")

    def test_ws_reconnecting_blocks_entry(self) -> None:
        hub = _ready_hub()
        hub._reconnect_in_progress = True
        ready, detail = hub.is_market_data_ready_for_entry("ETHUSDT")
        self.assertFalse(ready)
        self.assertEqual(detail, "WS_RECONNECTING")
        state, _ = _exchange_with_hub(hub).get_execution_safety("ETHUSDT")
        self.assertEqual(state, "RESYNC_REQUIRED")

    def test_ws_warming_blocks_entry(self) -> None:
        hub = _ready_hub()
        hub._last_real_ticker_at = 0.0
        ready, detail = hub.is_market_data_ready_for_entry("ETHUSDT")
        self.assertFalse(ready)
        self.assertEqual(detail, "WS_WARMUP")
        state, _ = _exchange_with_hub(hub).get_execution_safety("ETHUSDT")
        self.assertEqual(state, "WS_WARMING")

    def test_stale_data_blocks_entry(self) -> None:
        hub = _ready_hub()
        hub._last_real_ticker_at = time.monotonic() - 10_000
        ready, detail = hub.is_market_data_ready_for_entry("ETHUSDT")
        self.assertFalse(ready)
        self.assertEqual(detail, "STALE_DATA")
        state, _ = _exchange_with_hub(hub).get_execution_safety("ETHUSDT")
        self.assertEqual(state, "STALE_DATA")

    def test_healthy_fresh_data_allows_entry(self) -> None:
        hub = _ready_hub()
        ready, detail = hub.is_market_data_ready_for_entry("ETHUSDT")
        self.assertTrue(ready)
        self.assertEqual(detail, "")
        state, _ = _exchange_with_hub(hub).get_execution_safety("ETHUSDT")
        self.assertEqual(state, "EXECUTION_SAFE")


class TestOrderStateSafety(unittest.TestCase):
    def _executor(self, ledger: ExecutionLedger) -> tuple[TradeExecutor, MagicMock]:
        exchange = MagicMock()
        exchange.get_execution_safety.return_value = ("EXECUTION_SAFE", "")
        exchange.get_symbol_rules.return_value = _rules()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.optimize_and_set_leverage.return_value = 5
        db = MagicMock()
        db.is_symbol_on_cooldown.return_value = (False, "")
        executor = TradeExecutor(exchange, db)
        return executor, exchange

    def test_rejected_order_creates_no_position(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            executor, exchange = self._executor(ledger)
            exchange.execute_futures_order.side_effect = OrderExecutionError(
                "ORDER_REJECTED: insufficient margin"
            )
            with patch.object(Config, "DRY_RUN", False), patch(
                "executor.get_execution_ledger", return_value=ledger
            ), patch.object(executor, "_resolve_live_execution_price", return_value=100.0), patch.object(
                executor,
                "_resolve_execution_levels",
                return_value=(98.0, 102.0, 104.0, 106.0),
            ), patch.object(
                executor, "calculate_position_size", return_value=1.0
            ), patch.object(
                executor, "_build_partial_quantities", return_value={}
            ), patch(
                "executor.symbol_blocked_for_new_entry", return_value=(False, "")
            ):
                result = executor.execute_trade(
                    "ONGUSDT", "LONG", 1.0, 100.0, "SMC_TREND", 80.0
                )
            self.assertIsNone(result)
            self.assertEqual(exchange.execute_futures_order.call_count, 1)
            self.assertTrue(
                all(not rec.is_opened() for rec in ledger._records.values())
            )

    def test_accepted_uncertain_does_not_duplicate_or_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            executor, exchange = self._executor(ledger)
            exchange.execute_futures_order.return_value = {
                "orderId": 99,
                "status": "NEW",
            }
            with patch.object(Config, "DRY_RUN", False), patch(
                "executor.get_execution_ledger", return_value=ledger
            ), patch.object(executor, "_resolve_live_execution_price", return_value=100.0), patch.object(
                executor,
                "_resolve_execution_levels",
                return_value=(98.0, 102.0, 104.0, 106.0),
            ), patch.object(
                executor, "calculate_position_size", return_value=1.0
            ), patch.object(
                executor, "_build_partial_quantities", return_value={}
            ), patch(
                "executor.symbol_blocked_for_new_entry", return_value=(False, "")
            ):
                first = executor.execute_trade(
                    "ONGUSDT", "LONG", 1.0, 100.0, "SMC_TREND", 80.0
                )
                second = executor.execute_trade(
                    "ONGUSDT", "LONG", 1.0, 100.0, "SMC_TREND", 80.0
                )
            self.assertIsNone(first)
            self.assertIsNone(second)
            self.assertEqual(exchange.execute_futures_order.call_count, 1)
            unresolved = ledger.unresolved()
            self.assertTrue(unresolved)
            self.assertEqual(unresolved[0].phase, ExecutionPhase.ORDER_STATUS_UNCERTAIN)

    def test_late_fill_is_adopted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            executor, _exchange = self._executor(ledger)
            record = ledger.new_record(
                symbol="ETHUSDT", action="LONG", strategy="SMC_TREND", score=80.0
            )
            record.atr = 1.0
            record.quantity = 1.0
            record.entry = 100.0
            ledger.transition(
                record,
                ExecutionPhase.ORDER_STATUS_UNCERTAIN,
                binance_order_id="77",
            )
            with patch("executor.get_execution_ledger", return_value=ledger), patch.object(
                executor,
                "_complete_filled_entry",
                return_value={"trade_id": "t1", "symbol": "ETHUSDT"},
            ) as complete:
                adopted = executor.adopt_confirmed_fill(
                    {
                        "symbol": "ETHUSDT",
                        "orderId": "77",
                        "clientOrderId": record.client_order_id,
                        "status": "FILLED",
                        "avgPrice": 100.5,
                        "executedQty": 1.0,
                    }
                )
            self.assertEqual(adopted["trade_id"], "t1")
            complete.assert_called_once()

    def test_partial_fill_records_actual_qty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            executor, exchange = self._executor(ledger)
            record = ExecutionRecord(
                exec_id="EXEC-TEST-1",
                symbol="ETHUSDT",
                action="LONG",
                strategy="SMC_TREND",
                score=80.0,
                atr=1.0,
            )
            with patch("executor.get_execution_ledger", return_value=ledger), patch.object(
                executor,
                "_resolve_execution_levels",
                return_value=(98.0, 102.0, 104.0, 106.0),
            ), patch.object(
                executor, "_validate_stop_loss", return_value=(True, "")
            ), patch.object(
                executor, "_persist_trade_with_retry", return_value=True
            ), patch.object(
                executor, "_build_partial_quantities", return_value={}
            ), patch.object(Config, "ENABLE_NATIVE_TP_SL", False):
                result = executor._complete_filled_entry(
                    symbol="ETHUSDT",
                    action="LONG",
                    atr=1.0,
                    current_price=100.0,
                    strategy="SMC_TREND",
                    score=80.0,
                    structure={},
                    record=record,
                    response={
                        "status": "PARTIALLY_FILLED",
                        "orderId": "55",
                        "avgPrice": 100.2,
                        "executedQty": 0.4,
                    },
                    quantity=0.4,
                    leverage=5,
                    metadata={},
                    rules=_rules(),
                )
            self.assertIsNotNone(result)
            self.assertEqual(result["quantity"], 0.4)
            self.assertEqual(result["entry_price"], 100.2)
            self.assertEqual(record.phase, ExecutionPhase.POSITION_CONFIRMED)
            exchange.seed_position_after_fill.assert_called()

    def test_duplicate_execution_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = ExecutionLedger(persist_path=os.path.join(tmp, "u.json"))
            executor, exchange = self._executor(ledger)
            existing = ledger.new_record(
                symbol="BTCUSDT", action="LONG", strategy="RANGE_REVERSION", score=70.0
            )
            ledger.transition(existing, ExecutionPhase.ORDER_STATUS_UNCERTAIN)
            with patch.object(Config, "DRY_RUN", False), patch(
                "executor.get_execution_ledger", return_value=ledger
            ):
                result = executor.execute_trade(
                    "BTCUSDT", "SHORT", 1.0, 50.0, "SMC_TREND", 90.0
                )
            self.assertIsNone(result)
            exchange.execute_futures_order.assert_not_called()


class TestWatchdogAndWs(unittest.TestCase):
    def test_watchdog_skips_restart_when_lock_live(self) -> None:
        from watchdog import PositionWatchdog

        wd = PositionWatchdog.__new__(PositionWatchdog)
        wd.state = MagicMock()
        wd.state.should_alert.return_value = True
        wd.telegram = MagicMock()
        with patch.object(Config, "WATCHDOG_AUTO_RESTART_MAIN", True), patch(
            "os.path.isfile", return_value=True
        ), patch(
            "core.instance_lock.another_main_is_running", return_value=True
        ), patch.object(
            wd, "is_main_process_running", return_value=False
        ), patch("subprocess.Popen") as popen:
            wd._maybe_restart_main()
        popen.assert_not_called()

    def test_reconnect_is_single_flight(self) -> None:
        hub = _ready_hub()
        hub._last_reconnect_request_at = 0.0
        with patch.object(Config, "WS_RECONNECT_ENABLED", True), patch.object(
            Config, "ENABLE_WEBSOCKET_STREAMS", True
        ), patch.object(Config, "WS_RECONNECT_DEBOUNCE_SECONDS", 0.0), patch(
            "threading.Thread"
        ) as thread:
            hub._request_reconnect("first")
            hub._request_reconnect("second")
        self.assertEqual(thread.call_count, 1)
        self.assertTrue(hub._reconnect_in_progress)
        self.assertEqual(hub._last_real_ticker_at, 0.0)

    def test_kline_bootstrap_is_paced(self) -> None:
        hub = _ready_hub()
        hub._bootstrap_series_window = deque([time.monotonic()] * 12)
        called = []

        def fetcher(symbol: str, interval: str, limit: int) -> pd.DataFrame:
            called.append((symbol, interval))
            return pd.DataFrame()

        with patch.object(hub, "is_rest_blocked", return_value=(False, "")), patch.object(
            hub,
            "_pending_bootstrap_pairs",
            return_value=[("AAAUSDT", "5m")] * 20,
        ), patch.object(Config, "KLINE_BOOTSTRAP_MAX_SERIES_PER_MINUTE", 12):
            seeded = hub.bootstrap_klines_for_symbols(
                ["AAAUSDT"] * 20, ["5m"], fetcher
            )
        self.assertEqual(seeded, 0)
        self.assertEqual(called, [])


class TestStrategiesAndArbitration(unittest.TestCase):
    def test_drop_forming_bar(self) -> None:
        df = pd.DataFrame({"close": [1, 2, 3]})
        out = drop_forming_bar(df)
        self.assertEqual(list(out["close"]), [1, 2])

    def test_valid_strategies_evaluate_without_crash(self) -> None:
        from strategies import build_strategy_registry

        n = 80
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2026-01-01", periods=n, freq="5min"),
                "open": [100.0] * n,
                "high": [101.0] * n,
                "low": [99.0] * n,
                "close": [100.4] * n,
                "volume": [1_000.0] * n,
            }
        )
        candles = {
            Config.ENTRY_TIMEFRAME: df.copy(),
            Config.CONFIRM_TIMEFRAME: df.copy(),
            Config.TREND_TIMEFRAME: df.copy(),
        }
        snapshot = MarketSnapshot(
            symbol="ETHUSDT",
            price=100.4,
            ticker={"lastPrice": 100.4},
            book={},
            candles=candles,
            spread_pct=0.01,
            volume_24h=1e8,
            regime=RegimeLabel.UNCLEAR,
            timestamp_ms=1,
        )
        registry = build_strategy_registry()
        enabled = [s.tag for s in registry.enabled()]
        self.assertIn("BREAKOUT_RETEST", enabled)
        self.assertIn("TREND_MOMENTUM", enabled)
        self.assertIn("VOL_SQUEEZE", enabled)
        self.assertIn("FALSE_BREAKOUT_SFP", enabled)
        self.assertIn("PRICE_ACTION_REVERSAL", enabled)
        self.assertNotIn("OI_FUNDING", enabled)
        self.assertNotIn("ORDER_FLOW", enabled)
        self.assertNotIn("MTF_ALIGNMENT", enabled)
        self.assertNotIn("VP_KEYLEVEL", enabled)
        for strategy in registry.enabled():
            result = strategy.scan("ETHUSDT", candles, snapshot)
            self.assertIsNotNone(result)

    def test_conflicting_signals_are_arbitrated(self) -> None:
        long_c = _candidate(score=82.0, strategy="SMC_TREND", action="LONG")
        short_c = _candidate(score=90.0, strategy="RANGE_REVERSION", action="SHORT")
        winner, losers = CandidateArbitrator.pick_symbol_winner(
            "ETHUSDT", [long_c, short_c]
        )
        self.assertIsNotNone(winner)
        self.assertEqual(winner.action, "SHORT")
        self.assertEqual(winner.strategy, "RANGE_REVERSION")
        self.assertEqual(len(losers), 1)

    def test_same_symbol_cannot_open_two_strategy_trades(self) -> None:
        guard = SymbolConflictGuard(MagicMock(), MagicMock())
        guard.exchange.has_open_position.return_value = False
        guard.db.get_open_trades_for_symbol.return_value = []
        guard.db.is_symbol_on_cooldown.return_value = (False, "")
        first = _candidate(symbol="ONGUSDT", strategy="SMC_TREND", action="LONG")
        second = _candidate(symbol="ONGUSDT", strategy="VWAP_PULLBACK", action="LONG")
        ok, _ = guard.approve(first)
        self.assertTrue(ok)
        with patch.object(Config, "ALLOW_CROSS_STRATEGY_SCALE_IN", False):
            ok2, reason = guard.approve(second)
        self.assertFalse(ok2)
        self.assertIn("Scale-in disabled", reason)


class TestTestnetFailClosed(unittest.TestCase):
    def test_current_config_is_testnet(self) -> None:
        self.assertTrue(Config.USE_TESTNET)
        self.assertTrue(_env_use_testnet())

    def test_mainnet_impossible_without_explicit_env(self) -> None:
        with patch.dict(os.environ, {"BINANCE_ENV": "TESTNET", "USE_TESTNET": "false"}):
            self.assertTrue(_env_use_testnet())
        with patch.dict(os.environ, {"BINANCE_ENV": "MAINNET", "USE_TESTNET": "false"}):
            self.assertFalse(_env_use_testnet())


class TestRateLimitStillHard(unittest.TestCase):
    def test_429_and_1003_and_418(self) -> None:
        tracker = RestUsageTracker()
        tracker.note_http_response(
            SimpleNamespace(status_code=429, headers={"Retry-After": "8"})
        )
        self.assertTrue(tracker.in_safety_mode())
        tracker2 = RestUsageTracker()
        tracker2.note_binance_error(
            code=-1003, message="Too many requests", halt_seconds=300, banned=False
        )
        self.assertGreaterEqual(tracker2.safety_remaining(), 299)
        tracker3 = RestUsageTracker()
        tracker3.note_http_response(SimpleNamespace(status_code=418, headers={}))
        self.assertEqual(tracker3.snapshot()["state"], "IP_BANNED")


if __name__ == "__main__":
    unittest.main()
