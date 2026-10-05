"""Stage-1 watchlist cap, Stage-2 backtest gate, Stage-3 native TP/SL flag."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from config import Config
from core.candle_backtest import required_backtest_win_rate, run_15m_backtest
from core.validation_queue import AsyncBacktestValidator
from pipeline.event_scan_orchestrator import EventScanOrchestrator


def _run_with_outcomes(outcomes: list[float]):
    df = _ohlcv(501)
    remaining = list(outcomes)
    signal_idx = {"n": 0}

    def _signal(_df, idx):
        if idx >= 200 and (idx - 200) % 20 == 0 and signal_idx["n"] < len(outcomes):
            signal_idx["n"] += 1
            return "LONG"
        return None

    def _sim(_df, start, *_a, **_k):
        r = remaining.pop(0) if remaining else 1.0
        return r, start + 5

    with patch("core.candle_backtest.signal_at", side_effect=_signal), patch(
        "core.candle_backtest._simulate_trade", side_effect=_sim
    ):
        return run_15m_backtest(df)


def _ohlcv(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.0] * n,
            "close": [100.2] * n,
            "volume": [1_000.0] * n,
        }
    )


class TestLightweightWatchlist(unittest.TestCase):
    def test_scan_watchlist_is_funnel_universe(self) -> None:
        self.assertGreaterEqual(Config.normal_tier_universe_size(), 120)
        self.assertEqual(Config.NORMAL_TIER_COINS_PER_MINUTE, 2)
        self.assertEqual(Config.HOT_TIER_COINS_PER_MINUTE, 1)
        self.assertEqual(Config.SUPER_TIER_COINS_PER_MINUTE, 1)
        self.assertTrue(Config.ENABLE_THREE_TIER_FUNNEL)
        self.assertGreaterEqual(Config.scan_cycle_seconds(), 3600.0)
        self.assertEqual(Config.scan_warmup_seconds(), 0.0)

    def test_backtest_candle_limit_hard_capped_at_600(self) -> None:
        with patch.object(Config, "BACKTEST_CANDLE_LIMIT", 999):
            self.assertEqual(Config.backtest_candle_limit(), 600)
        with patch.object(Config, "BACKTEST_CANDLE_LIMIT", 50):
            self.assertEqual(Config.backtest_candle_limit(), 200)
        with patch.object(Config, "BACKTEST_CANDLE_LIMIT", 500):
            self.assertEqual(Config.backtest_candle_limit(), 500)

    def test_execution_scan_ranks_volume_and_caps(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._volume_ranks = {"LOWVOL": 40, "HOT1": 1, "HOT2": 2, "HOT3": 3}
        orch._tier1_symbols = []
        orch.priority_queue = MagicMock()
        orch.priority_queue.hot_symbols = ["LOWVOL", "HOT3", "HOT1", "HOT2"]
        orch.priority_queue.background_symbols = []
        orch.assignment_manager = MagicMock()
        orch.assignment_manager.hot_symbols.return_value = []
        with patch.object(Config, "SCAN_WATCHLIST_SIZE", 2), patch.object(
            Config, "HOT_SCAN_SIZE", 2
        ), patch.object(Config, "TOP_UNIVERSE_POOL_SIZE", 2):
            out = orch._execution_scan_symbols(include_hot=True)
        self.assertEqual(out, ["LOWVOL", "HOT3"])

    def test_lightweight_cycle_pages_universe_once(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._scan_gate_open = MagicMock(return_value=(False, ""))
        orch.maybe_refresh_tier1_periodic = MagicMock()
        orch._tier1_symbols = ["AAAUSDT"]
        orch.run_catchup = MagicMock()
        orch.conflict_guard = MagicMock()
        orch._fast_track_live_spikes = MagicMock()
        orch._drain_due_event_symbols = MagicMock(return_value=({}, []))
        orch._cycle_scan_symbols = MagicMock(return_value=["AAAUSDT"])
        orch._open_symbols = MagicMock(return_value=set())
        orch._ws_ticker_map = MagicMock(return_value={})
        orch._ws_book_map = MagicMock(return_value={})
        orch._partition_kline_ready = MagicMock(return_value=(["AAAUSDT"], []))
        orch._note_missing_scan_klines = MagicMock()
        orch._evaluate_symbols_ws = MagicMock(return_value=[])
        orch._dedupe_symbol_candidates = MagicMock(return_value=[])
        orch.priority_queue = MagicMock()
        orch.priority_queue.full_universe = ["AAAUSDT"]
        orch.priority_queue.hot_symbols = ["AAAUSDT"]
        orch.priority_queue.background_symbols = ["BBBUSDT"]
        orch.priority_queue.is_priority.return_value = True
        orch.assignment_manager = MagicMock()
        orch.assignment_manager.tier2_size = 0
        orch.db = MagicMock()
        with patch.object(Config, "ENABLE_LIGHTWEIGHT_SCANNER", True), patch.object(
            Config, "ENABLE_THREE_TIER_FUNNEL", False
        ):
            orch.process_priority_scan_cycle()
        orch._evaluate_symbols_ws.assert_called_once()
        orch._cycle_scan_symbols.assert_called_once()


class TestFifteenMinuteBacktest(unittest.TestCase):
    def test_rejects_thin_history(self) -> None:
        result = run_15m_backtest(_ohlcv(80))
        self.assertFalse(result.passed)
        self.assertIn("insufficient_15m_bars", result.reason)

    def test_accepts_450_closed_15m_bars(self) -> None:
        result = run_15m_backtest(_ohlcv(451))
        self.assertNotIn("insufficient_15m_bars", result.reason)

    def test_rejects_win_rate_below_60(self) -> None:
        df = _ohlcv(501)

        def _signal(_df, idx):
            if idx >= 200 and (idx - 200) % 20 == 0:
                return "LONG"
            return None

        with patch("core.candle_backtest.signal_at", side_effect=_signal), patch(
            "core.candle_backtest._simulate_trade",
            side_effect=lambda *_a, **_k: (-1.0, 250),
        ):
            result = run_15m_backtest(df)
        self.assertFalse(result.passed)
        self.assertLess(result.win_rate, 60.0)

    def test_passes_win_rate_above_60(self) -> None:
        df = _ohlcv(501)
        outcomes = [1.0, 1.0, 1.0, 1.0, -1.0] * 4

        def _signal(_df, idx):
            if idx >= 200 and (idx - 200) % 20 == 0:
                return "LONG"
            return None

        def _sim(_df, start, *_a, **_k):
            r = outcomes.pop(0) if outcomes else 1.0
            return r, start + 5

        with patch("core.candle_backtest.signal_at", side_effect=_signal), patch(
            "core.candle_backtest._simulate_trade", side_effect=_sim
        ):
            result = run_15m_backtest(df)
        self.assertTrue(result.passed)
        self.assertGreaterEqual(result.win_rate, 60.0)
        self.assertGreater(result.profit_r, 0.0)

    def test_required_win_rate_tiers(self) -> None:
        self.assertIsNone(required_backtest_win_rate(0))
        self.assertIsNone(required_backtest_win_rate(2))
        self.assertEqual(required_backtest_win_rate(3), 66.0)
        self.assertEqual(required_backtest_win_rate(4), 60.0)
        self.assertEqual(required_backtest_win_rate(5), 60.0)
        self.assertEqual(required_backtest_win_rate(12), 60.0)

    def test_rejects_insufficient_trade_samples(self) -> None:
        result = _run_with_outcomes([1.0, 1.0])
        self.assertFalse(result.passed)
        self.assertEqual(result.trades, 2)
        self.assertEqual(result.reason, "Insufficient historical trade samples")

    def test_passes_perfect_three_of_three(self) -> None:
        result = _run_with_outcomes([1.0, 1.0, 1.0])
        self.assertTrue(result.passed)
        self.assertEqual(result.trades, 3)
        self.assertEqual(result.win_rate, 100.0)

    def test_passes_two_of_three(self) -> None:
        result = _run_with_outcomes([1.0, 1.0, -1.0])
        self.assertTrue(result.passed)
        self.assertEqual(result.trades, 3)
        self.assertGreaterEqual(result.win_rate, 66.0)

    def test_rejects_one_of_three(self) -> None:
        result = _run_with_outcomes([1.0, -1.0, -1.0])
        self.assertFalse(result.passed)
        self.assertEqual(result.trades, 3)
        self.assertIn("win_rate", result.reason)

    def test_passes_three_of_four(self) -> None:
        result = _run_with_outcomes([1.0, 1.0, 1.0, -1.0])
        self.assertTrue(result.passed)
        self.assertEqual(result.trades, 4)
        self.assertGreaterEqual(result.win_rate, 60.0)

    def test_rejects_two_of_four(self) -> None:
        result = _run_with_outcomes([1.0, 1.0, -1.0, -1.0])
        self.assertFalse(result.passed)
        self.assertEqual(result.trades, 4)
        self.assertIn("win_rate", result.reason)


class TestValidationQueue(unittest.TestCase):
    def test_dedupes_same_symbol(self) -> None:
        validator = AsyncBacktestValidator(MagicMock())
        first = validator.enqueue(
            {"symbol": "QNTUSDT", "action": "LONG", "score": 80, "atr": 1.0}
        )
        second = validator.enqueue(
            {"symbol": "QNTUSDT", "action": "LONG", "score": 90, "atr": 1.0}
        )
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(validator._inbound.qsize(), 1)

    def test_validate_rejects_failed_backtest(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.fetch_historical_candles.return_value = _ohlcv(501)
        validator = AsyncBacktestValidator(exchange)
        failed = MagicMock(passed=False, win_rate=40.0, trades=8, reason="low wr")
        with patch("core.validation_queue.run_15m_backtest", return_value=failed):
            out = validator._validate(
                {"symbol": "QNTUSDT", "action": "LONG", "atr": 1.0, "score": 80}
            )
        self.assertIsNone(out)

    def test_validate_logs_insufficient_historical_samples(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.fetch_historical_candles.return_value = _ohlcv(501)
        validator = AsyncBacktestValidator(exchange)
        failed = MagicMock(
            passed=False,
            win_rate=0.0,
            trades=2,
            reason="Insufficient historical trade samples",
        )
        with patch("core.validation_queue.run_15m_backtest", return_value=failed), patch(
            "core.validation_queue.trade_logger"
        ) as log:
            out = validator._validate(
                {"symbol": "QNTUSDT", "action": "LONG", "atr": 1.0, "score": 80}
            )
        self.assertIsNone(out)
        msg = str(log.warning.call_args)
        self.assertIn("Insufficient historical trade samples", msg)

    def test_validate_approves_and_tags_metadata(self) -> None:
        exchange = MagicMock()
        exchange.is_rest_blocked.return_value = (False, "")
        exchange.fetch_historical_candles.return_value = _ohlcv(501)
        validator = AsyncBacktestValidator(exchange)
        passed = MagicMock(
            passed=True,
            win_rate=72.0,
            trades=10,
            wins=7,
            expectancy_r=0.4,
            profit_r=4.0,
            bars_used=500,
            last_atr=1.25,
            reason="passed",
        )
        with patch("core.validation_queue.run_15m_backtest", return_value=passed):
            out = validator._validate(
                {
                    "symbol": "QNTUSDT",
                    "action": "LONG",
                    "atr": 1.0,
                    "score": 80,
                    "structure_metadata": {},
                }
            )
        self.assertIsNotNone(out)
        self.assertTrue(out["backtest_validated"])
        self.assertEqual(out["atr"], 1.25)
        self.assertEqual(out["backtest_wins"], 7)
        self.assertGreaterEqual(out["structure_metadata"]["backtest_win_rate"], 60.0)


class TestStage3NativeExits(unittest.TestCase):
    def test_execute_candidates_attaches_native_tp_sl_after_validation(self) -> None:
        import main as main_mod

        executor = MagicMock()
        executor.execute_trade.return_value = {
            "action": "LONG",
            "symbol": "QNTUSDT",
            "entry_price": 100.0,
            "take_profit_1": 102.0,
            "stop_loss": 98.0,
            "quantity": 1.0,
        }
        candidate = {
            "symbol": "QNTUSDT",
            "action": "LONG",
            "atr": 1.0,
            "price": 100.0,
            "score": 90.0,
            "strategy": "VWAP_PULLBACK",
            "backtest_validated": True,
            "structure_metadata": {"backtest_validated": True},
        }
        with patch.object(Config, "DRY_RUN", False), patch.object(
            Config, "MAX_ENTRIES_PER_CYCLE", 3
        ), patch.object(
            Config, "ATTACH_NATIVE_TP_SL_AFTER_VALIDATION", True
        ), patch.object(
            Config, "ENABLE_NATIVE_TP_SL", False
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
        self.assertFalse(executor.execute_trade.call_args.kwargs["attach_native_exits"])


class TestTelegramBacktestLine(unittest.TestCase):
    def test_trade_alert_includes_backtest_win_rate(self) -> None:
        from telegram_bot import TelegramManager

        tg = TelegramManager.__new__(TelegramManager)
        tg.send_message = MagicMock()
        with patch.object(Config, "ENABLE_TP3_RUNNER", False):
            tg.send_trade_alert(
                action="LONG",
                symbol="QNTUSDT",
                price=100.0,
                tp1=102.0,
                sl=98.0,
                tp2=104.0,
                tp3=106.0,
                score=88.0,
                strategy="VWAP_PULLBACK",
                quantity=1.0,
                backtest_win_rate=68.5,
                backtest_wins=8,
                backtest_trades=12,
            )
        msg = tg.send_message.call_args[0][0]
        self.assertIn("Backtest WR:</b> 68.5% (8/12 Wins)", msg)


if __name__ == "__main__":
    unittest.main()
