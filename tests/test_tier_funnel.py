"""3-tier funnel pacing, promotion memory, and score ranges."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from config import Config
from core.execution_governor import ExecutionGovernor
from core.pace_clock import MinuteWindow
from core.strategy_score_ranges import classify_score, score_range_for
from core.tier_funnel import TierFunnel
from core.types import StrategyScore
from pipeline.event_scan_orchestrator import EventScanOrchestrator


def _score(symbol: str, strategy: str, score: float) -> StrategyScore:
    return StrategyScore(
        symbol=symbol,
        strategy=strategy,
        score=score,
        adjusted_score=score,
        min_score=70.0,
        normalized_score=score,
        action="LONG",
    )


class TestStrategyScoreRanges(unittest.TestCase):
    def test_smc_and_range_bands(self) -> None:
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False
        ):
            smc = score_range_for("SMC_TREND")
            rng = score_range_for("RANGE_REVERSION")
        self.assertEqual(smc.classify(69.0), "NORMAL")
        self.assertEqual(smc.classify(70.0), "HOT")
        self.assertEqual(smc.classify(80.0), "SUPER")
        self.assertEqual(rng.classify(64.0), "NORMAL")
        self.assertEqual(rng.classify(65.0), "HOT")
        self.assertEqual(rng.classify(75.0), "SUPER")
        self.assertEqual(classify_score("SMC_TREND", 81.0), smc.classify(81.0))


class TestPaceAndGovernor(unittest.TestCase):
    def test_minute_window_caps_takes(self) -> None:
        clock = MinuteWindow(2)
        now = 1_000.0
        self.assertEqual(clock.take(5, now=now), 2)
        self.assertEqual(clock.take(1, now=now + 10), 0)
        self.assertEqual(clock.take(1, now=now + 60), 1)

    def test_execution_governor_caps_four_per_minute(self) -> None:
        gov = ExecutionGovernor(max_per_minute=4)
        for _ in range(4):
            ok, _reason = gov.allow_entry_order()
            self.assertTrue(ok)
        ok, reason = gov.allow_entry_order()
        self.assertFalse(ok)
        self.assertIn("4/min", reason)


class TestTierFunnel(unittest.TestCase):
    def test_normal_lock_and_two_coins_per_minute(self) -> None:
        funnel = TierFunnel()
        symbols = [f"S{i:03d}USDT" for i in range(120)]
        locked = funnel.replace_normal_universe(symbols, now=10.0)
        self.assertEqual(len(locked), 120)
        first = funnel.take_normal(now=10.0)
        self.assertEqual(len(first), 2)
        self.assertEqual(funnel.take_normal(now=20.0), [])
        later = funnel.take_normal(now=70.0)
        self.assertEqual(len(later), 2)
        self.assertNotEqual(first, later)
        self.assertFalse(funnel.lock_expired(10.0 + 3 * 3600.0 - 1))
        self.assertTrue(funnel.lock_expired(10.0 + 3 * 3600.0))

    def test_promote_remembers_until_hot_demotes(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        scores = [
            _score("AAAUSDT", "SMC_TREND", 72.0),
            _score("AAAUSDT", "RANGE_REVERSION", 68.0),
            _score("AAAUSDT", "VWAP_PULLBACK", 66.0),
        ]
        rec = funnel.promote_from_normal("AAAUSDT", scores)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.strategy, "SMC_TREND")
        self.assertEqual(rec.backup_strategies, ("RANGE_REVERSION", "VWAP_PULLBACK"))
        self.assertIsNone(funnel.promote_from_normal("AAAUSDT", scores))
        funnel.on_backtest_failed("AAAUSDT", "wr")
        self.assertNotIn("AAAUSDT", funnel.hot_symbols)
        self.assertFalse(funnel.is_promoted("AAAUSDT"))
        self.assertIn("AAAUSDT", funnel.scan_pool())

    def test_hot_backup_then_super_or_demote(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT"], now=1.0)
        funnel.promote_from_normal(
            "AAAUSDT",
            [
                _score("AAAUSDT", "SMC_TREND", 72.0),
                _score("AAAUSDT", "RANGE_REVERSION", 68.0),
            ],
        )
        funnel.on_backtest_passed("AAAUSDT", {"backtest_validated": True})
        outcome = funnel.apply_hot_scores(
            "AAAUSDT",
            [
                _score("AAAUSDT", "SMC_TREND", 60.0),
                _score("AAAUSDT", "RANGE_REVERSION", 76.0),
            ],
        )
        self.assertEqual(outcome, "SUPER")
        self.assertIn("AAAUSDT", funnel.super_symbols)
        self.assertEqual(funnel.super_symbols[0], "AAAUSDT")

        funnel2 = TierFunnel()
        funnel2.replace_normal_universe(["BBBUSDT"], now=1.0)
        funnel2.promote_from_normal(
            "BBBUSDT", [_score("BBBUSDT", "SMC_TREND", 72.0)]
        )
        funnel2.on_backtest_passed("BBBUSDT")
        self.assertEqual(
            funnel2.apply_hot_scores(
                "BBBUSDT", [_score("BBBUSDT", "SMC_TREND", 50.0)]
            ),
            "NORMAL",
        )
        self.assertNotIn("BBBUSDT", funnel2.hot_symbols)

    def test_hot_rest_one_per_minute(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AUSDT", "BUSDT"], now=1.0)
        funnel.promote_from_normal("AUSDT", [_score("AUSDT", "SMC_TREND", 72.0)])
        funnel.promote_from_normal("BUSDT", [_score("BUSDT", "SMC_TREND", 73.0)])
        first = funnel.take_hot_for_rest(now=5.0)
        self.assertIsNotNone(first)
        self.assertIsNone(funnel.take_hot_for_rest(now=10.0))
        funnel.on_backtest_failed(first.symbol, "x")
        second = funnel.take_hot_for_rest(now=65.0)
        self.assertIsNotNone(second)
        self.assertNotEqual(second.symbol, first.symbol)

    def test_hot_rescore_uses_same_clock_and_can_promote_or_demote(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        funnel.promote_from_normal(
            "AAAUSDT",
            [
                _score("AAAUSDT", "SMC_TREND", 72.0),
                _score("AAAUSDT", "RANGE_REVERSION", 68.0),
            ],
        )
        funnel.promote_from_normal("BBBUSDT", [_score("BBBUSDT", "SMC_TREND", 73.0)])
        funnel.on_backtest_passed("AAAUSDT")
        rest = funnel.take_hot_for_rest(now=5.0)
        self.assertIsNotNone(rest)
        self.assertEqual(rest.symbol, "BBBUSDT")
        self.assertIsNone(funnel.take_hot_for_rescore(now=10.0))
        parked = funnel.take_hot_for_rescore(now=65.0)
        self.assertIsNotNone(parked)
        self.assertEqual(parked.symbol, "AAAUSDT")
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False
        ):
            self.assertEqual(
                funnel.apply_hot_scores(
                    "AAAUSDT",
                    [
                        _score("AAAUSDT", "SMC_TREND", 60.0),
                        _score("AAAUSDT", "RANGE_REVERSION", 68.0),
                    ],
                ),
                "HOT",
            )
            self.assertEqual(funnel._hot["AAAUSDT"].strategy, "RANGE_REVERSION")
            self.assertEqual(
                funnel.apply_hot_scores(
                    "AAAUSDT", [_score("AAAUSDT", "RANGE_REVERSION", 50.0)]
                ),
                "NORMAL",
            )
        self.assertNotIn("AAAUSDT", funnel.hot_symbols)

    def test_note_filled_clears_super_so_coin_is_not_redispatched(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT"], now=1.0)
        funnel.promote_from_normal("AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 72.0)])
        funnel.on_backtest_passed("AAAUSDT")
        self.assertEqual(
            funnel.apply_hot_scores(
                "AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 82.0)]
            ),
            "SUPER",
        )
        self.assertIn("AAAUSDT", funnel.super_symbols)
        funnel.note_filled("AAAUSDT")
        self.assertNotIn("AAAUSDT", funnel.super_symbols)
        self.assertFalse(funnel.is_promoted("AAAUSDT"))
        self.assertIsNone(funnel.take_super(now=70.0))
        again = funnel.promote_from_normal(
            "AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 72.0)]
        )
        self.assertIsNotNone(again)

    def test_release_hot_rest_allows_retry(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT"], now=1.0)
        funnel.promote_from_normal("AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 72.0)])
        first = funnel.take_hot_for_rest(now=5.0)
        self.assertIsNotNone(first)
        funnel.release_hot_rest("AAAUSDT")
        self.assertIsNone(funnel.take_hot_for_rest(now=10.0))
        retry = funnel.take_hot_for_rest(now=65.0)
        self.assertIsNotNone(retry)
        self.assertEqual(retry.symbol, "AAAUSDT")


class TestFunnelOrchestratorEmptyLog(unittest.TestCase):
    def test_empty_universe_still_rate_limited(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = None
        orch._tier1_symbols = []
        orch._last_empty_universe_log_at = 0.0
        orch._last_zero_candidate_log_at = 0.0
        orch._scan_gate_open = lambda: (False, "")  # type: ignore[method-assign]
        orch.maybe_refresh_tier1_periodic = lambda: None  # type: ignore[method-assign]
        orch.bootstrap_watchlist_once = lambda: []  # type: ignore[method-assign]
        orch.run_catchup = lambda: 0  # type: ignore[method-assign]
        orch.conflict_guard = MagicMock()
        orch.funnel = TierFunnel()
        orch._validator = None
        orch._telegram = None
        orch._open_symbols = lambda: set()  # type: ignore[method-assign]
        orch._ws_ticker_map = lambda: {}  # type: ignore[method-assign]
        orch._ws_book_map = lambda _t: {}  # type: ignore[method-assign]
        orch._partition_kline_ready = lambda s: ([], list(s))  # type: ignore[method-assign]
        orch._note_missing_scan_klines = lambda *a, **k: None  # type: ignore[method-assign]
        with patch.object(Config, "ENABLE_THREE_TIER_FUNNEL", True), patch(
            "pipeline.event_scan_orchestrator.scanner_logger"
        ) as log:
            orch.process_priority_scan_cycle()
            orch.process_priority_scan_cycle()
        info_msgs = [c[0][0] for c in log.info.call_args_list]
        empty = [m for m in info_msgs if "empty universe" in m]
        self.assertEqual(len(empty), 1)


class TestFunnelScanIndependentOfRestBan(unittest.TestCase):
    def test_normal_scoring_continues_when_rest_blocked(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = None
        orch._tier1_symbols = ["AAAUSDT", "BBBUSDT"]
        orch._last_empty_universe_log_at = 0.0
        orch._last_zero_candidate_log_at = 0.0
        orch._scan_gate_open = lambda: (True, "IP banned")  # type: ignore[method-assign]
        orch.maybe_refresh_tier1_periodic = MagicMock()
        orch.bootstrap_watchlist_once = MagicMock()
        orch.run_catchup = lambda: 0  # type: ignore[method-assign]
        orch.conflict_guard = MagicMock()
        orch.funnel = TierFunnel()
        orch.funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        orch._validator = None
        orch._telegram = None
        orch.exchange = MagicMock()
        orch.exchange.is_rest_blocked.return_value = (True, "ban")
        orch._open_symbols = lambda: set()  # type: ignore[method-assign]
        orch._ws_ticker_map = lambda: {}  # type: ignore[method-assign]
        orch._ws_book_map = lambda _t: {}  # type: ignore[method-assign]
        orch._partition_kline_ready = lambda s: (list(s), [])  # type: ignore[method-assign]
        orch._note_missing_scan_klines = lambda *a, **k: None  # type: ignore[method-assign]
        orch._score_symbol = MagicMock(return_value=[])
        with patch.object(Config, "ENABLE_THREE_TIER_FUNNEL", True), patch(
            "pipeline.event_scan_orchestrator.touch_scan_cycle"
        ):
            orch.process_priority_scan_cycle()
        orch.maybe_refresh_tier1_periodic.assert_not_called()
        orch.bootstrap_watchlist_once.assert_not_called()
        self.assertTrue(orch._score_symbol.called)


if __name__ == "__main__":
    unittest.main()
