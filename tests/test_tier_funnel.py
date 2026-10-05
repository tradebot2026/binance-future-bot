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


_FUNNEL_SCORE_ENV = {
    "SMC_TREND_HOT_SCORE": "",
    "SMC_TREND_SUPER_SCORE": "",
    "RANGE_REVERSION_HOT_SCORE": "",
    "RANGE_REVERSION_SUPER_SCORE": "",
}


def _score(
    symbol: str, strategy: str, score: float, *, final_score: float = 0.0
) -> StrategyScore:
    return StrategyScore(
        symbol=symbol,
        strategy=strategy,
        score=score,
        adjusted_score=score,
        min_score=70.0,
        normalized_score=score,
        action="LONG",
        final_score=final_score,
    )


class TestStrategyScoreRanges(unittest.TestCase):
    def test_smc_and_range_bands(self) -> None:
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False):
            smc = score_range_for("SMC_TREND")
            rng = score_range_for("RANGE_REVERSION")
        self.assertEqual(smc.classify(61.0), "NORMAL")
        self.assertEqual(smc.classify(62.0), "HOT")
        self.assertEqual(smc.classify(72.0), "SUPER")
        self.assertEqual(rng.classify(57.0), "NORMAL")
        self.assertEqual(rng.classify(58.0), "HOT")
        self.assertEqual(rng.classify(68.0), "SUPER")
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
        seeded = funnel.replace_normal_universe(symbols, now=10.0)
        self.assertEqual(len(seeded), 120)
        self.assertEqual(funnel.normal_symbols, [])
        first = funnel.take_normal(now=10.0)
        self.assertEqual(first, ["S000USDT"])
        self.assertEqual(funnel.normal_symbols, ["S000USDT"])
        self.assertEqual(funnel.take_normal(now=20.0), [])
        second = funnel.take_normal(now=40.0)
        self.assertEqual(second, ["S001USDT"])
        self.assertEqual(funnel.recently_scanned[0], "S001USDT")
        self.assertEqual(funnel.take_normal(now=50.0), [])
        later = funnel.take_normal(now=70.0)
        self.assertEqual(later, ["S002USDT"])
        self.assertEqual(funnel.ingested_count, 3)
        self.assertEqual(funnel.current_index, 3)
        self.assertEqual(funnel.pass_number, 1)
        self.assertFalse(funnel.lock_expired(10.0 + 3 * 3600.0 - 1))
        self.assertTrue(funnel.lock_expired(10.0 + 3 * 3600.0))
        self.assertEqual(funnel.flush_count, 1)

    def test_record_normal_scan_stores_score(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        self.assertEqual(funnel.flush_count, 1)
        funnel.record_normal_scan(
            "AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 62.0)]
        )
        self.assertAlmostEqual(funnel.normal_score("AAAUSDT"), 62.0)
        funnel.record_normal_scan("AAAUSDT", [])
        self.assertAlmostEqual(funnel.normal_score("AAAUSDT"), 62.0)
        funnel.record_normal_scan("BBBUSDT", [])
        self.assertEqual(funnel.normal_score("BBBUSDT"), 0.0)
        funnel.replace_normal_universe(["CCCUSDT"], now=2.0)
        self.assertEqual(funnel.flush_count, 2)

    def test_watchlist_uses_final_or_raw_score(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        self.assertEqual(funnel.take_normal(now=1.0), ["AAAUSDT"])
        self.assertEqual(funnel.take_normal(now=31.0), ["BBBUSDT"])
        funnel.record_normal_scan(
            "AAAUSDT",
            [_score("AAAUSDT", "SMC_TREND", 50.0, final_score=71.0)],
        )
        self.assertAlmostEqual(funnel.normal_score("AAAUSDT"), 71.0)
        funnel.record_normal_scan(
            "BBBUSDT",
            [_score("BBBUSDT", "SMC_TREND", 62.0, final_score=0.0)],
        )
        self.assertAlmostEqual(funnel.normal_score("BBBUSDT"), 62.0)
        snap = funnel.watchlist_snapshot(now=31.0)
        self.assertAlmostEqual(snap["normal_scores"]["AAAUSDT"], 71.0)
        self.assertAlmostEqual(snap["normal_scores"]["BBBUSDT"], 62.0)

    def test_typical_smc_score_promotes_to_hot(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False):
            rec = funnel.promote_from_normal(
                "AAAUSDT",
                [_score("AAAUSDT", "SMC_TREND", 50.0, final_score=64.0)],
            )
            self.assertIsNotNone(rec)
            self.assertAlmostEqual(rec.score, 64.0)
            self.assertIsNone(
                funnel.promote_from_normal(
                    "BBBUSDT", [_score("BBBUSDT", "SMC_TREND", 61.0)]
                )
            )
            self.assertIsNotNone(
                funnel.promote_from_normal(
                    "BBBUSDT", [_score("BBBUSDT", "SMC_TREND", 62.0)]
                )
            )

    def test_three_passes_flush_keeps_hot(self) -> None:
        funnel = TierFunnel()
        funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        t = 1.0
        last = ""
        for _ in range(6):
            got = funnel.take_normal(now=t)
            self.assertEqual(len(got), 1)
            last = got[0]
            t += 30.0
        self.assertEqual(funnel.recently_scanned[0], last)
        self.assertEqual(funnel.pass_number, 3)
        self.assertTrue(funnel.lock_expired(t))
        funnel.promote_from_normal("AAAUSDT", [_score("AAAUSDT", "SMC_TREND", 72.0)])
        flushed = funnel.flush_non_setup()
        self.assertIn("AAAUSDT", funnel.hot_symbols)
        self.assertFalse(funnel.has_candidate_universe)
        self.assertNotIn("BBBUSDT", funnel.normal_symbols)
        self.assertIn("BBBUSDT", flushed)
        self.assertNotIn("AAAUSDT", flushed)
        self.assertEqual(funnel.last_flushed, flushed)

    def test_hub_batch_kline_gc_rebuilds_once(self) -> None:
        import threading
        from collections import deque

        from market_data_hub import MarketDataHub

        hub = MarketDataHub.__new__(MarketDataHub)
        hub._lock = threading.RLock()
        hub._subscribed_kline_streams = {
            "btcusdt@kline_5m",
            "ethusdt@kline_5m",
            "solusdt@kline_5m",
        }
        hub._kline_bars = {
            ("BTCUSDT", "5m"): deque(),
            ("ETHUSDT", "5m"): deque(),
            ("SOLUSDT", "5m"): deque(),
        }
        hub._candles = {("BTCUSDT", "5m"): object()}
        hub._bootstrapped_pairs = {("BTCUSDT", "5m")}
        hub._ws_manager = object()
        hub._ws_running = True
        hub._rebuild_kline_socket_pool = MagicMock()
        with patch.object(Config, "get_ws_kline_intervals", return_value=["5m"]):
            dropped = hub.demote_symbols_klines(["BTCUSDT", "ETHUSDT"])
        self.assertEqual(dropped, 2)
        self.assertEqual(hub._subscribed_kline_streams, {"solusdt@kline_5m"})
        self.assertNotIn(("BTCUSDT", "5m"), hub._kline_bars)
        self.assertNotIn(("ETHUSDT", "5m"), hub._kline_bars)
        self.assertIn(("SOLUSDT", "5m"), hub._kline_bars)
        hub._rebuild_kline_socket_pool.assert_called_once()

    def test_watchlist_format_shows_pass_and_just_scanned(self) -> None:
        from telegram_alerts import format_watchlist_message

        text = format_watchlist_message(
            tier1_hot=[],
            tier1_background=[],
            tier1_full=["ETHUSDT", "BTCUSDT"],
            tier2_rows=[],
            hot_scan_interval=60.0,
            recently_scanned=["ETHUSDT", "BTCUSDT"],
            pass_number=2,
            current_index=42,
            universe_size=120,
            ingested_count=42,
            currently_scanning="ETHUSDT",
            flush_minutes=97,
            flush_count=2,
            normal_scores={"ETHUSDT": 62.0, "BTCUSDT": 0.0},
        )
        self.assertIn("3-Tier Dynamic Scan Funnel", text)
        self.assertIn("Pass 2 of 3", text)
        self.assertIn("Flush #2 (Next in 97 mins)", text)
        self.assertIn("[#42/120] ETHUSDT", text)
        self.assertIn("Normal Tier</b> (42/120)", text)
        self.assertIn("1. ETHUSDT [62%] 👈 (Just Scanned)", text)
        self.assertIn("2. BTCUSDT [0%]", text)
        self.assertIn("No Hot promotions yet", text)
        self.assertIn("No Super setups ready", text)
        self.assertIn("SCANNER: DYNAMIC 2-COIN/MIN ACTIVE", text)
        self.assertIn("RUNTIME STATUS", text)

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
        with patch.dict("os.environ", _FUNNEL_SCORE_ENV, clear=False), patch.object(
            Config, "USE_TESTNET", False
        ), patch.object(Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", False):
            self.assertEqual(
                funnel.apply_hot_scores(
                    "AAAUSDT",
                    [
                        _score("AAAUSDT", "SMC_TREND", 60.0),
                        _score("AAAUSDT", "RANGE_REVERSION", 64.0),
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


class TestFunnelKlineFlushGc(unittest.TestCase):
    def test_seed_funnel_gcs_flushed_klines_once(self) -> None:
        orch = EventScanOrchestrator.__new__(EventScanOrchestrator)
        orch._hub = MagicMock()
        orch._tier1_symbols = ["CCCUSDT", "DDDUSDT"]
        orch.funnel = TierFunnel()
        orch.funnel.replace_normal_universe(["AAAUSDT", "BBBUSDT"], now=1.0)
        orch.funnel.take_normal(now=1.0)
        orch._open_symbols = lambda: set()  # type: ignore[method-assign]
        orch._seed_funnel_universe(100.0)
        orch._hub.demote_symbols_klines.assert_called_once()
        flushed = orch._hub.demote_symbols_klines.call_args[0][0]
        self.assertIn("AAAUSDT", flushed)
        self.assertIn("BBBUSDT", flushed)
        self.assertTrue(orch.funnel.has_candidate_universe)


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
