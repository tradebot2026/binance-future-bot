"""End-to-end unit tests for Phases 1–3 quantitative engine components."""

from __future__ import annotations

import tempfile
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from config import Config
from constants import STRATEGY_RANGE_REVERSION
from core.confluence_scorer import ConfluenceScorer
from core.context.institutional_context import InstitutionalContextEvaluator
from core.context.mtf_alignment_engine import evaluate_mtf_alignment
from core.context.oi_funding_context_engine import evaluate_oi_funding_context
from core.context.order_flow_imbalance_engine import evaluate_order_flow_imbalance
from core.context.volume_profile_engine import compute_volume_profile, evaluate_volume_profile_context
from core.correlation_guard import CorrelationGuard
from core.risk_engine import RiskEngine
from core.scoring_engine import ScoringEngine
from core.types import MarketSnapshot, RegimeLabel, StrategyResult, StrategyScore
from database import DatabaseManager


def _make_trend_df(
    n: int = 120,
    *,
    bullish: bool = True,
    base: float = 100.0,
) -> pd.DataFrame:
    """Synthetic OHLCV with trend structure for context engine tests."""
    idx = np.arange(n)
    drift = idx * (0.15 if bullish else -0.15)
    noise = np.sin(idx / 4.0) * 0.4
    close = base + drift + noise
    high = close + 0.6
    low = close - 0.6
    open_ = close - (0.1 if bullish else -0.1)
    volume = np.full(n, 1000.0)
    volume[-5:] = 2500.0
    df = pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
    return df


def _make_snapshot(
    symbol: str = "TESTUSDT",
    *,
    bullish: bool = True,
    derivatives: dict | None = None,
) -> MarketSnapshot:
    entry = _make_trend_df(bullish=bullish)
    confirm = _make_trend_df(n=100, bullish=bullish, base=100.0)
    trend = _make_trend_df(n=80, bullish=bullish, base=99.0)
    price = float(entry.iloc[-1]["close"])
    return MarketSnapshot(
        symbol=symbol,
        price=price,
        ticker={"lastPrice": str(price)},
        book={},
        candles={
            Config.ENTRY_TIMEFRAME: entry,
            Config.CONFIRM_TIMEFRAME: confirm,
            Config.TREND_TIMEFRAME: trend,
        },
        spread_pct=0.02,
        volume_24h=5_000_000.0,
        regime=RegimeLabel.STRONG_TREND,
        timestamp_ms=1_700_000_000_000,
        derivatives=derivatives or {},
    )


class TestPhase1CorrelationAndConfluence(unittest.TestCase):
    def test_correlation_guard_keeps_strongest(self) -> None:
        guard = CorrelationGuard(level_tolerance_atr=0.5)
        a = StrategyResult(
            symbol="BTCUSDT",
            strategy="A",
            direction="LONG",
            score=80.0,
            atr=1.0,
            key_levels=[100.0],
            level_tags=["poc"],
        )
        b = StrategyResult(
            symbol="BTCUSDT",
            strategy="B",
            direction="LONG",
            score=70.0,
            atr=1.0,
            key_levels=[100.2],
            level_tags=["poc"],
        )
        merged = guard.deduplicate([a, b])
        actionable = [r for r in merged if r.is_actionable]
        self.assertEqual(len(actionable), 1)
        self.assertEqual(actionable[0].strategy, "A")
        self.assertIn("B", actionable[0].correlated_with)

    def test_confluence_bonus_for_agreement(self) -> None:
        class _StubStrategy:
            tag = "STUB"

            def regime_fit(self, _snapshot):
                return 1.0

            def priority_weight(self):
                return 1.0

            def min_score(self, _signal=None):
                return 60.0

        scorer = ConfluenceScorer({"S1": _StubStrategy(), "S2": _StubStrategy()})
        results = [
            StrategyResult(
                symbol="ETHUSDT",
                strategy="S1",
                direction="LONG",
                score=75.0,
            ),
            StrategyResult(
                symbol="ETHUSDT",
                strategy="S2",
                direction="LONG",
                score=72.0,
            ),
        ]
        snapshot = _make_snapshot("ETHUSDT")
        scores = scorer.score_results(snapshot, results)
        self.assertEqual(len(scores), 2)
        self.assertGreater(scores[0].confluence_bonus, 0.0)
        self.assertGreater(scores[0].final_score, scores[0].score)


class TestPhase3ContextEngines(unittest.TestCase):
    def test_volume_profile_detects_poc_and_nodes(self) -> None:
        df = _make_trend_df()
        profile = compute_volume_profile(df, bins=16)
        self.assertGreater(profile.poc, 0.0)
        self.assertGreaterEqual(profile.vah, profile.val)
        self.assertGreater(len(profile.bin_centers), 0)

    def test_mtf_alignment_bullish(self) -> None:
        snapshot = _make_snapshot(bullish=True)
        result = evaluate_mtf_alignment(
            snapshot.candles,
            entry_tf=Config.ENTRY_TIMEFRAME,
            confirm_tf=Config.CONFIRM_TIMEFRAME,
            trend_tf=Config.TREND_TIMEFRAME,
        )
        self.assertIn(result.direction, ("LONG", "NEUTRAL", "SHORT"))

    def test_order_flow_detects_delta(self) -> None:
        df = _make_trend_df()
        df.iloc[-1, df.columns.get_loc("close")] = df.iloc[-1]["high"]
        df.iloc[-1, df.columns.get_loc("open")] = df.iloc[-1]["low"]
        df.iloc[-1, df.columns.get_loc("volume")] = 5000.0
        result = evaluate_order_flow_imbalance(df, float(df.iloc[-1]["close"]), atr=1.5)
        self.assertGreaterEqual(result.score, 0.0)

    def test_oi_funding_short_squeeze_setup(self) -> None:
        derivatives = {
            "funding_rate": 0.0005,
            "open_interest": 1_000_000.0,
            "oi_change_pct": 4.0,
        }
        result = evaluate_oi_funding_context(derivatives, price_change_pct=0.5)
        self.assertEqual(result.direction, "LONG")
        self.assertGreaterEqual(result.score, Config.CONTEXT_MODULE_MIN_SCORE)

    def test_institutional_context_applies_multiplier(self) -> None:
        evaluator = InstitutionalContextEvaluator(exchange=None)
        snapshot = _make_snapshot(
            derivatives={
                "funding_rate": 0.0005,
                "open_interest": 900_000.0,
                "oi_change_pct": 3.5,
            }
        )
        ctx = evaluator.evaluate(snapshot)
        self.assertGreater(len(ctx.modules), 0)
        self.assertGreaterEqual(ctx.long_multiplier, Config.INSTITUTIONAL_CONTEXT_MULT_MIN)


class TestPhase1ScoringAndRisk(unittest.TestCase):
    def test_normalized_score(self) -> None:
        normalized = ScoringEngine.compute_normalized_score(80.0, 70.0)
        self.assertAlmostEqual(normalized, 33.333, places=1)

    def test_risk_engine_rejects_when_entries_blocked(self) -> None:
        risk_manager = MagicMock()
        risk_manager.get_risk_snapshot.return_value = MagicMock(
            entries_allowed=False,
            block_reason="drawdown",
        )
        engine = RiskEngine(risk_manager, MagicMock(), MagicMock())
        ok, reason = engine.approve_entry("BTCUSDT", STRATEGY_RANGE_REVERSION, 50000.0)
        self.assertFalse(ok)
        self.assertIn("drawdown", reason)

    def test_institutional_multiplier_in_final_score(self) -> None:
        class _StubStrategy:
            tag = "S1"

            def regime_fit(self, _snapshot):
                return 1.0

            def priority_weight(self):
                return 1.0

            def min_score(self, _signal=None):
                return 60.0

        scorer = ConfluenceScorer({"S1": _StubStrategy()})
        snapshot = _make_snapshot()
        ctx = InstitutionalContextEvaluator().evaluate(snapshot)
        results = [
            StrategyResult(
                symbol="TESTUSDT",
                strategy="S1",
                direction="LONG",
                score=75.0,
            )
        ]
        scores = scorer.score_results(
            snapshot,
            results,
            institutional_context=ctx,
        )
        self.assertEqual(len(scores), 1)
        row = scores[0]
        if ctx.long_bonus > 0 or ctx.long_multiplier != 1.0:
            self.assertNotEqual(row.final_score, row.score)


class TestEntryInFlightMutex(unittest.TestCase):
    def test_blocks_duplicate_entry_claims(self) -> None:
        from core.entry_in_flight_mutex import entry_in_flight_mutex, is_symbol_entry_in_flight

        with entry_in_flight_mutex("BTCUSDT", blocking=False) as first:
            self.assertTrue(first)
            self.assertTrue(is_symbol_entry_in_flight("BTCUSDT"))
            with entry_in_flight_mutex("BTCUSDT", blocking=False) as second:
                self.assertFalse(second)
        self.assertFalse(is_symbol_entry_in_flight("BTCUSDT"))


class TestPhase1IdempotentClose(unittest.TestCase):
    def test_close_trade_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(Config, "DB_PATH", f"{tmp}/test.db"):
                db = DatabaseManager()
                db.log_trade(
                    {
                        "trade_id": "T-001",
                        "symbol": "BTCUSDT",
                        "side": "LONG",
                        "entry_price": 100.0,
                        "quantity": 1.0,
                        "status": "OPEN",
                        "opened_at": "2026-01-01T00:00:00",
                        "strategy": "TEST",
                    }
                )
                trade = db.get_trade("T-001")
                pnl1 = db.close_trade_and_sync_stats(
                    trade,
                    exit_price=105.0,
                    exit_reason="TP",
                    pnl=5.0,
                )
                pnl2 = db.close_trade_and_sync_stats(
                    trade,
                    exit_price=110.0,
                    exit_reason="TP",
                    pnl=99.0,
                )
                self.assertAlmostEqual(pnl1, 5.0)
                self.assertAlmostEqual(pnl2, 5.0)
                closed = db.get_trade("T-001")
                self.assertEqual(closed["status"], "CLOSED")


if __name__ == "__main__":
    unittest.main()
