"""Testnet-relaxed strategy floors and VWAP distance fallback."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from config import Config
from engines.vwap_engine import evaluate_vwap_pullback
from core.scoring_engine import ScoringEngine
from constants import STRATEGY_VWAP_PULLBACK


def _ohlcv(close: float, *, bullish: bool = True) -> pd.DataFrame:
    rows = []
    for i in range(8):
        rows.append(
            {
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 100.0 + i,
                "rsi": 48.0,
                "vol_spike": False,
                "trend_bullish": bullish,
                "trend_bearish": not bullish,
            }
        )
    return pd.DataFrame(rows)


class TestTestnetStrategyRelax(unittest.TestCase):
    def test_effective_min_score_drops_on_testnet(self) -> None:
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
        ), patch.object(Config, "TESTNET_MIN_SCORE_RELAX", 10.0), patch.object(
            Config, "VWAP_MIN_SCORE", 70.0
        ):
            self.assertEqual(Config.effective_min_score(70.0), 60.0)
            self.assertEqual(
                ScoringEngine.strategy_min_score(STRATEGY_VWAP_PULLBACK), 60.0
            )

    def test_effective_min_score_unchanged_on_mainnet(self) -> None:
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
        ):
            self.assertEqual(Config.effective_min_score(70.0), 70.0)

    def test_vwap_allows_wider_distance_on_testnet(self) -> None:
        df = _ohlcv(100.0, bullish=True)
        with patch.object(Config, "USE_TESTNET", True), patch.object(
            Config, "TESTNET_RELAX_STRATEGY_THRESHOLDS", True
        ), patch.object(Config, "VWAP_MAX_DISTANCE_ATR_TESTNET", 1.25), patch.object(
            Config, "VWAP_MIN_SCORE", 70.0
        ), patch.object(Config, "TESTNET_MIN_SCORE_RELAX", 10.0):
            result = evaluate_vwap_pullback("LONG", df, df, 100.8, 1.0)
        self.assertFalse(any("too_far_from_vwap" in r for r in result.reasons))
        self.assertGreaterEqual(result.score, 60.0)
        self.assertTrue(result.passed)

    def test_vwap_rejects_wide_distance_on_mainnet(self) -> None:
        df = _ohlcv(100.0, bullish=True)
        with patch.object(Config, "USE_TESTNET", False), patch.object(
            Config, "VWAP_MAX_DISTANCE_ATR", 0.6
        ), patch.object(Config, "VWAP_MIN_SCORE", 70.0):
            result = evaluate_vwap_pullback("LONG", df, df, 100.8, 1.0)
        self.assertIn("too_far_from_vwap", result.reasons)
        self.assertFalse(result.passed)


if __name__ == "__main__":
    unittest.main()
