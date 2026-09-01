"""Regime classification and strategy routing."""

from __future__ import annotations

from typing import TYPE_CHECKING

from config import Config
from core.types import MarketSnapshot, RegimeLabel
from utils import safe_float

if TYPE_CHECKING:
    import pandas as pd


class RegimeRouter:
    """Classify market regime from WS-cached candles (no REST)."""

    STRONG_TREND_ADX = 25.0
    RANGE_MAX_ADX = 22.0
    SPIKE_ATR_PERCENTILE = 80.0

    @classmethod
    def classify(cls, snapshot: MarketSnapshot) -> RegimeLabel:
        trend_tf = Config.TREND_TIMEFRAME
        confirm_tf = Config.CONFIRM_TIMEFRAME
        entry_tf = Config.ENTRY_TIMEFRAME

        df_trend = snapshot.candles.get(trend_tf)
        df_confirm = snapshot.candles.get(confirm_tf)
        df_entry = snapshot.candles.get(entry_tf)

        if cls._is_expansion_spike(df_entry):
            return RegimeLabel.EXPANSION_SPIKE

        adx_1h = cls._latest_adx(df_trend)
        if adx_1h >= cls.STRONG_TREND_ADX and cls._trend_aligned(df_trend):
            return RegimeLabel.STRONG_TREND

        if adx_1h > 0 and adx_1h <= Config.RANGE_REGIME_MAX_ADX_1H:
            if cls._has_valid_range(df_trend):
                return RegimeLabel.RANGE_CHOP

        if cls._is_compression(df_confirm or df_entry):
            return RegimeLabel.COMPRESSION

        adx_15m = cls._latest_adx(df_confirm)
        if adx_15m >= cls.STRONG_TREND_ADX:
            return RegimeLabel.STRONG_TREND

        return RegimeLabel.UNCLEAR

    @staticmethod
    def _latest_adx(df: "pd.DataFrame | None") -> float:
        if df is None or df.empty or "adx" not in df.columns:
            return 0.0
        return safe_float(df["adx"].iloc[-1])

    @staticmethod
    def _trend_aligned(df: "pd.DataFrame | None") -> bool:
        if df is None or df.empty:
            return False
        row = df.iloc[-1]
        bullish = bool(row.get("trend_bullish", False))
        bearish = bool(row.get("trend_bearish", False))
        return bullish or bearish

    @staticmethod
    def _has_valid_range(df: "pd.DataFrame | None") -> bool:
        if df is None or len(df) < 10:
            return False
        lookback = min(Config.RANGE_LOOKBACK_BARS, len(df))
        window = df.iloc[-lookback:]
        high = float(window["high"].max())
        low = float(window["low"].min())
        return high > low > 0

    @staticmethod
    def _is_compression(df: "pd.DataFrame | None") -> bool:
        if df is None or len(df) < 30 or "atr" not in df.columns:
            return False
        atr_series = df["atr"].dropna()
        if len(atr_series) < 20:
            return False
        current = safe_float(atr_series.iloc[-1])
        median = safe_float(atr_series.tail(20).median())
        if median <= 0:
            return False
        return current < median * 0.85

    @staticmethod
    def _is_expansion_spike(df: "pd.DataFrame | None") -> bool:
        if df is None or len(df) < 20:
            return False
        if "atr" not in df.columns or "high" not in df.columns:
            return False
        row = df.iloc[-1]
        atr = safe_float(row.get("atr"))
        bar_range = safe_float(row.get("high")) - safe_float(row.get("low"))
        if atr <= 0:
            return False
        atr_median = safe_float(df["atr"].tail(20).median())
        if atr_median <= 0:
            return False
        return bar_range >= atr * 2.5 and atr >= atr_median * 1.5

    @classmethod
    def strategies_for_regime(cls, regime: RegimeLabel) -> set[str]:
        """Default strategy tags active per regime (Phase 1: SMC + Range only)."""
        mapping: dict[RegimeLabel, set[str]] = {
            RegimeLabel.STRONG_TREND: {"SMC_TREND"},
            RegimeLabel.RANGE_CHOP: {"RANGE_REVERSION"},
            RegimeLabel.COMPRESSION: set(),
            RegimeLabel.EXPANSION_SPIKE: set(),
            RegimeLabel.UNCLEAR: {"SMC_TREND", "RANGE_REVERSION"},
        }
        return mapping.get(regime, set())
