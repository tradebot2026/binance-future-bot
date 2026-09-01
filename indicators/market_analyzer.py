"""Vectorized market indicators shared across strategy modules."""

from __future__ import annotations

import pandas as pd
import ta

MIN_ANALYZER_BARS = 250


class MarketAnalyzer:
    """Vectorized indicators and live-safe SMC constructs (no lookahead)."""

    @staticmethod
    def apply_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or len(df) < MIN_ANALYZER_BARS:
            return df

        df = df.copy()

        df["ema_20"] = ta.trend.ema_indicator(df["close"], window=20)
        df["ema_50"] = ta.trend.ema_indicator(df["close"], window=50)
        df["ema_200"] = ta.trend.ema_indicator(df["close"], window=200)
        df["rsi"] = ta.momentum.rsi(df["close"], window=14)

        macd = ta.trend.MACD(df["close"])
        df["macd"] = macd.macd()
        df["macd_signal"] = macd.macd_signal()
        df["macd_hist"] = macd.macd_diff()

        df["atr"] = ta.volatility.average_true_range(
            high=df["high"],
            low=df["low"],
            close=df["close"],
            window=14,
        )
        df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
        df["vol_sma"] = df["volume"].rolling(window=20).mean()
        df["vol_spike"] = df["volume"] > (df["vol_sma"] * 2.0)

        df["trend_bullish"] = (df["close"] > df["ema_200"]) & (df["ema_50"] > df["ema_200"])
        df["trend_bearish"] = (df["close"] < df["ema_200"]) & (df["ema_50"] < df["ema_200"])

        df["swing_high"] = (
            (df["high"].shift(2) > df["high"].shift(4))
            & (df["high"].shift(2) > df["high"].shift(3))
            & (df["high"].shift(2) > df["high"].shift(1))
            & (df["high"].shift(2) > df["high"])
        )
        df["swing_low"] = (
            (df["low"].shift(2) < df["low"].shift(4))
            & (df["low"].shift(2) < df["low"].shift(3))
            & (df["low"].shift(2) < df["low"].shift(1))
            & (df["low"].shift(2) < df["low"])
        )

        df["last_swing_high"] = df["high"].shift(2).where(df["swing_high"]).ffill()
        df["last_swing_low"] = df["low"].shift(2).where(df["swing_low"]).ffill()

        min_fvg_gap = df["atr"] * 0.15
        df["fvg_bullish_raw"] = (df["low"] > df["high"].shift(2)) & (
            (df["low"] - df["high"].shift(2)) > min_fvg_gap
        )
        df["fvg_bearish_raw"] = (df["high"] < df["low"].shift(2)) & (
            (df["low"].shift(2) - df["high"]) > min_fvg_gap
        )
        df["fvg_bullish"] = df["fvg_bullish_raw"] & (df["low"].shift(1) > df["high"].shift(2))
        df["fvg_bearish"] = df["fvg_bearish_raw"] & (df["high"].shift(1) < df["low"].shift(2))

        df["break_high"] = (df["close"] > df["last_swing_high"].shift(1)) & (
            df["close"].shift(1) <= df["last_swing_high"].shift(1)
        )
        df["break_low"] = (df["close"] < df["last_swing_low"].shift(1)) & (
            df["close"].shift(1) >= df["last_swing_low"].shift(1)
        )

        df["bos_bullish"] = df["break_high"] & df["trend_bullish"]
        df["choch_bullish"] = df["break_high"] & df["trend_bearish"]
        df["bos_bearish"] = df["break_low"] & df["trend_bearish"]
        df["choch_bearish"] = df["break_low"] & df["trend_bullish"]

        df["is_bull_displacement"] = df["close"] > df["open"]
        df["is_bear_displacement"] = df["close"] < df["open"]
        df["liq_sweep_bullish"] = (
            (df["low"] < df["last_swing_low"].shift(1))
            & (df["close"] > df["last_swing_low"].shift(1))
        )
        df["liq_sweep_bearish"] = (
            (df["high"] > df["last_swing_high"].shift(1))
            & (df["close"] < df["last_swing_high"].shift(1))
        )

        df["ob_bullish_formed"] = (df["bos_bullish"] | df["choch_bullish"]) & df[
            "is_bear_displacement"
        ].shift(1)
        df["ob_bearish_formed"] = (df["bos_bearish"] | df["choch_bearish"]) & df[
            "is_bull_displacement"
        ].shift(1)

        df.dropna(subset=["ema_200", "last_swing_high", "last_swing_low"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    @staticmethod
    def get_latest_atr(df: pd.DataFrame) -> float:
        if "atr" in df.columns and not df.empty:
            return float(df["atr"].iloc[-1])
        return 0.0

    @staticmethod
    def extract_latest_signals(df: pd.DataFrame) -> tuple[dict, dict]:
        if len(df) < 2:
            return {}, {}
        return df.iloc[-1].to_dict(), df.iloc[-2].to_dict()
