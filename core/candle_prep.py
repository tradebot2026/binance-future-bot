"""Shared candle dataframe preparation — avoids strategy/context import cycles."""

from __future__ import annotations

from typing import Optional

import pandas as pd
import ta

from core.types import MarketSnapshot
from indicators.market_analyzer import MIN_ANALYZER_BARS, MarketAnalyzer
from utils import safe_float


def drop_forming_bar(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Drop the in-progress last candle so setups only see closed bars."""
    if df is None or df.empty:
        return df
    if len(df) < 2:
        return df
    return df.iloc[:-1].copy()


def prepare_df(df: Optional[pd.DataFrame], analyzer: MarketAnalyzer) -> Optional[pd.DataFrame]:
    """Apply full or light indicators depending on bar count."""
    df = drop_forming_bar(df)
    if df is None or df.empty or len(df) < 40:
        return None
    if len(df) >= MIN_ANALYZER_BARS:
        enriched = analyzer.apply_all_indicators(df)
        return enriched if not enriched.empty else None
    return _apply_light_indicators(df.copy())


def _apply_light_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_20"] = ta.trend.ema_indicator(df["close"], window=20)
    df["ema_50"] = ta.trend.ema_indicator(df["close"], window=50)
    df["ema_200"] = ta.trend.ema_indicator(df["close"], window=min(200, len(df) - 1))
    df["atr"] = ta.volatility.average_true_range(
        high=df["high"], low=df["low"], close=df["close"], window=14
    )
    df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
    df["rsi"] = ta.momentum.rsi(df["close"], window=14)
    df["vol_sma"] = df["volume"].rolling(window=20).mean()
    df["vol_spike"] = df["volume"] > (df["vol_sma"] * 1.8)

    lookback = min(5, len(df) - 3)
    df["swing_high"] = df["high"] == df["high"].rolling(lookback * 2 + 1, center=True).max()
    df["swing_low"] = df["low"] == df["low"].rolling(lookback * 2 + 1, center=True).min()
    df["last_swing_high"] = df["high"].where(df["swing_high"]).ffill()
    df["last_swing_low"] = df["low"].where(df["swing_low"]).ffill()

    df["trend_bullish"] = (df["close"] > df["ema_200"]) & (df["ema_50"] > df["ema_200"])
    df["trend_bearish"] = (df["close"] < df["ema_200"]) & (df["ema_50"] < df["ema_200"])
    df.dropna(subset=["atr", "ema_20"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def resolve_price(snapshot: MarketSnapshot, df: pd.DataFrame) -> float:
    if snapshot.price > 0:
        return snapshot.price
    return safe_float(df.iloc[-1]["close"])
