"""Fast 15m walk-forward backtest for Stoch_MTM / Trend Meter / VWAP rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import ta

from config import Config
from core.candle_prep import drop_forming_bar
from utils import safe_float


@dataclass
class BacktestResult:
    passed: bool = False
    win_rate: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    expectancy_r: float = 0.0
    profit_r: float = 0.0
    last_atr: float = 0.0
    reason: str = ""
    bars_used: int = 0


def apply_validation_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Stoch_MTM, Trend Meter, VWAP, and ATR on closed 15m bars."""
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"].replace(0, 0.0)

    out["ema_20"] = ta.trend.ema_indicator(close, window=20)
    out["ema_50"] = ta.trend.ema_indicator(close, window=50)
    out["ema_200"] = ta.trend.ema_indicator(close, window=200)
    out["atr"] = ta.volatility.average_true_range(high, low, close, window=14)
    out["adx"] = ta.trend.adx(high, low, close, window=14)
    out["stoch_k"] = ta.momentum.stoch(high, low, close, window=14, smooth_window=3)
    out["stoch_d"] = ta.momentum.stoch_signal(high, low, close, window=14, smooth_window=3)
    out["stoch_mtm"] = out["stoch_k"] - out["stoch_d"]

    typical = (high + low + close) / 3.0
    cum_vol = volume.cumsum()
    out["vwap"] = (typical * volume).cumsum() / cum_vol.replace(0, pd.NA)

    out["trend_meter"] = 0
    bull = (close > out["ema_200"]) & (out["ema_50"] > out["ema_200"])
    bear = (close < out["ema_200"]) & (out["ema_50"] < out["ema_200"])
    out.loc[bull, "trend_meter"] = 1
    out.loc[bear, "trend_meter"] = -1
    return out


def signal_at(df: pd.DataFrame, index: int) -> Optional[str]:
    """LONG/SHORT when Stoch_MTM, Trend Meter, and VWAP agree."""
    if index < 1 or index >= len(df):
        return None
    row = df.iloc[index]
    prev = df.iloc[index - 1]
    k = safe_float(row.get("stoch_k"))
    d = safe_float(row.get("stoch_d"))
    k_prev = safe_float(prev.get("stoch_k"))
    d_prev = safe_float(prev.get("stoch_d"))
    trend = int(safe_float(row.get("trend_meter")))
    close = safe_float(row.get("close"))
    vwap = safe_float(row.get("vwap"))
    adx = safe_float(row.get("adx"))
    if any(value != value for value in (k, d, close, vwap, adx)):
        return None
    if min(k, d, close, vwap) <= 0 or adx < 18:
        return None

    cross_up = k_prev <= d_prev and k > d
    cross_down = k_prev >= d_prev and k < d
    if trend > 0 and cross_up and k < 40 and close >= vwap * 0.997:
        return "LONG"
    if trend < 0 and cross_down and k > 60 and close <= vwap * 1.003:
        return "SHORT"
    return None


def _simulate_trade(
    df: pd.DataFrame,
    start: int,
    action: str,
    sl_mult: float,
    tp_mult: float,
) -> tuple[Optional[float], int]:
    """Return (R-multiple, last_index). None R means trade still open."""
    if start + 1 >= len(df):
        return None, start
    entry = safe_float(df.iloc[start + 1].get("open")) or safe_float(
        df.iloc[start].get("close")
    )
    atr = safe_float(df.iloc[start].get("atr"))
    if entry <= 0 or atr <= 0:
        return None, start + 1
    risk = max(atr * sl_mult, entry * 0.001)
    if action == "LONG":
        sl = entry - risk
        tp = entry + risk * (tp_mult / sl_mult if sl_mult else tp_mult)
    else:
        sl = entry + risk
        tp = entry - risk * (tp_mult / sl_mult if sl_mult else tp_mult)

    for idx in range(start + 1, len(df)):
        high = safe_float(df.iloc[idx].get("high"))
        low = safe_float(df.iloc[idx].get("low"))
        if action == "LONG":
            if low <= sl:
                return -1.0, idx
            if high >= tp:
                return tp_mult / sl_mult if sl_mult else 1.0, idx
        else:
            if high >= sl:
                return -1.0, idx
            if low <= tp:
                return tp_mult / sl_mult if sl_mult else 1.0, idx
    return None, len(df) - 1


def backtest_min_bars() -> int:
    """Accept 15m history with a 450-bar floor (fetch still requests 500)."""
    limit = max(int(getattr(Config, "BACKTEST_CANDLE_LIMIT", 500)), 200)
    floor = int(getattr(Config, "BACKTEST_MIN_BARS", 450))
    return max(min(floor, limit), 200)


def required_backtest_win_rate(trades: int) -> Optional[float]:
    """Min win-rate for a closed-trade sample, or None if the sample is too thin."""
    closed = int(trades)
    if closed >= 5:
        return float(Config.BACKTEST_MIN_WIN_RATE)
    if closed == 4:
        return 75.0
    if closed == 3:
        return 100.0
    return None


def run_15m_backtest(df: Optional[pd.DataFrame]) -> BacktestResult:
    """Walk-forward 15m simulation. Fail-closed on thin or losing history."""
    result = BacktestResult()
    min_bars = backtest_min_bars()
    warmup = max(int(Config.BACKTEST_WARMUP_BARS), 150)
    sl_mult = max(float(Config.SL_ATR_MULTIPLIER), 0.25)
    tp_mult = max(float(Config.TP1_ATR_MULTIPLIER), sl_mult)

    closed = drop_forming_bar(df)
    if closed is None or closed.empty or len(closed) < min_bars:
        result.reason = f"insufficient_15m_bars need={min_bars} have={0 if closed is None else len(closed)}"
        return result

    enriched = apply_validation_indicators(closed)
    result.bars_used = len(enriched)
    result.last_atr = safe_float(enriched.iloc[-1].get("atr")) if not enriched.empty else 0.0

    rs: list[float] = []
    triggers = 0
    idx = warmup
    while idx < len(enriched) - 1:
        action = signal_at(enriched, idx)
        if action is None:
            idx += 1
            continue
        triggers += 1
        r_mult, end_idx = _simulate_trade(enriched, idx, action, sl_mult, tp_mult)
        if r_mult is None:
            idx = max(end_idx + 1, idx + 1)
            continue
        rs.append(r_mult)
        idx = max(end_idx + 1, idx + 1)

    result.trades = len(rs)
    result.wins = sum(1 for r in rs if r > 0)
    result.losses = sum(1 for r in rs if r <= 0)
    result.profit_r = float(sum(rs))
    if result.trades:
        result.win_rate = 100.0 * result.wins / result.trades
        result.expectancy_r = result.profit_r / result.trades

    min_wr = required_backtest_win_rate(result.trades)
    if min_wr is None:
        result.reason = "Insufficient historical trade samples"
        return result
    if result.win_rate < min_wr:
        result.reason = f"win_rate {result.win_rate:.1f}% < {min_wr:.1f}%"
        return result
    if result.profit_r <= 0:
        result.reason = f"unprofitable expectancy_r={result.expectancy_r:.3f}"
        return result

    result.passed = True
    result.reason = "passed"
    return result
