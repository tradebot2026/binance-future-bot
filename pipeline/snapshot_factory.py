"""Build per-symbol MarketSnapshot objects from WS cache."""

from __future__ import annotations

import time
from typing import Any, Optional

import pandas as pd

from config import Config
from core.candle_prep import indicator_min_bars
from core.regime_router import RegimeRouter
from core.types import MarketSnapshot, RegimeLabel
from exchange import BinanceExchangeManager
from indicators.market_analyzer import MIN_ANALYZER_BARS
from utils import safe_float


def _required_bars_for_timeframe(timeframe: str) -> int:
    tf = str(timeframe or "").strip().lower()
    if tf in {"5m", "15m"}:
        # +1 so prepare_df still has INDICATOR_MIN_BARS after dropping the forming bar.
        return max(indicator_min_bars(), 150) + 1
    return MIN_ANALYZER_BARS


def _interval_seconds(timeframe: str) -> int:
    raw = str(timeframe or "5m").strip().lower()
    if len(raw) < 2:
        return 300
    unit = raw[-1]
    try:
        qty = int(raw[:-1] or "1")
    except ValueError:
        return 300
    qty = max(qty, 1)
    if unit == "m":
        return qty * 60
    if unit == "h":
        return qty * 3600
    if unit == "d":
        return qty * 86400
    return 300


def _timestamp_epoch(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        return float(pd.Timestamp(value).timestamp())
    except Exception:
        return None


def _normalize_cached_klines(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Sort, de-duplicate, and reject stale cache frames. Never calls REST."""
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame()
    out = df
    if "timestamp" in getattr(out, "columns", []):
        try:
            out = out.sort_values("timestamp")
            out = out.drop_duplicates(subset=["timestamp"], keep="last")
            out = out.reset_index(drop=True)
        except Exception:
            return pd.DataFrame()
        last_epoch = _timestamp_epoch(out["timestamp"].iloc[-1])
        if last_epoch is not None:
            interval = _interval_seconds(timeframe)
            if (time.time() - last_epoch) > (interval * 3 + 15.0):
                return pd.DataFrame()
    if len(out) < _required_bars_for_timeframe(timeframe):
        return pd.DataFrame()
    return out


class SnapshotFactory:
    """Assemble read-only snapshots for strategy evaluation."""

    def __init__(self, exchange: BinanceExchangeManager) -> None:
        self.exchange = exchange
        self._hub = getattr(exchange, "_market_data", None)
        self.candle_limit = Config.CANDLE_FETCH_LIMIT

    def build(
        self,
        symbol: str,
        *,
        price: float,
        ticker: dict[str, Any],
        book: dict[str, Any],
        volume_24h: float,
        volume_rank: int = 0,
        timeframes: Optional[tuple[str, ...]] = None,
        regime: Optional[RegimeLabel] = None,
    ) -> Optional[MarketSnapshot]:
        tfs = timeframes or (
            Config.ENTRY_TIMEFRAME,
            Config.CONFIRM_TIMEFRAME,
            Config.TREND_TIMEFRAME,
        )
        candles: dict[str, pd.DataFrame] = {}
        for tf in tfs:
            df = self._fetch_candles(symbol, tf)
            if df.empty:
                return None
            candles[tf] = df

        if price <= 0:
            entry_df = candles.get(Config.ENTRY_TIMEFRAME)
            if entry_df is not None and not entry_df.empty:
                price = safe_float(entry_df.iloc[-1].get("close"))

        spread_pct = self._spread_from_book(book)
        top_limit = Config.VP_BREAKOUT_TOP_VOLUME_LIMIT
        snapshot = MarketSnapshot(
            symbol=symbol.upper(),
            price=price,
            ticker=ticker,
            book=book,
            candles=candles,
            spread_pct=spread_pct,
            volume_24h=volume_24h,
            regime=regime or RegimeLabel.UNCLEAR,
            timestamp_ms=int(time.time() * 1000),
            volume_rank=volume_rank,
            is_top_volume=volume_rank > 0 and volume_rank <= top_limit,
        )
        if regime is None:
            snapshot = MarketSnapshot(
                symbol=snapshot.symbol,
                price=snapshot.price,
                ticker=snapshot.ticker,
                book=snapshot.book,
                candles=snapshot.candles,
                spread_pct=snapshot.spread_pct,
                volume_24h=snapshot.volume_24h,
                regime=RegimeRouter.classify(snapshot),
                timestamp_ms=snapshot.timestamp_ms,
                volume_rank=snapshot.volume_rank,
                is_top_volume=snapshot.is_top_volume,
            )
        return snapshot

    def has_complete_klines(
        self,
        symbol: str,
        timeframes: Optional[tuple[str, ...]] = None,
    ) -> bool:
        """True when every scan timeframe already has enough WS/cache bars."""
        return bool(self.kline_history_status(symbol, timeframes=timeframes)["ready"])

    def kline_history_status(
        self,
        symbol: str,
        timeframes: Optional[tuple[str, ...]] = None,
    ) -> dict[str, Any]:
        """Per-TF cache depth vs required bars. Never issues REST."""
        tfs = timeframes or (
            Config.ENTRY_TIMEFRAME,
            Config.CONFIRM_TIMEFRAME,
            Config.TREND_TIMEFRAME,
        )
        details: list[dict[str, Any]] = []
        problems: list[str] = []
        ready = True
        for tf in tfs:
            raw = self._raw_cached_candles(symbol, tf)
            have = 0 if raw is None or getattr(raw, "empty", True) else int(len(raw))
            need = _required_bars_for_timeframe(tf)
            ordered = True
            stale = False
            if have > 0 and "timestamp" in getattr(raw, "columns", []):
                try:
                    ordered = bool(raw["timestamp"].is_monotonic_increasing)
                except Exception:
                    ordered = True
                last_epoch = _timestamp_epoch(raw["timestamp"].iloc[-1])
                if last_epoch is not None:
                    interval = _interval_seconds(tf)
                    stale = (time.time() - last_epoch) > (interval * 3 + 15.0)
            ok = have >= need and ordered and not stale
            if not ok:
                ready = False
                bits = [f"have={have} need={need}"]
                if stale:
                    bits.append("stale")
                if not ordered:
                    bits.append("unordered")
                problems.append(f"{tf} {' '.join(bits)}")
            details.append(
                {
                    "timeframe": tf,
                    "have": have,
                    "need": need,
                    "ordered": ordered,
                    "stale": stale,
                    "ready": ok,
                }
            )
        reason = (
            "Insufficient kline history"
            if not problems
            else "Insufficient kline history " + "; ".join(problems)
        )
        return {"ready": ready, "reason": reason, "details": details}

    def _raw_cached_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        try:
            df = self.exchange.fetch_historical_candles(
                symbol, timeframe, limit=self.candle_limit, allow_rest=False
            )
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def _fetch_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        return _normalize_cached_klines(
            self._raw_cached_candles(symbol, timeframe), timeframe
        )

    @staticmethod
    def _spread_from_book(book: dict[str, Any]) -> float:
        if book.get("is_proxy"):
            return 0.0
        bid = safe_float(book.get("bidPrice"))
        ask = safe_float(book.get("askPrice"))
        if bid <= 0 or ask <= 0 or ask < bid:
            return 0.0
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return 0.0
        return ((ask - bid) / mid) * 100.0
