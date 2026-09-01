"""Build per-symbol MarketSnapshot objects from WS cache."""

from __future__ import annotations

import time
from typing import Any, Optional

import pandas as pd

from config import Config
from core.regime_router import RegimeRouter
from core.types import MarketSnapshot, RegimeLabel
from exchange import BinanceExchangeManager
from utils import safe_float


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

    def _fetch_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        try:
            return self.exchange.fetch_historical_candles(
                symbol, timeframe, limit=self.candle_limit, allow_rest=False
            )
        except Exception:
            return pd.DataFrame()

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
