"""Dynamic scan universe construction from WS caches."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import ta

from config import Config
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import error_logger, scanner_logger
from utils import safe_float


@dataclass
class UniverseFilterStats:
    total_tickers: int = 0
    rejected_quote: int = 0
    rejected_volume: int = 0
    rejected_spread: int = 0
    rejected_stagnant: int = 0
    rejected_atr: int = 0
    rejected_blacklist: int = 0
    rejected_no_price: int = 0
    candidates: list[tuple[str, float, float, float]] = field(default_factory=list)


@dataclass
class UniverseResult:
    symbols: list[str]
    price_map: dict[str, float]
    volume_ranks: dict[str, int]
    top_volume_symbols: list[str]
    stats: UniverseFilterStats


class UniverseBuilder:
    """Build dynamic scan universe (50–80 pairs) from WS ticker/book cache only."""

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
        entry_tf: Optional[str] = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.entry_tf = entry_tf or Config.ENTRY_TIMEFRAME
        self._hub = getattr(exchange, "_market_data", None)

    @staticmethod
    def _is_usdt_perpetual(symbol: str) -> bool:
        return symbol.endswith(Config.QUOTE_ASSET) and "_" not in symbol

    @staticmethod
    def _compute_spread_pct(book: dict[str, Any]) -> Optional[float]:
        if book.get("is_proxy"):
            return None
        bid = safe_float(book.get("bidPrice"))
        ask = safe_float(book.get("askPrice"))
        if bid <= 0 or ask <= 0 or ask < bid:
            return None
        mid = (bid + ask) / 2.0
        if mid <= 0:
            return None
        return ((ask - bid) / mid) * 100.0

    @staticmethod
    def _compute_24h_range_pct(ticker: dict[str, Any], last_price: float) -> float:
        high = safe_float(ticker.get("highPrice"))
        low = safe_float(ticker.get("lowPrice"))
        if last_price <= 0 or high <= 0 or low <= 0 or high < low:
            return 0.0
        return ((high - low) / last_price) * 100.0

    def _passes_atr_volatility_filter(self, symbol: str, last_price: float) -> bool:
        if not Config.ENABLE_UNIVERSE_ATR_FILTER or last_price <= 0 or not self._hub:
            return True

        limit = max(Config.UNIVERSE_ATR_LOOKBACK_BARS + 14, 30)
        df = self._hub.get_candles_cached_only(symbol, self.entry_tf, limit)
        if df.empty or len(df) < Config.UNIVERSE_ATR_LOOKBACK_BARS + 5:
            return True

        window = df.tail(Config.UNIVERSE_ATR_LOOKBACK_BARS + 14)
        atr_series = ta.volatility.average_true_range(
            high=window["high"],
            low=window["low"],
            close=window["close"],
            window=14,
        )
        atr = safe_float(atr_series.iloc[-1])
        if atr <= 0:
            return True
        return (atr / last_price) * 100.0 >= Config.MIN_UNIVERSE_ATR_PCT

    def _build_candidates(
        self, ticker_map: dict[str, dict[str, Any]], book_map: dict[str, dict[str, Any]]
    ) -> UniverseFilterStats:
        stats = UniverseFilterStats(total_tickers=len(ticker_map))
        self.db.cleanup_expired_blacklist()

        for symbol, ticker in ticker_map.items():
            if not self._is_usdt_perpetual(symbol):
                stats.rejected_quote += 1
                continue

            volume_24h = safe_float(ticker.get("quoteVolume"))
            if volume_24h < Config.MIN_24H_VOLUME_USDT:
                stats.rejected_volume += 1
                continue

            last_price = safe_float(ticker.get("lastPrice"))
            book = book_map.get(symbol, {})
            spread_pct = self._compute_spread_pct(book)

            if spread_pct is not None and spread_pct > Config.MAX_SPREAD_PERCENT:
                stats.rejected_spread += 1
                continue

            if last_price <= 0:
                bid = safe_float(book.get("bidPrice"))
                ask = safe_float(book.get("askPrice"))
                if bid > 0 and ask > 0 and not book.get("is_proxy"):
                    last_price = (bid + ask) / 2.0
            if last_price <= 0:
                stats.rejected_no_price += 1
                continue

            range_pct = self._compute_24h_range_pct(ticker, last_price)
            if range_pct < Config.MIN_24H_RANGE_PCT:
                stats.rejected_stagnant += 1
                continue

            if not self._passes_atr_volatility_filter(symbol, last_price):
                stats.rejected_atr += 1
                continue

            if self.db.is_blacklisted(symbol):
                stats.rejected_blacklist += 1
                continue

            stats.candidates.append((symbol, volume_24h, spread_pct or 0.0, range_pct))

        stats.candidates.sort(key=lambda row: row[1], reverse=True)
        return stats

    def build(
        self,
        priority_symbols: Optional[list[str]] = None,
    ) -> UniverseResult:
        try:
            ticker_map = self.exchange.get_futures_ticker_map()
            if not ticker_map:
                if self._hub and self._hub.is_ws_warming_up():
                    scanner_logger.debug(
                        "Universe build skipped — WS ticker cache warming up."
                    )
                else:
                    scanner_logger.warning(
                        "Universe empty — WS ticker cache not ready."
                    )
                return UniverseResult([], {}, {}, [], UniverseFilterStats())

            book_map = self.exchange.get_book_ticker_map()
            stats = self._build_candidates(ticker_map, book_map)
            selected = stats.candidates[: Config.MAX_SCAN_UNIVERSE]
            symbols = self._prioritize([row[0] for row in selected], priority_symbols)

            price_map: dict[str, float] = {}
            volume_ranks: dict[str, int] = {}
            for rank, (symbol, volume, _, _) in enumerate(stats.candidates, start=1):
                volume_ranks[symbol] = rank
                if symbol in symbols:
                    ticker = ticker_map.get(symbol, {})
                    price = safe_float(ticker.get("lastPrice"))
                    if price > 0:
                        price_map[symbol] = price

            top_n = Config.VP_BREAKOUT_TOP_VOLUME_LIMIT
            top_volume = [row[0] for row in stats.candidates[:top_n]]
            self._log_selection(symbols, stats)
            return UniverseResult(
                symbols=symbols,
                price_map=price_map,
                volume_ranks=volume_ranks,
                top_volume_symbols=top_volume,
                stats=stats,
            )
        except Exception as exc:
            error_logger.error("Failed to build tradable universe: %s", exc)
            return UniverseResult([], {}, {}, [], UniverseFilterStats())

    @staticmethod
    def _prioritize(
        symbols: list[str], priority_symbols: Optional[list[str]]
    ) -> list[str]:
        if not priority_symbols:
            return symbols
        priority_set = set(priority_symbols)
        front = [s for s in priority_symbols if s in symbols]
        rest = [s for s in symbols if s not in priority_set]
        return front + rest

    @staticmethod
    def _log_selection(symbols: list[str], stats: UniverseFilterStats) -> None:
        preview_n = max(Config.UNIVERSE_LOG_SYMBOL_PREVIEW, 0)
        preview = ", ".join(symbols[:preview_n]) if symbols else "none"
        extra = ""
        if len(symbols) > preview_n:
            extra = f" (+{len(symbols) - preview_n} more)"

        scanner_logger.info(
            "Universe created: %s active high-volume pairs selected for scanning "
            "(target %s-%s | tickers=%s | rejected: volume=%s spread=%s stagnant=%s "
            "atr=%s blacklist=%s no_price=%s).",
            len(symbols),
            Config.MIN_SCAN_UNIVERSE,
            Config.MAX_SCAN_UNIVERSE,
            stats.total_tickers,
            stats.rejected_volume,
            stats.rejected_spread,
            stats.rejected_stagnant,
            stats.rejected_atr,
            stats.rejected_blacklist,
            stats.rejected_no_price,
        )
        scanner_logger.info("Universe pairs: %s%s", preview, extra)

        if len(symbols) < Config.MIN_SCAN_UNIVERSE:
            scanner_logger.warning(
                "Universe below target minimum (%s/%s) — loosen filters or wait for WS book cache.",
                len(symbols),
                Config.MIN_SCAN_UNIVERSE,
            )
