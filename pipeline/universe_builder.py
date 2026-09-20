"""Dynamic scan universe construction from WS caches."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import ta

from config import Config
from core.opportunity_tracker import OpportunityTracker, TickerFeatures
from core.types import CoinLifecycle
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import error_logger, scanner_logger
from utils import safe_float


def _clamp_unit(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


@dataclass
class UniverseFilterStats:
    total_tickers: int = 0
    rejected_quote: int = 0
    rejected_volume: int = 0
    rejected_spread: int = 0
    rejected_stagnant: int = 0
    rejected_atr: int = 0
    rejected_blacklist: int = 0
    rejected_mega_cap: int = 0
    rejected_no_price: int = 0
    filter_profile: str = "strict"
    candidates: list[tuple[str, float, float, float, float, float]] = field(
        default_factory=list
    )
    features: list[TickerFeatures] = field(default_factory=list)


@dataclass(frozen=True)
class _FilterProfile:
    label: str
    min_volume: float
    min_range_pct: float
    min_atr_pct: float
    enable_atr: bool
    skip_stagnant_when_range_unknown: bool = True


@dataclass
class UniverseResult:
    symbols: list[str]
    price_map: dict[str, float]
    volume_ranks: dict[str, int]
    top_volume_symbols: list[str]
    extended_symbols: list[str]
    volatility_scores: dict[str, float]
    stats: UniverseFilterStats
    opportunity_scores: dict[str, float] = field(default_factory=dict)
    score_velocity: dict[str, float] = field(default_factory=dict)
    relative_ranks: dict[str, int] = field(default_factory=dict)
    lifecycle: dict[str, str] = field(default_factory=dict)
    hot_symbols: list[str] = field(default_factory=list)
    channel_scores: dict[str, tuple[float, float, float]] = field(default_factory=dict)


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
        self.tracker = OpportunityTracker()
        self._last_primary: set[str] = set()
        self._last_extended: set[str] = set()
        self._blacklist_symbols: set[str] = set()
        self._blacklist_cache_at: float = 0.0

    def _cached_blacklist(self, *, force: bool = False) -> set[str]:
        """Refresh DB blacklist at most every BLACKLIST_CACHE_SECONDS."""
        now = time.monotonic()
        ttl = max(Config.BLACKLIST_CACHE_SECONDS, 1.0)
        if (
            not force
            and self._blacklist_cache_at > 0
            and (now - self._blacklist_cache_at) < ttl
        ):
            return self._blacklist_symbols
        self.db.cleanup_expired_blacklist()
        try:
            symbols = self.db.get_active_blacklist_symbols()
        except AttributeError:
            symbols = set()
        self._blacklist_symbols = {str(sym).upper() for sym in symbols}
        self._blacklist_cache_at = now
        return self._blacklist_symbols

    def _is_symbol_blocked(self, symbol: str, blacklist: Optional[set[str]] = None) -> bool:
        upper = symbol.upper()
        if Config.is_mega_cap_blacklisted(upper):
            return True
        blocked = self._cached_blacklist() if blacklist is None else blacklist
        return upper in blocked

    def _observe_watch_set(self) -> set[str]:
        """Primary + extended pool plus non-dormant tracker records — not the full ticker map."""
        watch = set(self._last_primary)
        watch.update(self._last_extended)
        watch.update(self.tracker.known_symbols())
        return watch

    @staticmethod
    def _is_usdt_perpetual(symbol: str) -> bool:
        return symbol.endswith(Config.QUOTE_ASSET) and "_" not in symbol

    @staticmethod
    def _strict_profile() -> _FilterProfile:
        return _FilterProfile(
            label="strict",
            min_volume=Config.MIN_24H_VOLUME_USDT,
            min_range_pct=Config.MIN_24H_RANGE_PCT,
            min_atr_pct=Config.MIN_UNIVERSE_ATR_PCT,
            enable_atr=Config.ENABLE_UNIVERSE_ATR_FILTER,
        )

    @staticmethod
    def _testnet_profile() -> _FilterProfile:
        return _FilterProfile(
            label="testnet",
            min_volume=Config.TESTNET_MIN_24H_VOLUME_USDT,
            min_range_pct=Config.TESTNET_MIN_24H_RANGE_PCT,
            min_atr_pct=0.0,
            enable_atr=not Config.TESTNET_DISABLE_UNIVERSE_ATR_FILTER,
            skip_stagnant_when_range_unknown=True,
        )

    @staticmethod
    def _relaxed_profile() -> _FilterProfile:
        return _FilterProfile(
            label="relaxed",
            min_volume=Config.RELAXED_MIN_24H_VOLUME_USDT,
            min_range_pct=Config.RELAXED_MIN_24H_RANGE_PCT,
            min_atr_pct=0.0,
            enable_atr=False,
            skip_stagnant_when_range_unknown=True,
        )

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
    def _compute_24h_range_pct(
        ticker: dict[str, Any], last_price: float
    ) -> Optional[float]:
        """
        Return 24h range percent, or None when high/low are unavailable.
        None means skip stagnant rejection (valid price but no range metadata).
        """
        high = safe_float(ticker.get("highPrice"))
        low = safe_float(ticker.get("lowPrice"))
        if last_price <= 0 or high <= 0 or low <= 0 or high < low:
            return None
        return ((high - low) / last_price) * 100.0

    def _resolve_atr_pct(self, symbol: str, last_price: float) -> float:
        if last_price <= 0 or not self._hub:
            return 0.0

        limit = max(Config.UNIVERSE_ATR_LOOKBACK_BARS + 14, 30)
        df = self._hub.get_candles_cached_only(symbol, self.entry_tf, limit)
        if df.empty or len(df) < Config.UNIVERSE_ATR_LOOKBACK_BARS + 5:
            return 0.0

        window = df.tail(Config.UNIVERSE_ATR_LOOKBACK_BARS + 14)
        atr_series = ta.volatility.average_true_range(
            high=window["high"],
            low=window["low"],
            close=window["close"],
            window=14,
        )
        atr = safe_float(atr_series.iloc[-1])
        if atr <= 0:
            return 0.0
        return (atr / last_price) * 100.0

    def _passes_atr_volatility_filter(
        self,
        symbol: str,
        last_price: float,
        profile: _FilterProfile,
        atr_pct: float,
    ) -> bool:
        if not profile.enable_atr or last_price <= 0:
            return True
        if atr_pct <= 0:
            return True
        threshold = profile.min_atr_pct if profile.min_atr_pct > 0 else Config.MIN_UNIVERSE_ATR_PCT
        return atr_pct >= threshold

    @staticmethod
    def _composite_volatility_score(
        volume_24h: float,
        range_pct: float,
        atr_pct: float,
    ) -> float:
        vol_log = math.log10(max(volume_24h, 1.0))
        return (
            Config.UNIVERSE_RANK_VOLUME_WEIGHT * vol_log
            + Config.UNIVERSE_RANK_RANGE_WEIGHT * max(range_pct, 0.0)
            + Config.UNIVERSE_RANK_ATR_WEIGHT * max(atr_pct, 0.0)
        )

    def _resolve_last_price(
        self, symbol: str, ticker: dict[str, Any], book_map: dict[str, dict[str, Any]]
    ) -> float:
        last_price = safe_float(ticker.get("lastPrice"))
        if last_price > 0:
            return last_price
        book = book_map.get(symbol, {})
        bid = safe_float(book.get("bidPrice"))
        ask = safe_float(book.get("askPrice"))
        if bid > 0 and ask > 0 and not book.get("is_proxy"):
            return (bid + ask) / 2.0
        return 0.0

    def _ws_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Ticker cache only — never triggers REST fallback."""
        if self._hub:
            return self._hub.get_ticker_map() or {}
        return {}

    def _ws_book_map(
        self, ticker_map: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        if self._hub and self._hub.has_ws_book_data():
            return self._hub.get_ws_book_ticker_map()
        return self._book_proxy_from_tickers(ticker_map)

    @staticmethod
    def _book_proxy_from_tickers(
        ticker_map: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        proxy: dict[str, dict[str, Any]] = {}
        for symbol, row in ticker_map.items():
            last = safe_float(row.get("lastPrice"))
            high = safe_float(row.get("highPrice"))
            low = safe_float(row.get("lowPrice"))
            if last <= 0:
                continue
            bid = low if low > 0 else last
            ask = high if high > 0 else last
            if bid > ask:
                bid, ask = ask, bid
            proxy[symbol] = {
                "symbol": symbol,
                "bidPrice": bid,
                "askPrice": ask,
                "is_proxy": True,
            }
        return proxy

    def _kline_extras(self, symbol: str, last_price: float) -> dict[str, float]:
        """Optional structure extras from WS kline cache only."""
        if last_price <= 0 or not self._hub:
            return {}
        df = self._hub.get_candles_cached_only(symbol, self.entry_tf, 40)
        if df is None or getattr(df, "empty", True) or len(df) < 12:
            return {}
        highs = df["high"].astype(float)
        lows = df["low"].astype(float)
        closes = df["close"].astype(float)
        opens = df["open"].astype(float)
        volumes = df["volume"].astype(float)
        ranges = (highs - lows).clip(lower=0.0)
        median_range = float(ranges.tail(20).median() or 0.0)
        last_range = float(ranges.iloc[-1] or 0.0)
        bar_range_ratio = 0.0
        if median_range > 0:
            bar_range_ratio = min(last_range / median_range * 50.0, 100.0)

        last_high = float(highs.iloc[-1])
        last_low = float(lows.iloc[-1])
        last_open = float(opens.iloc[-1])
        last_close = float(closes.iloc[-1])
        wick_span = max(last_high - last_low, 1e-12)
        lower_wick = min(last_open, last_close) - last_low
        upper_wick = last_high - max(last_open, last_close)
        wick_rejection = max(lower_wick, upper_wick) / wick_span * 100.0

        vol_median = float(volumes.tail(20).median() or 0.0)
        last_vol = float(volumes.iloc[-1] or 0.0)
        volume_burst = (last_vol / vol_median) if vol_median > 0 else 0.0

        recent = closes.tail(6).tolist()
        ups = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i - 1])
        downs = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i - 1])
        structure_consistency = max(ups, downs) / max(len(recent) - 1, 1) * 100.0

        older_range = float(ranges.iloc[-12:-6].median() or 0.0)
        recent_range = float(ranges.tail(6).median() or 0.0)
        compression = 0.0
        if older_range > 0:
            compression = _clamp_unit(1.0 - (recent_range / older_range), 0.0, 1.0) * 100.0

        return {
            "bar_range_ratio": bar_range_ratio,
            "wick_rejection": wick_rejection,
            "volume_burst": volume_burst,
            "structure_consistency": structure_consistency,
            "compression": compression,
        }

    def _features_for_row(
        self,
        symbol: str,
        ticker: dict[str, Any],
        book: dict[str, Any],
        last_price: float,
        volume_24h: float,
        range_pct: float,
        atr_pct: float,
        spread_pct: float,
    ) -> TickerFeatures:
        open_price = safe_float(ticker.get("openPrice"))
        change_pct = 0.0
        if open_price > 0 and last_price > 0:
            change_pct = (last_price - open_price) / open_price * 100.0
        extras = self._kline_extras(symbol, last_price)
        return TickerFeatures(
            symbol=symbol.upper(),
            last_price=last_price,
            open_price=open_price,
            high_price=safe_float(ticker.get("highPrice")),
            low_price=safe_float(ticker.get("lowPrice")),
            volume_24h=volume_24h,
            spread_pct=spread_pct,
            range_pct=range_pct,
            change_pct=change_pct,
            atr_pct=atr_pct,
            bar_range_ratio=extras.get("bar_range_ratio", 0.0),
            wick_rejection=extras.get("wick_rejection", 0.0),
            volume_burst=extras.get("volume_burst", 0.0),
            structure_consistency=extras.get("structure_consistency", 0.0),
            compression=extras.get("compression", 0.0),
        )

    def _build_candidates(
        self,
        ticker_map: dict[str, dict[str, Any]],
        book_map: dict[str, dict[str, Any]],
        profile: _FilterProfile,
    ) -> UniverseFilterStats:
        stats = UniverseFilterStats(
            total_tickers=len(ticker_map),
            filter_profile=profile.label,
        )
        blacklist = self._cached_blacklist(force=True)

        for symbol, ticker in ticker_map.items():
            if not self._is_usdt_perpetual(symbol):
                stats.rejected_quote += 1
                continue

            volume_24h = safe_float(ticker.get("quoteVolume"))
            if volume_24h < profile.min_volume:
                stats.rejected_volume += 1
                continue

            last_price = self._resolve_last_price(symbol, ticker, book_map)
            if last_price <= 0:
                stats.rejected_no_price += 1
                continue

            book = book_map.get(symbol, {})
            spread_pct = self._compute_spread_pct(book)
            if spread_pct is not None and spread_pct > Config.MAX_SPREAD_PERCENT:
                stats.rejected_spread += 1
                continue

            range_pct = self._compute_24h_range_pct(ticker, last_price)
            if range_pct is not None and range_pct < profile.min_range_pct:
                stats.rejected_stagnant += 1
                continue

            if Config.is_mega_cap_blacklisted(symbol):
                stats.rejected_mega_cap += 1
                continue

            atr_pct = self._resolve_atr_pct(symbol, last_price)
            if not self._passes_atr_volatility_filter(
                symbol, last_price, profile, atr_pct
            ):
                stats.rejected_atr += 1
                continue

            if symbol.upper() in blacklist:
                stats.rejected_blacklist += 1
                continue

            vol_score = self._composite_volatility_score(
                volume_24h, range_pct or 0.0, atr_pct
            )
            spread_value = spread_pct or 0.0
            stats.features.append(
                self._features_for_row(
                    symbol,
                    ticker,
                    book,
                    last_price,
                    volume_24h,
                    range_pct or 0.0,
                    atr_pct,
                    spread_value,
                )
            )
            stats.candidates.append(
                (
                    symbol,
                    volume_24h,
                    spread_value,
                    range_pct or 0.0,
                    atr_pct,
                    vol_score,
                )
            )

        stats.candidates.sort(key=lambda row: row[5], reverse=True)
        return stats

    def _build_volume_fallback(
        self,
        ticker_map: dict[str, dict[str, Any]],
        book_map: dict[str, dict[str, Any]],
    ) -> UniverseFilterStats:
        """Last resort: top-N USDT perpetuals by 24h quote volume with valid price."""
        stats = UniverseFilterStats(
            total_tickers=len(ticker_map),
            filter_profile="volume_fallback",
        )
        blacklist = self._cached_blacklist()
        rows: list[tuple[str, float, float, float, float, float]] = []

        for symbol, ticker in ticker_map.items():
            if not self._is_usdt_perpetual(symbol):
                stats.rejected_quote += 1
                continue
            if Config.is_mega_cap_blacklisted(symbol):
                stats.rejected_mega_cap += 1
                continue
            if symbol.upper() in blacklist:
                stats.rejected_blacklist += 1
                continue

            last_price = self._resolve_last_price(symbol, ticker, book_map)
            if last_price <= 0:
                stats.rejected_no_price += 1
                continue

            volume_24h = safe_float(ticker.get("quoteVolume"))
            range_pct = self._compute_24h_range_pct(ticker, last_price)
            atr_pct = self._resolve_atr_pct(symbol, last_price)
            book = book_map.get(symbol, {})
            spread_pct = self._compute_spread_pct(book) or 0.0
            vol_score = self._composite_volatility_score(
                volume_24h, range_pct or 0.0, atr_pct
            )
            stats.features.append(
                self._features_for_row(
                    symbol,
                    ticker,
                    book,
                    last_price,
                    volume_24h,
                    range_pct or 0.0,
                    atr_pct,
                    spread_pct,
                )
            )
            rows.append(
                (symbol, volume_24h, spread_pct, range_pct or 0.0, atr_pct, vol_score)
            )

        rows.sort(key=lambda row: row[5], reverse=True)
        top_n = max(Config.UNIVERSE_FALLBACK_TOP_N, Config.UNIVERSE_RELAXED_MIN_SYMBOLS)
        stats.candidates = rows[:top_n]
        feature_map = {row.symbol: row for row in stats.features}
        stats.features = [
            feature_map[row[0]] for row in stats.candidates if row[0] in feature_map
        ]
        return stats

    def _select_profile_chain(self) -> list[_FilterProfile]:
        if Config.USE_TESTNET and Config.TESTNET_RELAX_UNIVERSE_FILTERS:
            return [self._testnet_profile()]
        return [self._strict_profile()]

    def _build_with_relaxation(
        self,
        ticker_map: dict[str, dict[str, Any]],
        book_map: dict[str, dict[str, Any]],
    ) -> UniverseFilterStats:
        target = Config.MIN_SCAN_UNIVERSE
        stats = UniverseFilterStats(total_tickers=len(ticker_map))

        for profile in self._select_profile_chain():
            stats = self._build_candidates(ticker_map, book_map, profile)
            if len(stats.candidates) >= target:
                return stats

        if (
            Config.ENABLE_UNIVERSE_FILTER_RELAXATION
            and len(stats.candidates) < target
            and stats.filter_profile != "relaxed"
        ):
            relaxed = self._build_candidates(
                ticker_map, book_map, self._relaxed_profile()
            )
            if len(relaxed.candidates) > len(stats.candidates):
                scanner_logger.info(
                    "Universe filters relaxed (%s -> %s candidates, profile=%s).",
                    len(stats.candidates),
                    len(relaxed.candidates),
                    relaxed.filter_profile,
                )
                stats = relaxed

        min_floor = max(Config.UNIVERSE_RELAXED_MIN_SYMBOLS, 1)
        if len(stats.candidates) < min_floor:
            fallback = self._build_volume_fallback(ticker_map, book_map)
            if len(fallback.candidates) > len(stats.candidates):
                scanner_logger.info(
                    "Universe volume fallback selected top %s symbols by 24h quote volume.",
                    len(fallback.candidates),
                )
                stats = fallback

        return stats

    def build(
        self,
        priority_symbols: Optional[list[str]] = None,
    ) -> UniverseResult:
        try:
            ticker_map = self._ws_ticker_map()
            if not ticker_map:
                if self._hub and not self._hub.is_ticker_cache_usable():
                    scanner_logger.debug(
                        "Universe build skipped — ticker cache unavailable."
                    )
                else:
                    scanner_logger.warning(
                        "Universe empty — no symbols passed filters."
                    )
                return UniverseResult([], {}, {}, [], [], {}, UniverseFilterStats())

            book_map = self._ws_book_map(ticker_map)
            stats = self._build_with_relaxation(ticker_map, book_map)
            self.tracker.ingest(stats.features)
            score_map = self.tracker.opportunity_scores()
            stats.candidates.sort(
                key=lambda row: score_map.get(row[0].upper(), row[5]),
                reverse=True,
            )

            pool_size = max(Config.TOP_UNIVERSE_POOL_SIZE, Config.MAX_SCAN_UNIVERSE)
            extended_size = max(Config.ROTATION_EXTENDED_POOL_SIZE, 0)
            primary_rows = stats.candidates[:pool_size]
            extended_rows = stats.candidates[pool_size : pool_size + extended_size]
            selected = primary_rows[: Config.MAX_SCAN_UNIVERSE]
            symbols = self._prioritize([row[0] for row in selected], priority_symbols)
            extended_symbols = [row[0] for row in extended_rows]
            self.tracker.apply_primary_pool(symbols)
            self._last_primary = {s.upper() for s in symbols}
            self._last_extended = {s.upper() for s in extended_symbols}

            price_map: dict[str, float] = {}
            volume_ranks: dict[str, int] = {}
            volatility_scores: dict[str, float] = {}
            opportunity_scores: dict[str, float] = {}
            channel_scores: dict[str, tuple[float, float, float]] = {}
            for rank, row in enumerate(stats.candidates, start=1):
                symbol = row[0]
                rec = self.tracker.get(symbol)
                volume_ranks[symbol] = rec.relative_rank if rec else rank
                score = rec.score if rec else row[5]
                volatility_scores[symbol] = score
                opportunity_scores[symbol] = score
                if rec is not None:
                    channel_scores[symbol] = (
                        rec.channels.momentum,
                        rec.channels.reversal,
                        rec.channels.structure,
                    )
                if symbol in symbols:
                    ticker = ticker_map.get(symbol, {})
                    price = self._resolve_last_price(symbol, ticker, book_map)
                    if price > 0:
                        price_map[symbol] = price

            top_n = Config.VP_BREAKOUT_TOP_VOLUME_LIMIT
            top_volume = [row[0] for row in stats.candidates[:top_n]]
            hot_symbols = [
                sym
                for sym in self.tracker.hot_symbols()
                if sym in {s.upper() for s in symbols}
                or sym in {s.upper() for s in extended_symbols}
            ]
            self._log_selection(symbols, stats, opportunity_scores, hot_symbols)
            return UniverseResult(
                symbols=symbols,
                price_map=price_map,
                volume_ranks=volume_ranks,
                top_volume_symbols=top_volume,
                extended_symbols=extended_symbols,
                volatility_scores=volatility_scores,
                stats=stats,
                opportunity_scores=opportunity_scores,
                score_velocity=self.tracker.score_velocity(),
                relative_ranks=self.tracker.relative_ranks(),
                lifecycle=self.tracker.lifecycle_map(),
                hot_symbols=hot_symbols,
                channel_scores=channel_scores,
            )
        except Exception as exc:
            error_logger.error("Failed to build tradable universe: %s", exc)
            return UniverseResult([], {}, {}, [], [], {}, UniverseFilterStats())

    def observe_live_tickers(self) -> list[str]:
        """Cheap WS-only spike pass between universe rebuilds. Returns fast-track symbols."""
        ticker_map = self._ws_ticker_map()
        if not ticker_map:
            return []
        watch = self._observe_watch_set()
        if not watch:
            return []
        book_map = self._ws_book_map(ticker_map)
        blacklist = self._cached_blacklist()
        features: list[TickerFeatures] = []
        for raw_symbol in watch:
            symbol = raw_symbol.upper()
            if not self._is_usdt_perpetual(symbol):
                continue
            if self._is_symbol_blocked(symbol, blacklist):
                continue
            ticker = ticker_map.get(symbol) or ticker_map.get(raw_symbol)
            if not ticker:
                continue
            last_price = self._resolve_last_price(symbol, ticker, book_map)
            if last_price <= 0:
                continue
            volume_24h = safe_float(ticker.get("quoteVolume"))
            range_pct = self._compute_24h_range_pct(ticker, last_price) or 0.0
            book = book_map.get(symbol, {}) or book_map.get(raw_symbol, {})
            spread_pct = self._compute_spread_pct(book) or 0.0
            extras: dict[str, float] = {}
            rec = self.tracker.get(symbol)
            if rec is not None and rec.lifecycle in (
                CoinLifecycle.HOT,
                CoinLifecycle.OPPORTUNITY,
                CoinLifecycle.ACTIVE,
                CoinLifecycle.WATCH,
            ):
                extras = self._kline_extras(symbol, last_price)
            open_price = safe_float(ticker.get("openPrice"))
            change_pct = 0.0
            if open_price > 0:
                change_pct = (last_price - open_price) / open_price * 100.0
            features.append(
                TickerFeatures(
                    symbol=symbol,
                    last_price=last_price,
                    open_price=open_price,
                    high_price=safe_float(ticker.get("highPrice")),
                    low_price=safe_float(ticker.get("lowPrice")),
                    volume_24h=volume_24h,
                    spread_pct=spread_pct,
                    range_pct=range_pct,
                    change_pct=change_pct,
                    bar_range_ratio=extras.get("bar_range_ratio", 0.0),
                    wick_rejection=extras.get("wick_rejection", 0.0),
                    volume_burst=extras.get("volume_burst", 0.0),
                    structure_consistency=extras.get("structure_consistency", 0.0),
                    compression=extras.get("compression", 0.0),
                )
            )
        return self.tracker.ingest(features)

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
    def _log_selection(
        symbols: list[str],
        stats: UniverseFilterStats,
        opportunity_scores: Optional[dict[str, float]] = None,
        hot_symbols: Optional[list[str]] = None,
    ) -> None:
        preview_n = max(Config.UNIVERSE_LOG_SYMBOL_PREVIEW, 0)
        preview = ", ".join(symbols[:preview_n]) if symbols else "none"
        extra = ""
        if len(symbols) > preview_n:
            extra = f" (+{len(symbols) - preview_n} more)"

        top_score = 0.0
        if opportunity_scores and symbols:
            top_score = max(
                (opportunity_scores.get(sym, 0.0) for sym in symbols),
                default=0.0,
            )
        scanner_logger.info(
            "Universe created: %s opportunity-ranked pairs selected for scanning "
            "(target %s-%s | profile=%s | tickers=%s | hot=%s | top_score=%.1f | "
            "rejected: volume=%s spread=%s stagnant=%s atr=%s mega_cap=%s "
            "blacklist=%s no_price=%s).",
            len(symbols),
            Config.MIN_SCAN_UNIVERSE,
            Config.MAX_SCAN_UNIVERSE,
            stats.filter_profile,
            stats.total_tickers,
            len(hot_symbols or []),
            top_score,
            stats.rejected_volume,
            stats.rejected_spread,
            stats.rejected_stagnant,
            stats.rejected_atr,
            stats.rejected_mega_cap,
            stats.rejected_blacklist,
            stats.rejected_no_price,
        )
        scanner_logger.info("Universe pairs: %s%s", preview, extra)

        if len(symbols) < Config.MIN_SCAN_UNIVERSE:
            scanner_logger.warning(
                "Universe below target minimum (%s/%s) — profile=%s.",
                len(symbols),
                Config.MIN_SCAN_UNIVERSE,
                stats.filter_profile,
            )
