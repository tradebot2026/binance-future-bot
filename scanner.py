"""
Market scanner module.
Universe filtering, multi-timeframe SMC analysis, confluence gates,
retest-based entry validation, and watchlist generation.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
import ta

from config import Config
from constants import STRATEGY_RANGE_REVERSION, STRATEGY_SMC_TREND
from database import DatabaseManager
from exceptions import ExchangeRateLimitError
from exchange import BinanceExchangeManager
from indicators.market_analyzer import MIN_ANALYZER_BARS, MarketAnalyzer
from logger import error_logger, scanner_logger, signal_logger
from pipeline.scanner_pipeline import StrategyScannerPipeline
from pipeline.universe_builder import UniverseBuilder, UniverseFilterStats
from range_engine import evaluate_range_setup
from smc_engine import (
    effective_smc_min_score,
    evaluate_confluence_gate,
    score_setup,
    validate_retest_entry,
)
from utils import safe_float

# Backward-compatible re-export
__all__ = ["MarketAnalyzer", "MarketScanner", "MIN_ANALYZER_BARS", "UniverseFilterStats"]


class MarketScanner:
    """Scans Binance Futures for SMC confluence + retest entry candidates."""

    def __init__(self, exchange: BinanceExchangeManager, db: DatabaseManager) -> None:
        self.exchange = exchange
        self.db = db
        self.analyzer = MarketAnalyzer()
        self.entry_tf = Config.ENTRY_TIMEFRAME
        self.confirm_tf = Config.CONFIRM_TIMEFRAME
        self.trend_tf = Config.TREND_TIMEFRAME
        self.candle_limit = Config.CANDLE_FETCH_LIMIT
        self._hub = getattr(exchange, "_market_data", None)
        self._scan_priority: list[str] = []
        self._near_miss_scores: dict[str, float] = {}
        self._pipeline = StrategyScannerPipeline(exchange, db)
        self._universe_builder = UniverseBuilder(exchange, db)

    @property
    def _last_universe_symbols(self) -> list[str]:
        return self._pipeline.last_universe_symbols

    @_last_universe_symbols.setter
    def _last_universe_symbols(self, value: list[str]) -> None:
        self._pipeline._last_universe_symbols = value

    def _scan_gate_open(self) -> tuple[bool, str]:
        if self._hub:
            return self._hub.is_scan_halted()
        return False, ""

    @staticmethod
    def _is_usdt_perpetual(symbol: str) -> bool:
        return symbol.endswith(Config.QUOTE_ASSET) and "_" not in symbol

    @staticmethod
    def _compute_spread_pct(book: dict[str, Any]) -> Optional[float]:
        """True bid-ask spread %; None when only a high/low proxy is available."""
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
        """Optional WS-cached ATR filter — no REST."""
        if not Config.ENABLE_UNIVERSE_ATR_FILTER or last_price <= 0:
            return True
        if not self._hub:
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
        atr_pct = (atr / last_price) * 100.0
        return atr_pct >= Config.MIN_UNIVERSE_ATR_PCT

    def _build_universe_candidates(
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

    def _log_universe_selection(
        self, symbols: list[str], stats: UniverseFilterStats
    ) -> None:
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

    def get_tradable_symbols(self) -> tuple[list[str], dict[str, float]]:
        """
        Build dynamic scan universe (50-80 pairs) from WS ticker/book cache only.
        Delegates to UniverseBuilder (backward-compatible wrapper).
        """
        result = self._universe_builder.build(priority_symbols=self._scan_priority)
        self._pipeline._last_universe_symbols = result.symbols
        return result.symbols, result.price_map

    def _fetch_candles_with_retry(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        try:
            df = self.exchange.fetch_historical_candles(
                symbol, timeframe, limit=limit, allow_rest=False
            )
            if not df.empty and len(df) >= MIN_ANALYZER_BARS:
                return df
        except ExchangeRateLimitError:
            return pd.DataFrame()
        except Exception as exc:
            error_logger.warning(
                "WS candle cache miss for %s %s: %s", symbol, timeframe, exc
            )
        return pd.DataFrame()

    def ensure_scan_klines_ready(self, symbols: Optional[list[str]] = None) -> int:
        """
        One-time REST kline seed for scan universe — runs outside scan_context.
        Skips pairs already bootstrapped; safe to call before each scan batch.
        """
        if not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP or not self._hub:
            return 0
        if self.exchange.in_scan_mode:
            return 0
        if symbols is None:
            symbols, _ = self.get_tradable_symbols()
        if not symbols:
            return 0

        timeframes = Config.get_scan_kline_intervals()
        with self.exchange.bootstrap_context():
            return self._hub.subscribe_and_bootstrap_klines(
                symbols,
                timeframes,
                self.exchange.fetch_bootstrap_klines_df,
            )

    def _prepare_scan_universe(self) -> tuple[List[str], dict[str, float]]:
        """Build universe and subscribe WS klines (REST bootstrap via ensure_scan_klines_ready)."""
        symbols, price_map = self.get_tradable_symbols()
        if not symbols:
            return [], {}

        if self._hub:
            self._hub.subscribe_kline_streams(symbols)

        return symbols, price_map

    def bootstrap_next_batch(self, batch_size: int = 9) -> int:
        """REST-seed missing kline buffers between scan cycles (never during evaluation)."""
        if not Config.ENABLE_REST_KLINE_BOOTSTRAP:
            return 0
        if self.exchange.in_scan_mode:
            return 0
        if not self.exchange._rest_reads_allowed():
            return 0

        symbols = self._last_universe_symbols
        if not symbols:
            symbols, _ = self.get_tradable_symbols()
            self._last_universe_symbols = symbols

        timeframes = Config.get_scan_kline_intervals()
        hub = self._hub
        if hub and symbols:
            hub.subscribe_kline_streams(symbols)
        pending: list[tuple[str, str]] = []
        limit = Config.CANDLE_FETCH_LIMIT
        hub = self._hub
        for symbol in self._last_universe_symbols:
            for tf in timeframes:
                if hub:
                    cached = hub.get_candles_cached_only(symbol, tf, limit)
                    if cached.empty or len(cached) < MIN_ANALYZER_BARS:
                        pending.append((symbol, tf))
                if len(pending) >= batch_size:
                    break
            if len(pending) >= batch_size:
                break

        if not pending or not hub:
            return 0

        seeded = 0
        for symbol, tf in pending[:batch_size]:
            df = self.exchange.rest_fetch_klines_df(symbol, tf, limit)
            if not df.empty:
                hub.seed_klines_from_dataframe(symbol, tf, df)
                seeded += 1
        if seeded:
            scanner_logger.info(
                "Inter-cycle REST bootstrap seeded %s/%s kline series.",
                seeded,
                len(pending[:batch_size]),
            )
        return seeded

    def _prioritize_symbols(self, symbols: list[str]) -> list[str]:
        """Rescan near-miss symbols (65-69 prior cycle) before the rest of the universe."""
        if not self._scan_priority:
            return symbols
        priority_set = set(self._scan_priority)
        front = [s for s in self._scan_priority if s in symbols]
        rest = [s for s in symbols if s not in priority_set]
        return front + rest

    def _note_near_miss(self, symbol: str, score: float) -> None:
        if Config.NEAR_MISS_SCORE_MIN <= score <= Config.NEAR_MISS_SCORE_MAX:
            prev = self._near_miss_scores.get(symbol, 0.0)
            if score >= prev:
                self._near_miss_scores[symbol] = score

    def _finalize_scan_priority(self) -> None:
        ranked = sorted(
            self._near_miss_scores.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        self._scan_priority = [
            symbol for symbol, _ in ranked[: Config.NEAR_MISS_PRIORITY_MAX]
        ]
        if self._scan_priority:
            scanner_logger.info(
                "Near-miss priority queue updated (%s symbols).",
                len(self._scan_priority),
            )
        self._near_miss_scores.clear()

    def _pick_directional_candidate(
        self,
        symbol: str,
        long_candidate: Dict[str, Any],
        short_candidate: Dict[str, Any],
        long_score: float,
        short_score: float,
        *,
        win_margin: float,
        resolve_margin: float,
        min_score_fn: Any,
    ) -> Dict[str, Any]:
        """Pick LONG/SHORT winner or resolve equilibrium in neutral/choppy regimes."""
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}

        if long_score <= 0 and short_score <= 0:
            return neutral

        if long_score > 0 and short_score <= 0:
            return long_candidate
        if short_score > 0 and long_score <= 0:
            return short_candidate

        gap = abs(long_score - short_score)
        winner = long_candidate if long_score >= short_score else short_candidate
        winner_score = max(long_score, short_score)
        loser_score = min(long_score, short_score)

        if gap >= win_margin:
            signal_logger.info(
                "Direction winner | %s LONG=%.1f SHORT=%.1f gap=%.1f",
                symbol,
                long_score,
                short_score,
                gap,
            )
            return long_candidate if long_score > short_score else short_candidate

        if (
            Config.ENABLE_DIRECTION_EQUILIBRIUM_RESOLVE
            and gap >= resolve_margin
            and winner.get("macro_trend", "NEUTRAL") == "NEUTRAL"
        ):
            confluence = str(winner.get("confluence", ""))
            min_required = min_score_fn(confluence, "NEUTRAL")
            if winner_score >= min_required:
                signal_logger.info(
                    "Equilibrium resolved | %s %s score=%.1f vs %.1f gap=%.1f min=%.1f",
                    symbol,
                    winner.get("action"),
                    winner_score,
                    loser_score,
                    gap,
                    min_required,
                )
                return winner

        self._note_near_miss(symbol, winner_score)
        signal_logger.info(
            "Equilibrium skip | %s LONG=%.1f SHORT=%.1f gap=%.1f (<%.1f)",
            symbol,
            long_score,
            short_score,
            gap,
            resolve_margin,
        )
        return neutral

    def _fetch_symbol_timeframes(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Fetch entry/confirm/trend candles with small pauses between requests."""
        timeframes = (self.entry_tf, self.confirm_tf, self.trend_tf)
        frames: list[pd.DataFrame] = []
        for idx, timeframe in enumerate(timeframes):
            frames.append(self._fetch_candles_with_retry(symbol, timeframe, self.candle_limit))
            if idx < len(timeframes) - 1 and Config.SCAN_TIMEFRAME_DELAY_SECONDS > 0:
                time.sleep(Config.SCAN_TIMEFRAME_DELAY_SECONDS)
        return frames[0], frames[1], frames[2]

    def _evaluate_direction(
        self,
        symbol: str,
        df_entry: pd.DataFrame,
        df_confirm: pd.DataFrame,
        df_trend: pd.DataFrame,
        action: str,
        price: float,
    ) -> Dict[str, Any]:
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        latest, previous = self.analyzer.extract_latest_signals(df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)

        if atr <= 0 or price <= 0:
            signal_logger.info(
                "REJECTED %s %s | stage=preflight | invalid_atr_or_price atr=%.6f price=%.6f",
                symbol,
                action,
                atr,
                price,
            )
            return neutral

        gate = evaluate_confluence_gate(action, df_entry, df_trend, df_confirm, price, atr)
        if not gate.passed:
            self.db.log_signal_rejection(symbol, action, 0.0, gate.reasons)
            signal_logger.info(
                "REJECTED %s %s | stage=confluence_gate | macro=%s confirm=%s | reasons=%s",
                symbol,
                action,
                gate.structure.macro_trend,
                gate.structure.confirm_trend,
                "; ".join(gate.reasons),
            )
            return neutral

        retest_ok, retest_reason = validate_retest_entry(action, price, gate.structure, atr)
        if not retest_ok:
            self.db.log_signal_rejection(symbol, action, 0.0, [retest_reason])
            signal_logger.info(
                "REJECTED %s %s | stage=retest | macro=%s | confluence=%s | reason=%s",
                symbol,
                action,
                gate.structure.macro_trend,
                gate.structure.confluence_type,
                retest_reason,
            )
            return neutral

        score = score_setup(action, latest, previous, gate.structure, gate.structure.macro_trend)
        min_score = effective_smc_min_score(
            gate.structure.confluence_type, gate.structure.macro_trend
        )

        if score < min_score:
            self._note_near_miss(symbol, score)
            reason = f"score_below_min_{score:.1f}_required_{min_score:.1f}"
            self.db.log_signal_rejection(symbol, action, score, [reason])
            signal_logger.info(
                "REJECTED %s %s | stage=score | score=%.1f min=%.1f | macro=%s | confluence=%s",
                symbol,
                action,
                score,
                min_score,
                gate.structure.macro_trend,
                gate.structure.confluence_type,
            )
            return neutral

        signal_logger.info(
            "APPROVED %s %s | score=%.1f | macro=%s confirm=%s | confluence=%s | price=%.6f atr=%.6f",
            symbol,
            action,
            score,
            gate.structure.macro_trend,
            gate.structure.confirm_trend,
            gate.structure.confluence_type,
            price,
            atr,
        )

        long_score = score if action == "LONG" else 0.0
        short_score = score if action == "SHORT" else 0.0

        return {
            "symbol": symbol,
            "action": action,
            "direction": action,
            "score": score,
            "long_score": long_score,
            "short_score": short_score,
            "atr": atr,
            "price": price,
            "strategy": STRATEGY_SMC_TREND,
            "timeframe": self.entry_tf,
            "macro_trend": gate.structure.macro_trend,
            "structure_metadata": gate.structure.to_dict(),
            "confluence": gate.structure.confluence_type,
        }

    def evaluate_single_symbol(
        self, symbol: str, price_map: Optional[dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Analyze entry, confirm, and trend timeframes for one symbol."""
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        try:
            df_entry, df_confirm, df_trend = self._fetch_symbol_timeframes(symbol)

            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            df_entry = self.analyzer.apply_all_indicators(df_entry)
            df_confirm = self.analyzer.apply_all_indicators(df_confirm)
            df_trend = self.analyzer.apply_all_indicators(df_trend)

            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            cached_price = safe_float((price_map or {}).get(symbol))
            if cached_price <= 0:
                cached_price = float(df_entry.iloc[-1]["close"])
            price = cached_price

            long_candidate = self._evaluate_direction(
                symbol, df_entry, df_confirm, df_trend, "LONG", price
            )
            short_candidate = self._evaluate_direction(
                symbol, df_entry, df_confirm, df_trend, "SHORT", price
            )

            long_score = safe_float(long_candidate.get("score"))
            short_score = safe_float(short_candidate.get("score"))

            return self._pick_directional_candidate(
                symbol,
                long_candidate,
                short_candidate,
                long_score,
                short_score,
                win_margin=Config.DIRECTION_WIN_MARGIN,
                resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
                min_score_fn=effective_smc_min_score,
            )

        except Exception as exc:
            error_logger.error("Evaluation failed for %s: %s", symbol, exc)
            return neutral

    def scan_market(self) -> List[Dict[str, Any]]:
        """Run SMC scan via modular pipeline (backward-compatible wrapper)."""
        self._pipeline._scan_priority = self._scan_priority
        results = self._pipeline.scan_smc()
        self._scan_priority = self._pipeline._scan_priority
        return results

    def _evaluate_range_direction(
        self,
        symbol: str,
        df_entry: pd.DataFrame,
        df_confirm: pd.DataFrame,
        df_trend: pd.DataFrame,
        action: str,
        price: float,
    ) -> Dict[str, Any]:
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        atr = self.analyzer.get_latest_atr(df_entry)
        if atr <= 0 or price <= 0:
            return neutral

        result = evaluate_range_setup(action, df_entry, df_trend, df_confirm, price, atr)
        if not result.passed:
            self.db.log_signal_rejection(
                symbol,
                action,
                result.score,
                result.reasons,
                strategy=STRATEGY_RANGE_REVERSION,
            )
            signal_logger.info(
                "REJECTED %s %s | stage=range_gate | reasons=%s",
                symbol,
                action,
                "; ".join(result.reasons),
            )
            return neutral

        signal_logger.info(
            "APPROVED %s %s | strategy=%s | score=%.1f | edge=%s | range=[%.6f, %.6f]",
            symbol,
            action,
            STRATEGY_RANGE_REVERSION,
            result.score,
            result.metadata.edge,
            result.metadata.range_low,
            result.metadata.range_high,
        )

        return {
            "symbol": symbol,
            "action": action,
            "direction": action,
            "score": result.score,
            "atr": atr,
            "price": price,
            "strategy": STRATEGY_RANGE_REVERSION,
            "timeframe": self.entry_tf,
            "structure_metadata": result.metadata.to_dict(),
            "confluence": result.metadata.edge,
        }

    def evaluate_range_symbol(
        self, symbol: str, price_map: Optional[dict[str, float]] = None
    ) -> Dict[str, Any]:
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        try:
            df_entry, df_confirm, df_trend = self._fetch_symbol_timeframes(symbol)
            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            df_entry = self.analyzer.apply_all_indicators(df_entry)
            df_confirm = self.analyzer.apply_all_indicators(df_confirm)
            df_trend = self.analyzer.apply_all_indicators(df_trend)
            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            cached_price = safe_float((price_map or {}).get(symbol))
            if cached_price <= 0:
                cached_price = float(df_entry.iloc[-1]["close"])

            long_c = self._evaluate_range_direction(
                symbol, df_entry, df_confirm, df_trend, "LONG", cached_price
            )
            short_c = self._evaluate_range_direction(
                symbol, df_entry, df_confirm, df_trend, "SHORT", cached_price
            )
            long_score = safe_float(long_c.get("score"))
            short_score = safe_float(short_c.get("score"))

            def _range_min_score(_confluence: str, _macro: str) -> float:
                return Config.RANGE_MIN_SCORE

            return self._pick_directional_candidate(
                symbol,
                long_c,
                short_c,
                long_score,
                short_score,
                win_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
                resolve_margin=Config.DIRECTION_EQUILIBRIUM_MIN_MARGIN,
                min_score_fn=_range_min_score,
            )
        except Exception as exc:
            error_logger.error("Range evaluation failed for %s: %s", symbol, exc)
            return neutral

    def scan_unified(self) -> List[Dict[str, Any]]:
        """Run all enabled strategies through the unified modular pipeline."""
        self._pipeline._scan_priority = self._scan_priority
        results = self._pipeline.scan_unified()
        self._scan_priority = self._pipeline._scan_priority
        return results

    def scan_range_market(self) -> List[Dict[str, Any]]:
        """Run RANGE scan via modular pipeline (backward-compatible wrapper)."""
        if not Config.ENABLE_RANGE_REGIME and not Config.ENABLE_STRATEGY_RANGE:
            return []
        self._pipeline._scan_priority = self._scan_priority
        results = self._pipeline.scan_range()
        self._scan_priority = self._pipeline._scan_priority
        return results
