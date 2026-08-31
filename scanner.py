"""
Market scanner module.
Universe filtering, multi-timeframe SMC analysis, confluence gates,
retest-based entry validation, and watchlist generation.
"""

from __future__ import annotations

import time
from collections import Counter
from typing import Any, Dict, List, Optional

import pandas as pd
import ta

from config import Config
from constants import STRATEGY_RANGE_REVERSION, STRATEGY_SMC_TREND
from database import DatabaseManager
from exceptions import ExchangeRateLimitError
from exchange import BinanceExchangeManager
from logger import error_logger, scanner_logger, signal_logger
from range_engine import evaluate_range_setup
from smc_engine import (
    effective_smc_min_score,
    evaluate_confluence_gate,
    score_setup,
    validate_retest_entry,
)
from utils import safe_float

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

    def _scan_gate_open(self) -> tuple[bool, str]:
        if self._hub:
            return self._hub.is_scan_halted()
        return False, ""

    def get_tradable_symbols(self) -> tuple[List[str], dict[str, float]]:
        """Filter universe by volume, spread, and blacklist (two bulk API calls)."""
        tradable: List[tuple[str, float]] = []
        price_map: dict[str, float] = {}

        try:
            ticker_map = self.exchange.get_futures_ticker_map()
            book_map = self.exchange.get_book_ticker_map()
            if not ticker_map:
                return [], {}

            self.db.cleanup_expired_blacklist()

            for symbol, ticker in ticker_map.items():
                if not symbol.endswith(Config.QUOTE_ASSET) or "_" in symbol:
                    continue

                volume_24h = safe_float(ticker.get("quoteVolume"))
                if volume_24h < Config.MIN_24H_VOLUME_USDT:
                    continue

                book = book_map.get(symbol, {})
                bid = safe_float(book.get("bidPrice"))
                ask = safe_float(book.get("askPrice"))
                if bid <= 0 or ask <= 0:
                    continue

                spread_pct = ((ask - bid) / bid) * 100.0
                if spread_pct > Config.MAX_SPREAD_PERCENT:
                    continue

                if self.db.is_blacklisted(symbol):
                    continue

                last_price = safe_float(ticker.get("lastPrice"))
                if last_price <= 0:
                    last_price = (bid + ask) / 2.0
                if last_price > 0:
                    price_map[symbol] = last_price

                tradable.append((symbol, volume_24h))

            tradable.sort(key=lambda item: item[1], reverse=True)
            symbols = self._prioritize_symbols(
                [s for s, _ in tradable[: Config.MAX_SCAN_UNIVERSE]]
            )
            scanner_logger.info(
                "Universe filtered: %s tradable (top %s by volume, %s priority).",
                len(symbols),
                Config.MAX_SCAN_UNIVERSE,
                min(len(self._scan_priority), len(symbols)),
            )
            return symbols, price_map
        except Exception as exc:
            error_logger.error("Failed to build tradable universe: %s", exc)
            return [], {}

    def _fetch_candles_with_retry(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                df = self.exchange.fetch_historical_candles(symbol, timeframe, limit=limit)
                if not df.empty and len(df) >= MIN_ANALYZER_BARS:
                    return df
            except ExchangeRateLimitError:
                raise
            except Exception as exc:
                if attempt == Config.MAX_RETRIES:
                    raise exc
                time.sleep(0.5)
        return pd.DataFrame()

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
        """Run a sequential, rate-limit-aware scan and return top candidates."""
        halted, halt_reason = self._scan_gate_open()
        if halted:
            scanner_logger.warning("SMC scan skipped — %s", halt_reason)
            return []

        scanner_logger.info(
            "Starting market scan cycle (sequential, pair_delay=%.2fs)...",
            Config.SCAN_PAIR_DELAY_SECONDS,
        )
        started = time.time()
        symbols, price_map = self.get_tradable_symbols()
        if not symbols:
            scanner_logger.warning("No tradable symbols found.")
            return []

        candidates: List[Dict[str, Any]] = []
        rejection_stats: Counter[str] = Counter()
        scanned = 0

        for symbol in symbols:
            if time.time() - started > Config.SCAN_TIMEOUT_SEC:
                scanner_logger.warning(
                    "Scan timeout after %ss — returning partial results (%s/%s scanned).",
                    Config.SCAN_TIMEOUT_SEC,
                    scanned,
                    len(symbols),
                )
                break

            result = self.evaluate_single_symbol(symbol, price_map=price_map)
            scanned += 1

            result_score = safe_float(result.get("score"))
            result_confluence = str(result.get("confluence", ""))
            result_macro = str(result.get("macro_trend", "NEUTRAL"))
            min_required = effective_smc_min_score(result_confluence, result_macro)

            if (
                result.get("action") not in (None, "NEUTRAL")
                and result_score >= min_required
            ):
                candidates.append(result)
                self.db.log_signal(
                    {
                        "symbol": result["symbol"],
                        "timeframe": self.entry_tf,
                        "direction": result["action"],
                        "score": result["score"],
                                "strategy": result.get("strategy", STRATEGY_SMC_TREND),
                        "reason": (
                            f"macro={result.get('macro_trend')}|"
                            f"confluence={result.get('confluence')}"
                        ),
                        "accepted": True,
                        "structure_metadata": result.get("structure_metadata"),
                    }
                )
            elif result.get("action") == "NEUTRAL":
                rejection_stats["neutral"] += 1

            if Config.SCAN_PAIR_DELAY_SECONDS > 0:
                time.sleep(Config.SCAN_PAIR_DELAY_SECONDS)

        candidates.sort(key=lambda item: safe_float(item.get("score")), reverse=True)
        top = candidates[: Config.MAX_POSITIONS]

        if top:
            self.db.update_watchlist(top)
            for item in top:
                scanner_logger.info(
                    "Candidate | %s | %s | score=%.1f | confluence=%s | price=%.6f",
                    item["symbol"],
                    item["action"],
                    safe_float(item["score"]),
                    item.get("confluence", "?"),
                    safe_float(item["price"]),
                )
        else:
            scanner_logger.info(
                "Scan complete — no candidates above threshold (scanned=%s/%s, neutral=%s).",
                scanned,
                len(symbols),
                rejection_stats.get("neutral", 0),
            )

        self._finalize_scan_priority()
        scanner_logger.info("Scan finished in %.2fs.", time.time() - started)
        return top

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

    def scan_range_market(self) -> List[Dict[str, Any]]:
        """Scan for mean-reversion setups when the market is ranging."""
        if not Config.ENABLE_RANGE_REGIME:
            return []

        halted, halt_reason = self._scan_gate_open()
        if halted:
            scanner_logger.warning("RANGE scan skipped — %s", halt_reason)
            return []

        scanner_logger.info(
            "Starting RANGE scan (pair_delay=%.2fs)...",
            Config.SCAN_PAIR_DELAY_SECONDS,
        )
        started = time.time()
        symbols, price_map = self.get_tradable_symbols()
        if not symbols:
            return []

        candidates: List[Dict[str, Any]] = []
        scanned = 0

        for symbol in symbols:
            if time.time() - started > Config.SCAN_TIMEOUT_SEC:
                break

            result = self.evaluate_range_symbol(symbol, price_map=price_map)
            scanned += 1

            if (
                result.get("action") not in (None, "NEUTRAL")
                and safe_float(result.get("score")) >= Config.RANGE_MIN_SCORE
            ):
                candidates.append(result)
                self.db.log_signal(
                    {
                        "symbol": result["symbol"],
                        "timeframe": self.entry_tf,
                        "direction": result["action"],
                        "score": result["score"],
                        "strategy": STRATEGY_RANGE_REVERSION,
                        "reason": f"edge={result.get('confluence')}|range_mode",
                        "accepted": True,
                        "structure_metadata": result.get("structure_metadata"),
                    }
                )

            if Config.SCAN_PAIR_DELAY_SECONDS > 0:
                time.sleep(Config.SCAN_PAIR_DELAY_SECONDS)

        candidates.sort(key=lambda item: safe_float(item.get("score")), reverse=True)
        top = candidates[: Config.MAX_RANGE_POSITIONS]

        if top:
            for item in top:
                scanner_logger.info(
                    "RANGE Candidate | %s | %s | score=%.1f | edge=%s",
                    item["symbol"],
                    item["action"],
                    safe_float(item["score"]),
                    item.get("confluence", "?"),
                )
        else:
            scanner_logger.info(
                "RANGE scan complete — no candidates (scanned=%s).", scanned
            )

        self._finalize_scan_priority()
        scanner_logger.info("RANGE scan finished in %.2fs.", time.time() - started)
        return top
