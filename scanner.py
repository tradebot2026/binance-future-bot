"""
Market scanner module.
Universe filtering, multi-timeframe SMC analysis, confluence gates,
retest-based entry validation, and watchlist generation.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from typing import Any, Dict, List

import pandas as pd
import ta

from config import Config
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import error_logger, scanner_logger, signal_logger
from smc_engine import (
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

    def get_tradable_symbols(self) -> List[str]:
        """Filter universe by volume, spread, and blacklist (two bulk API calls)."""
        tradable: List[tuple[str, float]] = []

        try:
            ticker_map = self.exchange.get_futures_ticker_map()
            book_map = self.exchange.get_book_ticker_map()
            if not ticker_map:
                return []

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

                tradable.append((symbol, volume_24h))

            tradable.sort(key=lambda item: item[1], reverse=True)
            symbols = [s for s, _ in tradable[: Config.MAX_SCAN_UNIVERSE]]
            scanner_logger.info(
                "Universe filtered: %s tradable (top %s by volume).",
                len(symbols),
                Config.MAX_SCAN_UNIVERSE,
            )
            return symbols
        except Exception as exc:
            error_logger.error("Failed to build tradable universe: %s", exc)
            return []

    def _fetch_candles_with_retry(
        self, symbol: str, timeframe: str, limit: int
    ) -> pd.DataFrame:
        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                df = self.exchange.fetch_historical_candles(symbol, timeframe, limit=limit)
                if not df.empty and len(df) >= MIN_ANALYZER_BARS:
                    return df
            except Exception as exc:
                if attempt == Config.MAX_RETRIES:
                    raise exc
                time.sleep(0.5)
        return pd.DataFrame()

    def _evaluate_direction(
        self,
        symbol: str,
        df_entry: pd.DataFrame,
        df_confirm: pd.DataFrame,
        df_trend: pd.DataFrame,
        action: str,
    ) -> Dict[str, Any]:
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        latest, previous = self.analyzer.extract_latest_signals(df_entry)
        atr = self.analyzer.get_latest_atr(df_entry)
        live_price = self.exchange.get_market_price(symbol)
        price = safe_float(live_price) if live_price else float(df_entry.iloc[-1]["close"])

        if atr <= 0 or price <= 0:
            return neutral

        gate = evaluate_confluence_gate(action, df_entry, df_trend, df_confirm, price, atr)
        if not gate.passed:
            self.db.log_signal_rejection(symbol, action, 0.0, gate.reasons)
            signal_logger.info(
                "Rejected %s %s | reasons=%s", symbol, action, ",".join(gate.reasons)
            )
            return neutral

        retest_ok, retest_reason = validate_retest_entry(action, price, gate.structure, atr)
        if not retest_ok:
            self.db.log_signal_rejection(symbol, action, 0.0, [retest_reason])
            signal_logger.info("Rejected %s %s | %s", symbol, action, retest_reason)
            return neutral

        score = score_setup(action, latest, previous, gate.structure, gate.structure.macro_trend)
        if score < Config.STRATEGY_MIN_SCORE:
            self.db.log_signal_rejection(
                symbol, action, score, [f"score_below_min_{score:.1f}"]
            )
            return neutral

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
            "strategy": "SMC_MULTITF",
            "timeframe": self.entry_tf,
            "macro_trend": gate.structure.macro_trend,
            "structure_metadata": gate.structure.to_dict(),
            "confluence": gate.structure.confluence_type,
        }

    def evaluate_single_symbol(self, symbol: str) -> Dict[str, Any]:
        """Analyze entry, confirm, and trend timeframes for one symbol."""
        neutral = {"symbol": symbol, "score": 0.0, "action": "NEUTRAL"}
        try:
            df_entry = self._fetch_candles_with_retry(symbol, self.entry_tf, self.candle_limit)
            df_confirm = self._fetch_candles_with_retry(symbol, self.confirm_tf, self.candle_limit)
            df_trend = self._fetch_candles_with_retry(symbol, self.trend_tf, self.candle_limit)

            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            df_entry = self.analyzer.apply_all_indicators(df_entry)
            df_confirm = self.analyzer.apply_all_indicators(df_confirm)
            df_trend = self.analyzer.apply_all_indicators(df_trend)

            if df_entry.empty or df_confirm.empty or df_trend.empty:
                return neutral

            long_candidate = self._evaluate_direction(
                symbol, df_entry, df_confirm, df_trend, "LONG"
            )
            short_candidate = self._evaluate_direction(
                symbol, df_entry, df_confirm, df_trend, "SHORT"
            )

            long_score = safe_float(long_candidate.get("score"))
            short_score = safe_float(short_candidate.get("score"))

            if long_score <= 0 and short_score <= 0:
                return neutral

            if abs(long_score - short_score) < 5.0:
                signal_logger.info(
                    "Equilibrium | %s LONG=%.1f SHORT=%.1f — skipped.",
                    symbol,
                    long_score,
                    short_score,
                )
                return neutral

            if long_score > short_score:
                return long_candidate
            return short_candidate

        except Exception as exc:
            error_logger.error("Evaluation failed for %s: %s", symbol, exc)
            return neutral

    def scan_market(self) -> List[Dict[str, Any]]:
        """Run a rate-limit-aware parallel scan and return top candidates."""
        scanner_logger.info("Starting market scan cycle...")
        started = time.time()
        symbols = self.get_tradable_symbols()
        if not symbols:
            scanner_logger.warning("No tradable symbols found.")
            return []

        candidates: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(self.evaluate_single_symbol, sym): sym for sym in symbols}
            try:
                for future in as_completed(futures, timeout=Config.SCAN_TIMEOUT_SEC):
                    result = future.result()
                    if (
                        result.get("action") not in (None, "NEUTRAL")
                        and safe_float(result.get("score")) >= Config.STRATEGY_MIN_SCORE
                    ):
                        candidates.append(result)
                        self.db.log_signal(
                            {
                                "symbol": result["symbol"],
                                "timeframe": self.entry_tf,
                                "direction": result["action"],
                                "score": result["score"],
                                "strategy": result.get("strategy", "SMC_MULTITF"),
                                "reason": (
                                    f"macro={result.get('macro_trend')}|"
                                    f"confluence={result.get('confluence')}"
                                ),
                                "accepted": True,
                                "structure_metadata": result.get("structure_metadata"),
                            }
                        )
            except FuturesTimeoutError:
                scanner_logger.error(
                    "Scan timeout after %ss — returning partial results.", Config.SCAN_TIMEOUT_SEC
                )

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
            scanner_logger.info("Scan complete — no candidates above threshold.")

        scanner_logger.info("Scan finished in %.2fs.", time.time() - started)
        return top
