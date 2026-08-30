"""
Binance USDT-M Futures exchange adapter.
Thread-safe symbol rules cache, rate limiting, balance caching, and order execution.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException

from config import Config
from exceptions import ExchangeError, ExchangeRateLimitError, OrderExecutionError
from logger import error_logger, system_logger, trade_logger
from utils import round_step_size, safe_float


@dataclass
class SymbolRules:
    price_precision: int
    quantity_precision: int
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "price_precision": self.price_precision,
            "quantity_precision": self.quantity_precision,
            "tick_size": self.tick_size,
            "step_size": self.step_size,
            "min_qty": self.min_qty,
            "min_notional": self.min_notional,
        }


@dataclass
class BalanceCache:
    value: float = 0.0
    updated_at: float = 0.0
    ttl_seconds: int = Config.BALANCE_CACHE_TTL_SECONDS

    def is_valid(self) -> bool:
        return self.value > 0 and (time.monotonic() - self.updated_at) < self.ttl_seconds

    def set(self, balance: float) -> None:
        self.value = balance
        self.updated_at = time.monotonic()

    def invalidate(self) -> None:
        self.updated_at = 0.0


class RateLimiter:
    """Simple inter-request throttle to reduce ban risk."""

    def __init__(self, min_interval_ms: int) -> None:
        self._min_interval = max(min_interval_ms, 0) / 1000.0
        self._lock = threading.Lock()
        self._last_request = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()


class BinanceExchangeManager:
    """
    Binance Futures integration with caching, retries, and thread-safe rule access.

    Strict rate-limit protection (equivalent to enableRateLimit=True in CCXT) is
    enforced via MIN_REQUEST_INTERVAL_MS throttling plus exponential backoff with
    jitter on HTTP 429/418 and Binance codes -1003/-1015.
    """

    RATE_LIMIT_CODES = frozenset({-1003, -1015, 429, 418})

    DEFAULT_RULES = SymbolRules(
        price_precision=2,
        quantity_precision=3,
        tick_size=0.01,
        step_size=0.001,
        min_qty=0.001,
        min_notional=5.0,
    )

    def __init__(self) -> None:
        self.api_key = Config.BINANCE_API_KEY
        self.api_secret = Config.BINANCE_API_SECRET
        self.testnet = Config.USE_TESTNET

        self.client = Client(
            api_key=self.api_key,
            api_secret=self.api_secret,
            testnet=self.testnet,
            requests_params={"timeout": Config.REQUEST_TIMEOUT},
        )
        self.recv_window_param = {"recvWindow": 60000}

        self._rules_lock = threading.RLock()
        self._symbol_rules_cache: dict[str, SymbolRules] = {}
        self._leverage_cache: dict[str, int] = {}
        self._rate_limiter = RateLimiter(Config.MIN_REQUEST_INTERVAL_MS)
        self._balance_cache = BalanceCache()
        self._critical_alerts: Any = None
        self._market_data: Any = None
        self._full_init_done = False

        ban = self.check_startup_ban()
        if ban.is_banned:
            error_logger.critical(
                "Binance IP ban detected at startup | until=%s | %s",
                ban.banned_until_iso or "unknown",
                ban.message,
            )
            system_logger.warning(
                "Deferring account/symbol REST init until ban expires (~%ss).",
                ban.seconds_remaining,
            )
        else:
            self.ensure_initialized()

        mode = "TESTNET" if self.testnet else "MAINNET"
        strict = "ON" if Config.ENABLE_STRICT_RATE_LIMIT else "OFF"
        system_logger.info(
            "Binance Futures exchange initialized (%s, strict_rate_limit=%s).",
            mode,
            strict,
        )

    def attach_critical_alerts(self, alerts: Any) -> None:
        """Optional hook for immediate Telegram critical notifications."""
        self._critical_alerts = alerts

    def attach_market_data(self, hub: Any) -> None:
        """Attach WebSocket + candle cache hub."""
        self._market_data = hub
        if hub and hasattr(self, "_startup_ban") and self._startup_ban.is_banned:
            hub.apply_startup_ban(self._startup_ban)

    def check_startup_ban(self) -> Any:
        from market_data_hub import BanStatus, check_binance_ban_status

        self._startup_ban = check_binance_ban_status(self.client)
        return self._startup_ban

    @staticmethod
    def _init_rest_pause() -> None:
        if Config.INIT_REST_DELAY_SECONDS > 0:
            time.sleep(Config.INIT_REST_DELAY_SECONDS)

    def get_startup_ban_status(self) -> Any:
        return getattr(self, "_startup_ban", None)

    def ensure_initialized(self) -> bool:
        """Run deferred REST init (account mode + symbol rules) once API is reachable."""
        if self._full_init_done:
            return True

        ban = self.get_startup_ban_status()
        if ban and ban.is_banned:
            if self._market_data:
                halted, _ = self._market_data.is_scan_halted()
                if halted:
                    return False
            else:
                return False

        self._init_rest_pause()
        self._configure_account()
        self._init_rest_pause()
        self.refresh_symbol_rules()
        self._full_init_done = True
        return True

    # ---------------- Internal helpers ----------------

    def _backoff_seconds(self, attempt: int) -> float:
        base = min(2 ** attempt, Config.API_BACKOFF_MAX_SECONDS)
        jitter = random.uniform(0.0, 1.0) if Config.ENABLE_STRICT_RATE_LIMIT else 0.0
        return base + jitter

    def _is_rate_limit_error(self, exc: BinanceAPIException) -> bool:
        if exc.code in self.RATE_LIMIT_CODES:
            return True
        message = str(exc.message).lower()
        return "429" in message or "418" in message or "rate limit" in message

    def _throttled_call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, Config.MAX_RETRIES + 1):
            if Config.ENABLE_STRICT_RATE_LIMIT:
                self._rate_limiter.wait()
            try:
                return func(*args, **kwargs)
            except BinanceAPIException as exc:
                last_error = exc
                if self._is_rate_limit_error(exc):
                    if exc.code == -1003 and self._market_data:
                        self._market_data.handle_rate_limit_error(exc)
                        raise ExchangeRateLimitError(str(exc.message)) from exc

                    sleep_seconds = self._backoff_seconds(attempt)
                    error_logger.warning(
                        "Rate limit hit (code=%s). Backoff %.1fs (attempt %s/%s).",
                        exc.code,
                        sleep_seconds,
                        attempt,
                        Config.MAX_RETRIES,
                    )
                    time.sleep(sleep_seconds)
                    if attempt == Config.MAX_RETRIES:
                        if self._market_data:
                            self._market_data.handle_rate_limit_error(exc)
                        if self._critical_alerts:
                            self._critical_alerts.notify(
                                "RATE_LIMIT",
                                f"Binance rate limit exhausted after {Config.MAX_RETRIES} retries",
                                exc=exc,
                            )
                        raise ExchangeRateLimitError(str(exc.message)) from exc
                    continue
                if self._critical_alerts and exc.code in (-1021, -2015, -2014):
                    self._critical_alerts.notify(
                        "API_DISCONNECT",
                        f"Binance API error code {exc.code}: {exc.message}",
                        exc=exc,
                    )
                raise ExchangeError(str(exc.message)) from exc
            except (ConnectionError, TimeoutError, OSError) as exc:
                last_error = exc
                sleep_seconds = self._backoff_seconds(attempt)
                error_logger.warning(
                    "Network error on API call (attempt %s/%s): %s — retry in %.1fs",
                    attempt,
                    Config.MAX_RETRIES,
                    exc,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                if attempt == Config.MAX_RETRIES:
                    if self._critical_alerts:
                        self._critical_alerts.notify(
                            "API_DISCONNECT",
                            "Binance API connection failed after retries",
                            exc=exc,
                        )
                    raise ExchangeError(str(exc)) from exc
            except Exception as exc:
                last_error = exc
                if attempt == Config.MAX_RETRIES:
                    raise ExchangeError(str(exc)) from exc
                time.sleep(self._backoff_seconds(attempt))
        raise ExchangeError(str(last_error))

    def _parse_symbol_rules(self, symbol_data: dict[str, Any]) -> SymbolRules:
        filters = {item["filterType"]: item for item in symbol_data.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_filter = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", lot_filter)
        notional_filter = filters.get("MIN_NOTIONAL", {})

        return SymbolRules(
            price_precision=int(symbol_data.get("pricePrecision", 2)),
            quantity_precision=int(symbol_data.get("quantityPrecision", 3)),
            tick_size=safe_float(price_filter.get("tickSize"), 0.01),
            step_size=safe_float(lot_filter.get("stepSize"), 0.001),
            min_qty=max(
                safe_float(lot_filter.get("minQty"), 0.001),
                safe_float(market_lot.get("minQty"), 0.001),
            ),
            min_notional=safe_float(notional_filter.get("notional"), 5.0),
        )

    # ---------------- Account setup ----------------

    def _configure_account(self) -> None:
        """Ensure hedge (dual-side) position mode is enabled for LONG/SHORT orders."""
        try:
            position_mode = self._throttled_call(
                self.client.futures_get_position_mode,
                **self.recv_window_param,
            )
            if not position_mode.get("dualSidePosition", False):
                self._throttled_call(
                    self.client.futures_change_position_mode,
                    dualSidePosition="true",
                    **self.recv_window_param,
                )
                system_logger.info("Hedge mode enabled on Binance Futures account.")
            else:
                system_logger.info("Hedge mode already active.")
        except BinanceAPIException as exc:
            # -4046: No need to change position side (already in requested mode)
            if exc.code != -4046:
                error_logger.error("Failed to configure hedge mode: %s", exc.message)

    def refresh_symbol_rules(self) -> None:
        for attempt in range(1, 4):
            try:
                exchange_info = self._throttled_call(self.client.futures_exchange_info)
                temp_cache: dict[str, SymbolRules] = {}
                for symbol_data in exchange_info.get("symbols", []):
                    symbol_name = symbol_data.get("symbol")
                    if not symbol_name:
                        continue
                    temp_cache[symbol_name] = self._parse_symbol_rules(symbol_data)

                with self._rules_lock:
                    self._symbol_rules_cache = temp_cache

                system_logger.info(
                    "Cached trading rules for %s symbols.", len(temp_cache)
                )
                return
            except Exception as exc:
                error_logger.warning(
                    "Symbol rules refresh failed (attempt %s/3): %s", attempt, exc
                )
                time.sleep(2)

        error_logger.error("Critical: symbol rules cache could not be initialized.")

    # ---------------- Public symbol rule accessors ----------------

    def get_symbol_precision(self, symbol: str) -> dict[str, Any]:
        """
        Thread-safe getter used by manager/executor.
        Returns precision dict with safe fallbacks if cache miss occurs.
        """
        rules = self.get_symbol_rules(symbol)
        return rules.to_dict()

    def get_symbol_rules(self, symbol: str) -> SymbolRules:
        with self._rules_lock:
            cached = self._symbol_rules_cache.get(symbol)
            if cached is not None:
                return cached

        error_logger.warning(
            "Symbol rules cache miss for %s — using conservative defaults.", symbol
        )
        return self.DEFAULT_RULES

    # ---------------- Balance ----------------

    def invalidate_balance_cache(self) -> None:
        self._balance_cache.invalidate()

    def get_futures_balance(self, force_refresh: bool = False) -> float:
        """
        Return available USDT balance.
        Uses in-memory cache unless force_refresh=True or TTL expired.
        """
        if not force_refresh and self._balance_cache.is_valid():
            return self._balance_cache.value

        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                account_info = self._throttled_call(
                    self.client.futures_account,
                    **self.recv_window_param,
                )
                for asset in account_info.get("assets", []):
                    if asset.get("asset") == Config.QUOTE_ASSET:
                        balance = safe_float(asset.get("availableBalance"))
                        if balance <= 0:
                            balance = safe_float(asset.get("crossWalletBalance"))
                        self._balance_cache.set(balance)
                        return balance
                return 0.0
            except Exception as exc:
                error_logger.warning(
                    "Balance fetch failed (attempt %s/%s): %s",
                    attempt,
                    Config.MAX_RETRIES,
                    exc,
                )
                time.sleep(min(2 ** attempt, 10))

        error_logger.error("All balance fetch attempts exhausted.")
        return self._balance_cache.value if self._balance_cache.value > 0 else 0.0

    # ---------------- Market data ----------------

    def fetch_historical_candles(
        self, symbol: str, timeframe: str, limit: int | None = None
    ) -> pd.DataFrame:
        fetch_limit = limit or Config.CANDLE_FETCH_LIMIT

        def _rest_fetch() -> pd.DataFrame:
            klines = self._throttled_call(
                self.client.futures_klines,
                symbol=symbol,
                interval=timeframe,
                limit=fetch_limit,
            )
            df = pd.DataFrame(
                klines,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_asset_volume",
                    "number_of_trades",
                    "taker_buy_base_asset_volume",
                    "taker_buy_quote_asset_volume",
                    "ignore",
                ],
            )
            if df.empty:
                return pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df[["timestamp", "open", "high", "low", "close", "volume"]]

        if self._market_data:
            try:
                return self._market_data.get_candles(
                    symbol, timeframe, fetch_limit, _rest_fetch
                )
            except ExchangeRateLimitError:
                raise
            except Exception as exc:
                error_logger.error(
                    "Cached candle fetch failed for %s %s: %s",
                    symbol,
                    timeframe,
                    exc,
                )
                return pd.DataFrame()

        try:
            return _rest_fetch()
        except Exception as exc:
            error_logger.error(
                "Failed to fetch candles for %s %s: %s", symbol, timeframe, exc
            )
            return pd.DataFrame()

    def get_market_price(self, symbol: str) -> Optional[float]:
        if self._market_data:
            cached = self._market_data.get_price(symbol)
            if cached is not None and cached > 0:
                return cached

        for attempt in range(1, Config.MAX_RETRIES + 1):
            try:
                ticker = self._throttled_call(
                    self.client.futures_symbol_ticker,
                    symbol=symbol,
                )
                price = safe_float(ticker.get("price"))
                return price if price > 0 else None
            except ExchangeRateLimitError:
                raise
            except Exception as exc:
                error_logger.warning(
                    "Price fetch failed for %s (attempt %s/%s): %s",
                    symbol,
                    attempt,
                    Config.MAX_RETRIES,
                    exc,
                )
                time.sleep(min(2 ** attempt, 5))
        return None

    def get_book_spread_percent(self, symbol: str) -> Optional[float]:
        """Bid/ask spread % via cached bookTicker map when available."""
        try:
            book_map = self.get_book_ticker_map()
            ticker = book_map.get(symbol.upper(), {})
            bid = safe_float(ticker.get("bidPrice"))
            ask = safe_float(ticker.get("askPrice"))
            if bid <= 0 or ask <= 0:
                return None
            return ((ask - bid) / bid) * 100.0
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.warning("Spread fetch failed for %s: %s", symbol, exc)
            return None

    def get_24h_quote_volume(self, symbol: str) -> float:
        try:
            if self._market_data and not self._market_data.ws_is_stale():
                row = self._market_data.get_ticker_map().get(symbol.upper(), {})
                volume = safe_float(row.get("quoteVolume"))
                if volume > 0:
                    return volume

            ticker_map = self.get_futures_ticker_map()
            row = ticker_map.get(symbol.upper(), {})
            return safe_float(row.get("quoteVolume"))
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.warning("24h volume fetch failed for %s: %s", symbol, exc)
            return 0.0

    def get_futures_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Return futures tickers from WebSocket cache, REST fallback if stale."""
        if self._market_data and not self._market_data.ws_is_stale():
            cached = self._market_data.get_ticker_map()
            if cached:
                return cached

        try:
            tickers = self._throttled_call(self.client.futures_ticker)
            result = {str(t["symbol"]): t for t in tickers if t.get("symbol")}
            if self._market_data and result:
                self._market_data.seed_tickers_from_rest(result)
            return result
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.error("Failed to fetch futures ticker map: %s", exc)
            if self._market_data:
                return self._market_data.get_ticker_map()
            return {}

    def get_book_ticker_map(self) -> dict[str, dict[str, Any]]:
        """Return book tickers with TTL cache (single REST refresh)."""
        if self._market_data:
            return self._market_data.get_book_ticker_map(self._fetch_book_ticker_map_rest)
        return self._fetch_book_ticker_map_rest()

    def _fetch_book_ticker_map_rest(self) -> dict[str, dict[str, Any]]:
        try:
            tickers = self._throttled_call(self.client.futures_orderbook_ticker)
            return {str(t["symbol"]): t for t in tickers if t.get("symbol")}
        except ExchangeRateLimitError:
            raise
        except Exception as exc:
            error_logger.error("Failed to fetch book ticker map: %s", exc)
            return {}

    # ---------------- Leverage ----------------

    def optimize_and_set_leverage(self, symbol: str) -> int:
        with self._rules_lock:
            if symbol in self._leverage_cache:
                return self._leverage_cache[symbol]

        target_leverage = Config.MAX_LEVERAGE
        try:
            if Config.AUTO_DETECT_MAX_LEVERAGE:
                brackets = self._throttled_call(
                    self.client.futures_leverage_bracket,
                    symbol=symbol,
                    **self.recv_window_param,
                )
                if brackets and isinstance(brackets, list):
                    max_exchange_leverage = int(
                        brackets[0]["brackets"][0]["initialLeverage"]
                    )
                    target_leverage = min(Config.MAX_LEVERAGE, max_exchange_leverage)

            self._throttled_call(
                self.client.futures_change_leverage,
                symbol=symbol,
                leverage=target_leverage,
                **self.recv_window_param,
            )
            with self._rules_lock:
                self._leverage_cache[symbol] = target_leverage
            system_logger.info("Leverage set for %s: %sx", symbol, target_leverage)
            return target_leverage
        except BinanceAPIException as exc:
            fallback = min(Config.MAX_LEVERAGE, 10)
            error_logger.error(
                "Leverage API error for %s (%s). Fallback %sx.",
                symbol,
                exc.message,
                fallback,
            )
            try:
                self._throttled_call(
                    self.client.futures_change_leverage,
                    symbol=symbol,
                    leverage=fallback,
                    **self.recv_window_param,
                )
                with self._rules_lock:
                    self._leverage_cache[symbol] = fallback
                return fallback
            except Exception as fallback_exc:
                raise ExchangeError(
                    f"Leverage fallback failed for {symbol}: {fallback_exc}"
                ) from fallback_exc

    # ---------------- Positions ----------------

    def fetch_open_positions(self) -> list[dict[str, Any]]:
        """Alias for get_all_open_positions — live exchange state."""
        return self.get_all_open_positions()

    def get_open_positions_count(self) -> int:
        """Count non-zero hedge-mode positions on the exchange."""
        return len(self.fetch_open_positions())

    def get_all_open_positions(self) -> list[dict[str, Any]]:
        """Return all non-zero hedge-mode positions from the exchange."""
        try:
            positions = self._throttled_call(self.client.futures_position_information)
            open_positions: list[dict[str, Any]] = []
            for pos in positions:
                quantity = abs(safe_float(pos.get("positionAmt")))
                if quantity <= 0:
                    continue
                open_positions.append(
                    {
                        "symbol": str(pos.get("symbol", "")),
                        "positionSide": str(pos.get("positionSide", "")),
                        "quantity": quantity,
                        "entry_price": safe_float(pos.get("entryPrice")),
                        "unrealized_pnl": safe_float(pos.get("unRealizedProfit")),
                    }
                )
            return open_positions
        except Exception as exc:
            error_logger.error("Failed to fetch all open positions: %s", exc)
            return []

    def get_unrealized_pnl_total(self) -> float:
        """Sum mark-to-market PnL across all open exchange positions."""
        try:
            positions = self._throttled_call(self.client.futures_position_information)
            total = 0.0
            for pos in positions:
                if abs(safe_float(pos.get("positionAmt"))) <= 0:
                    continue
                total += safe_float(pos.get("unRealizedProfit"))
            return total
        except Exception as exc:
            error_logger.error("Failed to fetch unrealized PnL: %s", exc)
            return 0.0

    def get_fill_price_from_order(
        self, symbol: str, order_response: dict[str, Any], fallback: float
    ) -> float:
        """Resolve a reliable fill price from an order response."""
        avg_price = safe_float(order_response.get("avgPrice"))
        if avg_price > 0:
            return avg_price

        order_id = order_response.get("orderId")
        if order_id is not None:
            try:
                order_info = self._throttled_call(
                    self.client.futures_get_order,
                    symbol=symbol,
                    orderId=order_id,
                )
                queried = safe_float(order_info.get("avgPrice"))
                if queried > 0:
                    return queried
                executed = safe_float(order_info.get("executedQty"))
                cum_quote = safe_float(order_info.get("cumQuote"))
                if executed > 0 and cum_quote > 0:
                    return cum_quote / executed
            except Exception as exc:
                error_logger.warning(
                    "Could not query fill price for %s order %s: %s",
                    symbol,
                    order_id,
                    exc,
                )

        live_price = self.get_market_price(symbol)
        if live_price is not None and live_price > 0:
            return live_price
        return fallback

    def has_open_position(self, symbol: str, position_side: str) -> bool:
        try:
            positions = self._throttled_call(
                self.client.futures_position_information,
                symbol=symbol,
            )
            for pos in positions:
                if pos.get("positionSide") == position_side:
                    if safe_float(pos.get("positionAmt")) != 0.0:
                        return True
            return False
        except Exception as exc:
            error_logger.error("Position check failed for %s: %s", symbol, exc)
            return False

    def get_position_quantity(self, symbol: str, position_side: str) -> float:
        try:
            positions = self._throttled_call(
                self.client.futures_position_information,
                symbol=symbol,
            )
            for pos in positions:
                if pos.get("positionSide") == position_side:
                    return abs(safe_float(pos.get("positionAmt")))
            return 0.0
        except Exception as exc:
            error_logger.error("Position size fetch failed for %s: %s", symbol, exc)
            return 0.0

    # ---------------- Orders ----------------

    def execute_futures_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        quantity: float,
        price: Optional[float] = None,
        reduce_only: bool = False,
    ) -> Optional[dict[str, Any]]:
        rules = self.get_symbol_rules(symbol)
        clean_qty = round_step_size(
            quantity, rules.step_size, rules.quantity_precision
        )
        if clean_qty < rules.min_qty:
            raise OrderExecutionError(
                f"Quantity {clean_qty} below min_qty {rules.min_qty} for {symbol}"
            )

        notional = clean_qty * (price or safe_float(self.get_market_price(symbol)))
        if notional < rules.min_notional:
            raise OrderExecutionError(
                f"Notional ${notional:.2f} below minimum ${rules.min_notional:.2f}"
            )

        order_params: dict[str, Any] = {
            "symbol": symbol,
            "side": side.upper(),
            "positionSide": position_side.upper(),
            "quantity": f"{clean_qty:.{rules.quantity_precision}f}",
        }

        if price is not None:
            clean_price = round_step_size(price, rules.tick_size, rules.price_precision)
            order_params["type"] = "LIMIT"
            order_params["price"] = f"{clean_price:.{rules.price_precision}f}"
            order_params["timeInForce"] = "GTC"
        else:
            order_params["type"] = "MARKET"

        try:
            response = self._throttled_call(
                self.client.futures_create_order,
                **order_params,
                **self.recv_window_param,
            )
            self.invalidate_balance_cache()
            trade_logger.info(
                "Order filled | %s | %s %s | qty=%s | reduce_only=%s",
                symbol,
                side,
                position_side,
                order_params["quantity"],
                reduce_only,
            )
            return response
        except BinanceAPIException as exc:
            error_logger.error(
                "Binance rejected order on %s: %s (code=%s)",
                symbol,
                exc.message,
                exc.code,
            )
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order rejected on {symbol} {side} {position_side}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(exc.message) from exc
        except BinanceOrderException as exc:
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Order exception on {symbol}: {exc.message}",
                    exc=exc,
                )
            raise OrderExecutionError(exc.message) from exc
        except Exception as exc:
            if self._critical_alerts:
                self._critical_alerts.notify(
                    "ORDER_FAILURE",
                    f"Unexpected order failure on {symbol}: {exc}",
                    exc=exc,
                )
            raise OrderExecutionError(str(exc)) from exc

    def close_position_quantity(
        self,
        symbol: str,
        position_side: str,
        quantity: float,
    ) -> Optional[dict[str, Any]]:
        close_side = "SELL" if position_side.upper() == "LONG" else "BUY"
        response = self.execute_futures_order(
            symbol=symbol,
            side=close_side,
            position_side=position_side,
            quantity=quantity,
        )
        self.invalidate_balance_cache()
        return response
