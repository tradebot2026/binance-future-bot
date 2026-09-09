"""
Trade management module.
Monitors open positions with metadata-driven partial exits (33/33/34),
dynamic break-even and TP-trailing stop loss, and exchange reconciliation.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from config import Config
from constants import (
    STRATEGY_RANGE_REVERSION,
    TRADE_STATUS_CLOSED,
    TRADE_STATUS_TP1_HIT,
    TRADE_STATUS_TP2_HIT,
    is_range_strategy,
)
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exceptions import OrderExecutionError
from logger import error_logger, trade_logger
from reconciliation import (
    confirm_external_close_allowed,
    is_within_position_grace_period,
    position_reconcile_guard,
)
from utils import round_step_size, safe_float, utc_now, utc_today_str

if TYPE_CHECKING:
    from telegram_bot import TelegramManager
    from scheduler import DailyScheduler
    from risk_manager import RiskManager


class TradeManager:
    """Algorithmic virtual SL/TP manager using absolute quantities from trade metadata."""

    TRAILING_ATR_MULTIPLIER = 1.0
    CLOSE_ORDER_MAX_RETRIES = 3
    CLOSE_ORDER_RETRY_DELAY_SECONDS = 1.0

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
        telegram: Optional["TelegramManager"] = None,
        scheduler: Optional["DailyScheduler"] = None,
        risk_manager: Optional["RiskManager"] = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.telegram = telegram
        self.scheduler = scheduler
        self.risk_manager = risk_manager
        self._monitored_symbols: set[str] = set()
        self._monitored_lock = threading.Lock()
        self._latest_ticks: dict[str, float] = {}
        self._tick_lock = threading.Lock()
        self._tick_event = threading.Event()
        self._monitor_stop = threading.Event()
        self._tick_worker = threading.Thread(
            target=self._price_tick_worker,
            name="price-tick-worker",
            daemon=True,
        )
        self._fast_monitor = threading.Thread(
            target=self._fast_monitor_loop,
            name="tp-sl-fast-monitor",
            daemon=True,
        )
        self._tick_worker.start()
        self._fast_monitor.start()

    def note_open_symbol(self, symbol: str) -> None:
        """Register a symbol for immediate WS tick monitoring after entry."""
        with self._monitored_lock:
            self._monitored_symbols.add(str(symbol).upper())

    def on_price_tick(self, symbol: str, price: float) -> None:
        """Queue open-trade evaluation on miniTicker updates (non-blocking for WS thread)."""
        if price <= 0:
            return
        symbol = symbol.upper()
        with self._monitored_lock:
            monitored = symbol in self._monitored_symbols
        if not monitored:
            if not self.db.get_open_trades_for_symbol(symbol):
                return
            with self._monitored_lock:
                self._monitored_symbols.add(symbol)
        with self._tick_lock:
            self._latest_ticks[symbol] = price
        self._tick_event.set()

    def _price_tick_worker(self) -> None:
        """Coalesce miniTicker bursts — always evaluate the latest price per symbol."""
        while not self._monitor_stop.is_set():
            self._tick_event.wait(timeout=0.25)
            self._tick_event.clear()
            with self._tick_lock:
                batch = dict(self._latest_ticks)
                self._latest_ticks.clear()
            for symbol, price in batch.items():
                try:
                    self._process_price_tick(symbol, price)
                except Exception as exc:
                    error_logger.error(
                        "Price tick worker failed for %s: %s",
                        symbol,
                        exc,
                        exc_info=True,
                    )

    def _fast_monitor_loop(self) -> None:
        """Dedicated 1s TP/SL loop — never waits on REST."""
        while not self._monitor_stop.wait(1.0):
            try:
                self.monitor_open_trades(ws_only=True)
            except Exception as exc:
                error_logger.error("Fast TP/SL monitor error: %s", exc, exc_info=True)

    def _trade_quantity_from_db(self, trade: dict[str, Any]) -> float:
        metadata = self.db.parse_trade_metadata(trade)
        original = safe_float(metadata.get("original_quantity"))
        if original > 0:
            return original
        return safe_float(trade.get("quantity"))

    def _process_price_tick(self, symbol: str, price: float) -> None:
        """Evaluate open trades for a symbol (REST-free, off WS callback thread)."""
        active_trades = [
            trade
            for trade in self.db.get_open_trades()
            if str(trade.get("symbol", "")).upper() == symbol
        ]
        if not active_trades:
            with self._monitored_lock:
                self._monitored_symbols.discard(symbol)
            return

        qty_map = self._position_qty_map(active_trades, ws_only=True)
        for trade in active_trades:
            try:
                self._monitor_single_trade(
                    trade, qty_map, price=price, ws_only=True
                )
            except Exception as exc:
                error_logger.error(
                    "Price tick monitor failed for %s: %s",
                    symbol,
                    exc,
                    exc_info=True,
                )

    def monitor_open_trades(self, *, ws_only: bool = False) -> None:
        """Evaluate all active DB trades against live prices and exchange state."""
        try:
            active_trades = self.db.get_open_trades()
            with self._monitored_lock:
                self._monitored_symbols = {
                    str(trade.get("symbol", "")).upper() for trade in active_trades
                }
            if not active_trades:
                return

            if not ws_only and not self.exchange.rest_account_reads_blocked():
                self.exchange.ensure_positions_cached(force=False)

            qty_map = self._position_qty_map(active_trades, ws_only=ws_only)

            for trade in active_trades:
                try:
                    self._monitor_single_trade(trade, qty_map, ws_only=ws_only)
                except Exception as exc:
                    error_logger.error(
                        "Trade monitor failed for %s: %s",
                        trade.get("symbol", "?"),
                        exc,
                        exc_info=True,
                    )
        except Exception as exc:
            error_logger.error("Trade monitoring failed: %s", exc, exc_info=True)

    def _position_qty_map(
        self,
        active_trades: list[dict[str, Any]],
        *,
        ws_only: bool = False,
    ) -> dict[tuple[str, str], float]:
        """WS/cache/DB quantities; REST bulk refresh only on the slow monitor path."""
        qty_map: dict[tuple[str, str], float] = {}
        for trade in active_trades:
            symbol = str(trade.get("symbol", "")).upper()
            side = str(trade.get("side", "LONG")).upper()
            if ws_only:
                qty = self.exchange.get_position_quantity_cached(symbol, side)
            else:
                qty = self.exchange.get_position_quantity(symbol, side)
            if qty > 0:
                qty_map[(symbol, side)] = qty
                continue
            db_qty = self._trade_quantity_from_db(trade)
            if db_qty > 0:
                qty_map[(symbol, side)] = db_qty

        if ws_only:
            return qty_map

        needs_bulk = any(
            qty_map.get(
                (str(t.get("symbol", "")).upper(), str(t.get("side", "LONG")).upper()),
                0.0,
            )
            <= 0
            for t in active_trades
        )
        if needs_bulk and not self.exchange.rest_account_reads_blocked():
            for pos in self.exchange.get_all_open_positions(force_refresh=False):
                symbol = str(pos.get("symbol", "")).upper()
                side = str(pos.get("positionSide", "")).upper()
                qty = safe_float(pos.get("quantity"))
                if qty > 0:
                    qty_map[(symbol, side)] = qty
        return qty_map

    def _monitor_single_trade(
        self,
        trade: dict[str, Any],
        qty_map: dict[tuple[str, str], float],
        price: Optional[float] = None,
        *,
        ws_only: bool = False,
    ) -> None:
        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        key = (str(symbol).upper(), str(position_side).upper())

        live_qty = qty_map.get(key, 0.0)
        if live_qty <= 0:
            db_qty = self._trade_quantity_from_db(trade)
            if db_qty > 0:
                live_qty = db_qty
                qty_map[key] = db_qty

        if live_qty <= 0 and not ws_only and not self.exchange.rest_account_reads_blocked():
            rest_qty = self.exchange.get_position_quantity_rest(symbol, position_side)
            if rest_qty is not None and rest_qty > 0:
                position_reconcile_guard.note_present(str(trade["trade_id"]))
                trade_logger.info(
                    "[%s] WS/cache missed position — REST confirms qty=%.8f",
                    symbol,
                    rest_qty,
                )
                live_qty = rest_qty
                qty_map[key] = rest_qty

        if live_qty <= 0:
            if ws_only:
                trade_logger.debug(
                    "[%s] TP/SL eval skipped — qty unavailable (WS-only path).",
                    symbol,
                )
                return
            if self._defer_external_close(trade, symbol):
                return
            self._mark_trade_closed(
                trade,
                reason="RECONCILED_EXTERNAL_CLOSE",
                exit_price=safe_float(
                    price
                    or self.exchange.get_market_price(symbol, position_side)
                ),
            )
            position_reconcile_guard.note_present(str(trade["trade_id"]))
            return

        position_reconcile_guard.note_present(str(trade["trade_id"]))

        if price is None or price <= 0:
            price = self.exchange.get_market_price(symbol, position_side)
        if price is None or price <= 0:
            trade_logger.warning(
                "[%s] TP/SL eval skipped — no mark/ticker price available.",
                symbol,
            )
            return

        fresh = self.db.get_trade(trade["trade_id"])
        if fresh:
            trade = fresh

        if is_range_strategy(str(trade.get("strategy", ""))):
            if self._check_range_hard_exits(trade, price):
                return

        if position_side == "LONG":
            self._manage_long_trade(trade, price)
        elif position_side == "SHORT":
            self._manage_short_trade(trade, price)

    _TF_BAR_SECONDS: dict[str, int] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
    }

    def _defer_external_close(self, trade: dict[str, Any], symbol: str) -> bool:
        """
        True when an external close should NOT run yet (grace period, pending
        confirms, or REST did not confirm positionAmt == 0).
        """
        trade_id = str(trade["trade_id"])
        if is_within_position_grace_period(trade):
            trade_logger.debug(
                "[%s] Position reconcile deferred — within %ss grace period.",
                symbol,
                int(Config.POSITION_GRACE_PERIOD_SECONDS),
            )
            return True

        miss_count, _ = position_reconcile_guard.register_missing(trade)
        if not confirm_external_close_allowed(self.exchange, trade):
            trade_logger.debug(
                "[%s] External close deferred (%s/%s checks, REST not confirmed flat).",
                symbol,
                miss_count,
                Config.POSITION_RECONCILE_MISS_THRESHOLD,
            )
            return True
        return False

    def _entry_bar_seconds(self, trade: dict[str, Any]) -> int:
        metadata = self.db.parse_trade_metadata(trade)
        tf = str(metadata.get("entry_timeframe") or Config.ENTRY_TIMEFRAME)
        return self._TF_BAR_SECONDS.get(tf, 300)

    def _range_bars_elapsed(self, trade: dict[str, Any]) -> int:
        opened_at_raw = trade.get("opened_at")
        if not opened_at_raw:
            return 0
        try:
            opened_at = datetime.fromisoformat(str(opened_at_raw))
            if opened_at.tzinfo is None:
                opened_at = opened_at.replace(tzinfo=timezone.utc)
            elapsed = (utc_now() - opened_at).total_seconds()
            bar_seconds = self._entry_bar_seconds(trade)
            return int(elapsed // bar_seconds) if bar_seconds > 0 else 0
        except ValueError:
            return 0

    def _fetch_confirm_adx(self, symbol: str) -> float:
        try:
            import ta

            df = self.exchange.fetch_historical_candles(
                symbol, Config.CONFIRM_TIMEFRAME, limit=80, allow_rest=False
            )
            if df.empty or len(df) < 20:
                return 0.0
            df = df.copy()
            df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
            return safe_float(df["adx"].iloc[-1])
        except Exception as exc:
            error_logger.warning("ADX fetch failed for %s: %s", symbol, exc)
            return 0.0

    def _fetch_entry_tf_last_closed_close(self, symbol: str, trade: dict[str, Any]) -> Optional[float]:
        """Return the close of the last fully closed entry-TF candle."""
        metadata = self.db.parse_trade_metadata(trade)
        tf = str(metadata.get("entry_timeframe") or Config.ENTRY_TIMEFRAME)
        try:
            df = self.exchange.fetch_historical_candles(
                symbol, tf, limit=5, allow_rest=False
            )
            if df.empty or len(df) < 2:
                return None
            return safe_float(df.iloc[-2]["close"])
        except Exception as exc:
            error_logger.warning(
                "Failed to fetch closed bar for %s (%s): %s", symbol, tf, exc
            )
            return None

    def _check_range_boundary_breakout(self, trade: dict[str, Any]) -> bool:
        """
        Exit only when a bar CLOSES beyond the range boundary on a bar AFTER entry.
        Avoids immediate exit on the entry candle or intra-bar wicks.
        """
        if self._range_bars_elapsed(trade) < 1:
            return False

        metadata = self.db.parse_trade_metadata(trade)
        atr = safe_float(metadata.get("atr_at_entry"))
        range_high = safe_float(metadata.get("range_high"))
        range_low = safe_float(metadata.get("range_low"))
        if atr <= 0 or range_high <= 0 or range_low <= 0:
            return False

        close = self._fetch_entry_tf_last_closed_close(trade["symbol"], trade)
        if close is None or close <= 0:
            return False

        breakout_buffer = atr * Config.RANGE_BREAKOUT_ATR_MULT
        side = str(trade.get("side", "LONG")).upper()

        if side == "LONG" and close < range_low - breakout_buffer:
            trade_logger.info(
                "[%s] RANGE boundary breakout exit (LONG) | close=%.6f range_low=%.6f buffer=%.6f",
                trade.get("symbol"),
                close,
                range_low,
                breakout_buffer,
            )
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="RANGE_BOUNDARY_BREAKOUT",
            )
            return True

        if side == "SHORT" and close > range_high + breakout_buffer:
            trade_logger.info(
                "[%s] RANGE boundary breakout exit (SHORT) | close=%.6f range_high=%.6f buffer=%.6f",
                trade.get("symbol"),
                close,
                range_high,
                breakout_buffer,
            )
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="RANGE_BOUNDARY_BREAKOUT",
            )
            return True

        return False

    def _check_range_hard_exits(self, trade: dict[str, Any], price: float) -> bool:
        """Range kill rules: ADX breakout, range boundary violation, time stop."""
        metadata = self.db.parse_trade_metadata(trade)

        if self._range_bars_elapsed(trade) >= Config.RANGE_TIME_STOP_BARS:
            trade_logger.info(
                "[%s] RANGE time stop (%s bars).",
                trade.get("symbol"),
                Config.RANGE_TIME_STOP_BARS,
            )
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="RANGE_TIME_STOP",
            )
            return True

        if self._check_range_boundary_breakout(trade):
            return True

        adx_15m = self._fetch_confirm_adx(trade["symbol"])
        if adx_15m >= Config.RANGE_EXIT_ADX_15M:
            trade_logger.info(
                "[%s] RANGE ADX exit | 15m ADX=%.1f >= %.1f",
                trade.get("symbol"),
                adx_15m,
                Config.RANGE_EXIT_ADX_15M,
            )
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="RANGE_ADX_BREAKOUT",
            )
            return True

        return False

    def _manage_long_trade(self, trade: dict[str, Any], current_price: float) -> None:
        for _ in range(4):
            trade = self.db.get_trade(trade["trade_id"]) or trade
            if trade.get("status") == TRADE_STATUS_CLOSED:
                return

            metadata = self.db.parse_trade_metadata(trade)

            stop_loss = safe_float(trade.get("stop_loss"))
            if stop_loss > 0 and current_price <= stop_loss:
                trade_logger.warning(
                    "[%s] STOP LOSS breach LONG | price=%.6f <= sl=%.6f",
                    trade.get("symbol"),
                    current_price,
                    stop_loss,
                )
                self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason="STOP_LOSS",
                )
                return

            if Config.ENABLE_TRAILING_STOP and metadata.get("trailing_active"):
                self._apply_trailing_stop(trade, current_price, is_long=True)

            metadata = self.db.parse_trade_metadata(trade)
            tp1 = safe_float(trade.get("take_profit_1"))
            tp2 = safe_float(trade.get("take_profit_2"))
            tp3 = safe_float(trade.get("take_profit_3"))

            acted = False
            if tp3 > 0 and current_price >= tp3 and not metadata.get("tp3_executed"):
                trade_logger.info(
                    "[%s] TP3 breach LONG | price=%.6f >= tp3=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp3,
                )
                self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
                acted = True
            elif tp2 > 0 and current_price >= tp2 and not metadata.get("tp2_executed"):
                trade_logger.info(
                    "[%s] TP2 breach LONG | price=%.6f >= tp2=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp2,
                )
                self._handle_take_profit(trade, level="TP2", reason="TP2")
                acted = True
            elif tp1 > 0 and current_price >= tp1 and not metadata.get("tp1_executed"):
                trade_logger.info(
                    "[%s] TP1 breach LONG | price=%.6f >= tp1=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp1,
                )
                self._handle_take_profit(trade, level="TP1", reason="TP1")
                acted = True

            if not acted:
                break

    def _manage_short_trade(self, trade: dict[str, Any], current_price: float) -> None:
        for _ in range(4):
            trade = self.db.get_trade(trade["trade_id"]) or trade
            if trade.get("status") == TRADE_STATUS_CLOSED:
                return

            metadata = self.db.parse_trade_metadata(trade)

            stop_loss = safe_float(trade.get("stop_loss"))
            if stop_loss > 0 and current_price >= stop_loss:
                trade_logger.warning(
                    "[%s] STOP LOSS breach SHORT | price=%.6f >= sl=%.6f",
                    trade.get("symbol"),
                    current_price,
                    stop_loss,
                )
                self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason="STOP_LOSS",
                )
                return

            if Config.ENABLE_TRAILING_STOP and metadata.get("trailing_active"):
                self._apply_trailing_stop(trade, current_price, is_long=False)

            metadata = self.db.parse_trade_metadata(trade)
            tp1 = safe_float(trade.get("take_profit_1"))
            tp2 = safe_float(trade.get("take_profit_2"))
            tp3 = safe_float(trade.get("take_profit_3"))

            acted = False
            if tp3 > 0 and current_price <= tp3 and not metadata.get("tp3_executed"):
                trade_logger.info(
                    "[%s] TP3 breach SHORT | price=%.6f <= tp3=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp3,
                )
                self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
                acted = True
            elif tp2 > 0 and current_price <= tp2 and not metadata.get("tp2_executed"):
                trade_logger.info(
                    "[%s] TP2 breach SHORT | price=%.6f <= tp2=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp2,
                )
                self._handle_take_profit(trade, level="TP2", reason="TP2")
                acted = True
            elif tp1 > 0 and current_price <= tp1 and not metadata.get("tp1_executed"):
                trade_logger.info(
                    "[%s] TP1 breach SHORT | price=%.6f <= tp1=%.6f",
                    trade.get("symbol"),
                    current_price,
                    tp1,
                )
                self._handle_take_profit(trade, level="TP1", reason="TP1")
                acted = True

            if not acted:
                break

    def _handle_take_profit(
        self,
        trade: dict[str, Any],
        level: str,
        reason: str,
    ) -> None:
        metadata = self.db.parse_trade_metadata(trade)
        executed_key = f"{level.lower()}_executed"

        if metadata.get(executed_key):
            return

        if not Config.ENABLE_PARTIAL_TP:
            quantity = self._remaining_close_quantity(trade)
            self._close_position(trade, quantity=quantity, reason=reason)
            return

        quantity_key = f"{level.lower()}_quantity"
        close_qty = safe_float(metadata.get(quantity_key))
        if close_qty <= 0:
            close_qty = self._remaining_close_quantity(trade)

        if level == "TP3":
            close_qty = self._remaining_close_quantity(trade)

        # Lock the TP level immediately to prevent repeated partial closes.
        metadata[executed_key] = True
        self.db.update_trade(trade["trade_id"], {"metadata": metadata})
        trade["metadata"] = metadata

        if not self._close_position(trade, quantity=close_qty, reason=reason, partial=True):
            metadata[executed_key] = False
            self.db.update_trade(trade["trade_id"], {"metadata": metadata})
            return

        updates: dict[str, Any] = {"metadata": metadata}
        new_sl: Optional[float] = None

        if level == "TP1":
            updates["status"] = TRADE_STATUS_TP1_HIT
            if Config.ENABLE_BREAK_EVEN:
                new_sl = safe_float(trade.get("entry_price"))
                updates["stop_loss"] = new_sl
                trade_logger.info(
                    "[%s] TP1 hit — stop loss moved to break-even (%.6f).",
                    trade["symbol"],
                    new_sl,
                )
        elif level == "TP2":
            updates["status"] = TRADE_STATUS_TP2_HIT
            new_sl = safe_float(trade.get("take_profit_1"))
            if new_sl > 0:
                updates["stop_loss"] = new_sl
                trade_logger.info(
                    "[%s] TP2 hit — stop loss moved to TP1 (%.6f).",
                    trade["symbol"],
                    new_sl,
                )
        elif level == "TP3":
            updates["status"] = TRADE_STATUS_CLOSED

        self.db.update_trade(trade["trade_id"], updates)
        trade.update(updates)

        if self.telegram:
            self.telegram.send_tp_level_alert(
                symbol=trade["symbol"],
                level=level,
                reason=reason,
                new_sl=new_sl,
            )

    def _apply_trailing_stop(
        self,
        trade: dict[str, Any],
        current_price: float,
        is_long: bool,
    ) -> None:
        metadata = self.db.parse_trade_metadata(trade)
        atr = safe_float(metadata.get("atr_at_entry"))
        if atr <= 0:
            return

        entry = safe_float(trade.get("entry_price"))
        current_sl = safe_float(trade.get("stop_loss"))
        best_price = safe_float(metadata.get("best_price"), entry)
        trail_distance = atr * self.TRAILING_ATR_MULTIPLIER
        rules = self.exchange.get_symbol_rules(trade["symbol"])

        updated = False
        if is_long:
            if current_price > best_price:
                best_price = current_price
                metadata["best_price"] = best_price
                updated = True
            if current_price > entry:
                metadata["trailing_active"] = True
                candidate_sl = round_step_size(
                    best_price - trail_distance,
                    rules.tick_size,
                    rules.price_precision,
                )
                if candidate_sl > current_sl:
                    self.db.update_trade(
                        trade["trade_id"],
                        {"stop_loss": candidate_sl, "metadata": metadata},
                    )
                    trade["stop_loss"] = candidate_sl
                    updated = True
        else:
            if current_price < best_price:
                best_price = current_price
                metadata["best_price"] = best_price
                updated = True
            if current_price < entry:
                metadata["trailing_active"] = True
                candidate_sl = round_step_size(
                    best_price + trail_distance,
                    rules.tick_size,
                    rules.price_precision,
                )
                if candidate_sl < current_sl:
                    self.db.update_trade(
                        trade["trade_id"],
                        {"stop_loss": candidate_sl, "metadata": metadata},
                    )
                    trade["stop_loss"] = candidate_sl
                    updated = True

        if updated and metadata.get("trailing_active"):
            self.db.update_trade(trade["trade_id"], {"metadata": metadata})

    def _remaining_close_quantity(self, trade: dict[str, Any]) -> float:
        """Prefer exchange-reported size; fall back to metadata remainder."""
        position_side = trade.get("side", "LONG")
        live_qty = self.exchange.get_position_quantity(trade["symbol"], position_side)
        if live_qty > 0:
            return live_qty

        metadata = self.db.parse_trade_metadata(trade)
        original = safe_float(metadata.get("original_quantity"), safe_float(trade.get("quantity")))
        remaining = original
        for key in ("tp1_quantity", "tp2_quantity", "tp3_quantity"):
            executed_key = key.replace("_quantity", "_executed")
            if metadata.get(executed_key):
                remaining -= safe_float(metadata.get(key))
        return max(remaining, 0.0)

    def _resolve_close_quantity(
        self,
        trade: dict[str, Any],
        requested_qty: float,
    ) -> float:
        """Clamp requested quantity to live position size with step-size rounding."""
        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        rules = self.exchange.get_symbol_rules(symbol)

        live_qty = self.exchange.get_position_quantity_cached(symbol, position_side)
        if live_qty <= 0:
            live_qty = self._trade_quantity_from_db(trade)
        if live_qty <= 0:
            live_qty = self.exchange.get_position_quantity(symbol, position_side)
        if live_qty <= 0:
            return 0.0

        qty = min(requested_qty, live_qty)
        return round_step_size(qty, rules.step_size, rules.quantity_precision)

    def _close_position(
        self,
        trade: dict[str, Any],
        quantity: float,
        reason: str,
        partial: bool = False,
    ) -> bool:
        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        trade_id = trade["trade_id"]

        close_qty = self._resolve_close_quantity(trade, quantity)
        if close_qty <= 0:
            if self._defer_external_close(trade, symbol):
                return False
            self._mark_trade_closed(
                trade,
                reason="RECONCILED_EXTERNAL_CLOSE",
                exit_price=safe_float(
                    self.exchange.get_market_price(symbol, position_side)
                ),
            )
            position_reconcile_guard.note_present(str(trade["trade_id"]))
            return False

        try:
            response: Optional[dict[str, Any]] = None
            for attempt in range(self.CLOSE_ORDER_MAX_RETRIES):
                try:
                    with self.exchange.execution_context():
                        response = self.exchange.close_position_quantity(
                            symbol=symbol,
                            position_side=position_side,
                            quantity=close_qty,
                        )
                    break
                except OrderExecutionError as exc:
                    if attempt >= self.CLOSE_ORDER_MAX_RETRIES - 1:
                        error_logger.error(
                            "Close order failed for %s (%s) after %s attempts: %s",
                            symbol,
                            reason,
                            self.CLOSE_ORDER_MAX_RETRIES,
                            exc,
                        )
                        return False
                    delay = self.CLOSE_ORDER_RETRY_DELAY_SECONDS * (attempt + 1)
                    trade_logger.warning(
                        "[%s] Close retry %s/%s in %.1fs | reason=%s | err=%s",
                        symbol,
                        attempt + 2,
                        self.CLOSE_ORDER_MAX_RETRIES,
                        delay,
                        reason,
                        exc,
                    )
                    time.sleep(delay)
        except OrderExecutionError as exc:
            error_logger.error("Close order failed for %s (%s): %s", symbol, reason, exc)
            return False

        if not response:
            return False

        exit_price = safe_float(response.get("avgPrice"))
        if exit_price <= 0:
            exit_price = safe_float(
                self.exchange.get_market_price(symbol, position_side)
            )

        realized = self._calculate_realized_pnl(
            side=position_side,
            entry=safe_float(trade.get("entry_price")),
            exit_price=exit_price,
            quantity=close_qty,
        )

        cumulative_pnl = safe_float(trade.get("pnl")) + realized
        self.db.update_trade(trade_id, {"pnl": cumulative_pnl})
        trade["pnl"] = cumulative_pnl

        self.db.add_daily_realized_pnl(utc_today_str(), realized)

        trade_logger.info(
            "[%s] Closed qty=%s | reason=%s | pnl=%.4f",
            symbol,
            close_qty,
            reason,
            realized,
        )

        live_remaining = self.exchange.get_position_quantity(symbol, position_side)
        is_full_close = live_remaining <= 0 or not partial

        if is_full_close:
            self._mark_trade_closed(trade, reason=reason, exit_price=exit_price)
        else:
            remaining_qty = self.exchange.get_position_quantity(symbol, position_side)
            if remaining_qty <= 0:
                self.exchange.clear_position_cache(symbol, position_side)
            elif self.telegram and "STOP" in reason.upper():
                self.telegram.send_close_alert(symbol=symbol, reason=reason, pnl=realized)

        return True

    def _calculate_realized_pnl(
        self,
        side: str,
        entry: float,
        exit_price: float,
        quantity: float,
    ) -> float:
        if side == "LONG":
            return (exit_price - entry) * quantity
        return (entry - exit_price) * quantity

    def _mark_trade_closed(
        self,
        trade: dict[str, Any],
        reason: str,
        exit_price: float,
    ) -> None:
        trade_id = trade["trade_id"]
        symbol = trade["symbol"]
        position_side = str(trade.get("side", "LONG")).upper()
        closed_at = utc_now().isoformat()

        opened_at_raw = trade.get("opened_at")
        duration: Optional[int] = None
        if opened_at_raw:
            try:
                opened_at = datetime.fromisoformat(str(opened_at_raw))
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)
                duration = int((utc_now() - opened_at).total_seconds())
            except ValueError:
                duration = None

        self.db.update_trade(
            trade_id,
            {
                "status": TRADE_STATUS_CLOSED,
                "closed_at": closed_at,
                "exit_reason": reason,
                "duration": duration,
            },
        )
        position_reconcile_guard.note_present(str(trade_id))
        self.exchange.clear_position_cache(symbol, position_side)
        with self._monitored_lock:
            self._monitored_symbols.discard(str(symbol).upper())
        self.exchange.invalidate_balance_cache()
        balance = self.exchange.get_futures_balance(force_refresh=False)
        self.db.record_closed_trade(
            date_str=utc_today_str(),
            current_balance=balance,
        )

        trade_logger.info(
            "[%s] Trade %s fully closed | reason=%s | total_pnl=%.4f",
            symbol,
            trade_id[:8],
            reason,
            safe_float(trade.get("pnl")),
        )

        if self.telegram:
            self.telegram.send_close_alert(
                symbol=symbol,
                reason=reason,
                pnl=safe_float(trade.get("pnl")),
                strategy=str(trade.get("strategy", "")),
            )

        outcome = "WIN" if safe_float(trade.get("pnl")) > 0 else "LOSS"
        strategy = str(trade.get("strategy", ""))
        pnl = safe_float(trade.get("pnl"))
        reason_upper = reason.upper()

        soft_exit = reason in (
            "RANGE_TIME_STOP",
            "RANGE_BOUNDARY_BREAKOUT",
        ) and pnl >= 0

        if is_range_strategy(strategy):
            if reason == "STOP_LOSS" or (pnl < 0 and "STOP" in reason_upper):
                self.db.set_symbol_cooldown(
                    symbol,
                    Config.RANGE_COOLDOWN_MINUTES,
                    reason="RANGE_STOP_LOSS",
                )
            elif soft_exit:
                self.db.set_symbol_cooldown(
                    symbol,
                    Config.RANGE_COOLDOWN_SOFT_MINUTES,
                    reason="RANGE_SOFT_EXIT",
                )
            else:
                self.db.set_symbol_cooldown(
                    symbol,
                    Config.RANGE_COOLDOWN_MINUTES,
                    reason="RANGE_CLOSE",
                )
        elif reason == "STOP_LOSS" or (pnl < 0 and "STOP" in reason_upper):
            outcome = "LOSS"
            self.db.set_symbol_cooldown(
                symbol,
                Config.SYMBOL_COOLDOWN_MINUTES,
                reason="STOP_LOSS",
            )
        elif soft_exit:
            self.db.set_symbol_cooldown(
                symbol,
                Config.SYMBOL_COOLDOWN_SOFT_MINUTES,
                reason="SOFT_EXIT",
            )
        elif pnl > 0:
            outcome = "WIN"

        self.db.record_signal_outcome(trade_id, outcome)

        if self.scheduler:
            self.scheduler.notify_trade_event()
        if self.risk_manager:
            self.risk_manager.notify_trade_event()

    def close_all_positions(self, reason: str = "MANUAL_CLOSE_ALL") -> dict[str, Any]:
        """Close all tracked DB trades and any remaining exchange positions."""
        closed: list[str] = []
        failed: list[str] = []
        handled: set[tuple[str, str]] = set()

        for trade in self.db.get_open_trades():
            symbol = str(trade.get("symbol", ""))
            position_side = str(trade.get("side", "LONG"))
            handled.add((symbol, position_side))
            try:
                ok = self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason=reason,
                )
                if ok:
                    closed.append(f"{symbol} {position_side}")
                else:
                    failed.append(f"{symbol} {position_side}")
            except Exception as exc:
                error_logger.error("Close-all failed for %s %s: %s", symbol, position_side, exc)
                failed.append(f"{symbol} {position_side}: {exc}")

        for pos in self.exchange.fetch_open_positions():
            symbol = str(pos.get("symbol", ""))
            position_side = str(pos.get("positionSide", ""))
            if (symbol, position_side) in handled:
                continue
            quantity = safe_float(pos.get("quantity"))
            if quantity <= 0:
                continue
            try:
                response = self.exchange.close_position_quantity(
                    symbol, position_side, quantity
                )
                if response:
                    closed.append(f"{symbol} {position_side} (exchange-only)")
                else:
                    failed.append(f"{symbol} {position_side} (exchange-only)")
            except Exception as exc:
                error_logger.error(
                    "Close-all exchange-only failed for %s %s: %s",
                    symbol,
                    position_side,
                    exc,
                )
                failed.append(f"{symbol} {position_side}: {exc}")

        return {"closed": closed, "failed": failed}
