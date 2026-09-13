"""
Trade management module.
Monitors open positions with metadata-driven partial exits (33/33/34),
dynamic break-even and TP-trailing stop loss, and exchange reconciliation.
"""

from __future__ import annotations

import queue
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from config import Config
from constants import (
    STRATEGY_RANGE_REVERSION,
    TP3_PORTION,
    TRADE_STATUS_CLOSED,
    TRADE_STATUS_TP1_HIT,
    TRADE_STATUS_TP2_HIT,
    is_range_strategy,
)
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exceptions import OrderExecutionError, PositionAlreadyClosedError
from logger import error_logger, trade_logger
from exit_coordinator import claim_exit, exit_claim_active, release_exit
from reconciliation import (
    confirm_external_close_allowed,
    is_exchange_sync_close_reason,
    is_within_position_grace_period,
    position_reconcile_guard,
    resolve_exchange_close_pnl,
)
from utils import escape_html, round_step_size, safe_float, utc_now, utc_today_str

if TYPE_CHECKING:
    from telegram_bot import TelegramManager
    from scheduler import DailyScheduler
    from risk_manager import RiskManager


@dataclass
class _CloseTask:
    trade: dict[str, Any]
    quantity: float
    reason: str
    partial: bool
    tp_level: Optional[str] = None
    result_event: Optional[threading.Event] = None
    result_box: Optional[list[bool]] = None


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
        self._close_queue: queue.Queue[Optional[_CloseTask]] = queue.Queue()
        self._close_inflight: set[str] = set()
        self._close_lock = threading.Lock()
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
        self._close_worker = threading.Thread(
            target=self._close_worker_loop,
            name="close-order-worker",
            daemon=True,
        )
        self._tick_worker.start()
        self._fast_monitor.start()
        self._close_worker.start()
        self._register_open_trade_symbols()
        from core.ops_heartbeat import touch_monitor_loop

        touch_monitor_loop(source="manager_init")

    def _register_open_trade_symbols(self) -> None:
        """Ensure every open DB trade receives miniTicker-driven TP/SL evaluation."""
        for trade in self.db.get_open_trades():
            symbol = str(trade.get("symbol", "")).upper()
            if symbol:
                with self._monitored_lock:
                    self._monitored_symbols.add(symbol)

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
            if batch:
                from core.ops_heartbeat import touch_monitor_loop

                touch_monitor_loop(source="price_tick_worker")
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
        """Dedicated 1s TP/SL loop with live price REST fallback when WS ticks stall."""
        from core.ops_heartbeat import touch_monitor_loop

        while not self._monitor_stop.wait(1.0):
            try:
                self._prefetch_live_prices_for_open_trades()
                self.monitor_open_trades(ws_only=True)
                touch_monitor_loop(source="position_monitor")
            except Exception as exc:
                error_logger.error("Fast TP/SL monitor error: %s", exc, exc_info=True)
                error_logger.error(traceback.format_exc())

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

            if not Config.ENABLE_SOFT_TP_SL:
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
                db_qty = self._trade_quantity_from_db(trade)
                if db_qty <= 0:
                    trade_logger.debug(
                        "[%s] TP/SL eval skipped — no DB/exchange qty (WS-only path).",
                        symbol,
                    )
                    return
                live_qty = db_qty
                qty_map[key] = db_qty
            elif self._defer_external_close(trade, symbol):
                return
            else:
                resolved = resolve_exchange_close_pnl(
                    self.exchange, self.db, trade
                )
                exit_px = resolved.exit_price
                if exit_px <= 0:
                    exit_px = safe_float(
                        price
                        or self.exchange.get_market_price(symbol, position_side)
                    )
                self._mark_trade_closed(
                    trade,
                    reason="RECONCILED_EXTERNAL_CLOSE",
                    exit_price=exit_px,
                    pnl=resolved.realized_pnl,
                    pnl_source=resolved.source,
                )
                position_reconcile_guard.note_present(str(trade["trade_id"]))
                return

        position_reconcile_guard.note_present(str(trade["trade_id"]))

        if price is None or price <= 0:
            price = self._resolve_monitor_price(symbol, position_side)
        if price is None or price <= 0:
            trade_logger.warning(
                "[%s] TP/SL eval skipped — no live ticker/mark price available.",
                symbol,
            )
            return

        fresh = self.db.get_trade(trade["trade_id"])
        if fresh:
            trade = fresh
        if trade.get("status") == TRADE_STATUS_CLOSED:
            return

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

    def _advance_profit_stop_ladder(
        self,
        trade: dict[str, Any],
        current_price: float,
        *,
        is_long: bool,
    ) -> None:
        """Trail SL into profit as price reaches TP rungs (entry after TP1, TP1 after TP2)."""
        if not Config.ENABLE_BREAK_EVEN:
            return

        entry = safe_float(trade.get("entry_price"))
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))
        sl = safe_float(trade.get("stop_loss"))
        symbol = str(trade.get("symbol", ""))
        rules = self.exchange.get_symbol_rules(symbol)
        target_sl: Optional[float] = None

        if is_long:
            if tp1 > 0 and current_price >= tp1 and entry > 0 and sl < entry:
                target_sl = entry
            if tp2 > 0 and current_price >= tp2 and tp1 > 0 and sl < tp1:
                target_sl = tp1
            if tp3 > 0 and current_price >= tp3 and tp2 > 0 and sl < tp2:
                target_sl = tp2
        else:
            if tp1 > 0 and current_price <= tp1 and entry > 0 and (sl <= 0 or sl > entry):
                target_sl = entry
            if tp2 > 0 and current_price <= tp2 and tp1 > 0 and (sl <= 0 or sl > tp1):
                target_sl = tp1
            if tp3 > 0 and current_price <= tp3 and tp2 > 0 and (sl <= 0 or sl > tp2):
                target_sl = tp2

        if target_sl is None:
            return
        target_sl = round_step_size(target_sl, rules.tick_size, rules.price_precision)
        if is_long and target_sl <= sl:
            return
        if not is_long and sl > 0 and target_sl >= sl:
            return

        self.db.update_trade(trade["trade_id"], {"stop_loss": target_sl})
        trade["stop_loss"] = target_sl
        trade_logger.info(
            "[%s] Profit SL trailed to %.6f | price=%.6f",
            symbol,
            target_sl,
            current_price,
        )

    def _manage_long_trade(self, trade: dict[str, Any], current_price: float) -> None:
        for _ in range(4):
            trade = self.db.get_trade(trade["trade_id"]) or trade
            if trade.get("status") == TRADE_STATUS_CLOSED:
                return

            metadata = self.db.parse_trade_metadata(trade)
            entry = safe_float(trade.get("entry_price"))

            self._advance_profit_stop_ladder(trade, current_price, is_long=True)

            if Config.ENABLE_TRAILING_STOP and entry > 0:
                if current_price > entry or metadata.get("runner_active"):
                    self._apply_trailing_stop(trade, current_price, is_long=True)

            stop_loss = safe_float(trade.get("stop_loss"))
            if stop_loss > 0 and current_price <= stop_loss:
                self._trigger_virtual_sl(trade, current_price, stop_loss)
                self._cancel_all_native_orders(trade)
                self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason="STOP_LOSS",
                )
                return

            metadata = self.db.parse_trade_metadata(trade)
            tp1 = safe_float(trade.get("take_profit_1"))
            tp2 = safe_float(trade.get("take_profit_2"))
            tp3 = safe_float(trade.get("take_profit_3"))
            runner_mode = bool(metadata.get("runner_mode", Config.ENABLE_TP3_RUNNER))

            acted = False
            if (
                tp3 > 0
                and not runner_mode
                and current_price >= tp3
                and not metadata.get("tp3_executed")
            ):
                self._trigger_virtual_tp(trade, "TP3", current_price, tp3)
                self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
                acted = True
            elif tp2 > 0 and current_price >= tp2 and not metadata.get("tp2_executed"):
                self._trigger_virtual_tp(trade, "TP2", current_price, tp2)
                self._handle_take_profit(trade, level="TP2", reason="TP2")
                acted = True
            elif tp1 > 0 and current_price >= tp1 and not metadata.get("tp1_executed"):
                self._trigger_virtual_tp(trade, "TP1", current_price, tp1)
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
            entry = safe_float(trade.get("entry_price"))

            self._advance_profit_stop_ladder(trade, current_price, is_long=False)

            if Config.ENABLE_TRAILING_STOP and entry > 0:
                if current_price < entry or metadata.get("runner_active"):
                    self._apply_trailing_stop(trade, current_price, is_long=False)

            stop_loss = safe_float(trade.get("stop_loss"))
            if stop_loss > 0 and current_price >= stop_loss:
                self._trigger_virtual_sl(trade, current_price, stop_loss)
                self._cancel_all_native_orders(trade)
                self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason="STOP_LOSS",
                )
                return

            metadata = self.db.parse_trade_metadata(trade)
            tp1 = safe_float(trade.get("take_profit_1"))
            tp2 = safe_float(trade.get("take_profit_2"))
            tp3 = safe_float(trade.get("take_profit_3"))
            runner_mode = bool(metadata.get("runner_mode", Config.ENABLE_TP3_RUNNER))

            acted = False
            if (
                tp3 > 0
                and not runner_mode
                and current_price <= tp3
                and not metadata.get("tp3_executed")
            ):
                self._trigger_virtual_tp(trade, "TP3", current_price, tp3)
                self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
                acted = True
            elif tp2 > 0 and current_price <= tp2 and not metadata.get("tp2_executed"):
                self._trigger_virtual_tp(trade, "TP2", current_price, tp2)
                self._handle_take_profit(trade, level="TP2", reason="TP2")
                acted = True
            elif tp1 > 0 and current_price <= tp1 and not metadata.get("tp1_executed"):
                self._trigger_virtual_tp(trade, "TP1", current_price, tp1)
                self._handle_take_profit(trade, level="TP1", reason="TP1")
                acted = True

            if not acted:
                break

    def _prefetch_live_prices_for_open_trades(self) -> None:
        """REST mark refresh for open symbols whose WS miniTicker is stale."""
        if self.exchange.is_rest_blocked()[0]:
            return
        hub = getattr(self.exchange, "_market_data", None)
        max_age = Config.VIRTUAL_TP_TICKER_MAX_AGE_SECONDS
        for trade in self.db.get_open_trades():
            symbol = str(trade.get("symbol", "")).upper()
            side = str(trade.get("side", "LONG")).upper()
            if not symbol:
                continue
            if hub is not None:
                fresh = hub.get_fresh_ticker_price(symbol, max_age_seconds=max_age)
                if fresh is not None and fresh > 0:
                    continue
            self.exchange.get_live_mark_price(symbol, side, allow_rest=True)

    def _resolve_monitor_price(
        self, symbol: str, position_side: str
    ) -> Optional[float]:
        """Resolve a live price for virtual TP/SL — never unbounded stale cache."""
        return self.exchange.get_live_mark_price(
            symbol, position_side, allow_rest=True
        )

    def _trigger_virtual_tp(
        self,
        trade: dict[str, Any],
        level: str,
        current_price: float,
        target_price: float,
    ) -> None:
        symbol = trade.get("symbol", "?")
        side = trade.get("side", "LONG")
        trade_logger.info(
            "[VIRTUAL_TP_TRIGGERED] Pair: %s %s | Level: %s | price=%.6f target=%.6f "
            "| Executing Market Close",
            symbol,
            side,
            level,
            current_price,
            target_price,
        )

    def _trigger_virtual_sl(
        self,
        trade: dict[str, Any],
        current_price: float,
        stop_loss: float,
    ) -> None:
        symbol = trade.get("symbol", "?")
        side = trade.get("side", "LONG")
        trade_logger.warning(
            "[VIRTUAL_SL_TRIGGERED] Pair: %s %s | price=%.6f sl=%.6f "
            "| Executing Market Close",
            symbol,
            side,
            current_price,
            stop_loss,
        )

    def _native_order_map(self, trade: dict[str, Any]) -> dict[str, str]:
        metadata = self.db.parse_trade_metadata(trade)
        order_map: dict[str, str] = {}
        sl_id = metadata.get("native_sl_order_id")
        if sl_id:
            order_map["SL"] = str(sl_id)
        tp_ids = metadata.get("native_tp_order_ids") or {}
        if isinstance(tp_ids, dict):
            for level, oid in tp_ids.items():
                if oid:
                    order_map[str(level).upper()] = str(oid)
        return order_map

    def _cancel_all_native_orders(self, trade: dict[str, Any]) -> None:
        if not Config.ENABLE_NATIVE_TP_SL:
            return
        symbol = str(trade.get("symbol", ""))
        position_side = str(trade.get("side", "LONG")).upper()
        order_map = self._native_order_map(trade)
        if not order_map:
            return
        canceled = self.exchange.cancel_native_exit_orders(
            symbol, position_side, order_map
        )
        if canceled:
            trade_logger.info(
                "[%s] Canceled %s native exit order(s) for %s.",
                symbol,
                canceled,
                position_side,
            )

    def _cancel_native_tp_order(self, trade: dict[str, Any], level: str) -> None:
        level_key = level.upper().replace("_FULL_CLOSE", "")
        metadata = self.db.parse_trade_metadata(trade)
        tp_ids = metadata.get("native_tp_order_ids") or {}
        order_id = tp_ids.get(level_key) if isinstance(tp_ids, dict) else None
        if order_id:
            self.exchange.cancel_order_by_id(str(trade.get("symbol", "")), str(order_id))

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

        if not self._close_position(
            trade,
            quantity=close_qty,
            reason=reason,
            partial=True,
            tp_level=level,
        ):
            self._rollback_tp_lock(trade, level)

    def _finalize_take_profit(
        self,
        trade: dict[str, Any],
        level: str,
        reason: str,
    ) -> None:
        fresh = self.db.get_trade(trade["trade_id"])
        if fresh:
            trade = fresh
        if trade.get("status") == TRADE_STATUS_CLOSED:
            return

        metadata = self.db.parse_trade_metadata(trade)
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
            if Config.ENABLE_TP3_RUNNER or metadata.get("runner_mode"):
                metadata["runner_active"] = True
                metadata["trailing_active"] = True
                updates["metadata"] = metadata
                trade_logger.info(
                    "[%s] TP2 hit — runner active (%.0f%% trailing via dynamic SL).",
                    trade["symbol"],
                    TP3_PORTION * 100,
                )
        elif level == "TP3":
            updates["status"] = TRADE_STATUS_CLOSED

        self.db.update_trade(trade["trade_id"], updates)

        if new_sl is not None and Config.ENABLE_NATIVE_TP_SL:
            metadata = self.db.parse_trade_metadata(trade)
            if metadata.get("native_orders_placed"):
                new_sl_id = self.exchange.refresh_native_stop_loss(
                    trade["symbol"],
                    str(trade.get("side", "LONG")),
                    new_sl,
                    old_sl_order_id=str(metadata.get("native_sl_order_id") or ""),
                )
                if new_sl_id:
                    metadata["native_sl_order_id"] = new_sl_id
                    self.db.update_trade(trade["trade_id"], {"metadata": metadata})

        if self.telegram:
            self.telegram.send_tp_level_alert(
                symbol=trade["symbol"],
                level=level,
                reason=reason,
                new_sl=new_sl,
            )

    def _rollback_tp_lock(self, trade: dict[str, Any], level: str) -> None:
        executed_key = f"{level.lower()}_executed"
        metadata = self.db.parse_trade_metadata(trade)
        metadata[executed_key] = False
        self.db.update_trade(trade["trade_id"], {"metadata": metadata})
        trade_logger.warning(
            "[%s] %s close failed — released TP lock for retry.",
            trade.get("symbol"),
            level,
        )

    def _close_worker_loop(self) -> None:
        """Run close orders and retries off the TP/SL monitor threads."""
        while not self._monitor_stop.is_set():
            try:
                task = self._close_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            if task is None:
                break
            trade_id = str(task.trade.get("trade_id", ""))
            inflight_key = f"{trade_id}:{task.reason}"
            try:
                ok = self._execute_close_order(
                    task.trade,
                    task.quantity,
                    task.reason,
                    partial=task.partial,
                    tp_level=task.tp_level,
                )
                if task.tp_level:
                    if ok:
                        self._finalize_take_profit(task.trade, task.tp_level, task.reason)
                    else:
                        self._rollback_tp_lock(task.trade, task.tp_level)
                if task.result_box is not None:
                    task.result_box[0] = ok
            except Exception as exc:
                error_logger.error(
                    "Close worker failed for %s: %s",
                    task.trade.get("symbol", "?"),
                    exc,
                    exc_info=True,
                )
                if task.tp_level:
                    self._rollback_tp_lock(task.trade, task.tp_level)
                if task.result_box is not None:
                    task.result_box[0] = False
            finally:
                release_exit(trade_id, "main")
                with self._close_lock:
                    self._close_inflight.discard(inflight_key)
                if task.result_event is not None:
                    task.result_event.set()
                self._close_queue.task_done()

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
        if metadata.get("runner_active"):
            trail_mult = Config.RUNNER_TRAIL_ATR_MULTIPLIER
        else:
            trail_mult = self.TRAILING_ATR_MULTIPLIER
        trail_distance = atr * trail_mult
        rules = self.exchange.get_symbol_rules(trade["symbol"])

        updated = False
        if is_long:
            if current_price > best_price:
                best_price = current_price
                metadata["best_price"] = best_price
                updated = True
            if metadata.get("runner_active") or current_price > entry:
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
            if metadata.get("runner_active") or current_price < entry:
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

    def _metadata_remaining_quantity(self, trade: dict[str, Any]) -> float:
        metadata = self.db.parse_trade_metadata(trade)
        original = safe_float(
            metadata.get("original_quantity"), safe_float(trade.get("quantity"))
        )
        remaining = original
        for key in ("tp1_quantity", "tp2_quantity", "tp3_quantity"):
            executed_key = key.replace("_quantity", "_executed")
            if metadata.get(executed_key):
                remaining -= safe_float(metadata.get(key))
        return max(remaining, 0.0)

    def _remaining_close_quantity(self, trade: dict[str, Any]) -> float:
        """Prefer exchange-reported size; fall back to metadata remainder."""
        position_side = trade.get("side", "LONG")
        live_qty = self.exchange.get_position_quantity(trade["symbol"], position_side)
        if live_qty > 0:
            return live_qty
        return self._metadata_remaining_quantity(trade)

    def _resolve_close_quantity(
        self,
        trade: dict[str, Any],
        requested_qty: float,
        *,
        partial: bool = False,
        exchange_only: bool = True,
    ) -> float:
        """Clamp requested quantity to live exchange position size."""
        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        rules = self.exchange.get_symbol_rules(symbol)

        live_qty = self.exchange.get_position_quantity_cached(symbol, position_side)
        if live_qty <= 0:
            live_qty = self.exchange.get_position_quantity(symbol, position_side)
        if live_qty <= 0 and not exchange_only:
            live_qty = self._metadata_remaining_quantity(trade)
        if live_qty <= 0 and partial and requested_qty > 0:
            live_qty = requested_qty
        if live_qty <= 0:
            return 0.0

        qty = min(requested_qty, live_qty)
        return round_step_size(qty, rules.step_size, rules.quantity_precision)

    def _reconcile_exchange_flat(
        self,
        trade: dict[str, Any],
        reason: str,
        *,
        exit_reason: str = "RECONCILED_REDUCE_ONLY",
    ) -> bool:
        """Mark trade closed when the exchange has no open position."""
        symbol = str(trade.get("symbol", ""))
        position_side = str(trade.get("side", "LONG")).upper()
        exit_price = safe_float(
            self.exchange.get_market_price(symbol, position_side)
        )
        trade_logger.warning(
            "[%s] Exchange flat — purging local trade | trigger=%s | exit_reason=%s",
            symbol,
            reason,
            exit_reason,
        )
        self._cancel_all_native_orders(trade)
        self.exchange.clear_position_cache(symbol, position_side)
        position_reconcile_guard.note_present(str(trade["trade_id"]))
        resolved = resolve_exchange_close_pnl(self.exchange, self.db, trade)
        if resolved.exit_price > 0:
            exit_price = resolved.exit_price
        if self.telegram:
            self.telegram.send_message(
                f"🔄 <b>{escape_html(symbol)}</b> synced: Already closed on exchange "
                f"(ReduceOnly rejected).\n"
                f"💰 <b>Exchange PnL:</b> ${resolved.realized_pnl:.4f} "
                f"<i>({escape_html(resolved.source)})</i>"
            )
        self._mark_trade_closed(
            trade,
            reason=exit_reason,
            exit_price=exit_price,
            pnl=resolved.realized_pnl,
            pnl_source=resolved.source,
            notify=True,
        )
        return True

    def _close_position(
        self,
        trade: dict[str, Any],
        quantity: float,
        reason: str,
        partial: bool = False,
        *,
        tp_level: Optional[str] = None,
        blocking: bool = False,
    ) -> bool:
        """Enqueue close for monitor paths; blocking=True waits for close-all/manual."""
        symbol = trade["symbol"]
        trade_id = str(trade["trade_id"])
        inflight_key = f"{trade_id}:{reason}"

        close_qty = self._resolve_close_quantity(
            trade,
            quantity,
            partial=partial,
            exchange_only=not partial,
        )
        if close_qty <= 0:
            if partial:
                if quantity > 0:
                    close_qty = quantity
                    trade_logger.info(
                        "[%s] Partial close qty from metadata=%s (exchange cache stale).",
                        symbol,
                        close_qty,
                    )
                else:
                    return False
            else:
                return self._reconcile_exchange_flat(trade, reason)

        with self._close_lock:
            if inflight_key in self._close_inflight:
                return True
            if exit_claim_active(trade_id) or not claim_exit(trade_id, "main"):
                trade_logger.debug(
                    "[%s] Close deferred — exit claim held by another process.",
                    symbol,
                )
                return False
            self._close_inflight.add(inflight_key)

        task = _CloseTask(
            trade=trade,
            quantity=close_qty,
            reason=reason,
            partial=partial,
            tp_level=tp_level,
        )
        if blocking:
            task.result_event = threading.Event()
            task.result_box = [False]
            self._close_queue.put(task)
            timeout = (
                self.CLOSE_ORDER_MAX_RETRIES
                * self.CLOSE_ORDER_RETRY_DELAY_SECONDS
                * 3
                + 15.0
            )
            if not task.result_event.wait(timeout=timeout):
                error_logger.warning(
                    "Blocking close timed out for %s (%s) after %.0fs.",
                    symbol,
                    reason,
                    timeout,
                )
            return bool(task.result_box[0])

        self._close_queue.put(task)
        return True

    def _execute_close_order(
        self,
        trade: dict[str, Any],
        quantity: float,
        reason: str,
        partial: bool = False,
        *,
        tp_level: Optional[str] = None,
    ) -> bool:
        trade_id = trade["trade_id"]
        fresh = self.db.get_trade(trade_id)
        if fresh:
            trade = fresh
        if trade.get("status") == TRADE_STATUS_CLOSED:
            return True

        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        close_qty = self._resolve_close_quantity(
            trade,
            quantity,
            partial=partial,
            exchange_only=False,
        )
        if close_qty <= 0:
            if partial:
                trade_logger.error(
                    "[%s] Partial close aborted — could not resolve qty for %s.",
                    symbol,
                    reason,
                )
                return False
            return self._reconcile_exchange_flat(trade, reason)

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
                except PositionAlreadyClosedError as exc:
                    trade_logger.warning(
                        "[%s] ReduceOnly rejected (-2022) — exchange flat | reason=%s | %s",
                        symbol,
                        reason,
                        exc,
                    )
                    return self._reconcile_exchange_flat(trade, reason)
                except OrderExecutionError as exc:
                    if PositionAlreadyClosedError.matches(exc):
                        return self._reconcile_exchange_flat(trade, reason)
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
        except PositionAlreadyClosedError as exc:
            trade_logger.warning(
                "[%s] ReduceOnly rejected (-2022) — exchange flat | reason=%s | %s",
                symbol,
                reason,
                exc,
            )
            return self._reconcile_exchange_flat(trade, reason)
        except OrderExecutionError as exc:
            if PositionAlreadyClosedError.matches(exc):
                return self._reconcile_exchange_flat(trade, reason)
            error_logger.error("Close order failed for %s (%s): %s", symbol, reason, exc)
            return False

        if not response:
            return False

        order_id = str(response.get("orderId", ""))
        fill_pnl = self.exchange.resolve_order_fill_pnl(
            symbol,
            order_id,
            position_side=str(position_side),
        )
        exit_price = fill_pnl.exit_price
        if exit_price <= 0:
            exit_price = safe_float(response.get("avgPrice"))
        if exit_price <= 0:
            exit_price = safe_float(
                self.exchange.get_market_price(symbol, position_side)
            )

        leg_pnl = fill_pnl.realized_pnl
        pnl_source = fill_pnl.source
        if fill_pnl.fill_count <= 0 or pnl_source == "unknown":
            trade_logger.warning(
                "[%s] Binance fill PnL unavailable for order %s — skipping local estimate.",
                symbol,
                order_id or "?",
            )
            leg_pnl = 0.0
            pnl_source = "unavailable"

        prior_pnl = safe_float(trade.get("realized_pnl"))
        if prior_pnl == 0:
            prior_pnl = safe_float(trade.get("pnl"))

        live_remaining = self.exchange.get_position_quantity(symbol, position_side)
        metadata_remaining = self._metadata_remaining_quantity(trade)
        is_full_close = (
            not partial
            or tp_level == "TP3"
            or reason == "TP3_FULL_CLOSE"
            or live_remaining <= 0
            or metadata_remaining <= 0
        )

        if is_full_close:
            from reconciliation import trade_opened_at_ms

            lifecycle = self.exchange.fetch_closed_position_realized_pnl(
                symbol,
                str(position_side),
                trade_opened_at_ms(trade),
            )
            if lifecycle.source in ("userTrades", "income", "ws") and (
                lifecycle.fill_count > 0 or abs(lifecycle.realized_pnl) > 0
            ):
                cumulative_pnl = lifecycle.realized_pnl
                pnl_source = lifecycle.source
            else:
                cumulative_pnl = prior_pnl + leg_pnl
        else:
            cumulative_pnl = prior_pnl + leg_pnl

        metadata = self.db.parse_trade_metadata(trade)
        metadata["last_fill_order_id"] = order_id
        metadata["last_fill_pnl_source"] = pnl_source
        self.db.update_trade(
            trade_id,
            {
                "pnl": cumulative_pnl,
                "realized_pnl": cumulative_pnl,
                "metadata": metadata,
            },
        )
        trade["pnl"] = cumulative_pnl
        trade["realized_pnl"] = cumulative_pnl

        daily_leg = cumulative_pnl - prior_pnl
        if daily_leg != 0:
            self.db.add_daily_realized_pnl(utc_today_str(), daily_leg)
        self.db.sync_daily_stats_from_trades(utc_today_str())

        trade_logger.info(
            "[%s] Closed qty=%s | reason=%s | leg_pnl=%.4f | total=%.4f | source=%s",
            symbol,
            close_qty,
            reason,
            leg_pnl,
            cumulative_pnl,
            pnl_source,
        )

        if partial and tp_level not in (None, "TP3") and reason != "TP3_FULL_CLOSE":
            metadata_remaining = self._metadata_remaining_quantity(trade)
            trade_logger.info(
                "[%s] Partial %s filled qty=%s | metadata_remaining=%.4f",
                symbol,
                reason,
                close_qty,
                max(metadata_remaining - close_qty, 0.0),
            )
            return True

        if is_full_close:
            self._mark_trade_closed(
                trade,
                reason=reason,
                exit_price=exit_price,
                book_daily_pnl=False,
                pnl=cumulative_pnl,
                pnl_source=pnl_source,
            )
        else:
            if live_remaining <= 0:
                self.exchange.clear_position_cache(symbol, position_side)
            elif self.telegram and "STOP" in reason.upper():
                self.telegram.send_close_alert(
                    symbol=symbol, reason=reason, pnl=leg_pnl
                )

        return True

    def _log_position_closed(
        self,
        symbol: str,
        position_side: str,
        reason: str,
        exit_price: float,
        pnl: float,
    ) -> None:
        trade_logger.info(
            "[POSITION_CLOSED] Pair: %s %s | Reason: %s | exit=%.6f | pnl=%.4f",
            symbol,
            position_side,
            reason,
            exit_price,
            pnl,
        )

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
        *,
        notify: bool = True,
        book_daily_pnl: bool = True,
        pnl: Optional[float] = None,
        pnl_source: str = "",
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

        position_reconcile_guard.note_present(str(trade_id))
        self._cancel_all_native_orders(trade)
        self.exchange.clear_position_cache(symbol, position_side)
        with self._monitored_lock:
            self._monitored_symbols.discard(str(symbol).upper())
        self.exchange.invalidate_balance_cache()
        balance = self.exchange.get_futures_balance(force_refresh=False)

        if pnl is None and is_exchange_sync_close_reason(reason):
            resolved = resolve_exchange_close_pnl(self.exchange, self.db, trade)
            pnl = resolved.realized_pnl
            pnl_source = resolved.source
            if resolved.exit_price > 0:
                exit_price = resolved.exit_price

        total_pnl = self.db.close_trade_and_sync_stats(
            trade,
            exit_price=exit_price,
            exit_reason=reason,
            closed_at=closed_at,
            duration=duration,
            book_daily_pnl=book_daily_pnl,
            current_balance=balance,
            pnl=pnl,
        )
        trade["pnl"] = total_pnl
        trade["realized_pnl"] = total_pnl
        self._log_position_closed(symbol, position_side, reason, exit_price, total_pnl)
        source_note = f" | pnl_source={pnl_source}" if pnl_source else ""
        trade_logger.info(
            "[%s] Trade %s fully closed | reason=%s | total_pnl=%.4f%s",
            symbol,
            trade_id[:8],
            reason,
            total_pnl,
            source_note,
        )

        if notify and self.telegram:
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
                    blocking=True,
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
