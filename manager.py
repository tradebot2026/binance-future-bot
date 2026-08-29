"""
Trade management module.
Monitors open positions with metadata-driven partial exits (33/33/34),
dynamic break-even and TP-trailing stop loss, and exchange reconciliation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, TYPE_CHECKING

from config import Config
from constants import (
    STRATEGY_RANGE_REVERSION,
    TRADE_STATUS_CLOSED,
    TRADE_STATUS_OPEN,
    TRADE_STATUS_TP1_HIT,
    TRADE_STATUS_TP2_HIT,
    is_range_strategy,
)
from database import DatabaseManager
from exchange import BinanceExchangeManager
from exceptions import OrderExecutionError
from logger import error_logger, trade_logger
from utils import round_step_size, safe_float, utc_now, utc_today_str

if TYPE_CHECKING:
    from telegram_bot import TelegramManager
    from scheduler import DailyScheduler
    from risk_manager import RiskManager


class TradeManager:
    """Algorithmic virtual SL/TP manager using absolute quantities from trade metadata."""

    TRAILING_ATR_MULTIPLIER = 1.0

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

    def monitor_open_trades(self) -> None:
        """Evaluate all active DB trades against live prices and exchange state."""
        try:
            active_trades = self.db.get_open_trades()
            if not active_trades:
                return

            for trade in active_trades:
                symbol = trade["symbol"]
                position_side = trade.get("side", "LONG")

                live_qty = self.exchange.get_position_quantity(symbol, position_side)
                if live_qty <= 0:
                    self._mark_trade_closed(
                        trade,
                        reason="RECONCILED_EXTERNAL_CLOSE",
                        exit_price=safe_float(self.exchange.get_market_price(symbol)),
                    )
                    continue

                price = self.exchange.get_market_price(symbol)
                if price is None or price <= 0:
                    continue

                fresh = self.db.get_trade(trade["trade_id"])
                if fresh:
                    trade = fresh

                if is_range_strategy(str(trade.get("strategy", ""))):
                    if self._check_range_hard_exits(trade, price):
                        continue

                if position_side == "LONG":
                    self._manage_long_trade(trade, price)
                elif position_side == "SHORT":
                    self._manage_short_trade(trade, price)
        except Exception as exc:
            error_logger.error("Trade monitoring failed: %s", exc)

    _TF_BAR_SECONDS: dict[str, int] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
    }

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
                symbol, Config.CONFIRM_TIMEFRAME, limit=80
            )
            if df.empty or len(df) < 20:
                return 0.0
            df = df.copy()
            df["adx"] = ta.trend.adx(df["high"], df["low"], df["close"], window=14)
            return safe_float(df["adx"].iloc[-1])
        except Exception as exc:
            error_logger.warning("ADX fetch failed for %s: %s", symbol, exc)
            return 0.0

    def _check_range_hard_exits(self, trade: dict[str, Any], price: float) -> bool:
        """Range kill rules: ADX breakout, range boundary violation, time stop."""
        metadata = self.db.parse_trade_metadata(trade)
        atr = safe_float(metadata.get("atr_at_entry"))
        range_high = safe_float(metadata.get("range_high"))
        range_low = safe_float(metadata.get("range_low"))

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

        if atr > 0 and range_high > 0 and range_low > 0:
            breakout_buffer = atr * Config.RANGE_BREAKOUT_ATR_MULT
            if price > range_high + breakout_buffer or price < range_low - breakout_buffer:
                trade_logger.info(
                    "[%s] RANGE boundary breakout exit | price=%.6f range=[%.6f, %.6f]",
                    trade.get("symbol"),
                    price,
                    range_low,
                    range_high,
                )
                self._close_position(
                    trade,
                    quantity=self._remaining_close_quantity(trade),
                    reason="RANGE_BOUNDARY_BREAKOUT",
                )
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
        metadata = self.db.parse_trade_metadata(trade)

        stop_loss = safe_float(trade.get("stop_loss"))
        if current_price <= stop_loss:
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="STOP_LOSS",
            )
            return

        if Config.ENABLE_TRAILING_STOP and metadata.get("trailing_active"):
            self._apply_trailing_stop(trade, current_price, is_long=True)
            trade = self.db.get_trade(trade["trade_id"]) or trade

        status = trade.get("status", TRADE_STATUS_OPEN)
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))

        if status == TRADE_STATUS_OPEN and current_price >= tp1 and not metadata.get("tp1_executed"):
            self._handle_take_profit(trade, level="TP1", reason="TP1")
            return

        metadata = self.db.parse_trade_metadata(trade)
        if status == TRADE_STATUS_TP1_HIT and current_price >= tp2 and not metadata.get("tp2_executed"):
            self._handle_take_profit(trade, level="TP2", reason="TP2")
            return

        metadata = self.db.parse_trade_metadata(trade)
        if status == TRADE_STATUS_TP2_HIT and current_price >= tp3 and not metadata.get("tp3_executed"):
            self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
            return

    def _manage_short_trade(self, trade: dict[str, Any], current_price: float) -> None:
        metadata = self.db.parse_trade_metadata(trade)

        stop_loss = safe_float(trade.get("stop_loss"))
        if current_price >= stop_loss:
            self._close_position(
                trade,
                quantity=self._remaining_close_quantity(trade),
                reason="STOP_LOSS",
            )
            return

        if Config.ENABLE_TRAILING_STOP and metadata.get("trailing_active"):
            self._apply_trailing_stop(trade, current_price, is_long=False)
            trade = self.db.get_trade(trade["trade_id"]) or trade

        status = trade.get("status", TRADE_STATUS_OPEN)
        tp1 = safe_float(trade.get("take_profit_1"))
        tp2 = safe_float(trade.get("take_profit_2"))
        tp3 = safe_float(trade.get("take_profit_3"))

        metadata = self.db.parse_trade_metadata(trade)
        if status == TRADE_STATUS_OPEN and current_price <= tp1 and not metadata.get("tp1_executed"):
            self._handle_take_profit(trade, level="TP1", reason="TP1")
            return

        metadata = self.db.parse_trade_metadata(trade)
        if status == TRADE_STATUS_TP1_HIT and current_price <= tp2 and not metadata.get("tp2_executed"):
            self._handle_take_profit(trade, level="TP2", reason="TP2")
            return

        metadata = self.db.parse_trade_metadata(trade)
        if status == TRADE_STATUS_TP2_HIT and current_price <= tp3 and not metadata.get("tp3_executed"):
            self._handle_take_profit(trade, level="TP3", reason="TP3_FULL_CLOSE")
            return

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
            self._mark_trade_closed(
                trade,
                reason="RECONCILED_EXTERNAL_CLOSE",
                exit_price=safe_float(self.exchange.get_market_price(symbol)),
            )
            return False

        try:
            response = self.exchange.close_position_quantity(
                symbol=symbol,
                position_side=position_side,
                quantity=close_qty,
            )
        except OrderExecutionError as exc:
            error_logger.error("Close order failed for %s (%s): %s", symbol, reason, exc)
            return False

        if not response:
            return False

        exit_price = safe_float(response.get("avgPrice"))
        if exit_price <= 0:
            exit_price = safe_float(self.exchange.get_market_price(symbol))

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

        balance = self.exchange.get_futures_balance(force_refresh=True)
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
        if is_range_strategy(strategy):
            outcome = "LOSS" if safe_float(trade.get("pnl")) < 0 else outcome
            self.db.set_symbol_cooldown(
                symbol,
                Config.RANGE_COOLDOWN_MINUTES,
                reason="RANGE_CLOSE",
            )
        elif "STOP" in reason.upper():
            outcome = "LOSS"
            self.db.set_symbol_cooldown(
                symbol,
                Config.SYMBOL_COOLDOWN_MINUTES,
                reason="STOP_LOSS",
            )
        elif safe_float(trade.get("pnl")) > 0:
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
