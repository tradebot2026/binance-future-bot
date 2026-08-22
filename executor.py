"""
Trade execution module.
Calculates position size, ATR-based SL/TP levels, partitions partial quantities,
and persists metadata for deterministic exits in the trade manager.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Optional

from config import Config
from constants import TP1_PORTION, TP2_PORTION, TP3_PORTION, TRADE_STATUS_OPEN
from database import DatabaseManager
from exchange import BinanceExchangeManager, SymbolRules
from exceptions import OrderExecutionError
from logger import error_logger, trade_logger
from utils import round_step_size, safe_float


class TradeExecutor:
    """Opens futures positions with validated sizing and structured DB metadata."""

    def __init__(self, exchange: BinanceExchangeManager, db: DatabaseManager) -> None:
        self.exchange = exchange
        self.db = db

    def calculate_sl_tp(
        self,
        action: str,
        entry_price: float,
        atr: float,
        rules: SymbolRules,
    ) -> tuple[float, float, float, float]:
        """Calculate stop loss and three take-profit levels using Config ATR multipliers."""
        sl_dist = atr * Config.SL_ATR_MULTIPLIER
        tp1_dist = atr * Config.TP1_ATR_MULTIPLIER
        tp2_dist = atr * Config.TP2_ATR_MULTIPLIER
        tp3_dist = atr * Config.TP3_ATR_MULTIPLIER

        if action == "LONG":
            sl = entry_price - sl_dist
            if sl <= 0:
                sl = entry_price * 0.995
            tp1 = entry_price + tp1_dist
            tp2 = entry_price + tp2_dist
            tp3 = entry_price + tp3_dist
        else:
            sl = entry_price + sl_dist
            tp1 = entry_price - tp1_dist
            tp2 = entry_price - tp2_dist
            tp3 = entry_price - tp3_dist

        return (
            round_step_size(sl, rules.tick_size, rules.price_precision),
            round_step_size(tp1, rules.tick_size, rules.price_precision),
            round_step_size(tp2, rules.tick_size, rules.price_precision),
            round_step_size(tp3, rules.tick_size, rules.price_precision),
        )

    def calculate_position_size(
        self,
        entry_price: float,
        sl_price: float,
        rules: SymbolRules,
    ) -> float:
        """Risk-based position sizing with notional and exchange filter guards."""
        balance = self.exchange.get_futures_balance()
        if balance <= 0:
            raise OrderExecutionError("Cannot size position: zero or unavailable balance.")

        risk_amount = balance * (Config.RISK_PER_TRADE_PERCENT / 100.0)
        sl_distance = abs(entry_price - sl_price)
        if sl_distance <= 0:
            return 0.0

        quantity = risk_amount / sl_distance
        quantity = round_step_size(quantity, rules.step_size, rules.quantity_precision)

        notional = quantity * entry_price
        max_notional = balance * Config.MAX_POSITION_VALUE_MULTIPLIER
        if notional > max_notional:
            trade_logger.warning(
                "Position size capped: notional $%.2f exceeds limit $%.2f.",
                notional,
                max_notional,
            )
            return 0.0

        if quantity < rules.min_qty:
            trade_logger.warning(
                "Quantity %.8f below min_qty %.8f.", quantity, rules.min_qty
            )
            return 0.0

        if notional < rules.min_notional:
            trade_logger.warning(
                "Notional $%.2f below minimum $%.2f.", notional, rules.min_notional
            )
            return 0.0

        return quantity

    def _build_partial_quantities(
        self, total_quantity: float, rules: SymbolRules
    ) -> dict[str, float]:
        """Partition entry quantity into fixed 25% / 25% / 50% absolute amounts."""
        tp1_qty = round_step_size(
            total_quantity * TP1_PORTION, rules.step_size, rules.quantity_precision
        )
        tp2_qty = round_step_size(
            total_quantity * TP2_PORTION, rules.step_size, rules.quantity_precision
        )
        tp3_qty = round_step_size(
            total_quantity - tp1_qty - tp2_qty,
            rules.step_size,
            rules.quantity_precision,
        )

        if tp3_qty <= 0:
            raise OrderExecutionError(
                "Partial TP partition invalid: TP3 quantity rounds to zero."
            )

        return {
            "tp1_quantity": tp1_qty,
            "tp2_quantity": tp2_qty,
            "tp3_quantity": tp3_qty,
            "original_quantity": total_quantity,
            "tp1_executed": False,
            "tp2_executed": False,
            "tp3_executed": False,
        }

    def _persist_trade_with_retry(
        self, trade_data: dict[str, Any], attempts: int = 3
    ) -> bool:
        """Retry DB persistence to reduce orphan exchange positions after fills."""
        for attempt in range(1, attempts + 1):
            try:
                self.db.log_trade(trade_data)
                return True
            except Exception as exc:
                error_logger.error(
                    "DB log attempt %s/%s failed for %s: %s",
                    attempt,
                    attempts,
                    trade_data.get("symbol"),
                    exc,
                )
                if attempt < attempts:
                    time.sleep(0.5 * attempt)
        return False

    def _persist_orphan_fill(self, trade_data: dict[str, Any]) -> None:
        """Write orphan fill details to disk when DB persistence fails."""
        path = os.path.join(Config.DATA_DIR, "orphan_fills.jsonl")
        try:
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(trade_data, default=str) + "\n")
        except OSError as exc:
            error_logger.error("Failed to write orphan fill recovery file: %s", exc)

    def execute_trade(
        self,
        symbol: str,
        action: str,
        atr: float,
        current_price: float,
        strategy: str = "DEFAULT",
        score: float = 0.0,
    ) -> Optional[dict[str, Any]]:
        """
        Execute a market entry and log the trade with metadata for manager exits.

        Returns a result dict on success (includes trade_id, SL/TP levels), else None.
        """
        if atr <= 0:
            error_logger.error("Execution aborted: invalid ATR (%.8f) for %s.", atr, symbol)
            return None

        if action not in ("LONG", "SHORT"):
            error_logger.error("Execution aborted: invalid action %s.", action)
            return None

        position_side = action
        if self.exchange.has_open_position(symbol, position_side):
            trade_logger.warning(
                "Execution skipped: %s %s already open on exchange.", symbol, position_side
            )
            return None

        rules = self.exchange.get_symbol_rules(symbol)
        sl, tp1, tp2, tp3 = self.calculate_sl_tp(action, current_price, atr, rules)

        try:
            quantity = self.calculate_position_size(current_price, sl, rules)
        except OrderExecutionError as exc:
            error_logger.error("Sizing failed for %s: %s", symbol, exc)
            return None

        if quantity <= 0:
            error_logger.error("Execution aborted: zero quantity for %s.", symbol)
            return None

        try:
            metadata = self._build_partial_quantities(quantity, rules)
        except OrderExecutionError as exc:
            error_logger.error("Partition failed for %s: %s", symbol, exc)
            return None

        metadata["atr_at_entry"] = atr
        metadata["trailing_active"] = False
        metadata["best_price"] = current_price

        try:
            leverage = self.exchange.optimize_and_set_leverage(symbol)
        except Exception as exc:
            error_logger.error("Leverage setup failed for %s: %s", symbol, exc)
            leverage = Config.MAX_LEVERAGE

        trade_side = "BUY" if action == "LONG" else "SELL"
        try:
            response = self.exchange.execute_futures_order(
                symbol=symbol,
                side=trade_side,
                position_side=position_side,
                quantity=quantity,
            )
        except OrderExecutionError as exc:
            error_logger.error("Order rejected for %s: %s", symbol, exc)
            return None

        if not response:
            return None

        fill_price = self.exchange.get_fill_price_from_order(
            symbol, response, fallback=current_price
        )
        sl, tp1, tp2, tp3 = self.calculate_sl_tp(action, fill_price, atr, rules)

        metadata["best_price"] = fill_price

        trade_id = str(uuid.uuid4())
        opened_at = datetime.now(UTC).isoformat()
        exchange_order_id = str(response.get("orderId", ""))
        margin_estimate = (quantity * fill_price) / max(leverage, 1)

        trade_data = {
            "trade_id": trade_id,
            "symbol": symbol,
            "side": action,
            "entry_price": fill_price,
            "quantity": quantity,
            "status": TRADE_STATUS_OPEN,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "take_profit_3": tp3,
            "stop_loss": sl,
            "pnl": 0.0,
            "opened_at": opened_at,
            "closed_at": None,
            "strategy": strategy,
            "score": score,
            "leverage": leverage,
            "margin": margin_estimate,
            "fee": 0.0,
            "exit_reason": None,
            "duration": None,
            "metadata": metadata,
            "exchange_order_id": exchange_order_id,
        }

        db_logged = self._persist_trade_with_retry(trade_data)
        if not db_logged:
            error_logger.error(
                "CRITICAL ORPHAN FILL | %s %s | orderId=%s | trade_id=%s",
                symbol,
                action,
                exchange_order_id,
                trade_id,
            )
            self._persist_orphan_fill(trade_data)

        trade_logger.info(
            "Entry executed | %s %s | qty=%s | fill=%.6f | SL=%.6f | TP1=%.6f | id=%s | db_logged=%s",
            symbol,
            action,
            quantity,
            fill_price,
            sl,
            tp1,
            trade_id[:8],
            db_logged,
        )

        return {
            "trade_id": trade_id,
            "symbol": symbol,
            "action": action,
            "entry_price": fill_price,
            "stop_loss": sl,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "take_profit_3": tp3,
            "quantity": quantity,
            "metadata": metadata,
            "exchange_order_id": exchange_order_id,
            "db_logged": db_logged,
            "orphan_fill": not db_logged,
        }
