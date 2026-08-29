"""
Trade execution module.
Structural SL, R-multiple TP ladder, tiered sizing, and metadata persistence.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Optional

from config import Config
from constants import TP1_PORTION, TP2_PORTION, TRADE_STATUS_OPEN, is_range_strategy
from database import DatabaseManager
from exchange import BinanceExchangeManager, SymbolRules
from exceptions import OrderExecutionError
from logger import error_logger, trade_logger
from range_engine import RangeMetadata, compute_range_sl_tp
from smc_engine import (
    StructureMetadata,
    check_opposing_liquidity_rr,
    compute_rr_ladder,
    compute_structural_sl,
    size_multiplier_for_score,
)
from utils import round_step_size, safe_float, utc_now


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
        structure: Optional[dict[str, Any]] = None,
    ) -> tuple[float, float, float, float]:
        """Structural stop with R-multiple take-profit ladder."""
        meta = StructureMetadata()
        if structure:
            for key, value in structure.items():
                if hasattr(meta, key):
                    setattr(meta, key, value)

        sl = compute_structural_sl(action, entry_price, atr, meta)
        sl, tp1, tp2, tp3 = compute_rr_ladder(action, entry_price, sl)

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
        score: float = 80.0,
        strategy: str = "DEFAULT",
    ) -> float:
        """Risk-based position sizing with tiered score multiplier."""
        balance = self.exchange.get_futures_balance()
        if balance <= 0:
            raise OrderExecutionError("Cannot size position: zero or unavailable balance.")

        if is_range_strategy(strategy):
            size_mult = Config.RANGE_SIZE_MULTIPLIER
        else:
            size_mult = size_multiplier_for_score(score)
        if size_mult <= 0:
            return 0.0

        risk_amount = balance * (Config.RISK_PER_TRADE_PERCENT / 100.0) * size_mult
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
        """Partition entry quantity into fixed 33% / 33% / 34% absolute amounts."""
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
        structure_metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Execute a market entry after structural SL / R:R validation.
        Returns result dict on success, else None.
        """
        if atr <= 0:
            error_logger.error("Execution aborted: invalid ATR (%.8f) for %s.", atr, symbol)
            return None

        if action not in ("LONG", "SHORT"):
            error_logger.error("Execution aborted: invalid action %s.", action)
            return None

        on_cooldown, cooldown_reason = self.db.is_symbol_on_cooldown(symbol)
        if on_cooldown:
            trade_logger.info(
                "Execution skipped: %s on cooldown (%s).", symbol, cooldown_reason
            )
            return None

        position_side = action
        if self.exchange.has_open_position(symbol, position_side):
            trade_logger.warning(
                "Execution skipped: %s %s already open on exchange.", symbol, position_side
            )
            return None

        rules = self.exchange.get_symbol_rules(symbol)
        structure = structure_metadata or {}
        range_mode = is_range_strategy(strategy)

        if range_mode:
            rmeta = RangeMetadata()
            for key, value in structure.items():
                if hasattr(rmeta, key):
                    setattr(rmeta, key, value)
            sl, tp1, tp2, tp3 = compute_range_sl_tp(action, current_price, atr, rmeta)
            sl = round_step_size(sl, rules.tick_size, rules.price_precision)
            tp1 = round_step_size(tp1, rules.tick_size, rules.price_precision)
            tp2 = round_step_size(tp2, rules.tick_size, rules.price_precision)
            tp3 = round_step_size(tp3, rules.tick_size, rules.price_precision)
        else:
            sl, tp1, tp2, tp3 = self.calculate_sl_tp(
                action, current_price, atr, rules, structure=structure
            )
            opposing = safe_float(structure.get("opposing_liquidity"))
            rr_ok, rr_reason = check_opposing_liquidity_rr(
                action, current_price, sl, opposing
            )
            if not rr_ok:
                trade_logger.info("Execution skipped %s: %s", symbol, rr_reason)
                self.db.log_signal_rejection(
                    symbol, action, score, [rr_reason], strategy=strategy
                )
                return None

        try:
            quantity = self.calculate_position_size(
                current_price, sl, rules, score=score, strategy=strategy
            )
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
        metadata["structure"] = structure
        metadata["strategy_tag"] = strategy
        if range_mode:
            metadata["size_multiplier"] = Config.RANGE_SIZE_MULTIPLIER
            metadata["range_high"] = safe_float(structure.get("range_high"))
            metadata["range_low"] = safe_float(structure.get("range_low"))
            metadata["equilibrium"] = safe_float(structure.get("equilibrium"))
            metadata["entry_timeframe"] = Config.ENTRY_TIMEFRAME
            metadata["range_edge"] = structure.get("edge", "")
        else:
            metadata["size_multiplier"] = size_multiplier_for_score(score)
        metadata["r_distance"] = abs(current_price - sl)

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
        if range_mode:
            rmeta = RangeMetadata()
            for key, value in structure.items():
                if hasattr(rmeta, key):
                    setattr(rmeta, key, value)
            sl, tp1, tp2, tp3 = compute_range_sl_tp(action, fill_price, atr, rmeta)
            sl = round_step_size(sl, rules.tick_size, rules.price_precision)
            tp1 = round_step_size(tp1, rules.tick_size, rules.price_precision)
            tp2 = round_step_size(tp2, rules.tick_size, rules.price_precision)
            tp3 = round_step_size(tp3, rules.tick_size, rules.price_precision)
        else:
            sl, tp1, tp2, tp3 = self.calculate_sl_tp(
                action, fill_price, atr, rules, structure=structure
            )
        metadata["best_price"] = fill_price
        metadata["r_distance"] = abs(fill_price - sl)

        trade_id = str(uuid.uuid4())
        opened_at = utc_now().isoformat()
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
            "Entry executed | %s %s | strategy=%s | qty=%s | fill=%.6f | SL=%.6f | TP1=%.6f | "
            "R=%.6f | size_mult=%.2f | id=%s",
            symbol,
            action,
            strategy,
            quantity,
            fill_price,
            sl,
            tp1,
            metadata["r_distance"],
            metadata["size_multiplier"],
            trade_id[:8],
        )

        return {
            "trade_id": trade_id,
            "symbol": symbol,
            "action": action,
            "strategy": strategy,
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
