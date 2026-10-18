"""
Trade execution module.
Structural SL, R-multiple TP ladder, tiered sizing, and metadata persistence.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from typing import Any, Optional

from config import Config
from constants import TP1_PORTION, TP2_PORTION, TRADE_STATUS_OPEN, is_range_strategy
from core.execution_ledger import ExecutionPhase, ExecutionRecord, get_execution_ledger
from database import DatabaseManager
from exchange import BinanceExchangeManager, SymbolRules
from exceptions import OrderExecutionError
from logger import error_logger, trade_logger
from engines.range_engine import RangeMetadata, compute_range_sl_tp
from engines.smc_engine import (
    StructureMetadata,
    check_opposing_liquidity_rr,
    compute_dynamic_tp_ladder,
    compute_rr_ladder,
    compute_structural_sl,
    size_multiplier_for_score,
)
from core.dca_plan import (
    apply_initial_dca_plan,
    build_dca_plan,
    dca_add_wallet_fraction,
    dca_enabled,
    dca_entry_count,
    entry_stage_of,
    last_resort_sl_hit,
    mark_dca_filled,
    resolved_final_sl,
    sl_is_valid_vs_entry,
    update_weighted_average,
)
from core.entry_in_flight_mutex import entry_in_flight_mutex
from core.execution_governor import ExecutionGovernor
from reconciliation import symbol_blocked_for_new_entry
from utils import (
    amount_to_precision,
    cap_quantity_to_notional,
    minimum_order_quantity,
    resolve_binance_fill_price,
    round_protective_price,
    round_step_size,
    safe_float,
    utc_now,
    utc_today_str,
)


def _is_hard_order_reject(exc: BaseException) -> bool:
    """True when Binance definitively rejected the order (safe to un-claim DCA)."""
    msg = str(exc).upper()
    return "ORDER_REJECTED" in msg or "FILTER FAILURE" in msg


_REJECT_LOG_AT: dict[str, float] = {}


def _positive_price(value: object) -> float:
    """Accept only a real finite price. Mocks and strings must not become 1.0."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    price = float(value)
    if price <= 0 or price != price or price == float("inf"):
        return 0.0
    return price


def _log_reject(
    tag: str, symbol: str, reason: str, *, strategy: str = ""
) -> None:
    """Rate-limited WARNING for scan or execution rejects."""
    symbol_key = symbol.upper()
    reason_lower = reason.lower()
    blocked_patterns = (
        "position already open",
        "active db trade",
        "entry gate closed",
        "symbol on cooldown",
        "entry in flight",
        "entry already in flight",
        "open position on",
        "exchange rest confirms",
        "entries paused",
        "max open positions",
        "max daily entries",
    )
    if any(pattern in reason_lower for pattern in blocked_patterns):
        suppress_key = f"{tag}:{symbol_key}"
    else:
        suppress_key = f"{tag}:{symbol_key}:{reason}"
    now = time.monotonic()
    interval = max(Config.CONFLICT_REJECT_LOG_INTERVAL_SECONDS, 60)
    last = _REJECT_LOG_AT.get(suppress_key, 0.0)
    if (now - last) < interval:
        return
    _REJECT_LOG_AT[suppress_key] = now

    suffix = f" | strategy={strategy}" if strategy else ""
    trade_logger.warning(
        "[%s] %s - Reason: %s%s",
        tag,
        symbol,
        reason,
        suffix,
    )


def log_scan_rejected(symbol: str, reason: str, *, strategy: str = "") -> None:
    """Scan-time drop (pick_best loser, no setup) — not an order failure."""
    _log_reject("SCAN_REJECTED", symbol, reason, strategy=strategy)


def log_execution_rejected(symbol: str, reason: str, *, strategy: str = "") -> None:
    """Explicit WARNING when an approved signal fails at execution gates."""
    _log_reject("EXECUTION_REJECTED", symbol, reason, strategy=strategy)


class TradeExecutor:
    """Opens futures positions with validated sizing and structured DB metadata."""

    def __init__(self, exchange: BinanceExchangeManager, db: DatabaseManager) -> None:
        self.exchange = exchange
        self.db = db
        self.telegram = None
        self._entry_governor = ExecutionGovernor()
        self._live_orphans: dict[str, dict[str, Any]] = {}
        self._live_orphans_lock = threading.Lock()

    def _notify_uncertain_order(
        self,
        *,
        symbol: str,
        side: str,
        status: str,
        client_order_id: str = "",
        order_id: str = "",
        context: str = "order",
    ) -> None:
        tg = getattr(self, "telegram", None)
        sender = getattr(tg, "send_uncertain_order_alert", None) if tg is not None else None
        if not callable(sender):
            return
        try:
            sender(
                symbol=symbol,
                side=side,
                status=status,
                client_order_id=client_order_id,
                order_id=order_id,
                context=context,
            )
        except Exception as exc:
            error_logger.debug("Uncertain-order Telegram alert failed: %s", exc)

    def _notify_protection(
        self,
        *,
        symbol: str,
        side: str,
        detail: str,
        verified: bool,
    ) -> None:
        tg = getattr(self, "telegram", None)
        sender = getattr(tg, "send_protection_alert", None) if tg is not None else None
        if not callable(sender):
            return
        try:
            sender(symbol=symbol, side=side, detail=detail, verified=verified)
        except Exception as exc:
            error_logger.debug("Protection Telegram alert failed: %s", exc)

    def _validate_stop_loss(
        self, action: str, entry_price: float, sl_price: float
    ) -> tuple[bool, str]:
        """Ensure stop loss is on the correct side of entry."""
        if entry_price <= 0 or sl_price <= 0:
            return False, "invalid_entry_or_sl_price"
        if action == "LONG" and sl_price >= entry_price:
            return (
                False,
                f"long_sl_must_be_below_entry entry={entry_price:.6f} sl={sl_price:.6f}",
            )
        if action == "SHORT" and sl_price <= entry_price:
            return (
                False,
                f"short_sl_must_be_above_entry entry={entry_price:.6f} sl={sl_price:.6f}",
            )
        return True, "ok"

    def _validate_tp_ladder(
        self,
        action: str,
        entry_price: float,
        tp1: float,
        tp2: float,
        tp3: float,
    ) -> tuple[bool, str]:
        """Ensure take-profit rungs are on the profitable side of entry."""
        if action == "LONG":
            for label, tp in (("TP1", tp1), ("TP2", tp2), ("TP3", tp3)):
                if tp > 0 and tp <= entry_price:
                    return (
                        False,
                        f"long_{label.lower()}_must_be_above_entry "
                        f"entry={entry_price:.6f} {label.lower()}={tp:.6f}",
                    )
            if tp1 > 0 and tp2 > 0 and tp2 <= tp1:
                return False, "long_tp2_must_be_above_tp1"
            if tp2 > 0 and tp3 > 0 and tp3 <= tp2:
                return False, "long_tp3_must_be_above_tp2"
        else:
            for label, tp in (("TP1", tp1), ("TP2", tp2), ("TP3", tp3)):
                if tp > 0 and tp >= entry_price:
                    return (
                        False,
                        f"short_{label.lower()}_must_be_below_entry "
                        f"entry={entry_price:.6f} {label.lower()}={tp:.6f}",
                    )
            if tp1 > 0 and tp2 > 0 and tp2 >= tp1:
                return False, "short_tp2_must_be_below_tp1"
            if tp2 > 0 and tp3 > 0 and tp3 >= tp2:
                return False, "short_tp3_must_be_below_tp2"
        return True, "ok"

    def _resolve_execution_levels(
        self,
        action: str,
        execution_price: float,
        atr: float,
        rules: SymbolRules,
        structure: dict[str, Any],
        strategy: str,
    ) -> Optional[tuple[float, float, float, float]]:
        """Compute SL/TP strictly from the confirmed execution entry price."""
        if execution_price <= 0 or atr <= 0:
            return None

        range_mode = is_range_strategy(strategy)
        if range_mode:
            rmeta = RangeMetadata()
            for key, value in structure.items():
                if hasattr(rmeta, key):
                    setattr(rmeta, key, value)
            sl, tp1, tp2, tp3 = compute_range_sl_tp(
                action, execution_price, atr, rmeta
            )
            sl = round_protective_price(
                sl, rules.tick_size, rules.price_precision, side=action, kind="SL"
            )
            tp1 = round_protective_price(
                tp1, rules.tick_size, rules.price_precision, side=action, kind="TP"
            )
            tp2 = round_protective_price(
                tp2, rules.tick_size, rules.price_precision, side=action, kind="TP"
            )
            tp3 = round_protective_price(
                tp3, rules.tick_size, rules.price_precision, side=action, kind="TP"
            )
        else:
            sl, tp1, tp2, tp3 = self.calculate_sl_tp(
                action, execution_price, atr, rules, structure=structure
            )

        sl_ok, sl_reason = self._validate_stop_loss(action, execution_price, sl)
        if not sl_ok:
            return None
        sl, tp1, tp2, tp3 = self._optimize_tp1_rr_if_needed(
            action, execution_price, sl, tp1, tp2, tp3, rules
        )

        tp_ok, tp_reason = self._validate_tp_ladder(
            action, execution_price, tp1, tp2, tp3
        )
        if not tp_ok:
            error_logger.error(
                "Invalid TP ladder for %s %s at entry %.6f — %s",
                strategy,
                action,
                execution_price,
                tp_reason,
            )
            return None
        return sl, tp1, tp2, tp3

    def _optimize_tp1_rr_if_needed(
        self,
        action: str,
        entry_price: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        rules: SymbolRules,
    ) -> tuple[float, float, float, float]:
        """
        Keep structure SL/TP when effective TP1 R:R is already acceptable.
        When R:R is poor, extend TP1 (and scale TP2/TP3 proportionally) so
        effective R:R reaches MIN_TP1_RISK_REWARD — SL is never moved or rejected.
        """
        if not Config.ENABLE_TP1_RR_OPTIMIZER:
            return sl, tp1, tp2, tp3

        sl_dist = abs(entry_price - sl)
        tp1_dist = abs(tp1 - entry_price)
        if sl_dist <= 0 or tp1 <= 0 or tp1_dist <= 0:
            return sl, tp1, tp2, tp3

        accept_rr = max(float(Config.MIN_TP1_RR_ACCEPT), 0.1)
        target_rr = max(float(Config.MIN_TP1_RISK_REWARD), accept_rr)
        effective_rr = (tp1_dist * TP1_PORTION) / sl_dist
        if effective_rr >= accept_rr:
            return sl, tp1, tp2, tp3

        # effective_rr = (tp1_dist * TP1_PORTION) / sl_dist  →  target_rr
        new_tp1_dist = sl_dist * target_rr / TP1_PORTION
        scale = new_tp1_dist / tp1_dist

        if action == "LONG":
            new_tp1 = entry_price + new_tp1_dist
            new_tp2 = entry_price + abs(tp2 - entry_price) * scale if tp2 > 0 else tp2
            new_tp3 = entry_price + abs(tp3 - entry_price) * scale if tp3 > 0 else tp3
        else:
            new_tp1 = entry_price - new_tp1_dist
            new_tp2 = entry_price - abs(tp2 - entry_price) * scale if tp2 > 0 else tp2
            new_tp3 = entry_price - abs(tp3 - entry_price) * scale if tp3 > 0 else tp3

        new_tp1 = round_protective_price(
            new_tp1, rules.tick_size, rules.price_precision, side=action, kind="TP"
        )
        new_tp2 = round_protective_price(
            new_tp2, rules.tick_size, rules.price_precision, side=action, kind="TP"
        )
        new_tp3 = round_protective_price(
            new_tp3, rules.tick_size, rules.price_precision, side=action, kind="TP"
        )

        trade_logger.info(
            "RR optimized — TP extended for effective 1:%.1f "
            "(rr was %.2f tp1_dist %.6f -> %.6f | scale %.2fx)",
            target_rr,
            effective_rr,
            tp1_dist,
            abs(new_tp1 - entry_price),
            scale,
        )
        return sl, new_tp1, new_tp2, new_tp3

    def _apply_execution_levels(
        self,
        *,
        trade_id: str,
        fill_price: float,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        metadata: dict[str, Any],
    ) -> None:
        """Persist entry-linked SL/TP after fill or entry-price refinement."""
        metadata["best_price"] = fill_price
        metadata["r_distance"] = abs(fill_price - sl)
        self.db.update_trade(
            trade_id,
            {
                "entry_price": fill_price,
                "stop_loss": sl,
                "take_profit_1": tp1,
                "take_profit_2": tp2,
                "take_profit_3": tp3,
                "metadata": metadata,
            },
        )

    def calculate_sl_tp(
        self,
        action: str,
        entry_price: float,
        atr: float,
        rules: SymbolRules,
        structure: Optional[dict[str, Any]] = None,
    ) -> tuple[float, float, float, float]:
        """Structural SL + dynamic ATR/structure take-profit ladder."""
        meta = StructureMetadata()
        structure = structure or {}
        for key, value in structure.items():
            if hasattr(meta, key):
                setattr(meta, key, value)

        sl = compute_structural_sl(action, entry_price, atr, meta)
        if Config.USE_DYNAMIC_TP_LADDER:
            tp1, tp2, tp3 = compute_dynamic_tp_ladder(
                action, entry_price, atr, meta, extra_structure=structure
            )
        else:
            _, tp1, tp2, tp3 = compute_rr_ladder(action, entry_price, sl)

        return (
            round_protective_price(
                sl, rules.tick_size, rules.price_precision, side=action, kind="SL"
            ),
            round_protective_price(
                tp1, rules.tick_size, rules.price_precision, side=action, kind="TP"
            ),
            round_protective_price(
                tp2, rules.tick_size, rules.price_precision, side=action, kind="TP"
            ),
            round_protective_price(
                tp3, rules.tick_size, rules.price_precision, side=action, kind="TP"
            ),
        )

    @staticmethod
    def _format_quantity(
        exchange: BinanceExchangeManager,
        quantity: float,
        rules: SymbolRules,
        symbol: str,
    ) -> float:
        try:
            return exchange.format_quantity(symbol, quantity)
        except Exception as exc:
            error_logger.debug(
                "Exchange format_quantity fallback for %s: %s", symbol, exc
            )
            return amount_to_precision(
                quantity, rules.step_size, rules.quantity_precision
            )

    def calculate_position_size(
        self,
        entry_price: float,
        sl_price: float,
        rules: SymbolRules,
        score: float = 80.0,
        strategy: str = "DEFAULT",
        symbol: str = "",
    ) -> float:
        """Risk-based position sizing with tiered score multiplier."""
        balance = self.exchange.get_futures_balance(force_refresh=False)
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
        try:
            if symbol:
                quantity = self._format_quantity(
                    self.exchange, quantity, rules, symbol
                )
            else:
                quantity = amount_to_precision(
                    quantity, rules.step_size, rules.quantity_precision
                )
        except Exception as exc:
            error_logger.error(
                "Quantity precision formatting failed | step=%s prec=%s | %s",
                rules.step_size,
                rules.quantity_precision,
                exc,
            )
            return 0.0

        min_valid_qty = minimum_order_quantity(
            entry_price,
            rules.min_qty,
            rules.min_notional,
            rules.step_size,
            rules.quantity_precision,
        )
        max_notional = balance * Config.MAX_POSITION_VALUE_MULTIPLIER

        if quantity <= 0 or quantity < rules.min_qty:
            bumped_risk = min_valid_qty * sl_distance
            bumped_notional = min_valid_qty * entry_price
            if (
                min_valid_qty >= rules.min_qty
                and bumped_notional >= rules.min_notional
                and bumped_notional <= max_notional
                and bumped_risk <= risk_amount * Config.MIN_NOTIONAL_RISK_TOLERANCE
            ):
                trade_logger.info(
                    "Quantity bumped to exchange minimum | raw=%.8f -> min=%.8f | "
                    "notional=$%.2f risk=$%.2f (budget=$%.2f)",
                    quantity,
                    min_valid_qty,
                    bumped_notional,
                    bumped_risk,
                    risk_amount,
                )
                quantity = min_valid_qty
            else:
                trade_logger.warning(
                    "Quantity %.8f below min_qty %.8f and minimum bump disallowed "
                    "(notional=$%.2f max=$%.2f risk=$%.2f budget=$%.2f).",
                    quantity,
                    rules.min_qty,
                    min_valid_qty * entry_price,
                    max_notional,
                    min_valid_qty * sl_distance,
                    risk_amount,
                )
                return 0.0

        notional = quantity * entry_price
        if notional > max_notional:
            trade_logger.warning(
                "Position size capped: notional $%.2f exceeds limit $%.2f.",
                notional,
                max_notional,
            )
            quantity, notional = cap_quantity_to_notional(
                quantity,
                entry_price,
                max_notional,
                rules.min_qty,
                rules.min_notional,
                rules.step_size,
                rules.quantity_precision,
            )
            if quantity <= 0:
                trade_logger.warning(
                    "Capped size invalid: max_notional=$%.2f min_notional=$%.2f "
                    "min_qty=%.8f.",
                    max_notional,
                    rules.min_notional,
                    rules.min_qty,
                )
                return 0.0
            trade_logger.info(
                "Position capped to max notional | qty=%.8f notional=$%.2f",
                quantity,
                notional,
            )

        if notional < rules.min_notional:
            if (
                min_valid_qty > quantity
                and (min_valid_qty * entry_price) <= max_notional
                and (min_valid_qty * sl_distance)
                <= risk_amount * Config.MIN_NOTIONAL_RISK_TOLERANCE
            ):
                quantity = min_valid_qty
                notional = quantity * entry_price
                trade_logger.info(
                    "Notional bumped to minimum | qty=%.8f notional=$%.2f",
                    quantity,
                    notional,
                )
            else:
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
        self._register_live_orphan(trade_data)
        path = os.path.join(Config.DATA_DIR, "orphan_fills.jsonl")
        try:
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(trade_data, default=str) + "\n")
        except OSError as exc:
            error_logger.error("Failed to write orphan fill recovery file: %s", exc)

    def _register_live_orphan(self, trade_data: dict[str, Any]) -> None:
        trade_id = str(trade_data.get("trade_id") or "")
        if not trade_id:
            return
        with self._live_orphans_lock:
            self._live_orphans[trade_id] = dict(trade_data)

    def live_orphan_trades(self) -> list[dict[str, Any]]:
        """Open fills the DB has not accepted yet — still monitored for SL/TP."""
        with self._live_orphans_lock:
            return [dict(row) for row in self._live_orphans.values()]

    def forget_live_orphan(self, trade_id: str) -> None:
        with self._live_orphans_lock:
            self._live_orphans.pop(str(trade_id or ""), None)

    def purge_orphans_present_in_db(self) -> None:
        with self._live_orphans_lock:
            ids = list(self._live_orphans)
        getter = getattr(self.db, "get_trade", None)
        if not callable(getter):
            return
        for trade_id in ids:
            try:
                if getter(trade_id) is not None:
                    self.forget_live_orphan(trade_id)
            except Exception:
                continue

    def _wallet_for_dca_plan(self) -> float:
        """Live wallet, else today's daily baseline. Never invent a balance."""
        try:
            wallet = float(self.exchange.get_futures_balance(force_refresh=False) or 0.0)
        except Exception:
            wallet = 0.0
        if wallet > 0:
            return wallet
        try:
            stats = self.db.get_daily_stats(utc_today_str()) or {}
            return safe_float(stats.get("start_balance"))
        except Exception:
            return 0.0

    def _revert_claimed_dca_stage(
        self, trade: dict[str, Any], step: int, metadata: dict[str, Any]
    ) -> None:
        if metadata.get("dca_uncertain_client_id") or metadata.get(
            "dca_uncertain_order_id"
        ):
            return
        revert = getattr(self.db, "revert_entry_stage", None)
        reverted = False
        if callable(revert):
            reverted = bool(revert(str(trade.get("trade_id") or ""), step))
        if reverted:
            prior = max(int(step) - 1, 1)
            trade["entry_stage"] = prior
            metadata["entry_stage"] = prior
            metadata["dca_entries"] = prior
            trade["metadata"] = metadata

    def _should_attach_native_exits(
        self,
        attach_native_exits: bool = False,
        metadata: Optional[dict[str, Any]] = None,
        structure: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Native TP/SL follows ENABLE_NATIVE_TP_SL (default True).

        Virtual in-process SL is a backup, not a substitute. Callers cannot
        enable native by passing attach_native_exits while the config flag is False.
        """
        del attach_native_exits, metadata, structure
        return bool(Config.ENABLE_NATIVE_TP_SL)

    def _place_native_exit_orders(
        self,
        *,
        trade_id: str,
        symbol: str,
        position_side: str,
        sl: float,
        tp1: float,
        tp2: float,
        tp3: float,
        metadata: dict[str, Any],
        quantity: float,
    ) -> bool:
        """Place exchange-native STOP_MARKET + TAKE_PROFIT_MARKET bracket after entry."""
        tp_specs: list[tuple[str, float, float]] = []
        if Config.ENABLE_PARTIAL_TP:
            tp_specs = [
                ("TP1", tp1, safe_float(metadata.get("tp1_quantity"))),
                ("TP2", tp2, safe_float(metadata.get("tp2_quantity"))),
                ("TP3", tp3, safe_float(metadata.get("tp3_quantity"))),
            ]
        else:
            tp_specs = [("TP3", tp3, quantity)]

        from core.exchange_protection import (
            alert_unprotected_position,
            apply_protection_metadata,
            ensure_native_stop,
            protection_is_verified,
            verify_protective_order_status,
        )

        telegram = getattr(self, "telegram", None)

        if sl <= 0:
            metadata["native_orders_placed"] = False
            metadata["native_protection_verified"] = False
            self.db.update_trade(trade_id, {"metadata": metadata})
            error_logger.critical(
                "PROTECTION_PLACEMENT_FAILED %s %s — invalid SL, native bracket skipped",
                symbol,
                position_side,
            )
            alert_unprotected_position(
                telegram, symbol=symbol, position_side=position_side, reason="invalid_sl"
            )
            return False

        old_id = str(metadata.get("native_sl_order_id") or "") or None
        ok, reason, oid = ensure_native_stop(
            self.exchange,
            symbol=symbol,
            position_side=position_side,
            sl_price=sl,
            old_sl_order_id=old_id,
        )
        apply_protection_metadata(metadata, reason=reason, order_id=oid)
        verified = bool(metadata.get("native_protection_verified"))
        self.db.update_trade(trade_id, {"metadata": metadata})
        if not verified:
            alert_unprotected_position(
                telegram,
                symbol=symbol,
                position_side=position_side,
                reason=reason,
            )
            return False

        tp_ids: dict[str, str] = {}
        for level, tp_price, tp_qty in tp_specs:
            if tp_price <= 0 or tp_qty <= 0:
                continue
            try:
                close_side = "SELL" if str(position_side).upper() == "LONG" else "BUY"
                tp_resp = self.exchange.place_conditional_order(
                    symbol,
                    close_side,
                    position_side,
                    "TAKE_PROFIT_MARKET",
                    tp_price,
                    quantity=tp_qty,
                )
                tp_id = str((tp_resp or {}).get("orderId") or "")
                if not tp_id:
                    continue
                status = verify_protective_order_status(self.exchange, symbol, tp_id)
                if status in {"CANCELED", "EXPIRED", "REJECTED", "UNVERIFIED"}:
                    error_logger.warning(
                        "Native %s unverified %s status=%s — SL remains %s",
                        level,
                        symbol,
                        status,
                        oid,
                    )
                    continue
                tp_ids[level.upper()] = tp_id
            except Exception as exc:
                error_logger.warning(
                    "Native %s order failed for %s %s: %s",
                    level,
                    symbol,
                    position_side,
                    exc,
                )
        if tp_ids:
            metadata["native_tp_order_ids"] = tp_ids
            self.db.update_trade(trade_id, {"metadata": metadata})
        trade_logger.info(
            "Native SL verified | %s %s | sl=%.6f | orderId=%s | tps=%s",
            symbol,
            position_side,
            sl,
            oid,
            list(tp_ids.keys()),
        )
        self._notify_protection(
            symbol=symbol,
            side=position_side,
            detail=f"verified id={oid}",
            verified=True,
        )
        return True

    def execute_trade(
        self,
        symbol: str,
        action: str,
        atr: float,
        current_price: float,
        strategy: str = "DEFAULT",
        score: float = 0.0,
        structure_metadata: Optional[dict[str, Any]] = None,
        attach_native_exits: bool = False,
    ) -> Optional[dict[str, Any]]:
        """
        Execute a market entry after structural SL / R:R validation.
        Returns result dict on success, else None.
        """
        if atr <= 0:
            log_execution_rejected(symbol, f"invalid ATR ({atr})", strategy=strategy)
            return None

        if action not in ("LONG", "SHORT"):
            log_execution_rejected(symbol, f"invalid action {action}", strategy=strategy)
            return None

        if Config.DRY_RUN:
            trade_logger.warning(
                "[DRY_RUN] skipped live order %s %s | strategy=%s | score=%.1f | price=%.6f",
                symbol,
                action,
                strategy,
                score,
                current_price,
            )
            log_execution_rejected(
                symbol, "DRY_RUN enabled — order not sent", strategy=strategy
            )
            return None

        allowed, pace_reason = self._entry_governor.allow_entry_order()
        if not allowed:
            self._entry_governor.note_blocked(symbol, pace_reason)
            log_execution_rejected(symbol, pace_reason, strategy=strategy)
            return None

        ledger = get_execution_ledger()
        existing = ledger.find_by_ids(symbol=symbol)
        if existing is not None and existing.is_unresolved():
            log_execution_rejected(
                symbol,
                f"duplicate execution blocked — {existing.exec_id} still {existing.phase.value}",
                strategy=strategy,
            )
            return None
        record = ledger.new_record(
            symbol=symbol,
            action=action,
            strategy=strategy,
            score=score,
        )
        record.atr = atr
        ledger.transition(
            record,
            ExecutionPhase.TRADE_APPROVED,
            extra="dispatched winner ready for order submission",
        )
        ledger.transition(
            record,
            ExecutionPhase.EXECUTION_QUEUED,
            extra=f"price={current_price:.6f}",
        )

        safety_fn = getattr(self.exchange, "get_execution_safety", None)
        if callable(safety_fn):
            try:
                raw_safety = safety_fn(symbol)
            except TypeError:
                raw_safety = safety_fn()
            if isinstance(raw_safety, tuple) and raw_safety and isinstance(raw_safety[0], str):
                safety_state, safety_reason = raw_safety[0], str(raw_safety[1] if len(raw_safety) > 1 else "")
                if safety_state != "EXECUTION_SAFE":
                    phase = {
                        "WS_DISCONNECTED": ExecutionPhase.WS_DISCONNECTED,
                        "RESYNC_REQUIRED": ExecutionPhase.RESYNC_REQUIRED,
                        "WS_WARMING": ExecutionPhase.RESYNC_REQUIRED,
                        "STALE_DATA": ExecutionPhase.RESYNC_REQUIRED,
                        "API_RATE_LIMITED": ExecutionPhase.RATE_LIMITED,
                        "IP_BANNED": ExecutionPhase.RATE_LIMITED,
                        "EXECUTION_PAUSED": ExecutionPhase.RATE_LIMITED,
                    }.get(safety_state, ExecutionPhase.RESYNC_REQUIRED)
                    ledger.transition(
                        record,
                        phase,
                        extra=safety_reason,
                        error_message=safety_reason,
                    )
                    log_execution_rejected(
                        symbol,
                        f"execution safety {safety_state} — {safety_reason}",
                        strategy=strategy,
                    )
                    return None

        trade_logger.info(
            "[EXECUTION_ATTEMPT] %s %s | %s | strategy=%s | score=%.1f | price=%.6f",
            record.exec_id,
            symbol,
            action,
            strategy,
            score,
            current_price,
        )

        with self.exchange.execution_context():
            live_price = self._resolve_live_execution_price(symbol, current_price)
            if live_price <= 0:
                ledger.transition(
                    record,
                    ExecutionPhase.ORDER_SUBMISSION_FAILED,
                    error_message="no live price",
                )
                log_execution_rejected(
                    symbol,
                    "no live price from REST or cache — cannot place order",
                    strategy=strategy,
                )
                return None
            return self._execute_trade_inner(
                symbol,
                action,
                atr,
                live_price,
                strategy,
                score,
                structure_metadata,
                record=record,
                attach_native_exits=attach_native_exits,
            )

    def _ws_needs_execution_rest_price(self, symbol: str, current_price: float) -> bool:
        if current_price <= 0:
            return True
        hub = None
        if hasattr(self.exchange, "get_market_data_hub"):
            hub = self.exchange.get_market_data_hub()
        if hub is None:
            return True
        requires_fn = getattr(hub, "execution_requires_rest_price", None)
        if callable(requires_fn):
            try:
                if requires_fn(symbol) is True:
                    return True
            except Exception:
                return True
        if getattr(hub, "_reconnect_in_progress", False) is True:
            return True
        warming_fn = getattr(hub, "is_ws_warming_up", None)
        if callable(warming_fn) and warming_fn() is True:
            return True
        stale_fn = getattr(hub, "ws_is_stale", None)
        if callable(stale_fn) and stale_fn() is True:
            return True
        snap_fn = getattr(hub, "get_ws_health_snapshot", None)
        if callable(snap_fn):
            snap = snap_fn()
            if isinstance(snap, dict):
                state = str(snap.get("state", "")).upper()
                if state in {"WARMING", "RECONNECTING", "STALE", "DEGRADED"}:
                    return True
        fresh_fn = getattr(hub, "get_fresh_ticker_price", None)
        if callable(fresh_fn):
            try:
                raw = fresh_fn(symbol)
            except Exception:
                return True
            if raw is None:
                return True
            if isinstance(raw, (int, float)) and float(raw) <= 0:
                return True
        return False

    def _fetch_execution_rest_price(self, symbol: str) -> float:
        """Immediate REST last price — never waits on WebSocket recovery."""
        for name in ("fetch_ticker", "get_symbol_price", "get_ticker"):
            fn = getattr(self.exchange, name, None)
            if not callable(fn):
                continue
            try:
                price = _positive_price(fn(symbol))
            except Exception as exc:
                trade_logger.debug(
                    "Execution REST %s failed for %s: %s", name, symbol, exc
                )
                continue
            if price > 0:
                return price
        if hasattr(self.exchange, "get_live_mark_price"):
            try:
                mark = _positive_price(
                    self.exchange.get_live_mark_price(symbol, allow_rest=True)
                )
            except Exception:
                mark = 0.0
            if mark > 0:
                return mark
        return 0.0

    def _cached_execution_price(self, symbol: str, current_price: float) -> float:
        hub = None
        if hasattr(self.exchange, "get_market_data_hub"):
            hub = self.exchange.get_market_data_hub()
        if hub is not None and hasattr(hub, "get_price"):
            try:
                cached = _positive_price(hub.get_price(symbol))
            except Exception:
                cached = 0.0
            if cached > 0:
                return cached
        return current_price if current_price > 0 else 0.0

    def _market_data_blocks_new_entry(self, symbol: str) -> bool:
        """True when WS/cache is explicitly unready for a NEW entry."""
        hub = None
        if hasattr(self.exchange, "get_market_data_hub"):
            hub = self.exchange.get_market_data_hub()
        if hub is None:
            return False
        ready_fn = getattr(hub, "is_market_data_ready_for_entry", None)
        if callable(ready_fn):
            try:
                raw = ready_fn(symbol)
            except TypeError:
                raw = ready_fn()
            if isinstance(raw, tuple) and raw and raw[0] is False:
                return True
        if getattr(hub, "_reconnect_in_progress", False) is True:
            return True
        warming_fn = getattr(hub, "is_ws_warming_up", None)
        if callable(warming_fn):
            try:
                if warming_fn() is True:
                    return True
            except Exception:
                pass
        stale_fn = getattr(hub, "ws_is_stale", None)
        if callable(stale_fn):
            try:
                if stale_fn() is True:
                    return True
            except Exception:
                pass
        return False

    def _execution_rest_halted(self) -> bool:
        """True only for a real Binance halt — not the used-weight governor."""
        snap_fn = getattr(self.exchange, "rest_usage_snapshot", None)
        if callable(snap_fn):
            try:
                snap = snap_fn() or {}
            except Exception:
                snap = {}
            if str(snap.get("state") or "") in {"IP_BANNED", "API_RATE_LIMITED"}:
                return True
        return False

    def _resolve_live_execution_price(self, symbol: str, current_price: float) -> float:
        """WS first, then one execution-lane REST ticker, then cached/signal price."""
        hub = None
        if hasattr(self.exchange, "get_market_data_hub"):
            hub = self.exchange.get_market_data_hub()
        if hub is not None:
            fresh_fn = getattr(hub, "get_fresh_ticker_price", None)
            if callable(fresh_fn):
                try:
                    fresh = _positive_price(fresh_fn(symbol))
                except Exception:
                    fresh = 0.0
                if fresh > 0:
                    return fresh

        if not self._execution_rest_halted():
            rest_price = self._fetch_execution_rest_price(symbol)
            if rest_price > 0:
                if hub is not None and hasattr(hub, "seed_tickers_from_rest"):
                    try:
                        hub.seed_tickers_from_rest(
                            {symbol.upper(): {"lastPrice": rest_price}}
                        )
                    except Exception:
                        pass
                trade_logger.info(
                    "Execution price REST fallback %s | signal=%.6f rest=%.6f",
                    symbol,
                    current_price,
                    rest_price,
                )
                return rest_price

        cached = self._cached_execution_price(symbol, current_price)
        if cached > 0:
            trade_logger.info(
                "Execution price cache/signal fallback %s | price=%.6f",
                symbol,
                cached,
            )
            return cached
        return 0.0

    def _execute_trade_inner(
        self,
        symbol: str,
        action: str,
        atr: float,
        current_price: float,
        strategy: str = "DEFAULT",
        score: float = 0.0,
        structure_metadata: Optional[dict[str, Any]] = None,
        record: Optional[ExecutionRecord] = None,
        attach_native_exits: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Execute entry under high-priority REST path (not blocked by scan loops)."""
        if Config.is_mega_cap_blacklisted(symbol):
            log_execution_rejected(
                symbol, "mega-cap pair excluded from trading", strategy=strategy
            )
            return None

        on_cooldown, cooldown_reason = self.db.is_symbol_on_cooldown(symbol)
        if on_cooldown:
            log_execution_rejected(
                symbol, f"symbol on cooldown ({cooldown_reason})", strategy=strategy
            )
            return None

        with entry_in_flight_mutex(symbol, blocking=False) as acquired:
            if not acquired:
                log_execution_rejected(
                    symbol,
                    "entry already in flight for symbol",
                    strategy=strategy,
                )
                return None

            blocked, block_reason = symbol_blocked_for_new_entry(
                self.exchange,
                self.db,
                symbol,
                ignore_in_flight=True,
            )
            if blocked:
                log_execution_rejected(symbol, block_reason, strategy=strategy)
                return None

            return self._place_entry_order(
                symbol,
                action,
                atr,
                current_price,
                strategy=strategy,
                score=score,
                structure_metadata=structure_metadata,
                record=record,
                attach_native_exits=attach_native_exits,
            )

    def _place_entry_order(
        self,
        symbol: str,
        action: str,
        atr: float,
        current_price: float,
        *,
        strategy: str = "DEFAULT",
        score: float = 0.0,
        structure_metadata: Optional[dict[str, Any]] = None,
        record: Optional[ExecutionRecord] = None,
        attach_native_exits: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Place entry order — caller must hold entry_in_flight_mutex for symbol."""
        position_side = action
        rules = self.exchange.get_symbol_rules(symbol)
        structure = structure_metadata or {}
        range_mode = is_range_strategy(strategy)

        pre_levels = self._resolve_execution_levels(
            action, current_price, atr, rules, structure, strategy
        )
        if not pre_levels:
            log_execution_rejected(
                symbol,
                "invalid SL/TP ladder at signal price — cannot size entry",
                strategy=strategy,
            )
            self.db.log_signal_rejection(
                symbol,
                action,
                score,
                ["invalid_sl_tp_at_signal_price"],
                strategy=strategy,
            )
            return None
        sl, tp1, tp2, tp3 = pre_levels
        if last_resort_sl_hit(action, current_price, sl) or not sl_is_valid_vs_entry(
            action, current_price, sl
        ):
            log_execution_rejected(
                symbol,
                f"live price already through planned SL "
                f"price={current_price:.6f} sl={sl:.6f}",
                strategy=strategy,
            )
            return None
        if not range_mode:
            opposing = safe_float(structure.get("opposing_liquidity"))
            rr_ok, rr_reason = check_opposing_liquidity_rr(
                action, current_price, sl, opposing
            )
            if not rr_ok:
                log_execution_rejected(symbol, rr_reason, strategy=strategy)
                self.db.log_signal_rejection(
                    symbol, action, score, [rr_reason], strategy=strategy
                )
                return None

        try:
            quantity = self.calculate_position_size(
                current_price,
                sl,
                rules,
                score=score,
                strategy=strategy,
                symbol=symbol,
            )
        except OrderExecutionError as exc:
            log_execution_rejected(symbol, f"position sizing failed — {exc}", strategy=strategy)
            return None
        except Exception as exc:
            error_logger.error(
                "Unexpected position sizing error for %s: %s", symbol, exc, exc_info=True
            )
            log_execution_rejected(
                symbol, f"position sizing failed — {exc}", strategy=strategy
            )
            return None

        if quantity <= 0:
            log_execution_rejected(
                symbol,
                "position size rounds to zero after capping/minimum checks",
                strategy=strategy,
            )
            return None

        try:
            metadata = self._build_partial_quantities(quantity, rules)
        except OrderExecutionError as exc:
            log_execution_rejected(symbol, f"TP partition invalid — {exc}", strategy=strategy)
            return None

        metadata["atr_at_entry"] = atr
        metadata["trailing_active"] = False
        metadata["runner_mode"] = Config.ENABLE_TP3_RUNNER
        metadata["runner_active"] = False
        metadata["tp3_is_estimate"] = Config.ENABLE_TP3_RUNNER
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
        if structure.get("backtest_validated"):
            metadata["backtest_validated"] = True
            metadata["backtest_win_rate"] = safe_float(structure.get("backtest_win_rate"))
            metadata["backtest_wins"] = int(safe_float(structure.get("backtest_wins")))
            metadata["backtest_trades"] = int(safe_float(structure.get("backtest_trades")))

        try:
            leverage = self.exchange.optimize_and_set_leverage(symbol)
        except Exception as exc:
            log_execution_rejected(
                symbol, f"leverage setup failed — {exc}", strategy=strategy
            )
            leverage = Config.MAX_LEVERAGE

        trade_side = "BUY" if action == "LONG" else "SELL"
        ledger = get_execution_ledger()
        client_order_id = record.client_order_id if record else ""
        if record:
            record.quantity = quantity
            record.entry = current_price
            record.sl = sl
            record.tp = tp1
            record.atr = atr
            ledger.transition(
                record,
                ExecutionPhase.ORDER_SUBMITTING,
                extra=(
                    f"qty={quantity:.8f} entry={current_price:.6f} "
                    f"sl={sl:.6f} tp1={tp1:.6f}"
                ),
                quantity=quantity,
                entry=current_price,
            )
        trade_logger.info(
            "[ORDER_SUBMITTING] dispatching %s %s %s | qty=%.8f | price=%.6f | strategy=%s | clientOrderId=%s",
            symbol,
            trade_side,
            position_side,
            quantity,
            current_price,
            strategy,
            client_order_id,
        )
        try:
            response = self.exchange.execute_futures_order(
                symbol=symbol,
                side=trade_side,
                position_side=position_side,
                quantity=quantity,
                new_client_order_id=client_order_id or None,
            )
        except OrderExecutionError as exc:
            msg = str(exc)
            if record:
                if msg.startswith("RATE_LIMITED"):
                    phase = ExecutionPhase.RATE_LIMITED
                elif msg.startswith("ORDER_REJECTED"):
                    phase = ExecutionPhase.ORDER_REJECTED
                else:
                    phase = ExecutionPhase.ORDER_SUBMISSION_FAILED
                ledger.transition(record, phase, error_message=msg)
            log_execution_rejected(symbol, f"order rejected — {exc}", strategy=strategy)
            return None

        if not response:
            if record:
                ledger.transition(
                    record,
                    ExecutionPhase.ORDER_SUBMISSION_FAILED,
                    error_message="empty order response",
                )
            log_execution_rejected(
                symbol, "exchange returned empty order response", strategy=strategy
            )
            return None

        order_status = str(response.get("status") or "").upper()
        exchange_order_id = str(response.get("orderId", ""))
        if record:
            ledger.transition(
                record,
                ExecutionPhase.ORDER_ID_RECEIVED
                if exchange_order_id
                else ExecutionPhase.BINANCE_ACK,
                binance_order_id=exchange_order_id,
                extra=f"status={order_status or 'UNKNOWN'}",
            )

        if order_status not in {"FILLED", "PARTIALLY_FILLED"}:
            if record:
                ledger.transition(
                    record,
                    ExecutionPhase.ORDER_ACCEPTED_NOT_FILLED,
                    binance_order_id=exchange_order_id,
                    extra=f"status={order_status or 'UNKNOWN'}",
                )
                ledger.transition(
                    record,
                    ExecutionPhase.ORDER_STATUS_UNCERTAIN,
                    binance_order_id=exchange_order_id,
                    extra="awaiting late fill / user-data / reconcile",
                )
            trade_logger.warning(
                "[ORDER_STATUS_UNCERTAIN] %s %s accepted orderId=%s status=%s — "
                "not opening a local position and not sending a second order",
                symbol,
                action,
                exchange_order_id or "?",
                order_status or "UNKNOWN",
            )
            self._notify_uncertain_order(
                symbol=symbol,
                side=action,
                status=order_status or "UNKNOWN",
                client_order_id=client_order_id,
                order_id=exchange_order_id,
                context="entry",
            )
            return None

        return self._complete_filled_entry(
            symbol=symbol,
            action=action,
            atr=atr,
            current_price=current_price,
            strategy=strategy,
            score=score,
            structure=structure,
            record=record,
            response=response,
            quantity=quantity,
            leverage=leverage,
            metadata=metadata,
            rules=rules,
            attach_native_exits=attach_native_exits,
        )

    def adopt_confirmed_fill(self, fill: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Idempotent late-fill adoption from user-data WS or REST reconcile."""
        ledger = get_execution_ledger()
        record = ledger.find_by_ids(
            client_order_id=str(fill.get("clientOrderId") or ""),
            binance_order_id=str(fill.get("orderId") or ""),
            symbol=str(fill.get("symbol") or ""),
        )
        if record is None or record.is_opened() or not record.is_unresolved():
            return None
        status = str(fill.get("status") or "FILLED").upper()
        if status not in {"FILLED", "PARTIALLY_FILLED"}:
            return None
        fill_qty = safe_float(fill.get("executedQty"))
        fill_price = safe_float(fill.get("avgPrice"))
        if fill_qty <= 0 or record.atr <= 0:
            return None
        if fill_price <= 0:
            fill_price = float(record.entry or 0.0)
        if fill_price <= 0:
            return None
        rules = self.exchange.get_symbol_rules(record.symbol)
        structure: dict[str, Any] = {}
        try:
            metadata = self._build_partial_quantities(fill_qty, rules)
        except OrderExecutionError:
            metadata = {}
        metadata["atr_at_entry"] = record.atr
        metadata["strategy_tag"] = record.strategy
        metadata["size_multiplier"] = size_multiplier_for_score(record.score)
        response = {
            "status": status,
            "orderId": fill.get("orderId") or record.binance_order_id,
            "avgPrice": fill_price,
            "executedQty": fill_qty,
        }
        trade_logger.info(
            "[LATE_FILL] adopting %s %s | %s | orderId=%s qty=%.8f px=%.6f",
            record.exec_id,
            record.symbol,
            record.action,
            response["orderId"],
            fill_qty,
            fill_price,
        )
        return self._complete_filled_entry(
            symbol=record.symbol,
            action=record.action,
            atr=record.atr,
            current_price=fill_price or record.entry,
            strategy=record.strategy,
            score=record.score,
            structure=structure,
            record=record,
            response=response,
            quantity=fill_qty,
            leverage=Config.MAX_LEVERAGE,
            metadata=metadata,
            rules=rules,
            attach_native_exits=bool(Config.ENABLE_NATIVE_TP_SL),
        )

    def _complete_filled_entry(
        self,
        *,
        symbol: str,
        action: str,
        atr: float,
        current_price: float,
        strategy: str,
        score: float,
        structure: dict[str, Any],
        record: Optional[ExecutionRecord],
        response: dict[str, Any],
        quantity: float,
        leverage: int,
        metadata: dict[str, Any],
        rules: SymbolRules,
        attach_native_exits: bool = False,
    ) -> Optional[dict[str, Any]]:
        position_side = action
        order_status = str(response.get("status") or "").upper()
        exchange_order_id = str(response.get("orderId", ""))
        if record and record.is_opened():
            return None
        fill_price = resolve_binance_fill_price(response, fallback=current_price)

        post_levels = self._resolve_execution_levels(
            action, fill_price, atr, rules, structure, strategy
        )
        if not post_levels:
            sl_ok, sl_reason = False, "invalid_sl_tp_at_fill_price"
        else:
            sl, tp1, tp2, tp3 = post_levels
            sl_ok, sl_reason = self._validate_stop_loss(action, fill_price, sl)

        if not sl_ok:
            error_logger.critical(
                "Post-fill invalid SL for %s %s — %s | closing orphan position.",
                symbol,
                action,
                sl_reason,
            )
            orphan_data = {
                "trade_id": str(uuid.uuid4()),
                "symbol": symbol,
                "side": action,
                "entry_price": fill_price,
                "quantity": quantity,
                "status": TRADE_STATUS_OPEN,
                "exchange_order_id": exchange_order_id,
                "opened_at": utc_now().isoformat(),
                "strategy": strategy,
                "metadata": metadata,
            }
            self._persist_orphan_fill(orphan_data)
            try:
                self.exchange.close_position_quantity(symbol, position_side, quantity)
            except Exception as exc:
                error_logger.error("Failed to close orphan %s: %s", symbol, exc)
            return None

        metadata.setdefault("size_multiplier", size_multiplier_for_score(score))
        metadata["best_price"] = fill_price
        metadata["r_distance"] = abs(fill_price - sl)
        if dca_enabled():
            wallet = self._wallet_for_dca_plan()
            apply_initial_dca_plan(
                metadata,
                fill_price,
                sl,
                structure=structure,
                wallet=wallet,
                entry1_qty=quantity,
                leverage=leverage,
                side=action,
            )
            metadata["avg_entry_price"] = fill_price
            metadata["dca_entry1_fill"] = fill_price
            metadata["dca_entry1_qty"] = quantity
            capped_sl = resolved_final_sl(metadata, action)
            if capped_sl > 0:
                sl = capped_sl
                metadata["r_distance"] = abs(fill_price - sl)
            if not sl_is_valid_vs_entry(action, fill_price, sl) or last_resort_sl_hit(
                action, fill_price, sl
            ):
                error_logger.critical(
                    "Post-fill wallet-capped SL invalid for %s %s | fill=%.6f sl=%.6f "
                    "— fail-safe close",
                    symbol,
                    action,
                    fill_price,
                    sl,
                )
                self._persist_orphan_fill(
                    {
                        "trade_id": str(uuid.uuid4()),
                        "symbol": symbol,
                        "side": action,
                        "entry_price": fill_price,
                        "quantity": quantity,
                        "status": TRADE_STATUS_OPEN,
                        "exchange_order_id": exchange_order_id,
                        "opened_at": utc_now().isoformat(),
                        "strategy": strategy,
                        "metadata": metadata,
                    }
                )
                try:
                    self.exchange.close_position_quantity(symbol, position_side, quantity)
                except Exception as exc:
                    error_logger.critical(
                        "FAILSAFE_CLOSE failed after invalid capped SL %s: %s",
                        symbol,
                        exc,
                    )
                return None

        trade_id = str(uuid.uuid4())
        opened_at = utc_now().isoformat()
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
            "entry_stage": 1,
        }

        db_logged = False
        try:
            db_logged = self._persist_trade_with_retry(trade_data)
            if db_logged:
                self.forget_live_orphan(trade_id)
            else:
                error_logger.error(
                    "CRITICAL ORPHAN FILL | %s %s | orderId=%s | trade_id=%s",
                    symbol,
                    action,
                    exchange_order_id,
                    trade_id,
                )
                self._persist_orphan_fill(trade_data)

            self.exchange.seed_position_after_fill(
                symbol, position_side, quantity, fill_price
            )
            if record:
                get_execution_ledger().transition(
                    record,
                    ExecutionPhase.ORDER_FILLED
                    if order_status == "FILLED"
                    else ExecutionPhase.PARTIALLY_FILLED,
                    binance_order_id=exchange_order_id,
                    quantity=quantity,
                    entry=fill_price,
                )

            place_native = self._should_attach_native_exits(
                attach_native_exits,
                metadata,
                structure,
            )
            if place_native:
                sl_for_native = sl if sl_is_valid_vs_entry(action, fill_price, sl) else 0.0
                native_ok = self._place_native_exit_orders(
                    trade_id=trade_id,
                    symbol=symbol,
                    position_side=position_side,
                    sl=sl_for_native,
                    tp1=tp1,
                    tp2=tp2,
                    tp3=tp3,
                    metadata=metadata,
                    quantity=quantity,
                )
                if not native_ok:
                    error_logger.critical(
                        "FAILSAFE_CLOSE %s %s after native SL could not be verified",
                        symbol,
                        action,
                    )
                    try:
                        self.exchange.close_position_quantity(
                            symbol, position_side, quantity
                        )
                    except Exception as exc:
                        error_logger.critical(
                            "FAILSAFE_CLOSE failed for %s %s: %s — position may be naked",
                            symbol,
                            action,
                            exc,
                        )
                    self._persist_orphan_fill(trade_data)
                    return None

            if not self.exchange.is_rest_blocked()[0]:
                refined = fill_price
                if safe_float(response.get("avgPrice")) <= 0:
                    refined = self.exchange.get_fill_price_from_order(
                        symbol, response, fallback=fill_price
                    )
                if refined > 0 and abs(refined - fill_price) > 1e-12:
                    fill_price = refined
                    trade_data["entry_price"] = fill_price
                    refined_levels = self._resolve_execution_levels(
                        action, fill_price, atr, rules, structure, strategy
                    )
                    if refined_levels:
                        sl, tp1, tp2, tp3 = refined_levels
                        trade_data.update(
                            {
                                "stop_loss": sl,
                                "take_profit_1": tp1,
                                "take_profit_2": tp2,
                                "take_profit_3": tp3,
                            }
                        )
                        if dca_enabled() and dca_entry_count(metadata) <= 1:
                            wallet = self._wallet_for_dca_plan()
                            metadata.update(
                                build_dca_plan(
                                    fill_price,
                                    sl,
                                    structure=structure,
                                    wallet=wallet,
                                    entry1_qty=quantity,
                                    leverage=leverage,
                                    side=action,
                                )
                            )
                            capped_sl = resolved_final_sl(metadata, action)
                            if capped_sl > 0:
                                sl = capped_sl
                                trade_data["stop_loss"] = sl
                        self._apply_execution_levels(
                            trade_id=trade_id,
                            fill_price=fill_price,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            tp3=tp3,
                            metadata=metadata,
                        )
                        if place_native:
                            sl_for_native = (
                                sl
                                if sl_is_valid_vs_entry(action, fill_price, sl)
                                else 0.0
                            )
                            native_ok = self._place_native_exit_orders(
                                trade_id=trade_id,
                                symbol=symbol,
                                position_side=position_side,
                                sl=sl_for_native,
                                tp1=tp1,
                                tp2=tp2,
                                tp3=tp3,
                                metadata=metadata,
                                quantity=quantity,
                            )
                            if not native_ok:
                                error_logger.critical(
                                    "FAILSAFE_CLOSE %s %s after refined native SL failed",
                                    symbol,
                                    action,
                                )
                                try:
                                    self.exchange.close_position_quantity(
                                        symbol, position_side, quantity
                                    )
                                except Exception as exc:
                                    error_logger.critical(
                                        "FAILSAFE_CLOSE failed for %s: %s",
                                        symbol,
                                        exc,
                                    )
                                self._persist_orphan_fill(trade_data)
                                return None
                    else:
                        self.db.update_trade(trade_id, {"entry_price": fill_price})
                    self.exchange.seed_position_after_fill(
                        symbol, position_side, quantity, fill_price
                    )
        except Exception as exc:
            error_logger.critical(
                "Post-fill processing failed for %s %s — persisting orphan: %s",
                symbol,
                action,
                exc,
            )
            self._persist_orphan_fill(trade_data)
            self.exchange.seed_position_after_fill(
                symbol, position_side, quantity, fill_price
            )
            return None

        try:
            from core.ops_heartbeat import touch_execution

            touch_execution(source="position_confirmed")
        except Exception:
            pass
        trade_logger.info(
            "[POSITION_CONFIRMED] %s %s | strategy=%s | qty=%s | fill=%.6f | SL=%.6f | TP1=%.6f | "
            "R=%.6f | size_mult=%.2f | trade_id=%s | exec=%s",
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
            record.exec_id if record else "",
        )
        if record:
            get_execution_ledger().transition(
                record,
                ExecutionPhase.POSITION_CONFIRMED,
                binance_order_id=exchange_order_id,
                extra=f"trade_id={trade_id[:8]} fill={fill_price:.6f}",
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
            "exec_id": record.exec_id if record else "",
            "db_logged": db_logged,
            "orphan_fill": not db_logged,
            "score": score,
        }

    def execute_dca_add(
        self,
        trade: dict[str, Any],
        step: int,
        current_price: float,
    ) -> Optional[dict[str, Any]]:
        """Add Entry 2 or 3 using 1% of wallet. Does not move TP or Base Entry Price."""
        if step not in (2, 3):
            return None
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "LONG").upper()
        if not symbol or current_price <= 0:
            return None
        metadata = self.db.parse_trade_metadata(trade)
        stage = entry_stage_of(trade, metadata)
        # Stage must already be claimed (from_stage+1) so restarts cannot re-fire.
        if stage != step:
            return None
        if metadata.get("dca_uncertain_client_id") or metadata.get("dca_uncertain_order_id"):
            error_logger.critical(
                "DCA entry %s %s skipped — previous add still uncertain; not resubmitting",
                step,
                symbol,
            )
            return None
        if not dca_enabled() or Config.DRY_RUN:
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None
        base = safe_float(metadata.get("base_entry_price")) or safe_float(
            trade.get("entry_price")
        )
        if base <= 0:
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None
        if self.exchange.is_rest_blocked()[0]:
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None

        balance = self._wallet_for_dca_plan()
        if balance <= 0:
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None
        try:
            rules = self.exchange.get_symbol_rules(symbol)
        except Exception as exc:
            error_logger.error(
                "DCA entry %s skipped for %s — symbol rules failed: %s",
                step,
                symbol,
                exc,
            )
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None
        notional = balance * dca_add_wallet_fraction()
        quantity = notional / current_price
        quantity = self._format_quantity(self.exchange, quantity, rules, symbol)
        min_qty = minimum_order_quantity(
            current_price,
            rules.min_qty,
            rules.min_notional,
            rules.step_size,
            rules.quantity_precision,
        )
        if quantity < min_qty:
            quantity = min_qty
        quantity, _ = cap_quantity_to_notional(
            quantity,
            current_price,
            balance * Config.MAX_POSITION_VALUE_MULTIPLIER,
            rules.min_qty,
            rules.min_notional,
            rules.step_size,
            rules.quantity_precision,
        )
        if quantity <= 0:
            self._revert_claimed_dca_stage(trade, step, metadata)
            return None

        order_side = "BUY" if side == "LONG" else "SELL"
        client_order_id = f"dca{step}{str(trade.get('trade_id') or 'x').replace('-', '')[:16]}"
        try:
            response = self.exchange.execute_futures_order(
                symbol=symbol,
                side=order_side,
                position_side=side,
                quantity=quantity,
                price=None,
                reduce_only=False,
                new_client_order_id=client_order_id,
            )
        except Exception as exc:
            error_logger.error(
                "DCA entry %s failed for %s: %s", step, symbol, exc
            )
            if _is_hard_order_reject(exc):
                revert = getattr(self.db, "revert_entry_stage", None)
                if callable(revert):
                    revert(str(trade.get("trade_id") or ""), step)
                return None
            self._mark_dca_uncertain(
                trade,
                metadata,
                step,
                client_order_id=client_order_id,
                order_id="",
                status="TIMEOUT_OR_UNKNOWN",
            )
            return None
        if not isinstance(response, dict):
            self._mark_dca_uncertain(
                trade,
                metadata,
                step,
                client_order_id=client_order_id,
                order_id="",
                status="NON_DICT_RESPONSE",
            )
            return None

        order_status = str(response.get("status") or "").upper()
        if order_status in {"CANCELED", "EXPIRED", "REJECTED"}:
            revert = getattr(self.db, "revert_entry_stage", None)
            if callable(revert):
                revert(str(trade.get("trade_id") or ""), step)
            return None
        if order_status not in {"FILLED", "PARTIALLY_FILLED"}:
            self._mark_dca_uncertain(
                trade,
                metadata,
                step,
                client_order_id=client_order_id,
                order_id=str(response.get("orderId") or ""),
                status=order_status or "UNKNOWN",
            )
            return None

        add_qty = safe_float(response.get("executedQty"))
        if add_qty <= 0:
            self._mark_dca_uncertain(
                trade,
                metadata,
                step,
                client_order_id=client_order_id,
                order_id=str(response.get("orderId") or ""),
                status=f"{order_status}_QTY0",
            )
            return None
        return self._apply_confirmed_dca_fill(
            trade, metadata, step, response, current_price, base
        )

    def _mark_dca_uncertain(
        self,
        trade: dict[str, Any],
        metadata: dict[str, Any],
        step: int,
        *,
        client_order_id: str,
        order_id: str,
        status: str,
    ) -> None:
        metadata["dca_uncertain_order_id"] = str(order_id or "")
        metadata["dca_uncertain_client_id"] = client_order_id
        metadata["dca_uncertain_step"] = step
        metadata["dca_uncertain_status"] = status
        metadata["dca_uncertain_at"] = time.time()
        error_logger.critical(
            "DCA entry %s %s uncertain (status=%s orderId=%s clientId=%s) — "
            "stage stays claimed; will reconcile via lookup; not resubmitting",
            step,
            trade.get("symbol"),
            status or "UNKNOWN",
            order_id or "?",
            client_order_id,
        )
        self.db.update_trade(str(trade["trade_id"]), {"metadata": metadata})
        trade["metadata"] = metadata
        self._notify_uncertain_order(
            symbol=str(trade.get("symbol") or ""),
            side=str(trade.get("side") or ""),
            status=status or "UNKNOWN",
            client_order_id=client_order_id,
            order_id=order_id,
            context=f"DCA{step}",
        )

    def adopt_confirmed_dca_fill(
        self,
        trade: dict[str, Any],
        order: dict[str, Any],
        step: int,
    ) -> Optional[dict[str, Any]]:
        """Apply a reconciled DCA fill. Never submits a new order."""
        metadata = self.db.parse_trade_metadata(trade)
        base = safe_float(metadata.get("base_entry_price")) or safe_float(
            trade.get("entry_price")
        )
        add_qty = safe_float(order.get("executedQty"))
        if add_qty <= 0:
            return None
        current_price = resolve_binance_fill_price(order, fallback=base)
        return self._apply_confirmed_dca_fill(
            trade, metadata, step, order, current_price, base
        )

    def _apply_confirmed_dca_fill(
        self,
        trade: dict[str, Any],
        metadata: dict[str, Any],
        step: int,
        response: dict[str, Any],
        current_price: float,
        base: float,
    ) -> Optional[dict[str, Any]]:
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "LONG").upper()
        add_qty = safe_float(response.get("executedQty"))
        if add_qty <= 0:
            return None
        prior_qty = safe_float(trade.get("quantity"))
        new_qty = prior_qty + add_qty
        mark_dca_filled(metadata, step)
        for key in (
            "dca_uncertain_order_id",
            "dca_uncertain_client_id",
            "dca_uncertain_step",
            "dca_uncertain_status",
            "dca_uncertain_at",
        ):
            metadata.pop(key, None)
        live_mark = 0.0
        try:
            live_mark = safe_float(self.exchange.get_market_price(symbol, side))
        except Exception:
            live_mark = 0.0
        fill_price = resolve_binance_fill_price(
            response, fallback=live_mark or current_price
        )
        metadata[f"dca_entry{step}_fill"] = fill_price
        metadata[f"dca_entry{step}_qty"] = add_qty
        prior_avg = safe_float(metadata.get("avg_entry_price")) or safe_float(
            trade.get("entry_price")
        )
        avg_entry = update_weighted_average(
            metadata,
            prior_avg=prior_avg,
            prior_qty=prior_qty,
            fill_price=fill_price,
            add_qty=add_qty,
        )
        patch: dict[str, Any] = {
            "quantity": new_qty,
            "entry_stage": step,
            "metadata": metadata,
        }
        if step >= 3:
            final_sl = resolved_final_sl(metadata, side)
            if final_sl > 0:
                patch["stop_loss"] = final_sl
                metadata["dca_sl_armed"] = True
                patch["metadata"] = metadata
                if Config.ENABLE_NATIVE_TP_SL:
                    from core.exchange_protection import (
                        apply_protection_metadata,
                        ensure_native_stop,
                        protection_is_verified,
                    )

                    ok, reason, oid = ensure_native_stop(
                        self.exchange,
                        symbol=symbol,
                        position_side=side,
                        sl_price=final_sl,
                        entry_price=base,
                        old_sl_order_id=str(metadata.get("native_sl_order_id") or "")
                        or None,
                    )
                    apply_protection_metadata(metadata, reason=reason, order_id=oid)
                    patch["metadata"] = metadata
                    if not protection_is_verified(reason, oid):
                        error_logger.critical(
                            "PROTECTION_PLACEMENT_FAILED native final SL after Entry 3 "
                            "for %s: %s — fail-safe close required",
                            symbol,
                            reason,
                        )
                        patch["dca_failsafe_close"] = True
        self.db.update_trade(str(trade["trade_id"]), patch)
        trade.update(patch)
        seed_px = avg_entry if avg_entry > 0 else base
        self.exchange.seed_position_after_fill(symbol, side, new_qty, seed_px)
        safety_sl = resolved_final_sl(metadata, side)
        check_px = live_mark or fill_price
        failsafe_close = bool(patch.get("dca_failsafe_close"))
        if safety_sl > 0 and last_resort_sl_hit(side, check_px, safety_sl):
            failsafe_close = True
            error_logger.critical(
                "DCA add %s %s filled through planned SL | live=%.6f sl=%.6f "
                "— fail-safe close",
                step,
                symbol,
                check_px,
                safety_sl,
            )
        sl_now = safe_float(patch.get("stop_loss")) or safe_float(trade.get("stop_loss"))
        trade_logger.info(
            "[DCA] Entry %s filled %s %s | add_qty=%.8f total=%.8f | "
            "fill=%.6f base_entry=%.6f entry3_ref=%.6f sl=%s (TP unchanged)",
            step,
            symbol,
            side,
            add_qty,
            new_qty,
            fill_price,
            base,
            safe_float(metadata.get("dca_entry3_px")),
            f"{sl_now:.6f}" if step >= 3 else "held",
        )
        return {
            "ok": True,
            "step": step,
            "symbol": symbol,
            "side": side,
            "fill_price": fill_price,
            "live_price": live_mark,
            "planned_price": safe_float(metadata.get(f"dca_entry{step}_px")),
            "add_qty": add_qty,
            "total_qty": new_qty,
            "base_entry": base,
            "entry2": safe_float(metadata.get("dca_entry2_px")),
            "entry3": safe_float(metadata.get("dca_entry3_px")),
            "stop_loss": sl_now,
            "sl_armed": step >= 3,
            "failsafe_close": failsafe_close,
        }
