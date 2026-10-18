"""Exchange-side stop-loss helpers.

Native STOP_MARKET orders are placed only when Config.ENABLE_NATIVE_TP_SL is True.
This module never flips that flag. Callers must treat a False result as
"no exchange protection" and follow the documented fail-safe.
"""

from __future__ import annotations

from typing import Any, Optional

from config import Config
from core.dca_plan import sl_is_valid_vs_entry
from logger import error_logger, trade_logger
from utils import escape_html, safe_float


_STOP_TYPES = {"STOP_MARKET", "STOP"}


def native_protection_enabled() -> bool:
    return bool(Config.ENABLE_NATIVE_TP_SL)


def list_open_stop_orders(
    exchange: Any,
    symbol: str,
    position_side: str,
) -> list[dict[str, Any]]:
    getter = getattr(exchange, "get_open_orders", None)
    if not callable(getter):
        return []
    try:
        orders = getter(str(symbol).upper()) or []
    except Exception as exc:
        error_logger.error("Failed to list open orders for %s: %s", symbol, exc)
        return []
    side = str(position_side).upper()
    out: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("positionSide") or "").upper() != side:
            continue
        if str(order.get("type") or "").upper() not in _STOP_TYPES:
            continue
        out.append(order)
    return out


def _stop_matches(order: dict[str, Any], sl_price: float, tick: float) -> bool:
    stop = safe_float(order.get("stopPrice"))
    if stop <= 0 or sl_price <= 0:
        return False
    tol = max(abs(tick), 1e-8) * 2.0
    return abs(stop - sl_price) <= tol


def ensure_native_stop(
    exchange: Any,
    *,
    symbol: str,
    position_side: str,
    sl_price: float,
    entry_price: float = 0.0,
    old_sl_order_id: Optional[str] = None,
) -> tuple[bool, str, Optional[str]]:
    """
    Ensure a reduce-only STOP_MARKET exists for the position.

    Replacement rule: place the new stop first; cancel the previous id only
    after the new order id is confirmed. Existing different-price stops are
    kept until replacement succeeds.

    Returns (ok, reason, order_id).
    """
    if not native_protection_enabled():
        # Not protected on the exchange. Callers must not treat this as success.
        return False, "native_disabled", None
    side = str(position_side).upper()
    symbol_u = str(symbol).upper()
    sl = float(sl_price or 0.0)
    if sl <= 0:
        return False, "invalid_sl_price", None
    if entry_price > 0 and not sl_is_valid_vs_entry(side, entry_price, sl):
        return False, "invalid_sl_side", None

    tick = 0.0
    rules_fn = getattr(exchange, "get_symbol_rules", None)
    if callable(rules_fn):
        try:
            rules = rules_fn(symbol_u)
            tick = float(getattr(rules, "tick_size", 0.0) or 0.0)
        except Exception:
            tick = 0.0

    existing = list_open_stop_orders(exchange, symbol_u, side)
    for order in existing:
        if _stop_matches(order, sl, tick):
            oid = str(order.get("orderId") or "") or None
            return True, "existing_stop_ok", oid

    place = getattr(exchange, "place_conditional_order", None)
    if not callable(place):
        if existing:
            kept = str(existing[0].get("orderId") or "") or old_sl_order_id
            return True, "placement_unavailable_kept_existing", kept
        return False, "placement_unavailable", None

    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        resp = place(
            symbol_u,
            close_side,
            side,
            "STOP_MARKET",
            sl,
            close_position=True,
        )
        new_id = str((resp or {}).get("orderId") or "") or None
    except Exception as exc:
        error_logger.critical(
            "PROTECTION_PLACEMENT_FAILED %s %s sl=%.6f: %s",
            symbol_u,
            side,
            sl,
            exc,
        )
        if existing:
            kept = str(existing[0].get("orderId") or "") or old_sl_order_id
            return True, "placement_failed_kept_existing", kept
        return False, f"placement_failed:{exc}", None

    if not new_id:
        if existing:
            kept = str(existing[0].get("orderId") or "") or old_sl_order_id
            return True, "empty_order_id_kept_existing", kept
        return False, "empty_order_id", None

    verified_status = verify_protective_order_status(exchange, symbol_u, new_id)
    if verified_status in {"CANCELED", "EXPIRED", "REJECTED"}:
        error_logger.critical(
            "PROTECTION_UNVERIFIED %s %s new id=%s status=%s — keeping existing stop",
            symbol_u,
            side,
            new_id,
            verified_status,
        )
        if existing:
            kept = str(existing[0].get("orderId") or "") or old_sl_order_id
            return True, "new_rejected_kept_existing", kept
        return False, f"new_status_{verified_status.lower()}", None
    if verified_status == "UNVERIFIED":
        error_logger.critical(
            "PROTECTION_UNVERIFIED %s %s new id=%s — lookup failed; not cancelling existing",
            symbol_u,
            side,
            new_id,
        )
        if existing:
            kept = str(existing[0].get("orderId") or "") or old_sl_order_id
            return True, "placed_unverified_kept_existing", kept
        return False, "placed_unverified", new_id

    cancel = getattr(exchange, "cancel_order_by_id", None)
    stale_ids = {str(o.get("orderId") or "") for o in existing if o.get("orderId")}
    if old_sl_order_id:
        stale_ids.add(str(old_sl_order_id))
    stale_ids.discard(new_id)
    if callable(cancel):
        for oid in stale_ids:
            if oid:
                try:
                    cancel(symbol_u, oid)
                except Exception as exc:
                    error_logger.warning(
                        "Stale native SL cancel failed %s %s: %s",
                        symbol_u,
                        oid,
                        exc,
                    )
    trade_logger.info(
        "Native SL confirmed | %s %s | sl=%.6f | orderId=%s | replaced=%s",
        symbol_u,
        side,
        sl,
        new_id,
        sorted(stale_ids),
    )
    try:
        from core.ops_heartbeat import touch_protection

        touch_protection(source="ensure_native_stop")
    except Exception:
        pass
    return True, "placed", new_id


def verify_protective_order_status(exchange: Any, symbol: str, order_id: str) -> str:
    """Public lookup: Binance status, NEW if listed among open orders, or UNVERIFIED."""
    return _verify_stop_order_status(exchange, symbol, order_id)


def _verify_stop_order_status(exchange: Any, symbol: str, order_id: str) -> str:
    """Return Binance status, NEW if listed among open orders, or UNVERIFIED."""
    lookup = getattr(exchange, "lookup_order_by_id", None)
    if callable(lookup):
        try:
            order = lookup(symbol, order_id)
        except Exception as exc:
            error_logger.warning(
                "Native SL lookup failed %s %s: %s", symbol, order_id, exc
            )
            order = None
        if isinstance(order, dict) and order:
            status = str(order.get("status") or "").upper()
            if status:
                return status
            if str(order.get("orderId") or "") == str(order_id):
                return "NEW"
    getter = getattr(exchange, "get_open_orders", None)
    if callable(getter):
        try:
            for order in getter(symbol) or []:
                if not isinstance(order, dict):
                    continue
                if str(order.get("orderId") or "") == str(order_id):
                    status = str(order.get("status") or "NEW").upper()
                    return status if status else "NEW"
        except Exception:
            pass
    return "UNVERIFIED"


# Intended SL is confirmed on the exchange (matched existing or lookup-verified NEW).
_VERIFIED_REASONS = {
    "existing_stop_ok",
    "placed",
}

# A stop still exists, but it may be the previous price. Not proof of the new SL.
_UNCERTAIN_REASONS = {
    "placement_unavailable_kept_existing",
    "placement_failed_kept_existing",
    "empty_order_id_kept_existing",
    "new_rejected_kept_existing",
    "placed_unverified_kept_existing",
    "placed_unverified",
}

VERIFIED = "VERIFIED"
UNCERTAIN = "UNCERTAIN"
MISSING = "MISSING"
NATIVE_DISABLED = "NATIVE_DISABLED"


def protection_is_verified(reason: str, order_id: Optional[str]) -> bool:
    """True only when native is on and the intended stop was lookup-confirmed."""
    if not native_protection_enabled():
        return False
    if not order_id:
        return False
    return str(reason or "") in _VERIFIED_REASONS


def protection_status(reason: str, order_id: Optional[str]) -> str:
    """Classify protection: VERIFIED / UNCERTAIN / MISSING / NATIVE_DISABLED."""
    if not native_protection_enabled():
        return NATIVE_DISABLED
    key = str(reason or "")
    if key in _VERIFIED_REASONS and order_id:
        return VERIFIED
    if key in _UNCERTAIN_REASONS and order_id:
        return UNCERTAIN
    if key == "placed_unverified" and order_id:
        return UNCERTAIN
    return MISSING


def apply_protection_metadata(
    metadata: dict[str, Any],
    *,
    reason: str,
    order_id: Optional[str],
) -> dict[str, Any]:
    """Persist protection ids without treating ACK or kept-stale as verified."""
    verified = protection_is_verified(reason, order_id)
    status = protection_status(reason, order_id)
    metadata["native_sl_order_id"] = str(order_id or "")
    metadata["native_protection_reason"] = reason
    metadata["native_protection_status"] = status
    metadata["native_protection_verified"] = verified
    metadata["native_orders_placed"] = verified
    return metadata


def alert_unprotected_position(
    telegram: Any,
    *,
    symbol: str,
    position_side: str,
    reason: str,
) -> None:
    """Urgent operator alert. Does not flatten the position."""
    error_logger.critical(
        "POSITION_UNPROTECTED %s %s reason=%s — virtual SL only; "
        "process death leaves the Binance position naked",
        symbol,
        position_side,
        reason,
    )
    if telegram is None:
        return
    detail = str(reason)[:200]
    try:
        sender = getattr(telegram, "send_protection_alert", None)
        if callable(sender):
            sender(
                symbol=str(symbol),
                side=str(position_side),
                detail=detail,
                verified=False,
            )
            return
        telegram.send_message(
            "🚨 <b>POSITION UNPROTECTED ON EXCHANGE</b>\n"
            f"{escape_html(str(symbol))} {escape_html(str(position_side))}\n"
            f"<i>{escape_html(detail)}</i>\n"
            "Virtual SL/TP dies if this process stops. Native TP/SL was not verified. "
            "Not auto-flattening."
        )
    except Exception:
        pass


def _record_protection_inventory(open_n: int, verified_n: int) -> None:
    try:
        from core.ops_heartbeat import set_native_protection_inventory

        set_native_protection_inventory(open_n, verified_n)
    except Exception:
        pass


def restore_open_trade_protection(
    exchange: Any,
    db: Any,
    *,
    manager: Any = None,
    telegram: Any = None,
    failsafe_on_failure: bool = True,
) -> dict[str, int]:
    """Ensure native SL exists for every open DB trade. Native-off is a no-op."""
    summary = {"ok": 0, "failed": 0, "skipped": 0, "uncertain": 0}
    try:
        trades = db.get_open_trades() or []
    except Exception as exc:
        error_logger.critical("Cannot restore protection — DB open-trades failed: %s", exc)
        _record_protection_inventory(0, 0)
        return summary
    open_n = len(trades)
    if not native_protection_enabled():
        _record_protection_inventory(open_n, 0)
        return summary
    verified_n = 0
    for trade in trades:
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "LONG").upper()
        sl = safe_float(trade.get("stop_loss"))
        entry = safe_float(trade.get("entry_price"))
        metadata = {}
        parse = getattr(db, "parse_trade_metadata", None)
        if callable(parse):
            try:
                metadata = parse(trade) or {}
            except Exception:
                metadata = {}
        if sl <= 0:
            sl = safe_float(metadata.get("dca_last_resort_sl"))
        if sl <= 0:
            summary["skipped"] += 1
            error_logger.critical(
                "PROTECTION_RESTORE skipped %s %s — no SL on record",
                symbol,
                side,
            )
            continue
        ok, reason, oid = ensure_native_stop(
            exchange,
            symbol=symbol,
            position_side=side,
            sl_price=sl,
            entry_price=entry,
            old_sl_order_id=str(metadata.get("native_sl_order_id") or "") or None,
        )
        apply_protection_metadata(metadata, reason=reason, order_id=oid)
        try:
            db.update_trade(str(trade.get("trade_id")), {"metadata": metadata})
        except Exception as exc:
            error_logger.warning("Could not persist restored SL id for %s: %s", symbol, exc)
        if protection_is_verified(reason, oid):
            summary["ok"] += 1
            verified_n += 1
            continue
        if protection_status(reason, oid) == UNCERTAIN:
            summary["uncertain"] += 1
            alert_unprotected_position(
                telegram,
                symbol=symbol,
                position_side=side,
                reason=f"uncertain_restore:{reason}",
            )
            continue
        summary["failed"] += 1
        detail = f"restore failed ({reason})"
        error_logger.critical(
            "PROTECTION_RESTORE_FAILED %s %s | %s",
            symbol,
            side,
            detail,
        )
        if telegram is not None:
            try:
                telegram.send_message(
                    f"🚨 <b>Native SL restore failed</b>\n{symbol} {side}\n<i>{detail}</i>"
                )
            except Exception:
                pass
        closer = getattr(manager, "failsafe_close_trade", None)
        if failsafe_on_failure and callable(closer):
            closer(trade, "PROTECTION_FAILED", detail)
    _record_protection_inventory(open_n, verified_n)
    return summary
