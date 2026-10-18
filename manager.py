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
from logger import error_logger, system_logger, trade_logger
from core.trade_close_mutex import trade_close_mutex
from exit_coordinator import (
    claim_exit,
    exit_claim_active,
    get_exit_claim_owner,
    release_exit,
)
from reconciliation import (
    confirm_external_close_allowed,
    exchange_orphan_as_trade,
    is_exchange_sync_close_reason,
    is_synthetic_exchange_orphan,
    is_within_position_grace_period,
    position_reconcile_guard,
    resolve_exchange_close_pnl,
    rest_position_quantity,
    unresolved_orphan_exchange_positions,
)
from utils import (
    escape_html,
    resolve_binance_fill_price,
    round_protective_price,
    round_step_size,
    safe_float,
    utc_now,
    utc_today_str,
)

from core.dca_plan import (
    dca_enabled,
    dca_plan_active,
    entry_stage_of,
    last_resort_sl_hit,
    next_dca_step,
    resolved_final_sl,
    should_trigger_add,
    sl_is_armed,
    sl_is_valid_vs_entry,
)

if TYPE_CHECKING:
    from telegram_bot import TelegramManager
    from scheduler import DailyScheduler
    from risk_manager import RiskManager
    from executor import TradeExecutor


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
    CLOSE_ORDER_MAX_RETRIES = 4
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
        self.executor: Optional["TradeExecutor"] = None
        self._dca_inflight: set[str] = set()
        self._dca_stage: dict[str, int] = {}
        self._monitored_symbols: set[str] = set()
        self._monitored_lock = threading.Lock()
        self._latest_ticks: dict[str, float] = {}
        self._tick_lock = threading.Lock()
        self._tick_event = threading.Event()
        self._monitor_stop = threading.Event()
        self._close_queue: queue.Queue[Optional[_CloseTask]] = queue.Queue()
        self._close_inflight: set[str] = set()
        self._close_done: dict[str, threading.Event] = {}
        self._close_ok: dict[str, bool] = {}
        self._close_lock = threading.Lock()
        self._market_close_attempts: dict[str, int] = {}
        self._market_close_exhausted: set[str] = set()
        self._qty_rest_confirm_at: dict[str, float] = {}
        self._force_rest_monitor = False
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
        rest_refresh = max(
            float(Config.MONITOR_INTERVAL_SECONDS),
            float(Config.MONITOR_REST_MARK_INTERVAL_SECONDS),
            15.0,
        )
        system_logger.info(
            "Position monitor started (ws_eval=1s, rest_refresh=%.0fs).",
            rest_refresh,
        )

    @property
    def stop_event(self) -> threading.Event:
        return self._monitor_stop

    def stop(self) -> None:
        """Stop TP/SL, tick, and close-order worker threads."""
        self._monitor_stop.set()
        self._tick_event.set()
        try:
            self._close_queue.put_nowait(None)
        except Exception:
            pass
        current = threading.current_thread()
        for worker in (self._tick_worker, self._fast_monitor, self._close_worker):
            if worker is current or not worker.is_alive():
                continue
            worker.join(timeout=8.0)

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
            if symbol not in self._monitored_symbols:
                return
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
        """Single TP/SL loop: WS-only each second, REST mark/position refresh on interval."""
        from core.ops_heartbeat import touch_monitor_loop

        last_rest_refresh = 0.0
        while not self._monitor_stop.wait(1.0):
            try:
                now = time.monotonic()
                user_ok = False
                hub = getattr(self.exchange, "_market_data", None)
                if hub is not None:
                    user_ok = bool(
                        hub.user_stream_has_account_data()
                        and not hub.user_stream_is_stale()
                    )
                rest_interval = max(float(Config.MONITOR_INTERVAL_SECONDS), 30.0)
                if not user_ok:
                    rest_interval = max(
                        float(Config.MONITOR_REST_FALLBACK_SECONDS),
                        rest_interval,
                        30.0,
                    )
                    rest_interval = min(rest_interval, 50.0)
                rest_due = (now - last_rest_refresh) >= rest_interval
                force_rest = bool(getattr(self, "_force_rest_monitor", False))
                # REST-confirm on interval or when a cache miss forced a
                # quantity check. User-stream gaps shorten rest_interval above.
                if (rest_due or force_rest) and not self.exchange.is_rest_blocked()[0]:
                    if not user_ok or force_rest:
                        self._prefetch_live_prices_for_open_trades()
                    self._force_rest_monitor = False
                    self.monitor_open_trades(ws_only=False)
                    last_rest_refresh = now
                else:
                    self.monitor_open_trades(ws_only=True)
                touch_monitor_loop(source="position_monitor")
            except Exception as exc:
                error_logger.error("Position monitor error: %s", exc, exc_info=True)
                error_logger.error(traceback.format_exc())

    def _trade_quantity_from_db(self, trade: dict[str, Any]) -> float:
        metadata = self.db.parse_trade_metadata(trade)
        original = safe_float(metadata.get("original_quantity"))
        if original > 0:
            return original
        return safe_float(trade.get("quantity"))

    def _open_trades_for_monitor(self) -> list[dict[str, Any]]:
        """DB trades, persist-fail fills, and exchange-only orphans for TP/SL."""
        try:
            trades = list(self.db.get_open_trades() or [])
        except Exception:
            trades = []
        seen_ids = {str(t.get("trade_id") or "") for t in trades}
        seen_keys = {
            (
                str(t.get("symbol") or "").upper(),
                str(t.get("side") or "LONG").upper(),
            )
            for t in trades
        }
        executor = getattr(self, "executor", None)
        getter = getattr(executor, "live_orphan_trades", None) if executor else None
        if callable(getter):
            for orphan in getter():
                oid = str(orphan.get("trade_id") or "")
                key = (
                    str(orphan.get("symbol") or "").upper(),
                    str(orphan.get("side") or "LONG").upper(),
                )
                if oid and oid not in seen_ids:
                    trades.append(orphan)
                    seen_ids.add(oid)
                    seen_keys.add(key)
        try:
            for pos in unresolved_orphan_exchange_positions():
                synthetic = exchange_orphan_as_trade(pos)
                oid = str(synthetic.get("trade_id") or "")
                key = (
                    str(synthetic.get("symbol") or "").upper(),
                    str(synthetic.get("side") or "LONG").upper(),
                )
                if not key[0] or oid in seen_ids or key in seen_keys:
                    continue
                trades.append(synthetic)
                seen_ids.add(oid)
                seen_keys.add(key)
        except Exception:
            pass
        return trades

    def _process_price_tick(self, symbol: str, price: float) -> None:
        """Evaluate open trades for a symbol (REST-free, off WS callback thread)."""
        if not Config.ENABLE_SOFT_TP_SL:
            return
        active_trades = [
            trade
            for trade in self._open_trades_for_monitor()
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
                    trade, qty_map, price=price, ws_only=True, allow_rest=False
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
            self._drain_pending_external_flats()
            active_trades = self._open_trades_for_monitor()
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
        """Exchange snapshot quantities only (0 = flat/missing). No DB ghost fill."""
        qty_map: dict[tuple[str, str], float] = {}
        for trade in active_trades:
            symbol = str(trade.get("symbol", "")).upper()
            side = str(trade.get("side", "LONG")).upper()
            if ws_only:
                qty = self.exchange.get_position_quantity_cached(symbol, side)
            else:
                qty = self.exchange.get_position_quantity(symbol, side)
            qty_map[(symbol, side)] = max(safe_float(qty), 0.0)

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

    def _rest_confirm_cached_qty(
        self,
        symbol: str,
        position_side: str,
        *,
        min_interval: float = 8.0,
    ) -> Optional[float]:
        """Urgent REST qty when the cache is 0. Throttled so the 1s loop cannot hammer REST."""
        try:
            if self.exchange.rest_account_reads_blocked():
                return None
        except Exception:
            return None
        key = f"{str(symbol).upper()}:{str(position_side).upper()}"
        stamps = getattr(self, "_qty_rest_confirm_at", None)
        if stamps is None:
            stamps = {}
            self._qty_rest_confirm_at = stamps
        now = time.monotonic()
        last = safe_float(stamps.get(key))
        if last > 0 and (now - last) < min_interval:
            return None
        stamps[key] = now
        getter = getattr(self.exchange, "get_position_quantity_rest", None)
        if not callable(getter):
            return None
        try:
            return getter(symbol, position_side, urgent=True)
        except TypeError:
            return getter(symbol, position_side)
        except Exception:
            return None

    def _monitor_single_trade(
        self,
        trade: dict[str, Any],
        qty_map: dict[tuple[str, str], float],
        price: Optional[float] = None,
        *,
        ws_only: bool = False,
        allow_rest: bool = True,
    ) -> None:
        symbol = trade["symbol"]
        position_side = trade.get("side", "LONG")
        key = (str(symbol).upper(), str(position_side).upper())
        synthetic = is_synthetic_exchange_orphan(trade)
        if not Config.ENABLE_SOFT_TP_SL:
            return

        live_qty = qty_map.get(key, 0.0)
        db_qty = self._trade_quantity_from_db(trade) if live_qty <= 0 else 0.0

        if live_qty <= 0:
            self._force_rest_monitor = True
            if allow_rest:
                rest_qty = self._rest_confirm_cached_qty(symbol, str(position_side))
                if rest_qty is not None and rest_qty > 0:
                    if not synthetic:
                        position_reconcile_guard.note_present(str(trade["trade_id"]))
                    trade_logger.info(
                        "[%s] WS/cache missed position — REST confirms qty=%.8f",
                        symbol,
                        rest_qty,
                    )
                    live_qty = rest_qty
                    qty_map[key] = rest_qty

        if live_qty <= 0:
            if synthetic:
                return
            if is_within_position_grace_period(trade):
                if db_qty <= 0:
                    return
                live_qty = db_qty
                qty_map[key] = db_qty
            elif ws_only:
                if db_qty > 0:
                    live_qty = db_qty
                    qty_map[key] = db_qty
                else:
                    trade_logger.debug(
                        "[%s] TP/SL eval skipped — exchange qty=0 (waiting REST/manual sync).",
                        symbol,
                    )
                    return
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
                    reason="RECONCILED_MANUAL_CLOSE",
                    exit_price=exit_px,
                    pnl=resolved.realized_pnl,
                    pnl_source=resolved.source,
                )
                position_reconcile_guard.note_present(str(trade["trade_id"]))
                return

        if synthetic:
            trade_logger.warning(
                "[%s] Exchange-only orphan still open qty=%.8f — "
                "virtual TP/SL skipped; watchdog covers flatten after grace.",
                symbol,
                live_qty,
            )
            return

        position_reconcile_guard.note_present(str(trade["trade_id"]))

        if price is None or price <= 0:
            price = self._resolve_monitor_price(symbol, position_side)
        ws_mark = self._ws_position_mark(symbol, position_side)
        if ws_mark is not None and ws_mark > 0:
            price = ws_mark
        if price is None or price <= 0:
            trade_logger.warning(
                "[%s] TP/SL eval skipped — no live ticker/mark price available.",
                symbol,
            )
            return
        try:
            from core.ops_heartbeat import touch_market_data

            touch_market_data(source="position_mark")
        except Exception:
            pass

        fresh = self.db.get_trade(trade["trade_id"])
        if fresh:
            trade = fresh
        if trade.get("status") == TRADE_STATUS_CLOSED:
            return
        trade = self._ensure_local_exit_memory(trade)

        if is_range_strategy(str(trade.get("strategy", ""))):
            if self._check_range_hard_exits(trade, price):
                return

        self._maybe_execute_dca(trade, price)
        fresh = self.db.get_trade(trade["trade_id"])
        if fresh:
            trade = fresh
        if trade.get("status") == TRADE_STATUS_CLOSED:
            return

        if position_side == "LONG":
            self._manage_long_trade(trade, price)
        elif position_side == "SHORT":
            self._manage_short_trade(trade, price)

    def _sync_dca_stage_from_db(self, trade_id: str) -> int:
        """Align in-memory DCA stage with SQLite so a revert can retry Entry 2/3."""
        if not trade_id:
            return 1
        fresh = None
        try:
            getter = getattr(self.db, "get_trade", None)
            if callable(getter):
                fresh = getter(trade_id)
        except Exception:
            fresh = None
        if not fresh:
            self._dca_stage.pop(trade_id, None)
            return 1
        metadata = self.db.parse_trade_metadata(fresh)
        stage = entry_stage_of(fresh, metadata)
        self._dca_stage[trade_id] = stage
        return stage

    def _maybe_execute_dca(self, trade: dict[str, Any], price: float) -> None:
        if not dca_enabled() or self.executor is None or price <= 0:
            return
        trade_id = str(trade.get("trade_id") or "")
        if not trade_id or trade_id in self._dca_inflight:
            return
        if trade_id.startswith("orphan-ex-") or is_synthetic_exchange_orphan(trade):
            return
        try:
            fresh = self.db.get_trade(trade_id)
        except Exception:
            fresh = None
        if fresh:
            trade = fresh
        metadata = self.db.parse_trade_metadata(trade)
        if not dca_plan_active(metadata):
            return
        stage = entry_stage_of(trade, metadata)
        self._dca_stage[trade_id] = stage
        step = next_dca_step(metadata, trade)
        if step is None or stage >= step:
            return
        side = str(trade.get("side") or "LONG").upper()
        if not should_trigger_add(side, price, metadata, step):
            return
        if self.risk_manager is not None:
            allowed, reason = self.risk_manager.can_add_dca_entry(step)
            if not allowed:
                trade_logger.info(
                    "[DCA] Entry %s skipped for %s — %s",
                    step,
                    trade.get("symbol"),
                    reason,
                )
                return
        # Persist the stage claim BEFORE the order so a restart cannot re-fire.
        metadata["entry_stage"] = stage
        if not self.db.claim_entry_stage(trade_id, stage, step, metadata):
            self._sync_dca_stage_from_db(trade_id)
            return
        self._dca_stage[trade_id] = step
        trade["entry_stage"] = step
        metadata["entry_stage"] = step
        metadata["dca_entries"] = step
        trade["metadata"] = metadata
        self._dca_inflight.add(trade_id)
        try:
            result = self.executor.execute_dca_add(trade, step, price)
            if result and result.get("failsafe_close"):
                self.failsafe_close_trade(
                    trade,
                    "INVALID_SL_FAILSAFE",
                    "DCA fill through planned SL or native protection failed",
                )
                return
            if result and self.telegram:
                try:
                    self.telegram.send_dca_alert(
                        symbol=str(result.get("symbol") or trade.get("symbol") or ""),
                        side=str(result.get("side") or trade.get("side") or "LONG"),
                        step=int(result.get("step") or step),
                        fill_price=safe_float(result.get("fill_price")),
                        planned_price=safe_float(result.get("planned_price")),
                        add_qty=safe_float(result.get("add_qty")),
                        total_qty=safe_float(result.get("total_qty")),
                        entry2=safe_float(result.get("entry2")),
                        entry3=safe_float(result.get("entry3")),
                        stop_loss=safe_float(result.get("stop_loss")),
                        sl_armed=bool(result.get("sl_armed")),
                        live_price=safe_float(result.get("live_price") or price),
                    )
                except Exception as exc:
                    error_logger.warning(
                        "DCA Telegram alert failed for %s: %s",
                        trade.get("symbol"),
                        exc,
                    )
        except Exception as exc:
            error_logger.error("DCA add failed for %s: %s", trade.get("symbol"), exc)
            revert = getattr(self.executor, "_revert_claimed_dca_stage", None)
            if callable(revert):
                try:
                    revert(trade, step, metadata)
                except Exception:
                    pass
        finally:
            self._dca_inflight.discard(trade_id)
            self._sync_dca_stage_from_db(trade_id)

    def _ensure_local_exit_memory(self, trade: dict[str, Any]) -> dict[str, Any]:
        """Rebuild local TP/SL in memory if they were never stored (no exchange orders)."""
        sl = safe_float(trade.get("stop_loss"))
        tp1 = safe_float(trade.get("take_profit_1"))
        if sl > 0 and tp1 > 0:
            return trade
        metadata = self.db.parse_trade_metadata(trade)
        atr = safe_float(metadata.get("atr_at_entry"))
        entry = safe_float(trade.get("entry_price"))
        side = str(trade.get("side", "LONG")).upper()
        if atr <= 0 or entry <= 0:
            return trade
        sl_mult = max(float(Config.SL_ATR_MULTIPLIER), 0.25)
        tp1_mult = max(float(Config.TP1_ATR_MULTIPLIER), sl_mult)
        tp2_mult = max(float(Config.TP2_ATR_MULTIPLIER), tp1_mult)
        tp3_mult = max(float(Config.TP3_ATR_MULTIPLIER), tp2_mult)
        if side == "LONG":
            new_sl = entry - atr * sl_mult
            new_tp1 = entry + atr * tp1_mult
            new_tp2 = entry + atr * tp2_mult
            new_tp3 = entry + atr * tp3_mult
        else:
            new_sl = entry + atr * sl_mult
            new_tp1 = entry - atr * tp1_mult
            new_tp2 = entry - atr * tp2_mult
            new_tp3 = entry - atr * tp3_mult
        patch = {}
        if sl <= 0:
            patch["stop_loss"] = new_sl
            trade["stop_loss"] = new_sl
        if tp1 <= 0:
            patch["take_profit_1"] = new_tp1
            patch["take_profit_2"] = new_tp2
            patch["take_profit_3"] = new_tp3
            trade["take_profit_1"] = new_tp1
            trade["take_profit_2"] = new_tp2
            trade["take_profit_3"] = new_tp3
        if patch:
            try:
                self.db.update_trade(trade["trade_id"], patch)
            except Exception:
                pass
            trade_logger.info(
                "[%s] Recalculated local TP/SL in memory | SL=%.6f TP1=%.6f",
                trade.get("symbol"),
                safe_float(trade.get("stop_loss")),
                safe_float(trade.get("take_profit_1")),
            )
        return trade

    _TF_BAR_SECONDS: dict[str, int] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
    }

    def _drain_pending_external_flats(self) -> None:
        """Close DB trades when ACCOUNT_UPDATE reports qty=0 and REST confirms flat."""
        hub = getattr(getattr(self, "exchange", None), "_market_data", None)
        drain = getattr(hub, "drain_pending_flat_positions", None)
        if not callable(drain):
            return
        try:
            events = drain()
        except Exception as exc:
            error_logger.debug("Pending-flat drain failed: %s", exc)
            return
        if not events:
            return
        try:
            open_trades = self.db.get_open_trades()
        except Exception:
            return
        by_key = {
            (
                str(t.get("symbol", "")).upper(),
                str(t.get("side", "LONG")).upper(),
            ): t
            for t in open_trades
        }
        lock = getattr(self, "_close_lock", None)
        inflight = getattr(self, "_close_inflight", set())
        for ev in events:
            symbol = str(ev.get("symbol", "")).upper()
            side = str(ev.get("positionSide") or ev.get("side") or "LONG").upper()
            trade = by_key.get((symbol, side))
            if not trade:
                continue
            trade_id = str(trade.get("trade_id", ""))
            if trade_id and exit_claim_active(trade_id):
                continue
            busy = False
            if trade_id:
                if lock is not None:
                    with lock:
                        busy = any(
                            str(k).startswith(f"{trade_id}:") for k in inflight
                        )
                else:
                    busy = any(
                        str(k).startswith(f"{trade_id}:") for k in inflight
                    )
            if busy:
                continue
            try:
                self._reconcile_exchange_flat(
                    trade,
                    "ACCOUNT_UPDATE_FLAT",
                    exit_reason="RECONCILED_MANUAL_CLOSE",
                    confirmed_by_exchange=True,
                    force_rest_inspect=True,
                )
            except Exception as exc:
                error_logger.warning(
                    "Manual-close sync failed for %s %s: %s",
                    symbol,
                    side,
                    exc,
                )

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
        """Range exits: boundary close = signal reversal. Time/ADX only if enabled."""
        if self._check_range_boundary_breakout(trade):
            return True

        if not Config.ENABLE_RANGE_AUXILIARY_EXITS:
            return False

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
        target_sl = round_protective_price(
            target_sl,
            rules.tick_size,
            rules.price_precision,
            side="LONG" if is_long else "SHORT",
            kind="SL",
        )
        if is_long and target_sl <= sl:
            return
        if not is_long and sl > 0 and target_sl >= sl:
            return

        self.db.update_trade(trade["trade_id"], {"stop_loss": target_sl})
        trade["stop_loss"] = target_sl
        self._sync_native_stop(trade, target_sl)
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
            dca_hold_sl = dca_plan_active(metadata) and not sl_is_armed(metadata, trade)

            if not dca_hold_sl:
                self._advance_profit_stop_ladder(trade, current_price, is_long=True)

                if Config.ENABLE_TRAILING_STOP and entry > 0:
                    if current_price > entry or metadata.get("runner_active"):
                        self._apply_trailing_stop(trade, current_price, is_long=True)

            stop_loss = safe_float(trade.get("stop_loss"))
            safety_sl = resolved_final_sl(metadata, "LONG")
            if dca_hold_sl:
                if last_resort_sl_hit("LONG", current_price, safety_sl):
                    stop_loss = safety_sl
                else:
                    stop_loss = 0.0
            elif safety_sl > 0 and sl_is_armed(metadata, trade) and stop_loss <= 0:
                stop_loss = safety_sl
            if stop_loss > 0 and current_price <= stop_loss:
                if entry > 0 and not sl_is_valid_vs_entry("LONG", entry, stop_loss):
                    self.failsafe_close_trade(
                        trade,
                        "INVALID_SL_FAILSAFE",
                        f"LONG SL not below entry (entry={entry:.6f} sl={stop_loss:.6f})",
                    )
                    return
                if not self._market_close_should_retry(
                    str(trade.get("trade_id", "")), "STOP_LOSS"
                ):
                    self._sync_native_stop(trade, stop_loss)
                    return
                self._trigger_virtual_sl(trade, current_price, stop_loss)
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
            dca_hold_sl = dca_plan_active(metadata) and not sl_is_armed(metadata, trade)

            if not dca_hold_sl:
                self._advance_profit_stop_ladder(trade, current_price, is_long=False)

                if Config.ENABLE_TRAILING_STOP and entry > 0:
                    if current_price < entry or metadata.get("runner_active"):
                        self._apply_trailing_stop(trade, current_price, is_long=False)

            stop_loss = safe_float(trade.get("stop_loss"))
            safety_sl = resolved_final_sl(metadata, "SHORT")
            if dca_hold_sl:
                if last_resort_sl_hit("SHORT", current_price, safety_sl):
                    stop_loss = safety_sl
                else:
                    stop_loss = 0.0
            elif safety_sl > 0 and sl_is_armed(metadata, trade) and stop_loss <= 0:
                stop_loss = safety_sl
            if stop_loss > 0 and current_price >= stop_loss:
                if entry > 0 and not sl_is_valid_vs_entry("SHORT", entry, stop_loss):
                    self.failsafe_close_trade(
                        trade,
                        "INVALID_SL_FAILSAFE",
                        f"SHORT SL not above entry (entry={entry:.6f} sl={stop_loss:.6f})",
                    )
                    return
                if not self._market_close_should_retry(
                    str(trade.get("trade_id", "")), "STOP_LOSS"
                ):
                    self._sync_native_stop(trade, stop_loss)
                    return
                self._trigger_virtual_sl(trade, current_price, stop_loss)
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
        if not self.exchange.can_make_background_rest_call(1):
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

    def _ws_position_mark(self, symbol: str, position_side: str) -> Optional[float]:
        """Prefer Binance user-stream mark (same family as native MARK_PRICE stops)."""
        hub = getattr(self.exchange, "_market_data", None)
        if hub is None:
            return None
        getter = getattr(hub, "get_ws_mark_price", None)
        if not callable(getter):
            return None
        try:
            mark = getter(symbol, position_side)
        except Exception:
            return None
        return mark if mark is not None and mark > 0 else None

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

    def _sync_native_stop(self, trade: dict[str, Any], new_sl: float) -> None:
        """Replace the exchange STOP when virtual SL trails. No-op if native is off."""
        if not Config.ENABLE_NATIVE_TP_SL or new_sl <= 0:
            return
        metadata = self.db.parse_trade_metadata(trade)
        from core.exchange_protection import (
            apply_protection_metadata,
            ensure_native_stop,
            protection_is_verified,
        )

        ok, reason, oid = ensure_native_stop(
            self.exchange,
            symbol=str(trade.get("symbol") or ""),
            position_side=str(trade.get("side") or "LONG"),
            sl_price=new_sl,
            entry_price=safe_float(trade.get("entry_price")),
            old_sl_order_id=str(metadata.get("native_sl_order_id") or "") or None,
        )
        apply_protection_metadata(metadata, reason=reason, order_id=oid)
        try:
            self.db.update_trade(str(trade.get("trade_id")), {"metadata": metadata})
            trade["metadata"] = metadata
        except Exception:
            pass
        if not protection_is_verified(reason, oid):
            error_logger.critical(
                "Native SL trail not verified for %s: %s",
                trade.get("symbol"),
                reason,
            )

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
            self._cancel_native_tp_order(trade, level)
            return

        self._cancel_native_tp_order(trade, level)
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
            from core.exchange_protection import (
                apply_protection_metadata,
                ensure_native_stop,
                protection_is_verified,
            )

            ok, reason, new_sl_id = ensure_native_stop(
                self.exchange,
                symbol=trade["symbol"],
                position_side=str(trade.get("side", "LONG")),
                sl_price=new_sl,
                entry_price=safe_float(trade.get("entry_price")),
                old_sl_order_id=str(metadata.get("native_sl_order_id") or "") or None,
            )
            apply_protection_metadata(metadata, reason=reason, order_id=new_sl_id)
            self.db.update_trade(trade["trade_id"], {"metadata": metadata})
            if not protection_is_verified(reason, new_sl_id):
                error_logger.critical(
                    "Native SL after %s not verified for %s: %s",
                    level,
                    trade["symbol"],
                    reason,
                )

        if self.telegram:
            fill_price = safe_float(metadata.get("last_close_fill_price"))
            fill_qty = safe_float(metadata.get("last_close_fill_qty"))
            fill_pnl = metadata.get("last_close_fill_pnl")
            live_px = 0.0
            try:
                live_px = safe_float(
                    self.exchange.get_market_price(
                        trade["symbol"], str(trade.get("side", "LONG"))
                    )
                )
            except Exception:
                live_px = 0.0
            self.telegram.send_tp_level_alert(
                symbol=trade["symbol"],
                level=level,
                reason=reason,
                new_sl=new_sl,
                pnl=safe_float(fill_pnl) if fill_pnl is not None else None,
                fill_price=fill_price,
                quantity=fill_qty,
                side=str(trade.get("side", "LONG")),
                live_price=live_px,
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
            ok = False
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
                ok = False
            finally:
                release_exit(trade_id, "main")
                with self._close_lock:
                    self._close_inflight.discard(inflight_key)
                    if not hasattr(self, "_close_ok"):
                        self._close_ok = {}
                    self._close_ok[inflight_key] = bool(ok)
                    done = getattr(self, "_close_done", {}).get(inflight_key)
                    if done is not None:
                        done.set()
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
        sl_moved = False
        new_sl = current_sl
        if is_long:
            if current_price > best_price:
                best_price = current_price
                metadata["best_price"] = best_price
                updated = True
            if metadata.get("runner_active") or current_price > entry:
                metadata["trailing_active"] = True
                candidate_sl = round_protective_price(
                    best_price - trail_distance,
                    rules.tick_size,
                    rules.price_precision,
                    side="LONG",
                    kind="SL",
                )
                if candidate_sl > current_sl:
                    self.db.update_trade(
                        trade["trade_id"],
                        {"stop_loss": candidate_sl, "metadata": metadata},
                    )
                    trade["stop_loss"] = candidate_sl
                    new_sl = candidate_sl
                    sl_moved = True
                    updated = True
        else:
            if current_price < best_price:
                best_price = current_price
                metadata["best_price"] = best_price
                updated = True
            if metadata.get("runner_active") or current_price < entry:
                metadata["trailing_active"] = True
                candidate_sl = round_protective_price(
                    best_price + trail_distance,
                    rules.tick_size,
                    rules.price_precision,
                    side="SHORT",
                    kind="SL",
                )
                if candidate_sl < current_sl:
                    self.db.update_trade(
                        trade["trade_id"],
                        {"stop_loss": candidate_sl, "metadata": metadata},
                    )
                    trade["stop_loss"] = candidate_sl
                    new_sl = candidate_sl
                    sl_moved = True
                    updated = True

        if sl_moved:
            self._sync_native_stop(trade, new_sl)
        if updated and metadata.get("trailing_active"):
            self.db.update_trade(trade["trade_id"], {"metadata": metadata})
        if sl_moved and self.telegram:
            try:
                self.telegram.send_sl_move_alert(
                    symbol=str(trade.get("symbol", "")),
                    old_sl=current_sl,
                    new_sl=new_sl,
                    live_price=current_price,
                    kind="TRAILING",
                    side=str(trade.get("side", "LONG")),
                )
            except Exception as exc:
                error_logger.warning(
                    "SL-move Telegram alert failed for %s: %s",
                    trade.get("symbol"),
                    exc,
                )

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
        if live_qty <= 0 and partial:
            if requested_qty > 0:
                live_qty = requested_qty
            elif not exchange_only:
                live_qty = self._metadata_remaining_quantity(trade)
        if live_qty <= 0:
            return 0.0

        qty = min(requested_qty, live_qty)
        return round_step_size(qty, rules.step_size, rules.quantity_precision)

    def _market_close_max_attempts(self) -> int:
        return max(int(getattr(Config, "MARKET_CLOSE_MAX_ATTEMPTS", 4)), 1)

    def _ensure_close_attempt_state(self) -> None:
        if not hasattr(self, "_market_close_attempts"):
            self._market_close_attempts = {}
        if not hasattr(self, "_market_close_exhausted"):
            self._market_close_exhausted = set()
        if not hasattr(self, "_market_close_exhausted_at"):
            self._market_close_exhausted_at: dict[str, float] = {}

    def _market_close_key(self, trade_id: str, reason: str) -> str:
        return f"{trade_id}:{reason}"

    def _market_close_is_exhausted(self, trade_id: str, reason: str) -> bool:
        if not trade_id:
            return False
        self._ensure_close_attempt_state()
        return self._market_close_key(trade_id, reason) in self._market_close_exhausted

    def _note_market_close_attempt(self, trade_id: str, reason: str) -> int:
        self._ensure_close_attempt_state()
        key = self._market_close_key(trade_id, reason)
        count = int(self._market_close_attempts.get(key, 0)) + 1
        self._market_close_attempts[key] = count
        if count >= self._market_close_max_attempts():
            self._market_close_exhausted.add(key)
            self._market_close_exhausted_at[key] = time.monotonic()
        return count

    def _market_close_should_retry(self, trade_id: str, reason: str) -> bool:
        """True when a market close may be queued. Restores native SL during cooldown."""
        if not self._market_close_is_exhausted(trade_id, reason):
            return True
        self._ensure_close_attempt_state()
        key = self._market_close_key(trade_id, reason)
        started = float(self._market_close_exhausted_at.get(key) or 0.0)
        if started > 0.0 and (time.monotonic() - started) >= 45.0:
            self._market_close_exhausted.discard(key)
            self._market_close_attempts.pop(key, None)
            self._market_close_exhausted_at.pop(key, None)
            return True
        return False

    def _clear_market_close_attempts(self, trade_id: str) -> None:
        self._ensure_close_attempt_state()
        prefix = f"{trade_id}:"
        for key in list(self._market_close_attempts):
            if key.startswith(prefix):
                self._market_close_attempts.pop(key, None)
                self._market_close_exhausted.discard(key)
                self._market_close_exhausted_at.pop(key, None)

    def _inspect_exchange_position_qty(
        self, symbol: str, position_side: str
    ) -> Optional[float]:
        """Urgent REST size check for close/SL cleanup. None = unavailable."""
        try:
            ctx = getattr(self.exchange, "execution_context", None)
            if callable(ctx):
                with ctx():
                    return rest_position_quantity(
                        self.exchange, symbol, position_side, urgent=True
                    )
            return rest_position_quantity(
                self.exchange, symbol, position_side, urgent=True
            )
        except Exception as exc:
            error_logger.warning(
                "Urgent REST position inspect failed for %s %s: %s",
                symbol,
                position_side,
                exc,
            )
            return None

    def _finalize_close_if_exchange_flat(
        self,
        trade: dict[str, Any],
        reason: str,
        *,
        exit_reason: Optional[str] = None,
    ) -> bool:
        """If Binance size is 0, close the local trade, book PnL, and notify."""
        return self._reconcile_exchange_flat(
            trade,
            reason,
            exit_reason=exit_reason or reason or "RECONCILED_REDUCE_ONLY",
            confirmed_by_exchange=True,
            force_rest_inspect=True,
        )

    def _close_after_reduce_only_reject(
        self, trade: dict[str, Any], reason: str
    ) -> bool:
        """-2022 / reduce-only miss: REST-inspect size and close DB if already flat."""
        flattened = self._finalize_close_if_exchange_flat(trade, reason)
        if flattened:
            self._clear_market_close_attempts(str(trade.get("trade_id", "")))
            return True
        trade_logger.warning(
            "[%s] ReduceOnly/miss after market close — REST did not confirm flat | trigger=%s",
            trade.get("symbol"),
            reason,
        )
        return False

    def _handle_failed_market_close(
        self, trade: dict[str, Any], reason: str
    ) -> bool:
        """After inner retries fail, inspect Binance size before looping again."""
        rest_qty = self._inspect_exchange_position_qty(
            str(trade.get("symbol", "")),
            str(trade.get("side", "LONG")),
        )
        if rest_qty is not None and rest_qty <= 0:
            return self._close_after_reduce_only_reject(trade, reason)
        trade_logger.warning(
            "[%s] Market close failed — REST size=%s | trigger=%s",
            trade.get("symbol"),
            rest_qty,
            reason,
        )
        return False

    def _reconcile_exchange_flat(
        self,
        trade: dict[str, Any],
        reason: str,
        *,
        exit_reason: str = "RECONCILED_REDUCE_ONLY",
        confirmed_by_exchange: bool = False,
        force_rest_inspect: bool = False,
    ) -> bool:
        """Mark trade closed only after REST confirms the exchange is flat."""
        symbol = str(trade.get("symbol", ""))
        position_side = str(trade.get("side", "LONG")).upper()
        inspect_now = bool(confirmed_by_exchange or force_rest_inspect)
        if inspect_now:
            rest_qty = self._inspect_exchange_position_qty(symbol, position_side)
            if rest_qty is None:
                rest_qty = rest_position_quantity(
                    self.exchange, symbol, position_side, urgent=force_rest_inspect
                )
            if rest_qty is None or rest_qty > 0:
                trade_logger.warning(
                    "[%s] Skip %s — REST did not confirm flat | trigger=%s | qty=%s",
                    symbol,
                    exit_reason,
                    reason,
                    rest_qty,
                )
                return False
        elif not confirm_external_close_allowed(self.exchange, trade):
            trade_logger.warning(
                "[%s] Skip %s — transient miss, waiting for REST confirm | trigger=%s",
                symbol,
                exit_reason,
                reason,
            )
            return False
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
        self._mark_trade_closed(
            trade,
            reason=exit_reason,
            exit_price=exit_price,
            pnl=resolved.realized_pnl,
            pnl_source=resolved.source,
            notify=True,
        )
        return True

    def failsafe_close_trade(self, trade: dict[str, Any], reason: str, detail: str) -> None:
        """Market-close a position that cannot be protected. Alerts the operator."""
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "")
        error_logger.critical(
            "FAILSAFE_CLOSE %s %s | reason=%s | %s",
            symbol,
            side,
            reason,
            detail,
        )
        if self.telegram:
            try:
                self.telegram.send_message(
                    f"🚨 <b>FAILSAFE CLOSE</b>\n"
                    f"{escape_html(symbol)} {escape_html(side)}\n"
                    f"Reason: {escape_html(reason)}\n"
                    f"<i>{escape_html(detail)}</i>"
                )
            except Exception as exc:
                error_logger.warning("Fail-safe Telegram alert failed: %s", exc)
        self._close_position(
            trade,
            quantity=self._remaining_close_quantity(trade),
            reason=reason,
        )

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
        if not partial:
            if self._market_close_is_exhausted(trade_id, reason):
                flattened = self._finalize_close_if_exchange_flat(trade, reason)
                if flattened:
                    return True
                trade_logger.warning(
                    "[%s] Market close exhausted for %s — skipping further retries | trigger=%s",
                    symbol,
                    trade_id,
                    reason,
                )
                return False
            if close_qty <= 0:
                flattened = self._finalize_close_if_exchange_flat(trade, reason)
                if flattened:
                    return True
                rest_qty = self._inspect_exchange_position_qty(
                    symbol, str(trade.get("side", "LONG"))
                )
                if rest_qty is not None and rest_qty > 0:
                    close_qty = rest_qty
                    trade_logger.warning(
                        "[%s] Cache miss on close qty; REST size=%.8f — retrying flatten | trigger=%s",
                        symbol,
                        close_qty,
                        reason,
                    )
                else:
                    trade_logger.warning(
                        "[%s] Close qty=0 and REST did not confirm remaining size | trigger=%s",
                        symbol,
                        reason,
                    )
                    return False
        elif close_qty <= 0:
            if quantity > 0:
                close_qty = quantity
                trade_logger.info(
                    "[%s] Partial close qty from metadata=%s (exchange cache stale).",
                    symbol,
                    close_qty,
                )
            else:
                return False

        if not hasattr(self, "_close_done"):
            self._close_done = {}
            self._close_ok = {}

        duplicate_done: Optional[threading.Event] = None
        already_inflight = False
        with self._close_lock:
            if inflight_key in self._close_inflight:
                already_inflight = True
                duplicate_done = self._close_done.get(inflight_key)
            else:
                if exit_claim_active(trade_id) or not claim_exit(trade_id, "main"):
                    trade_logger.debug(
                        "[%s] Close deferred — exit claim held by another process.",
                        symbol,
                    )
                    return False
                self._close_inflight.add(inflight_key)
                self._close_done[inflight_key] = threading.Event()
                self._close_ok[inflight_key] = False
                if not partial:
                    attempts = self._note_market_close_attempt(trade_id, reason)
                    trade_logger.info(
                        "[%s] Executing Market Close attempt %s/%s | trigger=%s | qty=%s",
                        symbol,
                        attempts,
                        self._market_close_max_attempts(),
                        reason,
                        close_qty,
                    )

        if already_inflight:
            if not blocking:
                return False
            if duplicate_done is not None:
                timeout = (
                    self.CLOSE_ORDER_MAX_RETRIES
                    * self.CLOSE_ORDER_RETRY_DELAY_SECONDS
                    * 3
                    + 15.0
                )
                if not duplicate_done.wait(timeout=timeout):
                    error_logger.warning(
                        "Duplicate close wait timed out for %s (%s) after %.0fs.",
                        symbol,
                        reason,
                        timeout,
                    )
                    return False
            return bool(self._close_ok.get(inflight_key, False))

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
            flattened = self._finalize_close_if_exchange_flat(trade, reason)
            if flattened:
                self._clear_market_close_attempts(str(trade_id))
            return flattened

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
                    return self._close_after_reduce_only_reject(trade, reason)
                except OrderExecutionError as exc:
                    if PositionAlreadyClosedError.matches(exc):
                        return self._close_after_reduce_only_reject(trade, reason)
                    if attempt >= self.CLOSE_ORDER_MAX_RETRIES - 1:
                        error_logger.error(
                            "Close order failed for %s (%s) after %s attempts: %s",
                            symbol,
                            reason,
                            self.CLOSE_ORDER_MAX_RETRIES,
                            exc,
                        )
                        return self._handle_failed_market_close(trade, reason)
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
            return self._close_after_reduce_only_reject(trade, reason)
        except OrderExecutionError as exc:
            if PositionAlreadyClosedError.matches(exc):
                return self._close_after_reduce_only_reject(trade, reason)
            error_logger.error("Close order failed for %s (%s): %s", symbol, reason, exc)
            return self._handle_failed_market_close(trade, reason)

        if not response:
            return self._handle_failed_market_close(trade, reason)

        order_id = str(response.get("orderId", ""))
        fill_pnl = self.exchange.resolve_order_fill_pnl(
            symbol,
            order_id,
            position_side=str(position_side),
        )
        live_mark = safe_float(self.exchange.get_market_price(symbol, position_side))
        exit_price = fill_pnl.exit_price
        if exit_price <= 0:
            exit_price = resolve_binance_fill_price(response, fallback=live_mark)

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
            cumulative_pnl, pnl_source, lifecycle_exit = (
                self._resolve_full_close_pnl(
                    trade,
                    symbol,
                    position_side,
                    prior_pnl=prior_pnl,
                    leg_pnl=leg_pnl,
                    leg_source=pnl_source,
                )
            )
            if lifecycle_exit > 0 and exit_price <= 0:
                exit_price = lifecycle_exit
        else:
            cumulative_pnl = prior_pnl + leg_pnl

        metadata = self.db.parse_trade_metadata(trade)
        metadata["last_fill_order_id"] = order_id
        metadata["last_fill_pnl_source"] = pnl_source
        metadata["last_close_fill_price"] = exit_price
        metadata["last_close_fill_qty"] = close_qty
        metadata["last_close_fill_pnl"] = leg_pnl
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

        return True

    def _resolve_full_close_pnl(
        self,
        trade: dict[str, Any],
        symbol: str,
        position_side: str,
        *,
        prior_pnl: float,
        leg_pnl: float,
        leg_source: str,
    ) -> tuple[float, str, float]:
        """Resolve total realized PnL for a full close with REST retries."""
        from reconciliation import trade_opened_at_ms

        opened_ms = trade_opened_at_ms(trade)
        lifecycle = None
        for attempt in range(1, 4):
            lifecycle = self.exchange.fetch_closed_position_realized_pnl(
                symbol,
                str(position_side),
                opened_ms,
            )
            if lifecycle.source in ("userTrades", "income", "ws") and (
                lifecycle.fill_count > 0 or abs(lifecycle.realized_pnl) > 0
            ):
                return (
                    lifecycle.realized_pnl,
                    lifecycle.source,
                    lifecycle.exit_price,
                )
            if attempt < 3:
                time.sleep(1.0 * attempt)

        if leg_source not in ("unknown", "unavailable") and leg_pnl != 0:
            return prior_pnl + leg_pnl, leg_source, 0.0

        resolved = resolve_exchange_close_pnl(self.exchange, self.db, trade)
        if resolved.fill_count > 0 or abs(resolved.realized_pnl) > 0:
            return resolved.realized_pnl, resolved.source, resolved.exit_price

        return prior_pnl + leg_pnl, leg_source or "unavailable", 0.0

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
    ) -> float:
        trade_id = str(trade["trade_id"])
        symbol = str(trade.get("symbol", ""))
        position_side = str(trade.get("side", "LONG")).upper()
        if is_synthetic_exchange_orphan(trade):
            trade_logger.info(
                "[%s] Synthetic exchange orphan %s — skip DB close.",
                symbol,
                trade_id,
            )
            return 0.0

        with trade_close_mutex(trade_id, blocking=True) as acquired:
            if not acquired:
                existing = self.db.get_trade(trade_id)
                return safe_float(
                    existing.get("realized_pnl") or existing.get("pnl")
                    if existing
                    else 0.0
                )

            fresh = self.db.get_trade(trade_id)
            if fresh:
                trade = fresh
            if trade.get("status") == TRADE_STATUS_CLOSED:
                return safe_float(trade.get("realized_pnl") or trade.get("pnl"))

            claim_owner = get_exit_claim_owner(trade_id)
            acquired_claim = False
            if claim_owner and claim_owner not in ("main", "close_pipeline"):
                trade_logger.debug(
                    "[%s] Close skipped — exit claim held by %s",
                    trade_id[:8],
                    claim_owner,
                )
                return safe_float(trade.get("realized_pnl") or trade.get("pnl"))
            if claim_owner not in ("main", "close_pipeline"):
                if not claim_exit(trade_id, "close_pipeline"):
                    if exit_claim_active(trade_id):
                        trade_logger.debug(
                            "[%s] Close skipped — exit claim contention",
                            trade_id[:8],
                        )
                        return safe_float(trade.get("realized_pnl") or trade.get("pnl"))
                else:
                    acquired_claim = True

            try:
                return self._finalize_trade_closed(
                    trade,
                    reason=reason,
                    exit_price=exit_price,
                    notify=notify,
                    book_daily_pnl=book_daily_pnl,
                    pnl=pnl,
                    pnl_source=pnl_source,
                )
            finally:
                if acquired_claim:
                    release_exit(trade_id, "close_pipeline")

    def _finalize_trade_closed(
        self,
        trade: dict[str, Any],
        reason: str,
        exit_price: float,
        *,
        notify: bool,
        book_daily_pnl: bool,
        pnl: Optional[float],
        pnl_source: str,
    ) -> float:
        trade_id = str(trade["trade_id"])
        symbol = str(trade.get("symbol", ""))
        position_side = str(trade.get("side", "LONG")).upper()
        closed_at = utc_now().isoformat()
        self._clear_market_close_attempts(trade_id)
        dca_stage = getattr(self, "_dca_stage", None)
        if isinstance(dca_stage, dict):
            dca_stage.pop(trade_id, None)
        inflight = getattr(self, "_dca_inflight", None)
        if isinstance(inflight, set):
            inflight.discard(trade_id)

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

        position_reconcile_guard.note_present(trade_id)
        self._cancel_all_native_orders(trade)
        self.exchange.clear_position_cache(symbol, position_side)
        with self._monitored_lock:
            self._monitored_symbols.discard(symbol.upper())
        balance = self.exchange.refresh_wallet_after_trade()

        if is_exchange_sync_close_reason(reason) and (
            pnl is None
            or (
                abs(safe_float(pnl)) < 1e-12
                and pnl_source not in ("userTrades", "income", "ws")
            )
        ):
            resolved = resolve_exchange_close_pnl(self.exchange, self.db, trade)
            pnl = resolved.realized_pnl
            pnl_source = resolved.source
            if resolved.exit_price > 0:
                exit_price = resolved.exit_price

        if str(pnl_source or "").lower() == "pending":
            book_daily_pnl = False

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
            live_px = 0.0
            try:
                live_px = safe_float(
                    self.exchange.get_market_price(symbol, position_side)
                )
            except Exception:
                live_px = 0.0
            close_px = exit_price if exit_price > 0 else live_px
            self.telegram.send_close_alert(
                symbol=symbol,
                reason=reason,
                pnl=safe_float(trade.get("pnl")),
                strategy=str(trade.get("strategy", "")),
                trade_id=trade_id,
                exit_price=close_px,
                live_price=live_px,
                side=position_side,
                pnl_source=pnl_source,
            )

        outcome = "WIN" if safe_float(trade.get("pnl")) > 0 else "LOSS"
        strategy = str(trade.get("strategy", ""))
        pnl_val = safe_float(trade.get("pnl"))
        reason_upper = reason.upper()

        soft_exit = reason in (
            "RANGE_TIME_STOP",
            "RANGE_BOUNDARY_BREAKOUT",
        ) and pnl_val >= 0

        cooldown_minutes = Config.POST_TRADE_COOLDOWN_MINUTES
        cooldown_reason = f"POST_TRADE_{reason}"

        if is_range_strategy(strategy):
            if reason == "STOP_LOSS" or (pnl_val < 0 and "STOP" in reason_upper):
                cooldown_minutes = max(cooldown_minutes, Config.RANGE_COOLDOWN_MINUTES)
                cooldown_reason = "RANGE_STOP_LOSS"
            elif soft_exit:
                cooldown_minutes = max(
                    cooldown_minutes, Config.RANGE_COOLDOWN_SOFT_MINUTES
                )
                cooldown_reason = "RANGE_SOFT_EXIT"
            else:
                cooldown_minutes = max(cooldown_minutes, Config.RANGE_COOLDOWN_MINUTES)
                cooldown_reason = "RANGE_CLOSE"
        elif reason == "STOP_LOSS" or (pnl_val < 0 and "STOP" in reason_upper):
            outcome = "LOSS"
            cooldown_minutes = max(cooldown_minutes, Config.SYMBOL_COOLDOWN_MINUTES)
            cooldown_reason = "STOP_LOSS"
        elif soft_exit:
            cooldown_minutes = max(
                cooldown_minutes, Config.SYMBOL_COOLDOWN_SOFT_MINUTES
            )
            cooldown_reason = "SOFT_EXIT"
        elif pnl_val > 0:
            outcome = "WIN"

        self.db.set_symbol_cooldown(symbol, cooldown_minutes, reason=cooldown_reason)
        self.db.record_signal_outcome(trade_id, outcome)

        if self.scheduler:
            self.scheduler.notify_trade_event()
        if self.risk_manager:
            self.risk_manager.notify_trade_event()
        return total_pnl

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
