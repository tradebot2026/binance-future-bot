"""
Captures realized PnL from Binance ORDER_TRADE_UPDATE WebSocket fill events.
Used to align bot DB/Telegram PnL with exchange trade history.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class FillPnlRecord:
    order_id: str
    symbol: str
    position_side: str
    realized_pnl: float
    commission: float
    fill_price: float
    fill_qty: float
    trade_id: str
    timestamp_ms: int
    source: str = "ws"


class FillPnlTracker:
    """Thread-safe cache of recent fill PnL keyed by Binance order id."""

    def __init__(self, *, max_orders: int = 512) -> None:
        self._lock = threading.RLock()
        self._by_order: dict[str, FillPnlRecord] = {}
        self._max_orders = max_orders

    def record(self, record: FillPnlRecord) -> None:
        if not record.order_id:
            return
        with self._lock:
            existing = self._by_order.get(record.order_id)
            if existing:
                record = FillPnlRecord(
                    order_id=record.order_id,
                    symbol=record.symbol or existing.symbol,
                    position_side=record.position_side or existing.position_side,
                    realized_pnl=existing.realized_pnl + record.realized_pnl,
                    commission=existing.commission + record.commission,
                    fill_price=record.fill_price or existing.fill_price,
                    fill_qty=existing.fill_qty + record.fill_qty,
                    trade_id=record.trade_id or existing.trade_id,
                    timestamp_ms=max(record.timestamp_ms, existing.timestamp_ms),
                    source=record.source,
                )
            self._by_order[record.order_id] = record
            if len(self._by_order) > self._max_orders:
                oldest = sorted(
                    self._by_order.items(),
                    key=lambda item: item[1].timestamp_ms,
                )
                for key, _ in oldest[: len(self._by_order) - self._max_orders]:
                    self._by_order.pop(key, None)

    def get(self, order_id: str) -> Optional[FillPnlRecord]:
        with self._lock:
            return self._by_order.get(str(order_id))

    def wait_for(self, order_id: str, *, timeout: float = 3.0) -> Optional[FillPnlRecord]:
        deadline = time.monotonic() + max(timeout, 0.0)
        order_id = str(order_id)
        while time.monotonic() < deadline:
            record = self.get(order_id)
            if record is not None:
                return record
            time.sleep(0.05)
        return self.get(order_id)
