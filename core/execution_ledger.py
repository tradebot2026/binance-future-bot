"""Traceable trade-execution state machine.

TRADE_APPROVED is a strategy/risk gate only. It is never a Binance fill.
"""

from __future__ import annotations

import enum
import json
import os
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from config import Config
from logger import trade_logger


class ExecutionPhase(str, enum.Enum):
    SIGNAL_DETECTED = "SIGNAL_DETECTED"
    TRADE_APPROVED = "TRADE_APPROVED"
    EXECUTION_QUEUED = "EXECUTION_QUEUED"
    ORDER_SUBMITTING = "ORDER_SUBMITTING"
    BINANCE_ACK = "BINANCE_ACK"
    ORDER_ID_RECEIVED = "ORDER_ID_RECEIVED"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_ACCEPTED_NOT_FILLED = "ORDER_ACCEPTED_NOT_FILLED"
    ORDER_STATUS_UNCERTAIN = "ORDER_STATUS_UNCERTAIN"
    ORDER_FILLED = "ORDER_FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    POSITION_CONFIRMED = "POSITION_CONFIRMED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_SUBMISSION_FAILED = "ORDER_SUBMISSION_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    WS_DISCONNECTED = "WS_DISCONNECTED"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"
    CANCELED = "CANCELED"


_OPENED_PHASES = frozenset(
    {
        ExecutionPhase.ORDER_FILLED,
        ExecutionPhase.PARTIALLY_FILLED,
        ExecutionPhase.POSITION_CONFIRMED,
    }
)

_TERMINAL_FAIL = frozenset(
    {
        ExecutionPhase.ORDER_REJECTED,
        ExecutionPhase.ORDER_SUBMISSION_FAILED,
        ExecutionPhase.RATE_LIMITED,
        ExecutionPhase.CANCELED,
    }
)

_UNRESOLVED = frozenset(
    {
        ExecutionPhase.ORDER_SUBMITTING,
        ExecutionPhase.BINANCE_ACK,
        ExecutionPhase.ORDER_ID_RECEIVED,
        ExecutionPhase.ORDER_ACCEPTED,
        ExecutionPhase.ORDER_ACCEPTED_NOT_FILLED,
        ExecutionPhase.ORDER_STATUS_UNCERTAIN,
    }
)


@dataclass
class ExecutionRecord:
    exec_id: str
    symbol: str
    action: str
    strategy: str
    score: float = 0.0
    phase: ExecutionPhase = ExecutionPhase.SIGNAL_DETECTED
    client_order_id: str = ""
    binance_order_id: str = ""
    quantity: float = 0.0
    entry: float = 0.0
    sl: float = 0.0
    tp: float = 0.0
    atr: float = 0.0
    error_code: int = 0
    error_message: str = ""
    http_status: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def is_opened(self) -> bool:
        return self.phase in _OPENED_PHASES

    def is_unresolved(self) -> bool:
        return self.phase in _UNRESOLVED


class ExecutionLedger:
    """In-memory ring of execution records + sequential EXEC- ids."""

    def __init__(self, maxlen: int = 80, persist_path: str = "") -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._day = ""
        self._records: dict[str, ExecutionRecord] = {}
        self._order: deque[str] = deque(maxlen=maxlen)
        self._persist_path = persist_path or os.path.join(
            Config.DATA_DIR, "uncertain_executions.json"
        )
        self._load_persisted()

    def new_record(
        self,
        *,
        symbol: str,
        action: str,
        strategy: str,
        score: float = 0.0,
    ) -> ExecutionRecord:
        exec_id = self._next_id()
        client_order_id = f"bfb{exec_id.replace('-', '')[-20:]}"[:36]
        record = ExecutionRecord(
            exec_id=exec_id,
            symbol=symbol.upper(),
            action=action.upper(),
            strategy=strategy,
            score=score,
            client_order_id=client_order_id,
        )
        with self._lock:
            self._records[exec_id] = record
            self._order.append(exec_id)
        self.transition(
            record,
            ExecutionPhase.SIGNAL_DETECTED,
            extra=f"score={score:.1f}",
        )
        return record

    def transition(
        self,
        record: ExecutionRecord,
        phase: ExecutionPhase,
        *,
        extra: str = "",
        error_code: int = 0,
        error_message: str = "",
        http_status: int = 0,
        binance_order_id: str = "",
        quantity: float = 0.0,
        entry: float = 0.0,
    ) -> None:
        record.phase = phase
        record.updated_at = time.time()
        if error_code:
            record.error_code = int(error_code)
        if error_message:
            record.error_message = str(error_message)[:240]
        if http_status:
            record.http_status = int(http_status)
        if binance_order_id:
            record.binance_order_id = str(binance_order_id)
        if quantity > 0:
            record.quantity = quantity
        if entry > 0:
            record.entry = entry
        bits = [
            f"[{phase.value}]",
            record.exec_id,
            record.symbol,
            record.action,
            f"strategy={record.strategy}",
        ]
        if extra:
            bits.append(extra)
        if record.binance_order_id:
            bits.append(f"orderId={record.binance_order_id}")
        if record.client_order_id:
            bits.append(f"clientOrderId={record.client_order_id}")
        if record.error_code or record.error_message:
            bits.append(f"code={record.error_code}")
            bits.append(record.error_message)
        trade_logger.info(" ".join(str(b) for b in bits if b))
        self._persist_unlocked()

    def unresolved(self) -> list[ExecutionRecord]:
        with self._lock:
            return [
                rec
                for exec_id in list(self._order)
                if (rec := self._records.get(exec_id)) is not None
                and rec.phase in _UNRESOLVED
            ]

    def find_by_ids(
        self,
        *,
        client_order_id: str = "",
        binance_order_id: str = "",
        symbol: str = "",
    ) -> Optional[ExecutionRecord]:
        client_order_id = str(client_order_id or "")
        binance_order_id = str(binance_order_id or "")
        symbol = symbol.upper()
        with self._lock:
            for rec in self._records.values():
                if client_order_id and rec.client_order_id == client_order_id:
                    return rec
                if binance_order_id and rec.binance_order_id == binance_order_id:
                    return rec
                if (
                    symbol
                    and rec.symbol == symbol
                    and rec.phase in _UNRESOLVED
                    and rec.client_order_id
                ):
                    return rec
        return None

    def queued_count(self) -> int:
        with self._lock:
            return sum(
                1
                for exec_id in self._order
                if self._records.get(exec_id)
                and self._records[exec_id].phase
                in {
                    ExecutionPhase.EXECUTION_QUEUED,
                    ExecutionPhase.ORDER_SUBMITTING,
                }
            )

    def recent_summary(self, limit: int = 8) -> dict[str, int]:
        counts = {
            "approved": 0,
            "submitted": 0,
            "filled": 0,
            "rejected": 0,
            "failed": 0,
        }
        with self._lock:
            ids = list(self._order)[-max(limit * 4, 20) :]
            for exec_id in ids:
                rec = self._records.get(exec_id)
                if rec is None:
                    continue
                if rec.phase in _OPENED_PHASES:
                    counts["filled"] += 1
                elif rec.phase == ExecutionPhase.ORDER_REJECTED:
                    counts["rejected"] += 1
                elif rec.phase in _TERMINAL_FAIL:
                    counts["failed"] += 1
                elif rec.phase in {
                    ExecutionPhase.BINANCE_ACK,
                    ExecutionPhase.ORDER_ID_RECEIVED,
                    ExecutionPhase.ORDER_ACCEPTED,
                    ExecutionPhase.ORDER_SUBMITTING,
                }:
                    counts["submitted"] += 1
                elif rec.phase in {
                    ExecutionPhase.TRADE_APPROVED,
                    ExecutionPhase.SIGNAL_DETECTED,
                    ExecutionPhase.EXECUTION_QUEUED,
                }:
                    counts["approved"] += 1
        return counts

    def _persist_unlocked(self) -> None:
        rows = []
        for exec_id in list(self._order):
            rec = self._records.get(exec_id)
            if rec is None or rec.phase not in _UNRESOLVED:
                continue
            payload = asdict(rec)
            payload["phase"] = rec.phase.value
            rows.append(payload)
        try:
            os.makedirs(os.path.dirname(self._persist_path) or ".", exist_ok=True)
            tmp = self._persist_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(rows, handle)
            os.replace(tmp, self._persist_path)
        except OSError:
            pass

    def _load_persisted(self) -> None:
        if not os.path.isfile(self._persist_path):
            return
        try:
            with open(self._persist_path, encoding="utf-8") as handle:
                rows = json.load(handle)
        except (OSError, json.JSONDecodeError, TypeError):
            return
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                phase = ExecutionPhase(str(row.get("phase") or "ORDER_STATUS_UNCERTAIN"))
            except ValueError:
                phase = ExecutionPhase.ORDER_STATUS_UNCERTAIN
            record = ExecutionRecord(
                exec_id=str(row.get("exec_id") or ""),
                symbol=str(row.get("symbol") or "").upper(),
                action=str(row.get("action") or "").upper(),
                strategy=str(row.get("strategy") or ""),
                score=float(row.get("score") or 0.0),
                phase=phase,
                client_order_id=str(row.get("client_order_id") or ""),
                binance_order_id=str(row.get("binance_order_id") or ""),
                quantity=float(row.get("quantity") or 0.0),
                entry=float(row.get("entry") or 0.0),
                sl=float(row.get("sl") or 0.0),
                tp=float(row.get("tp") or 0.0),
                atr=float(row.get("atr") or 0.0),
                created_at=float(row.get("created_at") or time.time()),
                updated_at=float(row.get("updated_at") or time.time()),
            )
            if not record.exec_id:
                continue
            self._records[record.exec_id] = record
            self._order.append(record.exec_id)

    def _next_id(self) -> str:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        with self._lock:
            if day != self._day:
                self._day = day
                self._seq = 0
            self._seq += 1
            return f"EXEC-{day}-{self._seq:05d}"


_LEDGER = ExecutionLedger()


def get_execution_ledger() -> ExecutionLedger:
    return _LEDGER
