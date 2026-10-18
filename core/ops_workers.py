"""
Background reconcile and SQLite maintenance workers.
Keep REST/VACUUM off the main scan/tick loop.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from config import Config
from logger import error_logger, system_logger
from core.ops_heartbeat import touch_reconcile_loop
from reconciliation import (
    recover_orphan_fills_from_disk,
    reconcile_positions,
    reconcile_uncertain_executions,
)


class BackgroundOps:
    """Two daemon threads: position reconcile + DB maintenance."""

    def __init__(
        self,
        exchange: Any,
        db: Any,
        *,
        telegram: Any = None,
        manager: Any = None,
        executor: Any = None,
        controller: Any = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.telegram = telegram
        self.manager = manager
        self.executor = executor
        self.controller = controller
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        rec = threading.Thread(
            target=self._reconcile_loop,
            name="ops-reconcile",
            daemon=True,
        )
        maint = threading.Thread(
            target=self._maintenance_loop,
            name="ops-db-maint",
            daemon=True,
        )
        rec.start()
        maint.start()
        self._threads = [rec, maint]
        system_logger.info(
            "Background ops started (reconcile every %.0fs, DB maint every %.0fs).",
            float(Config.RECONCILIATION_INTERVAL_SECONDS),
            float(Config.DB_MAINTENANCE_INTERVAL_SECONDS),
        )

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=8.0)
        self._threads = []

    def _shutdown_requested(self) -> bool:
        if self._stop.is_set():
            return True
        ctrl = self.controller
        if ctrl is not None and hasattr(ctrl, "is_shutdown_requested"):
            return bool(ctrl.is_shutdown_requested())
        return False

    def _rest_blocked(self) -> bool:
        blocked_fn = getattr(self.exchange, "is_rest_blocked", None)
        if not callable(blocked_fn):
            return False
        try:
            blocked, _ = blocked_fn()
            return bool(blocked)
        except Exception:
            return False

    def _reconcile_loop(self) -> None:
        rec_interval = max(float(Config.RECONCILIATION_INTERVAL_SECONDS), 30.0)
        uncertain_interval = max(
            float(getattr(Config, "UNCERTAIN_ORDER_RECONCILE_SECONDS", 30.0)), 5.0
        )
        last_uncertain = 0.0
        last_reconcile = time.monotonic()
        while not self._shutdown_requested():
            try:
                now = time.monotonic()
                touch_reconcile_loop()
                if (
                    now - last_uncertain >= uncertain_interval
                    and not self._rest_blocked()
                    and self.executor is not None
                ):
                    try:
                        late = reconcile_uncertain_executions(
                            self.exchange, self.executor, manager=self.manager
                        )
                        if late:
                            system_logger.info(
                                "Late-fill reconcile confirmed %s position(s).",
                                late,
                            )
                    except Exception as exc:
                        error_logger.error(
                            "Uncertain-order reconcile worker failed: %s", exc
                        )
                    try:
                        recovered = recover_orphan_fills_from_disk(self.db)
                        if recovered:
                            system_logger.info(
                                "Recovered %s orphan fill(s) into the trade DB.",
                                recovered,
                            )
                            purge = getattr(
                                self.executor, "purge_orphans_present_in_db", None
                            )
                            if callable(purge):
                                purge()
                    except Exception as exc:
                        error_logger.error(
                            "Orphan-fill disk recovery failed: %s", exc
                        )
                    last_uncertain = now
                if now - last_reconcile >= rec_interval:
                    if not self._rest_blocked():
                        try:
                            reconcile_positions(
                                self.exchange,
                                self.db,
                                self.telegram,
                                manager=self.manager,
                                context="periodic",
                            )
                        except Exception as exc:
                            error_logger.error(
                                "Periodic reconcile worker failed: %s", exc
                            )
                    try:
                        from core.exchange_protection import restore_open_trade_protection
                        from core.ops_heartbeat import touch_protection

                        restore_open_trade_protection(
                            self.exchange,
                            self.db,
                            manager=self.manager,
                            telegram=self.telegram,
                            failsafe_on_failure=False,
                        )
                        if Config.ENABLE_NATIVE_TP_SL:
                            touch_protection(source="periodic_verify")
                    except Exception as exc:
                        error_logger.error(
                            "Periodic protection inventory/verify failed: %s", exc
                        )
                    try:
                        self.db.cleanup_expired_cooldowns()
                    except Exception as exc:
                        error_logger.debug("Cooldown cleanup failed: %s", exc)
                    last_reconcile = now
            except Exception as exc:
                error_logger.error("Reconcile worker error: %s", exc)
            self._stop.wait(1.0)

    def _maintenance_loop(self) -> None:
        interval = max(float(Config.DB_MAINTENANCE_INTERVAL_SECONDS), 60.0)
        last_run = time.monotonic()
        while not self._shutdown_requested():
            try:
                now = time.monotonic()
                if now - last_run >= interval:
                    try:
                        self.db.run_maintenance(
                            retention_days=Config.DB_RETENTION_DAYS,
                            vacuum=True,
                        )
                    except Exception as exc:
                        error_logger.error("DB maintenance worker failed: %s", exc)
                    last_run = now
            except Exception as exc:
                error_logger.error("DB maintenance worker error: %s", exc)
            self._stop.wait(5.0)


def start_background_ops(
    exchange: Any,
    db: Any,
    *,
    telegram: Any = None,
    manager: Any = None,
    executor: Any = None,
    controller: Any = None,
) -> BackgroundOps:
    ops = BackgroundOps(
        exchange,
        db,
        telegram=telegram,
        manager=manager,
        executor=executor,
        controller=controller,
    )
    ops.start()
    return ops
