"""
Dynamic daily starting balance — resets at 00:00 UTC using USDT wallet balance.
Persisted to disk so mid-day restarts do not overwrite the day's baseline.
Matches Day PnL in risk_manager: current_wallet - daily_starting_balance.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from config import Config
from constants import DAILY_STATUS_ACTIVE
from logger import system_logger
from utils import safe_float, utc_today_str

if TYPE_CHECKING:
    from database import DatabaseManager
    from exchange import BinanceExchangeManager


_STATE_PATH = os.path.join(Config.DATA_DIR, "daily_balance_state.json")


@dataclass
class DailyBalanceState:
    utc_day: str
    daily_starting_balance: float


def _load_state() -> Optional[DailyBalanceState]:
    try:
        if not os.path.isfile(_STATE_PATH):
            return None
        with open(_STATE_PATH, encoding="utf-8") as handle:
            raw = json.load(handle)
        day = str(raw.get("utc_day", "")).strip()
        balance = safe_float(raw.get("daily_starting_balance"))
        if day and balance > 0:
            return DailyBalanceState(day, balance)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def _save_state(state: DailyBalanceState) -> None:
    try:
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        tmp = f"{_STATE_PATH}.tmp"
        payload = {
            "utc_day": state.utc_day,
            "daily_starting_balance": state.daily_starting_balance,
        }
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, _STATE_PATH)
    except OSError as exc:
        system_logger.warning("Failed to persist daily balance state: %s", exc)


def fetch_daily_wallet_baseline(
    exchange: "BinanceExchangeManager",
    *,
    force: bool = False,
) -> float:
    """USDT wallet balance — daily baseline and Day PnL use the same metric."""
    if hasattr(exchange, "fetch_live_account_snapshot"):
        snap = exchange.fetch_live_account_snapshot(
            include_today_income=False,
            force_refresh=force,
        )
        wallet = safe_float(snap.wallet_balance)
        if wallet > 0:
            return wallet
    wallet = exchange.get_futures_balance(force_refresh=force)
    if wallet <= 0 and force:
        wallet = exchange.get_futures_balance(force_refresh=True)
    return wallet


def fetch_account_equity(exchange: "BinanceExchangeManager", *, force: bool = False) -> float:
    """Alias for wallet baseline (kept for scheduler compatibility)."""
    return fetch_daily_wallet_baseline(exchange, force=force)


def ensure_daily_baseline(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    *,
    utc_day: Optional[str] = None,
    force_new_day: bool = False,
) -> float:
    """
    Return today's daily_starting_balance.
    - New UTC day: snapshot current wallet balance and persist.
    - Same day restart: reuse persisted baseline (never overwrite mid-day).
    """
    today = utc_day or utc_today_str()

    if force_new_day:
        wallet = fetch_daily_wallet_baseline(exchange, force=True)
        if wallet <= 0:
            persisted = _load_state()
            if persisted and persisted.daily_starting_balance > 0:
                wallet = persisted.daily_starting_balance
        if wallet <= 0:
            system_logger.warning(
                "Could not resolve wallet balance for new day %s.", today
            )
            return 0.0
        _save_state(DailyBalanceState(today, wallet))
        db.initialize_daily_stats(today, wallet)
        db.update_daily_balance(today, wallet, DAILY_STATUS_ACTIVE)
        system_logger.info(
            "UTC day rollover — new daily starting wallet for %s: $%.2f",
            today,
            wallet,
        )
        return wallet

    persisted = _load_state()
    stats = db.get_daily_stats(today)

    if (
        persisted
        and persisted.utc_day == today
        and persisted.daily_starting_balance > 0
    ):
        baseline = persisted.daily_starting_balance
        if not stats:
            db.initialize_daily_stats(today, baseline)
            db.update_daily_balance(today, baseline, DAILY_STATUS_ACTIVE)
        system_logger.debug(
            "Daily baseline restored from disk for %s: $%.2f",
            today,
            baseline,
        )
        return baseline

    if stats and safe_float(stats.get("start_balance")) > 0 and not force_new_day:
        baseline = safe_float(stats.get("start_balance"))
        _save_state(DailyBalanceState(today, baseline))
        return baseline

    wallet = fetch_daily_wallet_baseline(exchange, force=True)
    if wallet <= 0 and persisted and persisted.daily_starting_balance > 0:
        wallet = persisted.daily_starting_balance

    if wallet <= 0:
        system_logger.warning(
            "Could not resolve wallet balance for daily baseline on %s.",
            today,
        )
        return 0.0

    _save_state(DailyBalanceState(today, wallet))
    db.initialize_daily_stats(today, wallet)
    db.update_daily_balance(today, wallet, DAILY_STATUS_ACTIVE)
    system_logger.info(
        "Daily starting wallet set for %s: $%.2f.",
        today,
        wallet,
    )
    return wallet


def on_utc_day_rollover(
    exchange: "BinanceExchangeManager",
    db: "DatabaseManager",
    new_day: str,
) -> float:
    """Begin a new UTC trading day — baseline = current USDT wallet balance."""
    return ensure_daily_baseline(
        exchange, db, utc_day=new_day, force_new_day=True
    )
