"""
Cross-process exit claim lock — prevents main.py and watchdog.py double-closing.

Uses atomic JSON files under data/exit_claims/ (separate OS processes).
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

from config import Config


def _claims_dir() -> str:
    path = os.path.join(Config.DATA_DIR, "exit_claims")
    os.makedirs(path, exist_ok=True)
    return path


def _claim_path(trade_id: str) -> str:
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in trade_id)
    return os.path.join(_claims_dir(), f"{safe_id}.json")


def claim_exit(trade_id: str, owner: str, *, ttl_seconds: Optional[int] = None) -> bool:
    """
    Try to acquire exclusive exit rights for a trade.
    Returns True if claim acquired or refreshed by same owner.
    """
    ttl = ttl_seconds or Config.EXIT_CLAIM_TTL_SECONDS
    path = _claim_path(trade_id)
    now = time.time()

    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            existing_owner = str(data.get("owner", ""))
            ts = float(data.get("timestamp", 0))
            if (now - ts) < ttl and existing_owner and existing_owner != owner:
                return False
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    payload = {"owner": owner, "timestamp": now, "trade_id": trade_id}
    try:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        return True
    except OSError:
        return False


def release_exit(trade_id: str, owner: Optional[str] = None) -> None:
    path = _claim_path(trade_id)
    if not os.path.isfile(path):
        return
    if owner:
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            if str(data.get("owner", "")) != owner:
                return
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return
    try:
        os.remove(path)
    except OSError:
        pass


def exit_claim_active(trade_id: str, *, ttl_seconds: Optional[int] = None) -> bool:
    """True when another process holds a non-expired exit claim."""
    ttl = ttl_seconds or Config.EXIT_CLAIM_TTL_SECONDS
    path = _claim_path(trade_id)
    if not os.path.isfile(path):
        return False
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        ts = float(data.get("timestamp", 0))
        return (time.time() - ts) < ttl
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return False
