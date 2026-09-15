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


def _read_claim(path: str) -> Optional[dict]:
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None


def claim_exit(trade_id: str, owner: str, *, ttl_seconds: Optional[int] = None) -> bool:
    """
    Try to acquire exclusive exit rights for a trade (cross-process mutex).
    Returns True if claim acquired or refreshed by same owner.
    """
    ttl = ttl_seconds or Config.EXIT_CLAIM_TTL_SECONDS
    path = _claim_path(trade_id)
    now = time.time()
    payload = {"owner": owner, "timestamp": now, "trade_id": trade_id}
    encoded = json.dumps(payload)

    if os.path.isfile(path):
        data = _read_claim(path)
        if data:
            existing_owner = str(data.get("owner", ""))
            ts = float(data.get("timestamp", 0))
            if (now - ts) < ttl and existing_owner and existing_owner != owner:
                return False

    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        fd = os.open(path, flags)
        try:
            os.write(fd, encoded.encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        data = _read_claim(path)
        if not data:
            try:
                os.remove(path)
            except OSError:
                pass
            return claim_exit(trade_id, owner, ttl_seconds=ttl)
        existing_owner = str(data.get("owner", ""))
        ts = float(data.get("timestamp", 0))
        if existing_owner == owner or (now - ts) >= ttl:
            try:
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(encoded)
                return True
            except OSError:
                return False
        return False
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


def get_exit_claim_owner(trade_id: str, *, ttl_seconds: Optional[int] = None) -> Optional[str]:
    """Return the owner of a non-expired exit claim, or None."""
    ttl = ttl_seconds or Config.EXIT_CLAIM_TTL_SECONDS
    path = _claim_path(trade_id)
    if not os.path.isfile(path):
        return None
    data = _read_claim(path)
    if not data:
        return None
    ts = float(data.get("timestamp", 0))
    if (time.time() - ts) >= ttl:
        return None
    owner = str(data.get("owner", "")).strip()
    return owner or None


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
