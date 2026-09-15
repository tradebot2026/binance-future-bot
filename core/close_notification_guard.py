"""
Prevents duplicate Telegram close alerts for the same trade (main + reconciliation).
Cross-process safe via atomic claim files under data/close_notify_claims/.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

from config import Config


def _claims_dir() -> str:
    path = os.path.join(Config.DATA_DIR, "close_notify_claims")
    os.makedirs(path, exist_ok=True)
    return path


def _claim_path(trade_id: str) -> str:
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in trade_id)
    return os.path.join(_claims_dir(), f"{safe_id}.json")


def try_claim_close_notification(
    trade_id: str,
    *,
    ttl_seconds: int = 86400,
) -> bool:
    """
    Return True if this process may send the close Telegram alert for trade_id.
    Returns False if another handler already claimed it within ttl.
    """
    trade_id = str(trade_id or "").strip()
    if not trade_id:
        return True

    path = _claim_path(trade_id)
    now = time.time()
    payload = {"trade_id": trade_id, "timestamp": now}

    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            ts = float(data.get("timestamp", 0))
            if (now - ts) < ttl_seconds:
                return False
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        fd = os.open(path, flags)
        try:
            os.write(fd, json.dumps(payload).encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        return False
    except OSError:
        return True
