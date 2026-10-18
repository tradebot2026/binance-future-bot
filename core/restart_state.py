"""Persist crash auto-restart exhaustion so health cannot look OK after the cap."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from config import Config
from logger import error_logger, system_logger


def restart_state_path() -> str:
    os.makedirs(Config.DATA_DIR, exist_ok=True)
    return os.path.join(Config.DATA_DIR, "crash_restart_state.json")


def read_restart_state() -> dict[str, Any]:
    path = restart_state_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def write_restart_state(**fields: Any) -> None:
    path = restart_state_path()
    payload = read_restart_state()
    payload.update(fields)
    payload["updated_at"] = time.time()
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, path)
    except OSError as exc:
        error_logger.error("Could not write crash restart state: %s", exc)


def mark_crash_restart(count: int, limit: int) -> None:
    write_restart_state(
        crash_count=int(count),
        crash_limit=int(limit),
        exhausted=False,
    )


def mark_restart_exhausted(count: int, limit: int) -> None:
    write_restart_state(
        crash_count=int(count),
        crash_limit=int(limit),
        exhausted=True,
        exhausted_at=time.time(),
    )
    error_logger.critical(
        "CRASH_RESTART_EXHAUSTED count=%s limit=%s — auto-restart stopped. "
        "Virtual SL/TP is down until an operator starts main.py again. "
        "Start watchdog.py first. Do not treat the account as flat.",
        count,
        limit,
    )


def acknowledge_manual_start() -> dict[str, Any]:
    """Human started the process. Keep history but clear the exhausted latch."""
    prior = read_restart_state()
    if prior.get("exhausted"):
        system_logger.critical(
            "Previous crash auto-restart cap was exhausted (count=%s limit=%s). "
            "Manual start acknowledged — auto-restart budget is reset for this process.",
            prior.get("crash_count"),
            prior.get("crash_limit"),
        )
    write_restart_state(
        crash_count=0,
        exhausted=False,
        manual_start_at=time.time(),
        previous_exhausted=bool(prior.get("exhausted")),
    )
    return prior
