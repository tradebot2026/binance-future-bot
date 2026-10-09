"""
Global REST rate-limit guard for Binance Futures.
Token-bucket weight budgeting with hard-stop on 429 / -1003.
Tracks HTTP request count and X-MBX-USED-WEIGHT-1M headers.
"""

from __future__ import annotations

import enum
import threading
import time
from collections import deque
from typing import Any, Optional

from config import Config
from logger import system_logger


# Approximate Binance USD-M Futures endpoint weights (request weight units).
ENDPOINT_WEIGHTS: dict[str, int] = {
    "futures_ping": 1,
    "futures_exchange_info": 1,
    "futures_ticker": 1,
    "futures_orderbook_ticker": 2,
    "futures_klines": 5,
    "futures_account": 5,
    "futures_position_information": 5,
    "futures_get_order": 1,
    "futures_create_order": 1,
    "futures_change_leverage": 1,
    "futures_leverage_bracket": 1,
    "futures_symbol_ticker": 1,
    "futures_get_position_mode": 1,
    "futures_change_position_mode": 1,
    "futures_mark_price": 1,
    "futures_open_interest": 1,
    "futures_open_interest_hist": 1,
    "futures_account_balance": 5,
    "futures_income_history": 30,
    "futures_account_trades": 5,
}


def weight_for_call(func: Any, default: int = 1, **kwargs: Any) -> int:
    name = getattr(func, "__name__", "") or ""
    if name == "futures_ticker" and not kwargs.get("symbol"):
        return 40
    if name == "futures_klines":
        try:
            limit = int(kwargs.get("limit") or 500)
        except (TypeError, ValueError):
            limit = 500
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        if limit <= 1000:
            return 5
        return 10
    return ENDPOINT_WEIGHTS.get(name, default)


class ApiHealthState(str, enum.Enum):
    HEALTHY = "HEALTHY"
    HIGH_USAGE = "HIGH_USAGE"
    RATE_LIMIT_WARNING = "RATE_LIMIT_WARNING"
    API_RATE_LIMITED = "API_RATE_LIMITED"
    IP_BANNED = "IP_BANNED"


class RestComponent(str, enum.Enum):
    """Per-IP request-weight lanes (sums ≤ 1500 hard cap)."""

    ORDER = "order"
    HOT = "hot"
    NORMAL = "normal"


class ComponentRestPacer:
    """Strict inter-request gaps per REST lane (shared across exchange + bootstrap)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last: dict[str, float] = {}

    def reset(self) -> None:
        with self._lock:
            self._last.clear()

    def last_at(self, component: RestComponent) -> float:
        with self._lock:
            return float(self._last.get(component.value, 0.0) or 0.0)

    def wait(self, component: RestComponent) -> float:
        gap = rest_pace_seconds(component)
        with self._lock:
            now = time.monotonic()
            last = float(self._last.get(component.value, 0.0) or 0.0)
            earliest = (last + gap) if last > 0 else now
            start = max(now, earliest)
            pause = max(0.0, start - now)
            self._last[component.value] = start
        if pause > 0:
            time.sleep(pause)
        return pause


_LANE_PACER = ComponentRestPacer()


def rest_pace_seconds(component: RestComponent) -> float:
    if component == RestComponent.ORDER:
        return Config.rest_pace_order_seconds()
    if component == RestComponent.HOT:
        return Config.rest_pace_hot_seconds()
    return Config.rest_pace_normal_seconds()


def wait_rest_lane(component: RestComponent) -> float:
    """Block until this lane's interval has elapsed. Returns seconds slept."""
    return _LANE_PACER.wait(component)


def reset_rest_lane_pacer() -> None:
    _LANE_PACER.reset()


def _weight_thresholds() -> tuple[int, int, int]:
    """Return (normal_throttle_at, hard_at, exchange_limit) for used-weight."""
    limit = Config.rest_used_weight_limit()
    throttle = Config.rest_weight_throttle_threshold()
    hard = Config.rest_hard_weight_cap()
    return throttle, hard, limit


def _component_budget(component: RestComponent) -> int:
    if component == RestComponent.ORDER:
        return Config.rest_budget_order_weight()
    if component == RestComponent.HOT:
        return Config.rest_budget_hot_weight()
    return Config.rest_budget_normal_weight()


def _weight_pause_seconds(used_weight: int) -> float:
    """Pause Normal-scanner REST after a used-weight header; Hot/orders stay up until hard cap."""
    throttle, hard, limit = _weight_thresholds()
    if used_weight >= limit or used_weight >= hard:
        return max(float(Config.REST_WEIGHT_OVER_LIMIT_PAUSE_SECONDS), 45.0)
    if used_weight >= throttle:
        return max(float(Config.REST_WEIGHT_THROTTLE_SECONDS), 5.0)
    return 0.0


def kline_rest_delay_seconds(used_weight: int = 0) -> float:
    """Hot-lane gap between kline REST calls (~20/min); stretch only near 1500."""
    configured = float(getattr(Config, "KLINE_REST_MIN_INTERVAL_SECONDS", 3.0))
    base = max(configured, Config.rest_pace_hot_seconds())
    weight = int(used_weight or 0)
    hard = Config.rest_hard_weight_cap()
    if weight >= hard:
        return min(base + 1.0, 5.0)
    if weight >= Config.rest_weight_throttle_threshold():
        return min(base + 0.5, 4.0)
    return base


def maybe_pause_warmup_rest(used_weight: int) -> bool:
    """Pause 5–10s when warmup used-weight exceeds 500. Returns True if paused."""
    threshold = Config.warmup_rest_weight_pause()
    if int(used_weight or 0) <= threshold:
        return False
    pause = Config.warmup_rest_pause_seconds()
    system_logger.warning(
        "Warmup REST weight guard — used_weight=%s exceeds %s; pausing %.1fs.",
        int(used_weight),
        threshold,
        pause,
    )
    time.sleep(pause)
    return True


class RestUsageTracker:
    """
    Sliding 60s HTTP request counter + last used-weight header.
    Drives API SAFETY MODE and the used-weight REST governor.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._window: deque[float] = deque()
        self._used_weight_1m: int = 0
        self._used_weight_updated_at: float = 0.0
        self._weight_throttle_until: float = 0.0
        self._last_http_status: int = 0
        self._last_error_code: int = 0
        self._safety_until: float = 0.0
        self._state: ApiHealthState = ApiHealthState.HEALTHY
        self._reason: str = ""
        self._retry_after_seconds: float = 0.0
        self._logged_state: ApiHealthState = ApiHealthState.HEALTHY
        self._local_weight_window: deque[tuple[float, int, str]] = deque()

    def requests_last_minute(self) -> int:
        self._purge()
        with self._lock:
            return len(self._window)

    def snapshot(self) -> dict[str, Any]:
        self._purge()
        with self._lock:
            self._decay_used_weight_locked()
            self._purge_local_weight_locked()
            remaining = max(self._safety_until - time.monotonic(), 0.0)
            state = self._effective_state_locked(remaining)
            throttle_remaining = max(self._weight_throttle_until - time.monotonic(), 0.0)
            return {
                "state": state.value,
                "reason": self._reason,
                "requests_1m": len(self._window),
                "used_weight_1m": max(
                    int(self._used_weight_1m), self._local_weight_sum_locked()
                ),
                "header_used_weight_1m": int(self._used_weight_1m),
                "ip_limit": Config.rest_ip_request_limit(),
                "weight_limit": Config.rest_used_weight_limit(),
                "operational_weight_cap": Config.rest_hard_weight_cap(),
                "hard_weight_cap": Config.rest_hard_weight_cap(),
                "priority_throttle_total": Config.rest_weight_throttle_threshold(),
                "weight_order_1m": self._component_sum_locked(RestComponent.ORDER),
                "weight_hot_1m": self._component_sum_locked(RestComponent.HOT),
                "weight_normal_1m": self._component_sum_locked(RestComponent.NORMAL),
                "weight_throttle_remaining_seconds": throttle_remaining,
                "safety_remaining_seconds": remaining,
                "last_http_status": self._last_http_status,
                "last_error_code": self._last_error_code,
                "retry_after_seconds": self._retry_after_seconds,
            }

    def allows_background_rest(self) -> bool:
        """Normal-scanner REST — frozen first at the 1200 priority line."""
        return self.allows_component_rest(RestComponent.NORMAL)

    def allows_hot_rest(self) -> bool:
        """Hot-tier / kline bootstrap REST — stays up until the 1500 hard cap."""
        return self.allows_component_rest(RestComponent.HOT)

    def allows_component_rest(self, component: RestComponent, weight: int = 1) -> bool:
        snap = self.snapshot()
        state = str(snap.get("state") or "")
        if state in {
            ApiHealthState.API_RATE_LIMITED.value,
            ApiHealthState.IP_BANNED.value,
        }:
            return False
        used = int(snap.get("used_weight_1m") or 0)
        hard = Config.rest_hard_weight_cap()
        if used >= hard or used + max(int(weight), 1) > hard:
            return False
        if component == RestComponent.NORMAL:
            if used >= Config.rest_weight_throttle_threshold():
                return False
            if state == ApiHealthState.RATE_LIMIT_WARNING.value:
                return False
        budget = _component_budget(component)
        key = {
            RestComponent.ORDER: "weight_order_1m",
            RestComponent.HOT: "weight_hot_1m",
            RestComponent.NORMAL: "weight_normal_1m",
        }[component]
        if int(snap.get(key) or 0) + max(int(weight), 1) > budget:
            return False
        return True

    def projected_used_weight(self) -> int:
        """Max of Binance header and locally reserved weight in the last 60s."""
        self._purge()
        with self._lock:
            self._decay_used_weight_locked()
            self._purge_local_weight_locked()
            return max(int(self._used_weight_1m), self._local_weight_sum_locked())

    def try_reserve_background(self, weight: int) -> bool:
        """Reserve Normal-scanner weight, or skip the call."""
        return self.try_reserve(RestComponent.NORMAL, weight)

    def try_reserve_hot(self, weight: int) -> bool:
        """Reserve Hot/kline weight, or skip the call."""
        return self.try_reserve(RestComponent.HOT, weight)

    def try_reserve(self, component: RestComponent, weight: int) -> bool:
        """Atomically reserve component weight under the 1500 hard cap."""
        weight = max(int(weight), 1)
        hard = Config.rest_hard_weight_cap()
        throttle = Config.rest_weight_throttle_threshold()
        budget = _component_budget(component)
        with self._lock:
            self._decay_used_weight_locked()
            self._purge_local_weight_locked()
            projected = max(int(self._used_weight_1m), self._local_weight_sum_locked())
            if projected >= hard or projected + weight > hard:
                return False
            if component == RestComponent.NORMAL and (
                projected >= throttle or projected + weight > throttle
            ):
                return False
            used = self._component_sum_locked(component)
            if used + weight > budget:
                return False
            self._local_weight_window.append(
                (time.monotonic(), weight, component.value)
            )
            return True

    def note_outgoing_weight(self, weight: int) -> None:
        """Record sent REST weight on the order lane (does not block)."""
        weight = max(int(weight), 1)
        with self._lock:
            self._purge_local_weight_locked()
            self._local_weight_window.append(
                (time.monotonic(), weight, RestComponent.ORDER.value)
            )

    def allows_new_entries(self) -> bool:
        """Orders proceed unless the IP is halted or the 1500 hard cap is hit."""
        snap = self.snapshot()
        if snap["state"] in {
            ApiHealthState.API_RATE_LIMITED.value,
            ApiHealthState.IP_BANNED.value,
        }:
            return False
        return int(snap.get("used_weight_1m") or 0) < Config.rest_hard_weight_cap()

    def in_safety_mode(self) -> bool:
        snap = self.snapshot()
        return snap["state"] in {
            ApiHealthState.API_RATE_LIMITED.value,
            ApiHealthState.IP_BANNED.value,
        }

    def safety_remaining(self) -> float:
        with self._lock:
            return max(self._safety_until - time.monotonic(), 0.0)

    def force_clear_safety(self, reason: str = "ban timestamp expired") -> bool:
        """Strictly clear IP_BANNED / API_RATE_LIMITED so REST can resume."""
        with self._lock:
            was_halted = self._state in {
                ApiHealthState.API_RATE_LIMITED,
                ApiHealthState.IP_BANNED,
            } or self._safety_until > 0
            if not was_halted:
                return False
            self._safety_until = 0.0
            self._retry_after_seconds = 0.0
            self._last_error_code = 0
            self._state = ApiHealthState.HEALTHY
            self._reason = (reason or "ban window expired")[:200]
            self._log_state_locked(len(self._window), Config.rest_ip_request_limit())
            return True

    def weight_throttle_remaining(self) -> float:
        with self._lock:
            return max(self._weight_throttle_until - time.monotonic(), 0.0)

    def used_weight_1m(self) -> int:
        self._purge()
        with self._lock:
            self._decay_used_weight_locked()
            return int(self._used_weight_1m)

    def note_http_response(self, response: Any) -> None:
        """Record one HTTP round-trip (python-binance session response)."""
        now = time.monotonic()
        status = 0
        used_weight = 0
        retry_after = 0.0
        try:
            status = int(getattr(response, "status_code", 0) or 0)
        except (TypeError, ValueError):
            status = 0
        headers = getattr(response, "headers", None) or {}
        used_weight = _header_int(
            headers,
            "X-MBX-USED-WEIGHT-1M",
            "x-mbx-used-weight-1m",
        )
        retry_after = _header_float(headers, "Retry-After", "retry-after")
        with self._lock:
            self._window.append(now)
            self._last_http_status = status
            if used_weight > 0:
                self._used_weight_1m = used_weight
                self._used_weight_updated_at = now
                pause = _weight_pause_seconds(used_weight)
                if pause > 0:
                    self._weight_throttle_until = max(
                        self._weight_throttle_until, now + pause
                    )
                    throttle, hard, limit = _weight_thresholds()
                    if used_weight >= limit:
                        self._reason = (
                            f"used_weight_1m={used_weight} at/over limit {limit} "
                            f"— REST paused {int(pause)}s"
                        )
                    elif used_weight >= hard:
                        self._reason = (
                            f"used_weight_1m={used_weight} near weight cap "
                            f"(hard {hard}) — REST paused {int(pause)}s"
                        )
                    else:
                        self._reason = (
                            f"used_weight_1m={used_weight} over throttle "
                            f"{throttle} — REST paused {int(pause)}s"
                        )
            if retry_after > 0:
                self._retry_after_seconds = retry_after
            if status in (418, 429):
                halt = max(
                    retry_after,
                    float(Config.rate_limit_scanner_halt_seconds()),
                    900.0,
                )
                if status == 418:
                    halt = max(halt, float(Config.IP_BAN_HALT_SECONDS), 900.0)
                    self._state = ApiHealthState.IP_BANNED
                    self._reason = f"HTTP {status} — IP banned / WAF"
                else:
                    self._state = ApiHealthState.API_RATE_LIMITED
                    self._reason = f"HTTP {status} — too many requests"
                self._safety_until = max(self._safety_until, now + halt)

    def note_transport_error(self, exc: BaseException) -> None:
        with self._lock:
            self._reason = f"transport error: {exc}"[:160]

    def note_binance_error(
        self,
        *,
        code: int,
        message: str,
        halt_seconds: float,
        banned: bool = False,
    ) -> None:
        now = time.monotonic()
        halt = max(float(halt_seconds), 0.0)
        with self._lock:
            self._last_error_code = int(code)
            self._reason = (message or f"Binance code {code}")[:200]
            if halt > 0:
                self._safety_until = max(self._safety_until, now + halt)
                self._retry_after_seconds = halt
            if banned or code == 418:
                self._state = ApiHealthState.IP_BANNED
            elif code in (-1003, -1015, 429):
                self._state = ApiHealthState.API_RATE_LIMITED

    def _purge(self) -> None:
        cutoff = time.monotonic() - 60.0
        with self._lock:
            while self._window and self._window[0] < cutoff:
                self._window.popleft()
            self._decay_used_weight_locked()
            self._purge_local_weight_locked()

    def _decay_used_weight_locked(self) -> None:
        """Drop stale used-weight headers after Binance's rolling 60s window."""
        if self._used_weight_updated_at <= 0:
            return
        if time.monotonic() - self._used_weight_updated_at >= 60.0:
            self._used_weight_1m = 0
            self._used_weight_updated_at = 0.0

    def _purge_local_weight_locked(self) -> None:
        cutoff = time.monotonic() - 60.0
        while self._local_weight_window and self._local_weight_window[0][0] < cutoff:
            self._local_weight_window.popleft()

    def _local_weight_sum_locked(self) -> int:
        return int(sum(item[1] for item in self._local_weight_window))

    def _component_sum_locked(self, component: RestComponent) -> int:
        name = component.value
        total = 0
        for item in self._local_weight_window:
            if len(item) >= 3 and item[2] == name:
                total += int(item[1])
        return total

    def _effective_state_locked(self, remaining: float) -> ApiHealthState:
        self._decay_used_weight_locked()
        if remaining > 0 and self._state in {
            ApiHealthState.API_RATE_LIMITED,
            ApiHealthState.IP_BANNED,
        }:
            self._log_state_locked(len(self._window), Config.rest_ip_request_limit())
            return self._state
        ip_limit = max(Config.rest_ip_request_limit(), 1)
        count = len(self._window)
        throttle_at, hard_at, weight_limit = _weight_thresholds()
        self._purge_local_weight_locked()
        used_weight = max(int(self._used_weight_1m), self._local_weight_sum_locked())
        throttle_remaining = max(self._weight_throttle_until - time.monotonic(), 0.0)

        if used_weight >= hard_at or used_weight >= weight_limit:
            self._state = ApiHealthState.RATE_LIMIT_WARNING
            pause_note = (
                f" (pause {int(throttle_remaining)}s)"
                if throttle_remaining > 0
                else " — REST frozen until weight decays"
            )
            self._reason = (
                f"used_weight_1m={used_weight} at/over hard cap {hard_at}"
                f"{pause_note}"
            )
            self._log_state_locked(count, ip_limit)
            return self._state
        if used_weight >= throttle_at:
            self._state = ApiHealthState.HIGH_USAGE
            self._reason = (
                f"used_weight_1m={used_weight} — Normal scanner throttled "
                f"(>{throttle_at}); Hot/orders stay up until {hard_at}"
            )
            self._log_state_locked(count, ip_limit)
            return self._state
        if count >= int(ip_limit * 0.85):
            self._state = ApiHealthState.RATE_LIMIT_WARNING
            self._reason = f"{count} HTTP requests in 60s (limit {ip_limit})"
            self._log_state_locked(count, ip_limit)
            return self._state
        if count >= int(ip_limit * 0.55):
            self._state = ApiHealthState.HIGH_USAGE
            self._reason = f"{count} HTTP requests in 60s (limit {ip_limit})"
            self._log_state_locked(count, ip_limit)
            return self._state
        self._state = ApiHealthState.HEALTHY
        if remaining <= 0 and throttle_remaining <= 0:
            self._reason = ""
        self._log_state_locked(count, ip_limit)
        return self._state

    def _log_state_locked(self, count: int, ip_limit: int) -> None:
        if self._state == self._logged_state:
            return
        self._logged_state = self._state
        system_logger.info(
            "[API_HEALTH] %s | requests_1m=%s used_weight_1m=%s ip_limit=%s | %s",
            self._state.value,
            count,
            self._used_weight_1m,
            ip_limit,
            self._reason or "ok",
        )


def _header_int(headers: Any, *keys: str) -> int:
    for key in keys:
        try:
            raw = headers.get(key)
        except Exception:
            raw = None
        if raw is None:
            continue
        try:
            return int(float(str(raw)))
        except (TypeError, ValueError):
            continue
    return 0


def _header_float(headers: Any, *keys: str) -> float:
    for key in keys:
        try:
            raw = headers.get(key)
        except Exception:
            raw = None
        if raw is None:
            continue
        try:
            return float(str(raw))
        except (TypeError, ValueError):
            continue
    return 0.0


class RestTokenBucket:
    """
    Thread-safe token bucket for Binance REST request-weight budgeting.
    Triggers a hard stop (zero tokens + sleep gate) on rate-limit violations.
    """

    def __init__(self, capacity: float, refill_per_second: float) -> None:
        self._capacity = max(capacity, 1.0)
        self._tokens = self._capacity
        self._refill_rate = max(refill_per_second, 0.01)
        self._last_refill = time.monotonic()
        self._hard_stop_until = 0.0
        self._lock = threading.Lock()

    def trigger_hard_stop(self, seconds: float) -> None:
        """Drain bucket and block all REST until cooldown elapses."""
        pause = max(seconds, 0.0)
        with self._lock:
            self._hard_stop_until = max(
                self._hard_stop_until, time.monotonic() + pause
            )
            self._tokens = 0.0

    def hard_stop_remaining(self) -> float:
        with self._lock:
            return max(self._hard_stop_until - time.monotonic(), 0.0)

    def is_hard_stopped(self) -> bool:
        return self.hard_stop_remaining() > 0.0

    def clear_hard_stop(self) -> None:
        """Release a leftover REST hard-stop after the ban window expires."""
        with self._lock:
            self._hard_stop_until = 0.0
            self._tokens = self._capacity
            self._last_refill = time.monotonic()

    def acquire(self, weight: int = 1) -> None:
        """Block until `weight` tokens are available (never spin-retry on ban)."""
        weight = max(weight, 1)
        while True:
            with self._lock:
                now = time.monotonic()
                stop_remaining = self._hard_stop_until - now
                if stop_remaining > 0:
                    wait = stop_remaining
                else:
                    elapsed = now - self._last_refill
                    if elapsed > 0:
                        self._tokens = min(
                            self._capacity,
                            self._tokens + elapsed * self._refill_rate,
                        )
                        self._last_refill = now
                    if self._tokens >= weight:
                        self._tokens -= weight
                        return
                    deficit = weight - self._tokens
                    wait = deficit / self._refill_rate
            time.sleep(min(max(wait, 0.05), 30.0))


class RestBlockLogSuppressor:
    """Emit repeated REST-block warnings at most once per interval."""

    def __init__(self, interval_seconds: int) -> None:
        self._interval = max(interval_seconds, 30)
        self._last_logged_at = 0.0
        self._last_reason = ""
        self._lock = threading.Lock()

    def should_log(self, reason: str) -> bool:
        now = time.monotonic()
        with self._lock:
            if (
                now - self._last_logged_at < self._interval
                and reason == self._last_reason
            ):
                return False
            self._last_logged_at = now
            self._last_reason = reason
            return True

    def reset(self) -> None:
        with self._lock:
            self._last_logged_at = 0.0
            self._last_reason = ""


def build_default_token_bucket() -> RestTokenBucket:
    per_minute = max(Config.rest_hard_weight_cap(), 1)
    return RestTokenBucket(
        capacity=float(per_minute),
        refill_per_second=per_minute / 60.0,
    )
