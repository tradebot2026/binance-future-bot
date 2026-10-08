"""Paced one-time kline bootstrap — WS-first with batched REST fallback."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from config import Config
from exceptions import ExchangeRateLimitError
from logger import error_logger, system_logger
from rest_rate_guard import kline_rest_delay_seconds, maybe_pause_warmup_rest


class KlineBootstrapAborted(Exception):
    """Raised internally when REST kline bootstrap must stop (ban / budget)."""


_OHLCV_COLS = ("timestamp", "open", "high", "low", "close", "volume")


def backtest_history_buffer_limit() -> int:
    """Dedicated WS history size — at least backtest min_bars, not the scan buffer."""
    try:
        limit_fn = getattr(Config, "backtest_candle_limit", None)
        limit = int(limit_fn()) if callable(limit_fn) else int(
            getattr(Config, "BACKTEST_CANDLE_LIMIT", 500)
        )
    except Exception:
        limit = 500
    floor = int(getattr(Config, "BACKTEST_MIN_BARS", 450))
    return max(limit, floor, 200)


def _normalize_closed_bar(row: dict) -> Optional[dict]:
    if not row:
        return None
    if row.get("closed") is False:
        return None
    open_ms = int(row.get("open_ms") or 0)
    ts = row.get("timestamp")
    if open_ms <= 0 and ts is not None:
        open_ms = int(pd.Timestamp(ts).timestamp() * 1000)
    if open_ms <= 0:
        return None
    return {
        "timestamp": ts if ts is not None else pd.to_datetime(open_ms, unit="ms"),
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "volume": row.get("volume"),
        "open_ms": open_ms,
        "closed": True,
    }


def merge_closed_kline_bars(
    bars: Optional[deque],
    rows: list[dict],
    *,
    maxlen: int | None = None,
) -> deque:
    """Rebuild a time-ordered closed-bar deque, newest-capped at maxlen."""
    limit = maxlen if maxlen and maxlen > 0 else None
    if limit is None and bars is not None and getattr(bars, "maxlen", None):
        limit = int(bars.maxlen)
    by_ms: dict[int, dict] = {}
    for existing in list(bars or []):
        item = _normalize_closed_bar(existing)
        if item is not None:
            by_ms[item["open_ms"]] = item
    for row in rows:
        item = _normalize_closed_bar(row)
        if item is not None:
            by_ms[item["open_ms"]] = item
    return deque((by_ms[key] for key in sorted(by_ms)), maxlen=limit)


def merge_closed_kline_bar(bars: deque, row: dict) -> bool:
    """Upsert one closed WS/REST bar. Returns True when history was written."""
    item = _normalize_closed_bar(row)
    if item is None:
        return False
    open_ms = item["open_ms"]
    if not bars:
        bars.append(item)
        return True
    last_ms = int(bars[-1].get("open_ms") or 0)
    if last_ms == open_ms:
        bars[-1] = item
        return True
    if last_ms < open_ms:
        bars.append(item)
        return True
    merged = merge_closed_kline_bars(bars, [item], maxlen=bars.maxlen)
    bars.clear()
    bars.extend(merged)
    return True


def bars_to_ohlcv_dataframe(
    bars: Optional[deque], limit: int | None = None
) -> pd.DataFrame:
    """Convert a closed-bar deque to the OHLCV frame used by 15m backtests."""
    if not bars:
        return pd.DataFrame()
    rows = list(bars)
    if limit is not None and int(limit) > 0:
        rows = rows[-int(limit) :]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    cols = [col for col in _OHLCV_COLS if col in frame.columns]
    if not cols:
        return pd.DataFrame()
    return frame[cols].copy()


@dataclass
class BootstrapResult:
    seeded: int = 0
    failed: int = 0
    timed_out: int = 0
    elapsed_seconds: float = 0.0
    pending: int = 0
    aborted: bool = False


async def _fetch_pair(
    semaphore: asyncio.Semaphore,
    sym: str,
    interval: str,
    limit: int,
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    request_timeout: float,
    inter_request_delay: float,
) -> tuple[str, str, Optional[pd.DataFrame], Optional[str]]:
    async with semaphore:
        try:
            df = await asyncio.wait_for(
                asyncio.to_thread(rest_fetcher, sym, interval, limit),
                timeout=request_timeout,
            )
            if df is None or df.empty:
                return sym, interval, None, "empty"
            return sym, interval, df, None
        except asyncio.TimeoutError:
            return sym, interval, None, "timeout"
        except ExchangeRateLimitError as exc:
            return sym, interval, None, f"rate_limit:{exc}"
        except Exception as exc:
            return sym, interval, None, str(exc)
        finally:
            if inter_request_delay > 0:
                await asyncio.sleep(inter_request_delay)


async def _run_parallel_fetch(
    pairs: list[tuple[str, str]],
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    limit: int,
    *,
    concurrency: int,
    request_timeout: float,
    overall_timeout: float,
    inter_request_delay: float,
) -> list[tuple[str, str, Optional[pd.DataFrame], Optional[str]]]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))
    task_map: dict[asyncio.Task, tuple[str, str]] = {}
    for sym, interval in pairs:
        task = asyncio.create_task(
            _fetch_pair(
                semaphore,
                sym,
                interval,
                limit,
                rest_fetcher,
                request_timeout,
                inter_request_delay,
            )
        )
        task_map[task] = (sym, interval)

    if not task_map:
        return []

    done, pending = await asyncio.wait(task_map.keys(), timeout=overall_timeout)
    results: list[tuple[str, str, Optional[pd.DataFrame], Optional[str]]] = []
    for task in done:
        try:
            results.append(task.result())
        except Exception as exc:
            sym, interval = task_map[task]
            results.append((sym, interval, None, str(exc)))

    for task in pending:
        task.cancel()
        sym, interval = task_map[task]
        results.append((sym, interval, None, "overall_timeout"))

    return results


def run_parallel_kline_bootstrap(
    pairs: list[tuple[str, str]],
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    limit: int,
    min_bars: int,
    seed_fn: Callable[[str, str, pd.DataFrame], None],
    mark_bootstrapped: Callable[[str, str], None],
) -> BootstrapResult:
    """
    Fetch historical klines concurrently and seed the WS cache.
    Never blocks indefinitely — per-request and overall timeouts apply.
    """
    if not pairs:
        return BootstrapResult()

    concurrency = Config.ws_kline_bootstrap_concurrency()
    request_timeout = Config.WS_KLINE_BOOTSTRAP_REQUEST_TIMEOUT_SECONDS
    overall_timeout = Config.WS_KLINE_BOOTSTRAP_OVERALL_TIMEOUT_SECONDS
    inter_request_delay = kline_rest_delay_seconds(0)
    started = time.monotonic()

    system_logger.info(
        "One-time kline bootstrap starting — %s series "
        "(limit=%s bars, concurrency=%s, timeout=%ss).",
        len(pairs),
        limit,
        concurrency,
        request_timeout,
    )

    import concurrent.futures

    async def _fetch_all() -> list[tuple[str, str, Optional[pd.DataFrame], Optional[str]]]:
        return await _run_parallel_fetch(
            pairs,
            rest_fetcher,
            limit,
            concurrency=concurrency,
            request_timeout=request_timeout,
            overall_timeout=overall_timeout,
            inter_request_delay=inter_request_delay,
        )

    # Keep asyncio off the main thread so python-binance WS never shares its loop.
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, _fetch_all())
        raw_results = future.result(timeout=overall_timeout + 10)

    seeded = 0
    failed = 0
    timed_out = 0

    for sym, interval, df, err in raw_results:
        if err == "overall_timeout":
            failed += 1
            continue
        if err and str(err).startswith("rate_limit:"):
            failed += 1
            system_logger.warning(
                "Kline bootstrap halted on rate limit for %s %s", sym, interval
            )
            break
        if err == "timeout":
            timed_out += 1
            failed += 1
            continue
        if err or df is None or df.empty or len(df) < min_bars:
            failed += 1
            if err and err not in ("empty", "timeout"):
                error_logger.debug("Kline bootstrap skip %s %s: %s", sym, interval, err)
            continue
        try:
            seed_fn(sym, interval, df)
            mark_bootstrapped(sym, interval)
            seeded += 1
        except Exception as exc:
            failed += 1
            error_logger.warning(
                "Kline bootstrap seed failed for %s %s: %s", sym, interval, exc
            )

    elapsed = time.monotonic() - started
    system_logger.info(
        "Kline bootstrap finished in %.1fs — seeded=%s failed=%s "
        "(timeouts=%s) of %s series.",
        elapsed,
        seeded,
        failed,
        timed_out,
        len(pairs),
    )
    return BootstrapResult(
        seeded=seeded,
        failed=failed,
        timed_out=timed_out,
        elapsed_seconds=elapsed,
        pending=len(pairs),
    )


def run_batched_kline_bootstrap(
    pairs: list[tuple[str, str]],
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    limit: int,
    min_bars: int,
    seed_fn: Callable[[str, str, pd.DataFrame], None],
    mark_bootstrapped: Callable[[str, str], None],
    *,
    can_fetch: Callable[[], bool] | None = None,
    max_symbols_per_batch: int | None = None,
    batch_cooldown_seconds: float | None = None,
    request_delay_seconds: float | None = None,
    max_pairs: int | None = None,
    warmup_mode: bool = False,
    used_weight_fn: Callable[[], int] | None = None,
    warmup_seed_fn: Callable[[str, str], None] | None = None,
    complete_min_bars: int | None = None,
) -> BootstrapResult:
    """
    REST bootstrap in symbol batches — sequential TFs with delay between every request.
    Aborts cleanly when can_fetch() returns False (ban / budget circuit breaker).
    Warmup mode uses 0.5–1.0s pacing and pauses 5–10s when used-weight exceeds 500.
    """
    if not pairs:
        return BootstrapResult()

    batch_size = max(max_symbols_per_batch or Config.KLINE_BOOTSTRAP_BATCH_SYMBOLS, 1)
    batch_size = min(batch_size, 2)
    if batch_cooldown_seconds is None:
        batch_pause = max(float(Config.KLINE_BOOTSTRAP_BATCH_COOLDOWN_SECONDS), 0.0)
    else:
        batch_pause = max(float(batch_cooldown_seconds), 0.0)
    if warmup_mode:
        delay = (
            Config.warmup_kline_delay_seconds()
            if request_delay_seconds is None
            else min(max(float(request_delay_seconds), 0.5), 1.0)
        )
    else:
        delay = (
            kline_rest_delay_seconds(0)
            if request_delay_seconds is None
            else max(float(request_delay_seconds), 1.0)
        )
    done_bars = max(
        int(complete_min_bars or min_bars),
        min_bars,
    )

    by_symbol: dict[str, list[str]] = defaultdict(list)
    for sym, interval in pairs:
        key = str(sym or "").upper()
        if not key:
            continue
        by_symbol[key].append(interval)

    # One pass per symbol this call — never re-queue the same coin inside a batch.
    symbol_order = list(dict.fromkeys(by_symbol.keys()))
    if max_pairs is not None:
        trimmed: dict[str, list[str]] = defaultdict(list)
        count = 0
        for sym in symbol_order:
            for interval in by_symbol[sym]:
                if count >= max_pairs:
                    break
                trimmed[sym].append(interval)
                count += 1
            if count >= max_pairs:
                break
        by_symbol = trimmed
        symbol_order = list(by_symbol.keys())

    started = time.monotonic()
    seeded = 0
    failed = 0
    aborted = False

    system_logger.info(
        "Batched kline bootstrap — %s symbols, %s series "
        "(batch=%s symbols, delay=%ss, batch_pause=%ss, warmup=%s).",
        len(symbol_order),
        sum(len(v) for v in by_symbol.values()),
        batch_size,
        delay,
        batch_pause,
        warmup_mode,
    )

    for batch_idx in range(0, len(symbol_order), batch_size):
        if can_fetch is not None and not can_fetch():
            system_logger.warning(
                "Kline bootstrap REST aborted — budget/ban circuit breaker "
                "(seeded=%s, batch=%s).",
                seeded,
                batch_idx // batch_size + 1,
            )
            aborted = True
            break

        batch_symbols = symbol_order[batch_idx : batch_idx + batch_size]
        for sym in batch_symbols:
            for interval in by_symbol[sym]:
                if can_fetch is not None and not can_fetch():
                    aborted = True
                    break
                if warmup_mode and used_weight_fn is not None:
                    maybe_pause_warmup_rest(used_weight_fn())
                elif used_weight_fn is not None:
                    weight = int(used_weight_fn() or 0)
                    hard = Config.rest_hard_weight_cap()
                    if weight >= hard:
                        system_logger.warning(
                            "Kline bootstrap paused — used_weight=%s over hard cap %s.",
                            weight,
                            hard,
                        )
                        aborted = True
                        break
                try:
                    df = rest_fetcher(sym, interval, limit)
                except ExchangeRateLimitError as exc:
                    system_logger.warning(
                        "Kline bootstrap halted on rate limit for %s %s: %s",
                        sym,
                        interval,
                        exc,
                    )
                    aborted = True
                    break
                except KlineBootstrapAborted as exc:
                    system_logger.warning(
                        "Kline bootstrap circuit breaker: %s", exc
                    )
                    aborted = True
                    break
                except Exception as exc:
                    failed += 1
                    error_logger.debug(
                        "Kline bootstrap skip %s %s: %s", sym, interval, exc
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue

                if df is None or df.empty or len(df) < min_bars:
                    failed += 1
                    if delay > 0:
                        time.sleep(delay)
                    continue

                try:
                    seed_fn(sym, interval, df)
                    if len(df) >= done_bars:
                        mark_bootstrapped(sym, interval)
                    elif warmup_seed_fn is not None:
                        warmup_seed_fn(sym, interval)
                    seeded += 1
                except Exception as exc:
                    failed += 1
                    error_logger.warning(
                        "Kline bootstrap seed failed for %s %s: %s",
                        sym,
                        interval,
                        exc,
                    )
                if delay > 0:
                    time.sleep(delay)

            if aborted:
                break

        if aborted:
            break

        if batch_idx + batch_size < len(symbol_order) and batch_pause > 0:
            time.sleep(batch_pause)

    elapsed = time.monotonic() - started
    pending = max(len(pairs) - seeded - failed, 0)
    system_logger.info(
        "Batched kline bootstrap finished in %.1fs — seeded=%s failed=%s "
        "aborted=%s pending~=%s.",
        elapsed,
        seeded,
        failed,
        aborted,
        pending,
    )
    return BootstrapResult(
        seeded=seeded,
        failed=failed,
        timed_out=0,
        elapsed_seconds=elapsed,
        pending=pending,
        aborted=aborted,
    )


def run_paced_kline_bootstrap(
    pairs: list[tuple[str, str]],
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    limit: int,
    min_bars: int,
    seed_fn: Callable[[str, str, pd.DataFrame], None],
    mark_bootstrapped: Callable[[str, str], None],
    *,
    delay_seconds: float | None = None,
    can_fetch: Callable[[], bool] | None = None,
    max_pairs: int | None = None,
) -> BootstrapResult:
    """Legacy paced bootstrap — delegates to batched runner (1 symbol per batch)."""
    delay = max(
        delay_seconds if delay_seconds is not None else kline_rest_delay_seconds(0),
        1.0,
    )
    return run_batched_kline_bootstrap(
        pairs,
        rest_fetcher,
        limit,
        min_bars,
        seed_fn,
        mark_bootstrapped,
        can_fetch=can_fetch,
        max_symbols_per_batch=1,
        batch_cooldown_seconds=0.0,
        request_delay_seconds=delay,
        max_pairs=max_pairs,
    )
