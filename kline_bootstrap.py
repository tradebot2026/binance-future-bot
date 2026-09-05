"""Parallel one-time kline bootstrap — async REST fetch with concurrency + timeouts."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from config import Config
from logger import error_logger, system_logger


@dataclass
class BootstrapResult:
    seeded: int = 0
    failed: int = 0
    timed_out: int = 0
    elapsed_seconds: float = 0.0
    pending: int = 0


async def _fetch_pair(
    semaphore: asyncio.Semaphore,
    sym: str,
    interval: str,
    limit: int,
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    request_timeout: float,
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
        except Exception as exc:
            return sym, interval, None, str(exc)


async def _run_parallel_fetch(
    pairs: list[tuple[str, str]],
    rest_fetcher: Callable[[str, str, int], pd.DataFrame],
    limit: int,
    *,
    concurrency: int,
    request_timeout: float,
    overall_timeout: float,
) -> list[tuple[str, str, Optional[pd.DataFrame], Optional[str]]]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))
    task_map: dict[asyncio.Task, tuple[str, str]] = {}
    for sym, interval in pairs:
        task = asyncio.create_task(
            _fetch_pair(semaphore, sym, interval, limit, rest_fetcher, request_timeout)
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

    concurrency = Config.WS_KLINE_BOOTSTRAP_CONCURRENCY
    request_timeout = Config.WS_KLINE_BOOTSTRAP_REQUEST_TIMEOUT_SECONDS
    overall_timeout = Config.WS_KLINE_BOOTSTRAP_OVERALL_TIMEOUT_SECONDS
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
    """
    Fetch historical klines one pair at a time with a sleep between requests.
    Keeps REST weight usage low during startup and background seeding.
    """
    if not pairs:
        return BootstrapResult()

    delay = (
        delay_seconds
        if delay_seconds is not None
        else Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS
    )
    delay = max(delay, 0.0)
    work = pairs if max_pairs is None else pairs[: max(max_pairs, 0)]
    started = time.monotonic()
    seeded = 0
    failed = 0

    system_logger.info(
        "Paced kline bootstrap starting — %s series (limit=%s bars, delay=%ss).",
        len(work),
        limit,
        delay,
    )

    for sym, interval in work:
        if can_fetch is not None and not can_fetch():
            system_logger.info(
                "Paced kline bootstrap paused — budget/ban gate (seeded=%s).",
                seeded,
            )
            break
        try:
            df = rest_fetcher(sym, interval, limit)
        except Exception as exc:
            failed += 1
            error_logger.debug("Paced kline bootstrap skip %s %s: %s", sym, interval, exc)
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
            mark_bootstrapped(sym, interval)
            seeded += 1
        except Exception as exc:
            failed += 1
            error_logger.warning(
                "Paced kline bootstrap seed failed for %s %s: %s", sym, interval, exc
            )

        if delay > 0:
            time.sleep(delay)

    elapsed = time.monotonic() - started
    system_logger.info(
        "Paced kline bootstrap finished in %.1fs — seeded=%s failed=%s of %s series.",
        elapsed,
        seeded,
        failed,
        len(work),
    )
    return BootstrapResult(
        seeded=seeded,
        failed=failed,
        timed_out=0,
        elapsed_seconds=elapsed,
        pending=max(len(pairs) - len(work), 0),
    )
