"""Paced one-time kline bootstrap — WS-first with batched REST fallback."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd

from config import Config
from exceptions import ExchangeRateLimitError
from logger import error_logger, system_logger


class KlineBootstrapAborted(Exception):
    """Raised internally when REST kline bootstrap must stop (ban / budget)."""


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

    concurrency = Config.WS_KLINE_BOOTSTRAP_CONCURRENCY
    request_timeout = Config.WS_KLINE_BOOTSTRAP_REQUEST_TIMEOUT_SECONDS
    overall_timeout = Config.WS_KLINE_BOOTSTRAP_OVERALL_TIMEOUT_SECONDS
    inter_request_delay = max(
        Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
        Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS,
        0.3,
    )
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
) -> BootstrapResult:
    """
    REST bootstrap in symbol batches — min 1s between requests, cooldown between batches.
    Aborts cleanly when can_fetch() returns False (ban / budget circuit breaker).
    """
    if not pairs:
        return BootstrapResult()

    batch_size = max(max_symbols_per_batch or Config.KLINE_BOOTSTRAP_BATCH_SYMBOLS, 1)
    batch_pause = max(
        batch_cooldown_seconds or Config.KLINE_BOOTSTRAP_BATCH_COOLDOWN_SECONDS,
        0.0,
    )
    default_delay = max(
        Config.KLINE_REST_MIN_INTERVAL_SECONDS,
        Config.WS_KLINE_BOOTSTRAP_REST_DELAY_SECONDS,
        Config.KLINE_BOOTSTRAP_INTER_REQUEST_DELAY_SECONDS,
        0.3,
    )
    delay = max(
        request_delay_seconds if request_delay_seconds is not None else default_delay,
        0.3,
    )

    by_symbol: dict[str, list[str]] = defaultdict(list)
    for sym, interval in pairs:
        by_symbol[sym.upper()].append(interval)

    symbol_order = list(by_symbol.keys())
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
        "(batch=%s symbols, delay=%ss, batch_pause=%ss).",
        len(symbol_order),
        sum(len(v) for v in by_symbol.values()),
        batch_size,
        delay,
        batch_pause,
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
                    mark_bootstrapped(sym, interval)
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
        delay_seconds if delay_seconds is not None else Config.KLINE_REST_MIN_INTERVAL_SECONDS,
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
