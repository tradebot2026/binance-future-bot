"""Event-driven scan orchestrator — candle closes → score → tier → execute."""

from __future__ import annotations

import time
from typing import Any, Optional

from config import Config
from core.bot_health import touch_scan_cycle
from core.assignment_manager import AssignmentManager
from core.event_scheduler import EventScheduler
from core.portfolio_allocator import PortfolioAllocator
from core.scan_priority_queue import ScanPriorityQueue
from core.scoring_engine import ScoringEngine
from core.symbol_conflict_guard import SymbolConflictGuard
from core.tier_funnel import HotRecord, TierFunnel
from core.types import CandleCloseEvent, SignalCandidate, StrategyScore
from database import DatabaseManager
from exchange import BinanceExchangeManager
from executor import log_execution_rejected, log_scan_rejected
from indicators.market_analyzer import MIN_ANALYZER_BARS
from logger import log_trade_approved, scanner_logger
from pipeline.snapshot_factory import SnapshotFactory
from pipeline.universe_builder import UniverseBuilder
from rest_rate_guard import maybe_pause_warmup_rest
from strategies import build_strategy_registry


class EventScanOrchestrator:
    """
    Coordinates Tier-1 watchlist, candle-close scoring, Tier-2 promotion,
    and execution candidate generation without breaking legacy guards.
    """

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.registry = build_strategy_registry(db=db, exchange=exchange)
        self.universe_builder = UniverseBuilder(exchange, db)
        self.snapshot_factory = SnapshotFactory(exchange)
        self.scoring_engine = ScoringEngine(self.registry, exchange=exchange)
        self.assignment_manager = AssignmentManager()
        self.event_scheduler = EventScheduler()
        self.priority_queue = ScanPriorityQueue()
        self.conflict_guard = SymbolConflictGuard(exchange, db)
        self.portfolio_allocator = PortfolioAllocator(exchange, db)
        self._hub = getattr(exchange, "_market_data", None)
        self._tier1_symbols: list[str] = []
        self._price_map: dict[str, float] = {}
        self._volume_ranks: dict[str, int] = {}
        self._last_catchup_at: float = 0.0
        self._last_universe_refresh_at: float = 0.0
        self._kline_cache_misses: list[str] = []
        self._kline_pending_log_at: dict[str, float] = {}
        self._last_ticker_unavail_log_at: float = 0.0
        self._last_empty_universe_log_at: float = 0.0
        self._last_zero_candidate_log_at: float = 0.0
        self.funnel = TierFunnel(notify=self._notify_funnel)
        self._validator: Any = None
        self._telegram: Any = None

    def attach_validator(self, validator: Any) -> None:
        self._validator = validator
        if validator is None:
            return
        setter = getattr(validator, "attach_funnel", None)
        if callable(setter):
            setter(self.funnel)

    def attach_telegram(self, telegram: Any) -> None:
        self._telegram = telegram

    def _notify_funnel(self, text: str) -> None:
        tg = self._telegram
        if tg is None:
            return
        send = getattr(tg, "send_message", None)
        if callable(send):
            send(text)

    def _seed_funnel_universe(self, now: float) -> None:
        """Flush inactive Normal memory, seed candidates, GC flushed kline streams."""
        keep = self._open_symbols()
        self.funnel.replace_normal_universe(self._tier1_symbols, now=now, keep=keep)
        self._gc_flushed_klines(self.funnel.last_flushed)

    def _gc_flushed_klines(self, symbols: list[str]) -> None:
        if not symbols or self._hub is None:
            return
        batch = getattr(self._hub, "demote_symbols_klines", None)
        if callable(batch):
            batch(symbols)
            return
        for symbol in symbols:
            drop = getattr(self._hub, "demote_symbol_klines", None)
            if callable(drop):
                drop(symbol)

    @property
    def tier1_symbols(self) -> list[str]:
        return list(self._tier1_symbols)

    def note_kline_cache_miss(self, symbol: str) -> None:
        """Queue a symbol for paced REST kline bootstrap after this scan cycle."""
        sym = str(symbol or "").upper()
        if not sym or sym in self._kline_cache_misses:
            return
        self._kline_cache_misses.append(sym)

    def take_kline_cache_misses(self, count: int = 1) -> list[str]:
        n = max(int(count), 1)
        taken = self._kline_cache_misses[:n]
        self._kline_cache_misses = self._kline_cache_misses[n:]
        return taken

    def _log_kline_bootstrap_pending(self, symbol: str) -> None:
        """Rate-limited notice — not SCAN_REJECTED. Cache is bootstrapping, not a setup fail."""
        key = str(symbol or "").upper()
        if not key:
            return
        now = time.monotonic()
        last = self._kline_pending_log_at.get(key, 0.0)
        if (now - last) < 60.0:
            return
        self._kline_pending_log_at[key] = now
        scanner_logger.info(
            "[SCAN_KLINE_BOOTSTRAP] %s — snapshot deferred, REST/WS kline bootstrap queued",
            key,
        )

    def _partition_kline_ready(
        self, symbols: list[str]
    ) -> tuple[list[str], list[str]]:
        ready: list[str] = []
        missing: list[str] = []
        for raw in symbols:
            symbol = str(raw or "").upper()
            if not symbol:
                continue
            if self.snapshot_factory.has_complete_klines(symbol):
                ready.append(symbol)
            else:
                missing.append(symbol)
        return ready, missing

    def _note_missing_scan_klines(
        self,
        symbols: list[str],
        *,
        events: Optional[dict[str, CandleCloseEvent]] = None,
    ) -> None:
        for symbol in symbols:
            self.note_kline_cache_miss(symbol)
            self._log_kline_bootstrap_pending(symbol)
            event = (events or {}).get(symbol)
            if event is not None:
                self.event_scheduler.requeue(event, delay_seconds=2.0)

    def _next_bootstrap_symbol(self, queued: list[str]) -> str:
        """Prefer a hot miss; otherwise the first queued not-ready symbol."""
        if not queued:
            return ""
        try:
            hot = {str(s).upper() for s in (self.priority_queue.hot_symbols or [])}
        except Exception:
            hot = set()
        for raw in queued:
            symbol = str(raw or "").upper()
            if symbol and symbol in hot:
                return symbol
        return str(queued[0] or "").upper()

    def _bootstrap_missing_scan_klines(self, symbols: list[str]) -> int:
        """Governor-gated REST backfill for one not-ready symbol. Never in scan_context."""
        if not symbols or self._hub is None:
            return 0
        if self.exchange.in_scan_mode:
            return 0
        if not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP:
            return 0
        if getattr(self.exchange, "_ws_reconnect_or_warmup", lambda: False)():
            return 0
        can_boot = getattr(self.exchange, "can_bootstrap_klines_rest", None)
        if callable(can_boot) and not can_boot():
            return 0
        can_rest = getattr(self.exchange, "can_make_background_rest_call", None)
        if callable(can_rest) and not can_rest(2):
            return 0

        missing = [
            symbol
            for symbol in symbols
            if symbol and not self.snapshot_factory.has_complete_klines(symbol)
        ]
        if not missing:
            return 0

        target = self._next_bootstrap_symbol(missing)
        if not target:
            return 0

        timeframes = Config.get_scan_kline_intervals()
        try:
            self._hub.subscribe_kline_streams([target])
            with self.exchange.bootstrap_context():
                seeded = self._hub.bootstrap_klines_for_symbols(
                    [target],
                    timeframes,
                    self.exchange.fetch_bootstrap_klines_df,
                    max_pairs=len(timeframes),
                )
        except Exception as exc:
            scanner_logger.warning(
                "[SCAN_KLINE_BOOTSTRAP] %s failed — %s",
                target,
                exc,
            )
            return 0

        scanner_logger.info(
            "[SCAN_KLINE_BOOTSTRAP] %s — seeded=%s series (missing TFs only)",
            target,
            seeded,
        )
        return int(seeded or 0)

    def populate_warmup_klines(self) -> int:
        """One symbol of paced REST klines during WARMUP_MODE cache populate."""
        if self._hub is None or not getattr(self._hub, "in_scan_warmup", lambda: False)():
            return 0
        if self.exchange.in_scan_mode:
            return 0
        if not Config.ENABLE_WS_KLINE_STARTUP_BOOTSTRAP:
            return 0
        if getattr(self.exchange, "_ws_reconnect_or_warmup", lambda: False)():
            return 0

        usage = getattr(self.exchange, "_rest_usage", None)
        weight = 0
        if usage is not None:
            fn = getattr(usage, "projected_used_weight", None)
            if callable(fn):
                try:
                    weight = int(fn() or 0)
                except Exception:
                    weight = 0
        can_boot = getattr(self.exchange, "can_bootstrap_klines_rest", None)
        can_rest = getattr(self.exchange, "can_make_background_rest_call", None)
        if callable(can_boot) and not can_boot():
            maybe_pause_warmup_rest(weight)
            return 0
        if callable(can_rest) and not can_rest(2):
            maybe_pause_warmup_rest(weight)
            return 0

        if not self._tier1_symbols:
            self.bootstrap_watchlist_once()
        symbols = list(self.priority_queue.hot_symbols or self._tier1_symbols)
        if not symbols:
            return 0

        timeframes = Config.get_scan_kline_intervals()
        pending = self._hub._pending_bootstrap_pairs(
            symbols,
            timeframes,
            min_bars=Config.warmup_kline_fetch_limit(),
            limit=Config.warmup_kline_fetch_limit(),
            warmup=True,
        )
        if not pending:
            return 0
        target = pending[0][0]

        try:
            self._hub.subscribe_kline_streams([target])
            with self.exchange.bootstrap_context():
                seeded = self._hub.bootstrap_klines_for_symbols(
                    [target],
                    timeframes,
                    self.exchange.fetch_bootstrap_klines_df,
                    max_pairs=len(timeframes),
                    warmup=True,
                )
        except Exception as exc:
            scanner_logger.warning(
                "[SCAN_KLINE_BOOTSTRAP] %s REST populate failed — %s",
                target,
                exc,
            )
            return 0
        if seeded:
            scanner_logger.info(
                "[SCAN_KLINE_BOOTSTRAP] %s populated %s series (limit=%s).",
                target,
                seeded,
                Config.warmup_kline_fetch_limit(),
            )
        return int(seeded or 0)

    def _evaluate_symbols_ws(
        self,
        symbols: list[str],
        *,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
        event_by_symbol: Optional[dict[str, CandleCloseEvent]] = None,
        pace: bool = True,
    ) -> list[SignalCandidate]:
        if not symbols:
            return []
        candidates: list[SignalCandidate] = []
        delay = Config.scan_symbol_delay_seconds() if pace else 0.0
        for index, symbol in enumerate(symbols):
            if index > 0 and delay > 0:
                time.sleep(delay)
            with self.exchange.scan_context():
                event = (event_by_symbol or {}).get(symbol)
                bar_open_ms = 0
                eval_tf = timeframe
                if event is not None:
                    bar_open_ms = event.bar_open_ms
                    eval_tf = event.timeframe
                elif self._hub:
                    closed = self._hub.get_last_closed_bar_open_ms(symbol, timeframe)
                    if closed:
                        bar_open_ms = closed
                signal = self._evaluate_symbol(
                    symbol,
                    bar_open_ms=bar_open_ms,
                    timeframe=eval_tf,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                    mark_event=event,
                )
            if signal is not None:
                candidates.append(signal)
        return candidates

    def warmup_and_evaluate_kline_misses(self) -> list[dict[str, Any]]:
        """Bootstrap one not-ready coin (REST missing TFs only), then evaluate if complete."""
        halted, reason = self._scan_gate_open()
        if halted:
            scanner_logger.debug("Kline warmup skipped — %s", reason)
            return []

        queued = self.take_kline_cache_misses(16)
        if not queued:
            return []

        target = self._next_bootstrap_symbol(queued)
        leftovers = [symbol for symbol in queued if symbol != target]
        for symbol in leftovers:
            self.note_kline_cache_miss(symbol)
        if not target:
            return []

        if self._hub is not None:
            self._hub.subscribe_kline_streams([target])

        if not self.snapshot_factory.has_complete_klines(target):
            try:
                self._bootstrap_missing_scan_klines([target])
            except Exception as exc:
                scanner_logger.warning(
                    "[SCAN_KLINE_BOOTSTRAP] %s raised — %s",
                    target,
                    exc,
                )

        ready, still = self._partition_kline_ready([target])
        for symbol in still:
            self.note_kline_cache_miss(symbol)
        if not ready:
            return []

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME
        candidates = self._evaluate_symbols_ws(
            ready,
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )
        dict_results = [c.to_dict() for c in candidates]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Kline warmup scan dispatching %s execution candidate(s) from %s warmed symbol(s).",
                len(dict_results),
                len(ready),
            )
        return dict_results

    def _maybe_fetch_single_closed_kline(self, symbols: list[str]) -> None:
        """At most one limit=2 REST kline for a nearly-complete hot symbol."""
        if bool(getattr(Config, "SCAN_WS_ONLY", True)):
            return
        if not symbols or self._hub is None or self.exchange.in_scan_mode:
            return
        if getattr(self.exchange, "_ws_reconnect_or_warmup", lambda: False)():
            return
        can_boot = getattr(self.exchange, "can_bootstrap_klines_rest", None)
        if callable(can_boot) and not can_boot():
            return
        can_rest = getattr(self.exchange, "can_make_background_rest_call", None)
        if callable(can_rest) and not can_rest(2):
            return

        try:
            hot = {str(s).upper() for s in (self.priority_queue.hot_symbols or [])}
        except Exception:
            hot = set()
        target = next((s for s in symbols if s.upper() in hot), "")
        if not target:
            return

        timeframes = Config.get_scan_kline_intervals()
        need_tf = ""
        for tf in timeframes:
            cached = self._hub.get_candles_cached_only(
                target, tf, Config.CANDLE_FETCH_LIMIT
            )
            n = 0 if cached is None or getattr(cached, "empty", True) else len(cached)
            if MIN_ANALYZER_BARS - 2 <= n < MIN_ANALYZER_BARS:
                need_tf = tf
                break
        if not need_tf:
            return

        try:
            with self.exchange.bootstrap_context():
                df = self.exchange.fetch_bootstrap_klines_df(target, need_tf, 2)
        except Exception:
            return
        if df is None or getattr(df, "empty", True):
            return
        try:
            self._hub.seed_klines_from_dataframe(target, need_tf, df)
        except Exception:
            return
        scanner_logger.info(
            "[SCAN_KLINE_TOPUP] %s %s — fetched latest closed bar (limit=2)",
            target,
            need_tf,
        )

    def refresh_tier1_universe(
        self, *, force: bool = False, allow_rest: bool = False
    ) -> list[str]:
        """Rebuild Tier-1 watchlist from WS ticker cache (no REST poll)."""
        now = time.monotonic()
        if (
            not force
            and self._tier1_symbols
            and (now - self._last_universe_refresh_at)
            < Config.EVENT_CATCHUP_INTERVAL_SECONDS
        ):
            return self._tier1_symbols

        if self._hub:
            if allow_rest:
                ready = self._hub.ensure_ticker_cache_ready(
                    rest_seeder=self.exchange.fetch_futures_ticker_map_rest,
                )
            else:
                ready = bool(self._hub.get_ticker_map())
            if not ready:
                now_log = time.monotonic()
                last_log = float(self._last_ticker_unavail_log_at or 0.0)
                if last_log <= 0.0 or (now_log - last_log) >= 45.0:
                    self._last_ticker_unavail_log_at = now_log
                    count = len(self._hub.get_ticker_map()) if self._hub else 0
                    scanner_logger.info(
                        "Waiting on WS miniTicker/bookTicker — ticker cache empty "
                        "(%s symbols). Next notice in 45s.",
                        count,
                    )
                return self._tier1_symbols

        universe = self.universe_builder.build()
        pool_cap = min(
            len(universe.symbols),
            Config.normal_tier_universe_size()
            if Config.ENABLE_THREE_TIER_FUNNEL
            else Config.TOP_UNIVERSE_POOL_SIZE,
        )
        self._tier1_symbols = universe.symbols[:pool_cap]
        if Config.ENABLE_THREE_TIER_FUNNEL:
            now_lock = time.monotonic()
            if self.funnel.lock_expired(now_lock) or not self.funnel.has_candidate_universe:
                self._seed_funnel_universe(now_lock)
        trigger_scores = dict(universe.opportunity_scores)
        for sym, score in self.assignment_manager._last_best.items():
            trigger_scores.setdefault(sym, score.normalized_score)
        self.priority_queue.update(
            self._tier1_symbols,
            extended_symbols=universe.extended_symbols,
            trigger_scores=trigger_scores,
            lifecycle=universe.lifecycle,
            fast_track=universe.hot_symbols,
        )
        self._price_map = universe.price_map
        self._volume_ranks = universe.volume_ranks
        self._last_universe_refresh_at = now

        if self._hub and self._tier1_symbols:
            if Config.ENABLE_THREE_TIER_FUNNEL:
                live = (
                    self.funnel.normal_symbols
                    + self.funnel.hot_symbols
                    + self.funnel.super_symbols
                )
                if live:
                    self._hub.subscribe_kline_streams(live)
            else:
                self._hub.subscribe_kline_streams(self._tier1_symbols)

        scanner_logger.info(
            "Tier1 watchlist refreshed — pool=%s priority=%s rotating=%s "
            "evaluated=%s hot_fast_track=%s (pool_cap=%s).",
            len(self._tier1_symbols),
            len(self.priority_queue.hot_symbols),
            len(self.priority_queue.background_symbols),
            self.priority_queue.rotation.evaluated_count,
            len(universe.hot_symbols),
            Config.TOP_UNIVERSE_POOL_SIZE,
        )
        return self._tier1_symbols

    def bootstrap_watchlist_once(self) -> list[str]:
        """One-shot REST ticker seed + Tier-1 watchlist, even in SCAN_WS_ONLY."""
        if self._hub is not None and bool(getattr(Config, "STARTUP_TICKER_REST_SEED", True)):
            fetcher = getattr(
                self.exchange, "fetch_universe_bootstrap_ticker_map", None
            )
            if callable(fetcher) and not self.exchange.in_scan_mode:
                try:
                    with self.exchange.bootstrap_context():
                        self._hub.seed_universe_from_rest_once(fetcher)
                except Exception as exc:
                    scanner_logger.warning("Universe REST seed skipped — %s", exc)
        return self.refresh_tier1_universe(force=True, allow_rest=False)

    def maybe_refresh_tier1_periodic(self) -> None:
        """Rebuild Normal-tier universe after the 3-hour lock (or bootstrap if empty)."""
        now = time.monotonic()
        if not self._tier1_symbols:
            self.bootstrap_watchlist_once()
            return
        if Config.ENABLE_THREE_TIER_FUNNEL and not self.funnel.lock_expired(now):
            return
        if (
            not Config.ENABLE_THREE_TIER_FUNNEL
            and (now - self._last_universe_refresh_at)
            < Config.TIER1_REFRESH_INTERVAL_SECONDS
        ):
            return

        old_symbols = set(self._tier1_symbols)
        self.refresh_tier1_universe(force=True)
        new_symbols = set(self._tier1_symbols)
        added = new_symbols - old_symbols
        dropped = old_symbols - new_symbols
        if added or dropped:
            scanner_logger.info(
                "Tier1 rotation — added=%s dropped=%s (refresh every %ss).",
                len(added),
                len(dropped),
                Config.TIER1_REFRESH_INTERVAL_SECONDS,
            )
        if dropped:
            open_symbols = {
                str(t.get("symbol", "")).upper()
                for t in self.db.get_open_trades()
            }
            self.assignment_manager.prune_outside_watchlist(
                self._tier1_symbols,
                open_symbols=open_symbols,
            )

    def on_candle_close(self, symbol: str, timeframe: str, bar_open_ms: int) -> None:
        """WS callback — enqueue staggered evaluation."""
        if symbol.upper() not in {s.upper() for s in self._tier1_symbols}:
            return
        self.event_scheduler.on_candle_close(symbol, timeframe, bar_open_ms)

    def run_catchup(self) -> int:
        """Fallback timer — recover missed candle closes after WS drops."""
        now = time.monotonic()
        if (now - self._last_catchup_at) < Config.EVENT_CATCHUP_INTERVAL_SECONDS:
            return 0
        self._last_catchup_at = now

        if not self._tier1_symbols:
            self.refresh_tier1_universe()

        if not self._hub:
            return 0

        return self.event_scheduler.run_catchup(
            self._tier1_symbols,
            self._hub.get_last_closed_bar_open_ms,
        )

    def process_due_events(self) -> list[dict[str, Any]]:
        """Drain due candle-close events and return execution candidates."""
        halted, reason = self._scan_gate_open()
        if halted:
            scanner_logger.warning("Event scan skipped — %s", reason)
            return []

        self.maybe_refresh_tier1_periodic()
        if not self._tier1_symbols:
            self.refresh_tier1_universe()

        self.run_catchup()
        self.conflict_guard.reset_cycle()
        candidates = self._dedupe_symbol_candidates(
            self._process_due_event_candidates()
        )
        dict_results = [c.to_dict() for c in candidates]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Event scan produced %s execution candidate(s).",
                len(dict_results),
            )
        return dict_results

    def process_priority_scan_cycle(self) -> list[dict[str, Any]]:
        """
        3-tier funnel tick: 1 Normal coin / 30s (2/min), 1 Hot REST/min, 1 Super eval/min.
        Only Super-tier candidates are returned for execution.
        Normal/Hot always tick (even when entries are paused or REST is banned).
        """
        funnel_on = bool(Config.ENABLE_THREE_TIER_FUNNEL)
        rest_blocked = self._rest_is_blocked()
        if not funnel_on:
            halted, reason = self._scan_gate_open()
            if halted:
                scanner_logger.warning("Priority scan skipped — %s", reason)
                return []

        if funnel_on and rest_blocked:
            if not self._tier1_symbols and not self.funnel.has_candidate_universe:
                self._log_empty_universe()
        else:
            self.maybe_refresh_tier1_periodic()
            if not self._tier1_symbols:
                self.bootstrap_watchlist_once()

        self.run_catchup()
        self.conflict_guard.reset_cycle()
        if funnel_on:
            return self._process_funnel_cycle()

        self._fast_track_live_spikes()
        event_by_symbol, event_symbols = self._drain_due_event_symbols()
        symbols = self._cycle_scan_symbols(event_symbols)
        if not symbols:
            self._log_empty_universe()
            return []
        return self._evaluate_and_collect(symbols, event_by_symbol, pace=True)

    def _process_funnel_cycle(self) -> list[dict[str, Any]]:
        now = time.monotonic()
        if (
            self._tier1_symbols
            and (
                self.funnel.lock_expired(now)
                or not self.funnel.has_candidate_universe
            )
        ):
            self._seed_funnel_universe(now)

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME

        normal_due = self.funnel.take_normal(now=now)
        if normal_due and self._hub:
            self._hub.subscribe_kline_streams(normal_due)
        ready, missing = self._partition_kline_ready(normal_due)
        self._note_missing_scan_klines(missing)
        for symbol in ready:
            if symbol in open_symbols:
                continue
            scores = self._score_symbol(
                symbol,
                timeframe=primary_tf,
                open_symbols=open_symbols,
                ticker_map=ticker_map,
                book_map=book_map,
            )
            if scores:
                self.funnel.promote_from_normal(symbol, scores)

        if not self._rest_is_blocked():
            self._enqueue_due_hot_rest(now)
        self._ingest_hot_backtests(
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )
        self._rescore_due_hot(
            now,
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )

        super_rec = self.funnel.take_super(now=now)
        candidates: list[SignalCandidate] = []
        if super_rec is not None:
            if super_rec.symbol in open_symbols:
                self.funnel.note_filled(super_rec.symbol)
            else:
                signal = self._evaluate_super_symbol(
                    super_rec.symbol,
                    timeframe=primary_tf,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                )
                if signal is not None:
                    candidates.append(signal)
                else:
                    self.funnel.demote_super(
                        super_rec.symbol, reason="super_setup_not_ready"
                    )

        universe_total = len(self.funnel.scan_pool()) or len(self._tier1_symbols)
        touch_scan_cycle(
            scanned=len(normal_due) + (1 if super_rec else 0),
            universe_total=max(universe_total, 1),
        )
        dict_results = [c.to_dict() for c in self._dedupe_symbol_candidates(candidates)]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Super tier dispatching %s execution candidate(s) "
                "(normal=%s hot=%s super=%s).",
                len(dict_results),
                len(self.funnel.normal_symbols),
                len(self.funnel.hot_symbols),
                len(self.funnel.super_symbols),
            )
        elif not self.funnel.has_candidate_universe and not self._tier1_symbols:
            self._log_empty_universe()
        else:
            self._log_zero_candidates_quiet(
                normal=len(normal_due),
                hot=len(self.funnel.hot_symbols),
                super_n=len(self.funnel.super_symbols),
            )
        return dict_results

    def _log_empty_universe(self) -> None:
        now_log = time.monotonic()
        last_log = float(self._last_empty_universe_log_at or 0.0)
        if last_log <= 0.0 or (now_log - last_log) >= 60.0:
            self._last_empty_universe_log_at = now_log
            hot_n = 0
            funnel = getattr(self, "funnel", None)
            if funnel is not None:
                hot_n = len(funnel.hot_symbols)
            elif getattr(self, "assignment_manager", None) is not None:
                hot_n = int(getattr(self.assignment_manager, "tier2_size", 0) or 0)
            scanner_logger.info(
                "Priority scan produced 0 execution candidates "
                "(empty universe, hot=%s). Next notice in 60s.",
                hot_n,
            )

    def _log_zero_candidates_quiet(self, *, normal: int, hot: int, super_n: int) -> None:
        now_log = time.monotonic()
        last_log = float(self._last_zero_candidate_log_at or 0.0)
        if last_log > 0.0 and (now_log - last_log) < 60.0:
            return
        self._last_zero_candidate_log_at = now_log
        scanner_logger.debug(
            "Funnel tick — 0 Super candidates (normal_eval=%s hot=%s super=%s).",
            normal,
            hot,
            super_n,
        )

    def _enqueue_hot_backtest(self, rec: HotRecord) -> None:
        if self._validator is None:
            return
        payload = {
            "symbol": rec.symbol,
            "strategy": rec.strategy,
            "score": rec.score,
            "action": rec.action if rec.action in ("LONG", "SHORT") else "LONG",
            "structure_metadata": {
                "backup_strategies": list(rec.backup_strategies),
                "funnel_tier": "HOT",
                **rec.metadata,
            },
        }
        try:
            if self._validator.enqueue(payload):
                self.funnel.note_hot_rest_started(rec.symbol)
        except Exception as exc:
            scanner_logger.warning(
                "[TIER_HOT] queue failed for %s: %s", rec.symbol, exc
            )

    def _enqueue_due_hot_rest(self, now: float) -> None:
        if self._validator is None:
            return
        rec = self.funnel.take_hot_for_rest(now=now)
        if rec is None:
            return
        self._enqueue_hot_backtest(rec)

    def _ingest_hot_backtests(
        self,
        *,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> None:
        validator = self._validator
        if validator is None:
            return
        approved = []
        drain = getattr(validator, "drain_approved", None)
        if callable(drain):
            approved = drain(max_n=4)
        for payload in approved:
            symbol = str(payload.get("symbol", "")).upper()
            rec = self.funnel.on_backtest_passed(symbol, payload)
            if rec is None:
                continue
            scores = self._score_symbol(
                symbol,
                timeframe=timeframe,
                open_symbols=open_symbols,
                ticker_map=ticker_map,
                book_map=book_map,
            )
            if not scores:
                self.funnel.on_backtest_failed(symbol, "hot_rescore_empty")
                continue
            self.funnel.apply_hot_scores(symbol, scores, candidate=payload)

    def _rescore_due_hot(
        self,
        now: float,
        *,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> None:
        rec = self.funnel.take_hot_for_rescore(now=now)
        if rec is None:
            return
        if rec.symbol in open_symbols:
            self.funnel.note_filled(rec.symbol)
            return
        scores = self._score_symbol(
            rec.symbol,
            timeframe=timeframe,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )
        if not scores:
            return
        payload = {
            "symbol": rec.symbol,
            "strategy": rec.strategy,
            "score": rec.score,
            "action": rec.action if rec.action in ("LONG", "SHORT") else "LONG",
            "structure_metadata": {
                "backup_strategies": list(rec.backup_strategies),
                "funnel_tier": "HOT",
                **rec.metadata,
            },
        }
        self.funnel.apply_hot_scores(rec.symbol, scores, candidate=payload)

    def _score_symbol(
        self,
        symbol: str,
        *,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> list[StrategyScore]:
        snapshot = self._build_eval_snapshot(symbol, ticker_map, book_map)
        if snapshot is None:
            self.note_kline_cache_miss(symbol)
            self._log_kline_bootstrap_pending(symbol)
            return []
        bar_open_ms = 0
        if self._hub:
            closed = self._hub.get_last_closed_bar_open_ms(symbol, timeframe)
            if closed:
                bar_open_ms = closed
        with self.exchange.scan_context():
            scores, _first = self.scoring_engine.evaluate_symbol_detailed(
                snapshot,
                bar_open_ms=bar_open_ms,
                timeframe=timeframe,
            )
        return scores or []

    def _build_eval_snapshot(
        self,
        symbol: str,
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> Any:
        symbol = symbol.upper()
        ticker = ticker_map.get(symbol, {})
        book = book_map.get(symbol, {})
        price = self._resolve_eval_price(symbol, ticker)
        volume_24h = float(ticker.get("quoteVolume", 0) or 0)
        volume_rank = self._volume_ranks.get(symbol, 0)
        return self.snapshot_factory.build(
            symbol,
            price=price,
            ticker=ticker,
            book=book,
            volume_24h=volume_24h,
            volume_rank=volume_rank,
        )

    def _evaluate_super_symbol(
        self,
        symbol: str,
        *,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> Optional[SignalCandidate]:
        signal = self._evaluate_symbol(
            symbol,
            bar_open_ms=0,
            timeframe=timeframe,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )
        if signal is None:
            return None
        from core.strategy_score_ranges import score_range_for

        band = score_range_for(signal.strategy)
        if not band.meets_super(signal.score):
            log_scan_rejected(
                symbol,
                f"super score {signal.score:.1f} below {band.super_score:.1f}",
                strategy=signal.strategy,
            )
            return None
        rec = self.funnel._super.get(symbol.upper())
        if rec is not None:
            meta = dict(rec.candidate.get("structure_metadata") or {})
            meta.update(signal.structure_metadata or {})
            meta["funnel_tier"] = "SUPER"
            meta["backup_strategies"] = list(rec.backup_strategies)
            for key in (
                "backtest_validated",
                "backtest_win_rate",
                "backtest_wins",
                "backtest_trades",
            ):
                if key in rec.candidate:
                    meta.setdefault(key, rec.candidate[key])
            signal.structure_metadata = meta
        return signal

    def _evaluate_and_collect(
        self,
        symbols: list[str],
        event_by_symbol: dict[str, CandleCloseEvent],
        *,
        pace: bool,
    ) -> list[dict[str, Any]]:
        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME
        ready, missing = self._partition_kline_ready(symbols)
        self._note_missing_scan_klines(missing, events=event_by_symbol)
        candidates = self._evaluate_symbols_ws(
            ready,
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
            event_by_symbol=event_by_symbol,
            pace=pace,
        )
        candidates = self._dedupe_symbol_candidates(candidates)
        scanned_bg = [
            symbol
            for symbol in symbols
            if not self.priority_queue.is_priority(symbol)
        ]
        if scanned_bg:
            self.priority_queue.rotation.mark_evaluated(scanned_bg)
        self.priority_queue.mark_hot_scan_complete()
        universe_total = len(self.priority_queue.full_universe) or len(
            self._tier1_symbols
        )
        touch_scan_cycle(
            scanned=len(symbols),
            universe_total=max(universe_total, len(symbols)),
        )
        dict_results = [c.to_dict() for c in candidates]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Priority scan dispatching %s execution candidate(s) "
                "from %s symbols.",
                len(dict_results),
                len(symbols),
            )
        return dict_results

    def process_hot_scan_cycle(self, *, pace: bool = True) -> list[SignalCandidate]:
        """Tier 1 — WS-only scan of high-activity + Tier-2 symbols."""
        run_hot = True if not pace else self.priority_queue.should_run_hot_scan()
        symbols = self._execution_scan_symbols(include_hot=run_hot)
        if not symbols:
            return []

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME
        ready, missing = self._partition_kline_ready(symbols)
        self._note_missing_scan_klines(missing)
        candidates = self._evaluate_symbols_ws(
            ready,
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
            pace=pace,
        )

        if run_hot:
            self.priority_queue.mark_hot_scan_complete()
        if candidates:
            scanner_logger.info(
                "Hot scan produced %s execution candidate(s) from %s symbols.",
                len(candidates),
                len(symbols),
            )
        return candidates

    def process_background_scan_cycle(self) -> list[SignalCandidate]:
        """Tier 2 — rotate background symbols in small WS-only batches."""
        if not self.priority_queue.should_run_background_batch():
            return []

        batch = self.priority_queue.next_background_batch()
        if not batch:
            return []

        hot_set = {s.upper() for s in self.priority_queue.hot_symbols}
        batch = [symbol for symbol in batch if symbol.upper() not in hot_set]
        if not batch:
            self.priority_queue.mark_last_background_batch_evaluated()
            return []

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME
        ready, missing = self._partition_kline_ready(batch)
        self._note_missing_scan_klines(missing)
        candidates = self._evaluate_symbols_ws(
            ready,
            timeframe=primary_tf,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
        )

        self.priority_queue.mark_last_background_batch_evaluated()
        if candidates:
            scanner_logger.info(
                "Background scan produced %s execution candidate(s) from batch=%s.",
                len(candidates),
                len(batch),
            )
        return candidates

    def _process_due_event_candidates(self) -> list[SignalCandidate]:
        """Score due candle-close events (internal — returns SignalCandidate list)."""
        events = self.event_scheduler.drain_due(limit=Config.TIER2_HOT_SIZE * 3)
        if not events:
            return []

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        event_by_symbol: dict[str, CandleCloseEvent] = {}
        symbols: list[str] = []
        for event in events:
            key = event.symbol.upper()
            if key in event_by_symbol:
                continue
            event_by_symbol[key] = event
            symbols.append(key)
        ready, missing = self._partition_kline_ready(symbols)
        self._note_missing_scan_klines(missing, events=event_by_symbol)
        candidates = self._evaluate_symbols_ws(
            ready,
            timeframe=events[0].timeframe,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
            event_by_symbol=event_by_symbol,
            pace=True,
        )

        if candidates:
            scanner_logger.info(
                "Event scan produced %s execution candidate(s) from %s event(s).",
                len(candidates),
                len(events),
            )
        return candidates

    def _evaluate_symbol(
        self,
        symbol: str,
        *,
        bar_open_ms: int,
        timeframe: str,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
        mark_event: Optional[CandleCloseEvent] = None,
    ) -> Optional[SignalCandidate]:
        symbol = symbol.upper()
        skip_rotation_memory = Config.USE_TESTNET
        if (
            not skip_rotation_memory
            and self.priority_queue.rotation.is_in_evaluated_memory(symbol)
            and not self.priority_queue.is_priority(symbol)
            and not self.assignment_manager.is_hot(symbol)
        ):
            log_scan_rejected(
                symbol, "rotation evaluated-memory cooldown — skipped rescan"
            )
            if mark_event is not None:
                self.event_scheduler.mark_evaluated(
                    symbol, mark_event.timeframe, mark_event.bar_open_ms
                )
            return None

        ticker = ticker_map.get(symbol, {})
        book = book_map.get(symbol, {})
        price = self._resolve_eval_price(symbol, ticker)
        volume_24h = float(ticker.get("quoteVolume", 0) or 0)
        volume_rank = self._volume_ranks.get(symbol, 0)

        snapshot = self.snapshot_factory.build(
            symbol,
            price=price,
            ticker=ticker,
            book=book,
            volume_24h=volume_24h,
            volume_rank=volume_rank,
        )
        if snapshot is None:
            self.note_kline_cache_miss(symbol)
            self._log_kline_bootstrap_pending(symbol)
            if mark_event is not None:
                self.event_scheduler.requeue(mark_event, delay_seconds=2.0)
            return None

        scores, first_pass = self.scoring_engine.evaluate_symbol_detailed(
            snapshot,
            bar_open_ms=bar_open_ms,
            timeframe=timeframe,
        )
        if not scores:
            log_scan_rejected(symbol, "no strategy scores produced after scan")
            if mark_event is not None:
                self.event_scheduler.mark_evaluated(
                    symbol, mark_event.timeframe, mark_event.bar_open_ms
                )
            return None

        tier2_candidate = self.scoring_engine.pick_best_for_tier2(scores)
        promoted, demoted, gc_symbol = False, False, None
        if tier2_candidate is not None:
            scanner_logger.debug(
                "Tier2 check %s | strategy=%s raw=%.1f min=%.1f norm=%.1f",
                symbol,
                tier2_candidate.strategy,
                tier2_candidate.score,
                tier2_candidate.min_score,
                tier2_candidate.normalized_score,
            )
            promoted, demoted, gc_symbol = self.assignment_manager.update(
                tier2_candidate, open_symbols=open_symbols
            )

        if gc_symbol and self._hub:
            self.assignment_manager.gc_demoted(
                gc_symbol, self._hub.demote_symbol_klines
            )
        if promoted:
            self.priority_queue.rotation.clear_evaluated([symbol])
            if self._hub:
                self._hub.subscribe_kline_streams([symbol])

        best = self.scoring_engine.pick_best(scores)
        if mark_event is not None:
            self.event_scheduler.mark_evaluated(
                symbol, mark_event.timeframe, mark_event.bar_open_ms
            )

        if best is None:
            log_scan_rejected(
                symbol,
                f"pick_best produced no valid winner from {len(scores)} scored setup(s)",
            )
            return None

        losers = [
            row
            for row in scores
            if row.strategy != best.strategy and row.score > 0
        ]
        if losers:
            log_scan_rejected(
                symbol,
                (
                    f"Lower score than winner {best.strategy} {best.action} "
                    f"raw={best.score:.1f} final={best.final_score:.1f} rejected="
                    + ",".join(
                        f"{row.strategy}:{row.action}:{row.score:.0f}"
                        for row in losers[:6]
                    )
                ),
                strategy=best.strategy,
            )

        if best.score < best.min_score:
            log_scan_rejected(
                symbol,
                f"score {best.score:.1f} below strategy minimum {best.min_score:.1f}",
                strategy=best.strategy,
            )
            return None

        signal = self.scoring_engine.signal_from_first_pass(
            snapshot, best, first_pass
        )
        if signal is None:
            signal = self.scoring_engine.signal_for_assignment(snapshot, best)
        if signal is None:
            log_execution_rejected(
                symbol,
                "signal revalidation failed after strategy approval",
                strategy=best.strategy,
            )
            return None

        ok, reason = self.conflict_guard.approve(signal)
        if not ok:
            self.conflict_guard.reject_with_log(signal, reason)
            return None

        signal = self._apply_portfolio_allocator(signal)
        if signal is None:
            return None

        rec = self.universe_builder.tracker.get(symbol)
        if rec is not None:
            meta = dict(signal.structure_metadata or {})
            meta.update(
                {
                    "opportunity_score": rec.score,
                    "score_velocity": rec.velocity,
                    "relative_rank": rec.relative_rank,
                    "lifecycle": rec.lifecycle.value,
                    "channel_momentum": rec.channels.momentum,
                    "channel_reversal": rec.channels.reversal,
                    "channel_structure": rec.channels.structure,
                    "dominant_channel": rec.channels.dominant,
                }
            )
            signal.structure_metadata = meta

        self.db.log_signal(
            {
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "direction": signal.action,
                "score": signal.score,
                "strategy": signal.strategy,
                "reason": f"scan={timeframe}|regime={signal.regime}",
                "accepted": True,
                "structure_metadata": signal.structure_metadata,
            }
        )
        log_trade_approved(
            signal.symbol,
            signal.action,
            signal.strategy,
            signal.score,
            extra=f"regime={signal.regime} fit={signal.regime_fit:.2f}",
        )
        return signal

    def _apply_portfolio_allocator(
        self, signal: SignalCandidate
    ) -> Optional[SignalCandidate]:
        """Margin/slot budget gate — skipped when ENABLE_PORTFOLIO_ALLOCATOR=False."""
        if not Config.ENABLE_PORTFOLIO_ALLOCATOR:
            return signal
        alloc = self.portfolio_allocator.approve(signal)
        if not alloc.approved:
            self.conflict_guard.reject_with_log(
                signal, alloc.reason, context="allocator"
            )
            return None
        meta = dict(signal.structure_metadata or {})
        meta["risk_budget_usdt"] = alloc.risk_budget_usdt
        meta["allocator_size_multiplier"] = alloc.size_multiplier
        signal.structure_metadata = meta
        return signal

    @staticmethod
    def _dedupe_symbol_candidates(
        candidates: list[SignalCandidate],
    ) -> list[SignalCandidate]:
        """Keep one candidate per symbol — first channel wins ties, higher score replaces."""
        if len(candidates) < 2:
            return candidates
        best: dict[str, SignalCandidate] = {}
        order: list[str] = []
        for candidate in candidates:
            key = candidate.symbol.upper()
            existing = best.get(key)
            if existing is None:
                best[key] = candidate
                order.append(key)
                continue
            if candidate.adjusted_score > existing.adjusted_score:
                log_scan_rejected(
                    existing.symbol,
                    (
                        f"Lower score than winner {candidate.strategy} "
                        f"{candidate.action} adj={candidate.adjusted_score:.1f}"
                    ),
                    strategy=existing.strategy,
                )
                best[key] = candidate
        if len(best) == len(candidates):
            return candidates
        return [best[key] for key in order]

    def _process_event(
        self,
        event: CandleCloseEvent,
        *,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> Optional[SignalCandidate]:
        return self._evaluate_symbol(
            event.symbol,
            bar_open_ms=event.bar_open_ms,
            timeframe=event.timeframe,
            open_symbols=open_symbols,
            ticker_map=ticker_map,
            book_map=book_map,
            mark_event=event,
        )

    def _fast_track_live_spikes(self) -> None:
        """Promote WS volume/volatility spikes onto the priority queue this cycle."""
        try:
            hot = self.universe_builder.observe_live_tickers()
        except Exception:
            return
        if not hot:
            return
        promoted = self.priority_queue.fast_track(hot)
        if not promoted:
            return
        cap = max(Config.TIER1_WATCHLIST_SIZE, Config.TOP_UNIVERSE_POOL_SIZE)
        existing = {s.upper() for s in self._tier1_symbols}
        for sym in promoted:
            if sym not in existing and len(self._tier1_symbols) < cap:
                self._tier1_symbols.append(sym)
                existing.add(sym)
        if self._hub:
            self._hub.subscribe_kline_streams(promoted)
        scanner_logger.info(
            "Fast-track priority queue +%s symbols (%s).",
            len(promoted),
            ", ".join(promoted[:8]),
        )

    def _drain_due_event_symbols(
        self,
    ) -> tuple[dict[str, CandleCloseEvent], list[str]]:
        events = self.event_scheduler.drain_due(limit=Config.scan_cycle_symbol_count())
        event_by_symbol: dict[str, CandleCloseEvent] = {}
        symbols: list[str] = []
        for event in events:
            key = str(event.symbol).upper()
            if not key or key in event_by_symbol:
                continue
            event_by_symbol[key] = event
            symbols.append(key)
        return event_by_symbol, symbols

    def _cycle_scan_symbols(self, extra: Optional[list[str]] = None) -> list[str]:
        """Page through up to 60 coins in queue order (5s each)."""
        cap = Config.scan_cycle_symbol_count()
        seen: set[str] = set()
        out: list[str] = []
        rows: list[str] = []
        rows.extend(extra or [])
        try:
            rows.extend(self.priority_queue.hot_symbols or [])
        except Exception:
            pass
        try:
            rows.extend(self.assignment_manager.hot_symbols() or [])
        except Exception:
            pass
        try:
            rows.extend(self.priority_queue.background_symbols or [])
        except Exception:
            pass
        rows.extend(self._tier1_symbols or [])
        for raw in rows:
            key = str(raw or "").upper()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
            if len(out) >= cap:
                break
        return out

    def _execution_scan_symbols(self, *, include_hot: bool) -> list[str]:
        """Top watchlist coins for a lightweight /watchlist refresh."""
        extra = self.priority_queue.hot_symbols if include_hot else []
        return self._cycle_scan_symbols(extra)

    def _resolve_eval_price(self, symbol: str, ticker: dict[str, Any]) -> float:
        """Prefer cached last price; never REST inside scan_context."""
        price = float(self._price_map.get(symbol, 0.0) or 0.0)
        if price <= 0:
            price = float(ticker.get("lastPrice", 0) or 0)
        if price <= 0 and self._hub is not None:
            try:
                cached = self._hub.get_price(symbol)
            except Exception:
                cached = None
            if cached:
                price = float(cached)
        return price if price > 0 else 0.0

    def _ws_ticker_map(self) -> dict[str, Any]:
        if self._hub:
            return self._hub.get_ticker_map() or {}
        return {}

    def _ws_book_map(self, ticker_map: dict[str, Any]) -> dict[str, Any]:
        if self._hub and self._hub.has_ws_book_data():
            return self._hub.get_ws_book_ticker_map()
        return self.universe_builder._ws_book_map(ticker_map)

    def _open_symbols(self) -> set[str]:
        try:
            positions = self.exchange.get_all_open_positions(force_refresh=False)
            return {str(p.get("symbol", "")).upper() for p in positions if p.get("symbol")}
        except Exception:
            return set()

    def _scan_gate_open(self) -> tuple[bool, str]:
        if self._hub:
            return self._hub.is_scan_halted()
        return False, ""

    def _rest_is_blocked(self) -> bool:
        exchange = getattr(self, "exchange", None)
        if exchange is None:
            return False
        checker = getattr(exchange, "is_rest_blocked", None)
        if not callable(checker):
            return False
        try:
            blocked, _reason = checker()
            return bool(blocked)
        except Exception:
            return False

    def tier2_summary(self) -> list[tuple[str, str, float]]:
        if Config.ENABLE_THREE_TIER_FUNNEL:
            return self.funnel.hot_summary()
        return self.assignment_manager.tier2_summary()
