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
from core.types import CandleCloseEvent, SignalCandidate
from database import DatabaseManager
from exchange import BinanceExchangeManager
from executor import log_execution_rejected, log_scan_rejected
from logger import log_trade_approved, scanner_logger
from pipeline.snapshot_factory import SnapshotFactory
from pipeline.universe_builder import UniverseBuilder
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

    def refresh_tier1_universe(self, *, force: bool = False) -> list[str]:
        """Rebuild Tier-1 watchlist and subscribe WS klines."""
        now = time.monotonic()
        if (
            not force
            and self._tier1_symbols
            and (now - self._last_universe_refresh_at)
            < Config.EVENT_CATCHUP_INTERVAL_SECONDS
        ):
            return self._tier1_symbols

        if self._hub:
            ready = self._hub.ensure_ticker_cache_ready(
                rest_seeder=self.exchange.fetch_futures_ticker_map_rest,
            )
            if not ready:
                scanner_logger.warning(
                    "Tier1 refresh skipped — ticker cache unavailable (WS+REST)."
                )
                return self._tier1_symbols

        universe = self.universe_builder.build()
        pool_cap = min(len(universe.symbols), Config.TOP_UNIVERSE_POOL_SIZE)
        self._tier1_symbols = universe.symbols[:pool_cap]
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

    def maybe_refresh_tier1_periodic(self) -> None:
        """Rebuild Tier-1 from live volume ranks on a fixed interval (default 30 min)."""
        now = time.monotonic()
        if not self._tier1_symbols:
            self.refresh_tier1_universe(force=True)
            return
        if (
            now - self._last_universe_refresh_at
        ) < Config.TIER1_REFRESH_INTERVAL_SECONDS:
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
        Tiered scan cycle:
        - Hot watchlist: WS-only poll every HOT_SCAN_INTERVAL_SECONDS
        - Background queue: rotate small batches with pacing
        - Candle-close events: existing event-driven path
        """
        halted, reason = self._scan_gate_open()
        if halted:
            scanner_logger.warning("Priority scan skipped — %s", reason)
            return []

        self.maybe_refresh_tier1_periodic()
        if not self._tier1_symbols:
            self.refresh_tier1_universe()

        self.run_catchup()
        self.conflict_guard.reset_cycle()

        self._fast_track_live_spikes()

        candidates: list[SignalCandidate] = []
        candidates.extend(self.process_hot_scan_cycle())
        candidates.extend(self.process_background_scan_cycle())
        candidates.extend(self._process_due_event_candidates())
        candidates = self._dedupe_symbol_candidates(candidates)

        universe_total = len(self.priority_queue.full_universe) or len(
            self._tier1_symbols
        )
        hot_count = len(self.priority_queue.hot_symbols)
        touch_scan_cycle(
            scanned=max(hot_count, universe_total),
            universe_total=max(universe_total, hot_count),
        )

        dict_results = [c.to_dict() for c in candidates]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Priority scan dispatching %s execution candidate(s).",
                len(dict_results),
            )
        else:
            scanner_logger.info(
                "Priority scan produced 0 execution candidates "
                "(tier2=%s hot=%s background=%s).",
                self.assignment_manager.tier2_size,
                hot_count,
                len(self.priority_queue.background_symbols),
            )
        return dict_results

    def process_hot_scan_cycle(self) -> list[SignalCandidate]:
        """Tier 1 — frequent WS-only scan of high-activity + Tier-2 symbols."""
        run_hot = self.priority_queue.should_run_hot_scan()
        symbols = self._execution_scan_symbols(include_hot=run_hot)
        if not symbols:
            return []

        open_symbols = self._open_symbols()
        ticker_map = self._ws_ticker_map()
        book_map = self._ws_book_map(ticker_map)
        trigger_tfs = Config.get_scan_trigger_timeframes()
        primary_tf = trigger_tfs[0] if trigger_tfs else Config.ENTRY_TIMEFRAME
        candidates: list[SignalCandidate] = []

        with self.exchange.scan_context():
            for symbol in symbols:
                bar_open_ms = 0
                if self._hub:
                    closed = self._hub.get_last_closed_bar_open_ms(symbol, primary_tf)
                    if closed:
                        bar_open_ms = closed
                signal = self._evaluate_symbol(
                    symbol,
                    bar_open_ms=bar_open_ms,
                    timeframe=primary_tf,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                )
                if signal is not None:
                    candidates.append(signal)

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
        candidates: list[SignalCandidate] = []

        with self.exchange.scan_context():
            for symbol in batch:
                bar_open_ms = 0
                if self._hub:
                    closed = self._hub.get_last_closed_bar_open_ms(symbol, primary_tf)
                    if closed:
                        bar_open_ms = closed
                signal = self._evaluate_symbol(
                    symbol,
                    bar_open_ms=bar_open_ms,
                    timeframe=primary_tf,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                )
                if signal is not None:
                    candidates.append(signal)

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
        candidates: list[SignalCandidate] = []

        with self.exchange.scan_context():
            for event in events:
                signal = self._evaluate_symbol(
                    event.symbol,
                    bar_open_ms=event.bar_open_ms,
                    timeframe=event.timeframe,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                    mark_event=event,
                )
                if signal is not None:
                    candidates.append(signal)

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
            log_scan_rejected(
                symbol, "snapshot unavailable (WS kline cache miss)"
            )
            if mark_event is not None:
                self.event_scheduler.mark_evaluated(
                    symbol, mark_event.timeframe, mark_event.bar_open_ms
                )
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

    def _execution_scan_symbols(self, *, include_hot: bool) -> list[str]:
        """Hot queue plus already-promoted Tier-2 names (execution, not a 4th scanner)."""
        symbols: list[str] = []
        seen: set[str] = set()
        rows: list[str] = []
        if include_hot:
            rows.extend(self.priority_queue.hot_symbols)
        rows.extend(self.assignment_manager.hot_symbols())
        for raw in rows:
            key = str(raw).upper()
            if not key or key in seen:
                continue
            seen.add(key)
            symbols.append(key)
        return symbols

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

    def tier2_summary(self) -> list[tuple[str, str, float]]:
        return self.assignment_manager.tier2_summary()
