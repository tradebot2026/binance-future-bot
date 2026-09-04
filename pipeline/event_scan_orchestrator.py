"""Event-driven scan orchestrator — candle closes → score → tier → execute."""

from __future__ import annotations

import time
from typing import Any, Optional

from config import Config
from core.assignment_manager import AssignmentManager
from core.event_scheduler import EventScheduler
from core.scoring_engine import ScoringEngine
from core.symbol_conflict_guard import SymbolConflictGuard
from core.types import CandleCloseEvent, SignalCandidate
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import scanner_logger
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
        self.registry = build_strategy_registry(db=db)
        self.universe_builder = UniverseBuilder(exchange, db)
        self.snapshot_factory = SnapshotFactory(exchange)
        self.scoring_engine = ScoringEngine(self.registry)
        self.assignment_manager = AssignmentManager()
        self.event_scheduler = EventScheduler()
        self.conflict_guard = SymbolConflictGuard(exchange, db)
        self._hub = getattr(exchange, "_market_data", None)
        self._tier1_symbols: list[str] = []
        self._price_map: dict[str, float] = {}
        self._volume_ranks: dict[str, int] = {}
        self._last_catchup_at: float = 0.0
        self._last_universe_refresh_at: float = 0.0

    @property
    def tier1_symbols(self) -> list[str]:
        return list(self._tier1_symbols)

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

        universe = self.universe_builder.build()
        cap = min(len(universe.symbols), Config.TIER1_WATCHLIST_SIZE)
        self._tier1_symbols = universe.symbols[:cap]
        self._price_map = universe.price_map
        self._volume_ranks = universe.volume_ranks
        self._last_universe_refresh_at = now

        if self._hub and self._tier1_symbols:
            self._hub.subscribe_kline_streams(self._tier1_symbols)

        scanner_logger.info(
            "Tier1 watchlist refreshed — %s symbols (cap=%s).",
            len(self._tier1_symbols),
            Config.TIER1_WATCHLIST_SIZE,
        )
        return self._tier1_symbols

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

        if not self._tier1_symbols:
            self.refresh_tier1_universe(force=True)

        events = self.event_scheduler.drain_due(limit=Config.TIER2_HOT_SIZE * 3)
        if not events:
            return []

        self.conflict_guard.reset_cycle()
        open_symbols = self._open_symbols()
        ticker_map = self.exchange.get_futures_ticker_map()
        book_map = self.exchange.get_book_ticker_map()
        candidates: list[SignalCandidate] = []

        with self.exchange.scan_context():
            for event in events:
                signal = self._process_event(
                    event,
                    open_symbols=open_symbols,
                    ticker_map=ticker_map,
                    book_map=book_map,
                )
                if signal is not None:
                    candidates.append(signal)

        dict_results = [c.to_dict() for c in candidates]
        if dict_results:
            self.db.update_watchlist(dict_results)
            scanner_logger.info(
                "Event scan produced %s execution candidate(s) from %s event(s).",
                len(dict_results),
                len(events),
            )
        return dict_results

    def _process_event(
        self,
        event: CandleCloseEvent,
        *,
        open_symbols: set[str],
        ticker_map: dict[str, Any],
        book_map: dict[str, Any],
    ) -> Optional[SignalCandidate]:
        symbol = event.symbol.upper()
        ticker = ticker_map.get(symbol, {})
        book = book_map.get(symbol, {})
        price = self._price_map.get(symbol, float(ticker.get("lastPrice", 0) or 0))
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
            return None

        scores = self.scoring_engine.evaluate_symbol(
            snapshot,
            bar_open_ms=event.bar_open_ms,
            timeframe=event.timeframe,
        )
        best = self.scoring_engine.pick_best(scores)
        if best is None:
            self.event_scheduler.mark_evaluated(symbol, event.timeframe, event.bar_open_ms)
            return None

        promoted, demoted, gc_symbol = self.assignment_manager.update(
            best, open_symbols=open_symbols
        )
        if gc_symbol and self._hub:
            self.assignment_manager.gc_demoted(
                gc_symbol, self._hub.demote_symbol_klines
            )
        if promoted and self._hub:
            self._hub.subscribe_kline_streams([symbol])

        self.event_scheduler.mark_evaluated(symbol, event.timeframe, event.bar_open_ms)

        if not self.assignment_manager.is_hot(symbol):
            return None

        signal = self.scoring_engine.signal_for_assignment(snapshot, best)
        if signal is None:
            return None

        ok, reason = self.conflict_guard.approve(signal)
        if not ok:
            self.conflict_guard.reject_with_log(signal, reason)
            return None

        self.db.log_signal(
            {
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "direction": signal.action,
                "score": signal.score,
                "strategy": signal.strategy,
                "reason": f"event={event.timeframe}|regime={signal.regime}",
                "accepted": True,
                "structure_metadata": signal.structure_metadata,
            }
        )
        return signal

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
