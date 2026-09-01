"""Unified modular strategy scan pipeline."""

from __future__ import annotations

import time
from collections import Counter
from typing import Iterable, Optional

from config import Config
from constants import STRATEGY_RANGE_REVERSION, STRATEGY_SMC_TREND
from core.candidate_arbitrator import CandidateArbitrator
from core.portfolio_allocator import PortfolioAllocator
from core.regime_router import RegimeRouter
from core.strategy_registry import StrategyRegistry
from core.symbol_conflict_guard import SymbolConflictGuard
from core.types import SignalCandidate
from database import DatabaseManager
from exchange import BinanceExchangeManager
from logger import error_logger, scanner_logger, signal_logger
from pipeline.snapshot_factory import SnapshotFactory
from pipeline.universe_builder import UniverseBuilder
from smc_engine import effective_smc_min_score
from strategies import build_strategy_registry
from utils import safe_float


class StrategyScannerPipeline:
    """
    Orchestrates universe build, regime routing, strategy evaluation,
    arbitration, conflict guard, and portfolio allocation.
    """

    def __init__(
        self,
        exchange: BinanceExchangeManager,
        db: DatabaseManager,
        registry: Optional[StrategyRegistry] = None,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.registry = registry or build_strategy_registry(db=db)
        self.universe_builder = UniverseBuilder(exchange, db)
        self.snapshot_factory = SnapshotFactory(exchange)
        self.conflict_guard = SymbolConflictGuard(exchange, db)
        self.portfolio_allocator = PortfolioAllocator(exchange, db)
        self._hub = getattr(exchange, "_market_data", None)
        self._scan_priority: list[str] = []
        self._near_miss_scores: dict[str, float] = {}
        self._last_universe_symbols: list[str] = []

    @property
    def last_universe_symbols(self) -> list[str]:
        return list(self._last_universe_symbols)

    def _scan_gate_open(self) -> tuple[bool, str]:
        if self._hub:
            return self._hub.is_scan_halted()
        return False, ""

    def _subscribe_klines(self, symbols: list[str]) -> None:
        """Subscribe WS kline streams only — no REST (bootstrap runs outside scan loop)."""
        if not self._hub or not symbols:
            return
        timeframes = [
            Config.ENTRY_TIMEFRAME,
            Config.CONFIRM_TIMEFRAME,
            Config.TREND_TIMEFRAME,
        ]
        self._hub.subscribe_kline_streams(symbols, timeframes)

    def run(
        self,
        *,
        strategy_tags: Optional[Iterable[str]] = None,
        max_results: Optional[int] = None,
        use_regime_filter: bool = False,
        apply_conflict_guard: bool = True,
        apply_portfolio_allocator: bool = False,
        reset_conflict_cycle: bool = True,
    ) -> list[dict]:
        """
        Execute a full scan cycle and return backward-compatible candidate dicts.
        """
        halted, halt_reason = self._scan_gate_open()
        if halted:
            scanner_logger.warning("Scan skipped — %s", halt_reason)
            return []

        strategies = self.registry.filter_tags(strategy_tags)
        if not strategies:
            scanner_logger.warning("No enabled strategies matched filter.")
            return []

        tag_label = ",".join(s.tag for s in strategies)
        scanner_logger.info(
            "Pipeline scan starting | strategies=%s | pair_delay=%.2fs",
            tag_label,
            Config.SCAN_PAIR_DELAY_SECONDS,
        )

        if reset_conflict_cycle:
            self.conflict_guard.reset_cycle()

        started = time.time()
        universe = self.universe_builder.build(priority_symbols=self._scan_priority)
        symbols = universe.symbols
        self._last_universe_symbols = symbols
        if not symbols:
            scanner_logger.warning("No tradable symbols found.")
            return []

        self._subscribe_klines(symbols)
        ticker_map = self.exchange.get_futures_ticker_map()
        book_map = self.exchange.get_book_ticker_map()

        winners: list[SignalCandidate] = []
        rejection_stats: Counter[str] = Counter()
        scanned = 0
        cap = max_results or Config.MAX_POSITIONS

        with self.exchange.scan_context():
            for symbol in symbols:
                if time.time() - started > Config.SCAN_TIMEOUT_SEC:
                    scanner_logger.warning(
                        "Scan timeout after %ss — partial results (%s/%s).",
                        Config.SCAN_TIMEOUT_SEC,
                        scanned,
                        len(symbols),
                    )
                    break

                ticker = ticker_map.get(symbol, {})
                book = book_map.get(symbol, {})
                price = universe.price_map.get(symbol, safe_float(ticker.get("lastPrice")))
                volume_24h = safe_float(ticker.get("quoteVolume"))
                volume_rank = universe.volume_ranks.get(symbol, 0)

                snapshot = self.snapshot_factory.build(
                    symbol,
                    price=price,
                    ticker=ticker,
                    book=book,
                    volume_24h=volume_24h,
                    volume_rank=volume_rank,
                )
                if snapshot is None:
                    rejection_stats["snapshot_miss"] += 1
                    continue

                regime = snapshot.regime
                symbol_candidates: list[SignalCandidate] = []

                for strategy in strategies:
                    if use_regime_filter:
                        allowed = strategy.allowed_regimes()
                        if allowed and regime.value not in allowed:
                            continue
                        regime_tags = RegimeRouter.strategies_for_regime(regime)
                        if regime_tags and strategy.tag not in regime_tags:
                            if not self._legacy_single_strategy_mode(strategy_tags):
                                continue

                    if strategy.requires_top_volume() and not snapshot.is_top_volume:
                        continue

                    fit = strategy.regime_fit(snapshot)
                    if fit <= 0:
                        continue

                    try:
                        signal = strategy.evaluate(snapshot)
                    except Exception as exc:
                        error_logger.error(
                            "Strategy %s failed on %s: %s", strategy.tag, symbol, exc
                        )
                        continue

                    if signal is None:
                        continue

                    signal = CandidateArbitrator.apply_regime_fit(signal, fit)
                    if not self._passes_min_score(signal):
                        rejection_stats["below_min_score"] += 1
                        continue

                    symbol_candidates.append(signal)

                winner, losers = CandidateArbitrator.pick_symbol_winner(
                    symbol, symbol_candidates
                )
                for loser in losers:
                    self.conflict_guard.reject_with_log(
                        loser, "Lost symbol arbitration", context="arbitrator"
                    )

                scanned += 1
                if winner is None:
                    rejection_stats["neutral"] += 1
                else:
                    if apply_conflict_guard:
                        ok, reason = self.conflict_guard.approve(winner)
                        if not ok:
                            self.conflict_guard.reject_with_log(winner, reason)
                            rejection_stats["conflict"] += 1
                        else:
                            if apply_portfolio_allocator:
                                alloc = self.portfolio_allocator.approve(winner)
                                if not alloc.approved:
                                    self.conflict_guard.reject_with_log(
                                        winner, alloc.reason, context="allocator"
                                    )
                                    rejection_stats["allocator"] += 1
                                else:
                                    winners.append(winner)
                                    self._log_accepted_signal(winner)
                            else:
                                winners.append(winner)
                                self._log_accepted_signal(winner)
                    else:
                        winners.append(winner)
                        self._log_accepted_signal(winner)

                if Config.SCAN_PAIR_DELAY_SECONDS > 0:
                    time.sleep(Config.SCAN_PAIR_DELAY_SECONDS)

        ranked = CandidateArbitrator.rank_global(winners, cap)
        dict_results = [c.to_dict() for c in ranked]

        if ranked:
            self.db.update_watchlist(dict_results)
            for item in ranked:
                scanner_logger.info(
                    "Candidate | %s | %s | %s | score=%.1f adj=%.1f | price=%.6f",
                    item.symbol,
                    item.strategy,
                    item.action,
                    item.score,
                    item.adjusted_score,
                    item.price,
                )
        else:
            scanner_logger.info(
                "Pipeline scan complete — no candidates (scanned=%s/%s stats=%s).",
                scanned,
                len(symbols),
                dict(rejection_stats),
            )

        self._finalize_scan_priority()
        scanner_logger.info("Pipeline scan finished in %.2fs.", time.time() - started)
        return dict_results

    @staticmethod
    def _legacy_single_strategy_mode(strategy_tags: Optional[Iterable[str]]) -> bool:
        if not strategy_tags:
            return False
        tags = {str(t).upper() for t in strategy_tags}
        return len(tags) == 1

    def _passes_min_score(self, signal: SignalCandidate) -> bool:
        if signal.strategy == STRATEGY_RANGE_REVERSION:
            return signal.score >= Config.RANGE_MIN_SCORE
        if signal.strategy == STRATEGY_SMC_TREND:
            confluence = signal.confluence
            macro = signal.macro_trend or "NEUTRAL"
            return signal.score >= effective_smc_min_score(confluence, macro)
        return signal.score >= Config.STRATEGY_MIN_SCORE

    def _log_accepted_signal(self, signal: SignalCandidate) -> None:
        self.db.log_signal(
            {
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "direction": signal.action,
                "score": signal.score,
                "strategy": signal.strategy,
                "reason": f"regime={signal.regime}|confluence={signal.confluence}",
                "accepted": True,
                "structure_metadata": signal.structure_metadata,
            }
        )
        if Config.NEAR_MISS_SCORE_MIN <= signal.score <= Config.NEAR_MISS_SCORE_MAX:
            prev = self._near_miss_scores.get(signal.symbol, 0.0)
            if signal.score >= prev:
                self._near_miss_scores[signal.symbol] = signal.score

    def _finalize_scan_priority(self) -> None:
        ranked = sorted(
            self._near_miss_scores.items(),
            key=lambda item: item[1],
            reverse=True,
        )
        self._scan_priority = [
            symbol for symbol, _ in ranked[: Config.NEAR_MISS_PRIORITY_MAX]
        ]

    def scan_smc(self) -> list[dict]:
        """Backward-compatible SMC-only scan wrapper."""
        return self.run(
            strategy_tags=[STRATEGY_SMC_TREND],
            max_results=Config.MAX_SMC_POSITIONS,
            use_regime_filter=False,
            apply_conflict_guard=True,
            reset_conflict_cycle=True,
        )

    def scan_range(self) -> list[dict]:
        """Backward-compatible RANGE-only scan wrapper."""
        if not Config.ENABLE_RANGE_REGIME and not Config.ENABLE_STRATEGY_RANGE:
            return []
        return self.run(
            strategy_tags=[STRATEGY_RANGE_REVERSION],
            max_results=Config.MAX_RANGE_POSITIONS,
            use_regime_filter=False,
            apply_conflict_guard=True,
            apply_portfolio_allocator=Config.ENABLE_PORTFOLIO_ALLOCATOR,
            reset_conflict_cycle=False,
        )

    def scan_unified(self) -> list[dict]:
        """Run all enabled strategies with regime filter and full guards."""
        return self.run(
            strategy_tags=None,
            max_results=Config.MAX_ENTRIES_PER_CYCLE,
            use_regime_filter=True,
            apply_conflict_guard=True,
            apply_portfolio_allocator=Config.ENABLE_PORTFOLIO_ALLOCATOR,
            reset_conflict_cycle=True,
        )
