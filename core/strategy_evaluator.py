"""Unified multi-strategy evaluation — scan, correlate, score."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import Config
from core.confluence_scorer import ConfluenceScorer
from core.context.institutional_context import InstitutionalContextEvaluator
from core.correlation_guard import CorrelationGuard
from core.strategy_registry import StrategyRegistry
from core.types import MarketSnapshot, StrategyResult, StrategyScore
from logger import error_logger


class StrategyEvaluator:
    """
    Strategy Engine -> Correlation Guard -> Confluence Scorer
    Shared by event-driven and batch scan paths.
    """

    def __init__(
        self,
        registry: StrategyRegistry,
        *,
        correlation_guard: Optional[CorrelationGuard] = None,
        confluence_scorer: Optional[ConfluenceScorer] = None,
        institutional: Optional[InstitutionalContextEvaluator] = None,
    ) -> None:
        self.registry = registry
        self.correlation_guard = correlation_guard or CorrelationGuard()
        self.confluence_scorer = confluence_scorer or ConfluenceScorer(
            {s.tag: s for s in registry.all()}
        )
        self.institutional = institutional

    def evaluate(
        self,
        snapshot: MarketSnapshot,
        *,
        bar_open_ms: int = 0,
        timeframe: str = "",
    ) -> list[StrategyScore]:
        scores, _deduped = self.evaluate_detailed(
            snapshot, bar_open_ms=bar_open_ms, timeframe=timeframe
        )
        return scores

    def evaluate_detailed(
        self,
        snapshot: MarketSnapshot,
        *,
        bar_open_ms: int = 0,
        timeframe: str = "",
    ) -> tuple[list[StrategyScore], list[StrategyResult]]:
        institutional_context = None
        if self.institutional is not None:
            institutional_context = self.institutional.evaluate(snapshot)

        results = self._scan_all(snapshot)
        deduped = self.correlation_guard.deduplicate(results)
        scores = self.confluence_scorer.score_results(
            snapshot,
            deduped,
            bar_open_ms=bar_open_ms,
            timeframe=timeframe,
            institutional_context=institutional_context,
        )
        return scores, deduped

    def evaluate_batch(
        self,
        snapshots: list[MarketSnapshot],
        *,
        bar_open_ms: int = 0,
        timeframe: str = "",
    ) -> dict[str, list[StrategyScore]]:
        """Parallel multi-pair evaluation (thread pool — WS-safe, no extra REST)."""
        if not snapshots:
            return {}
        if not Config.ENABLE_ASYNC_STRATEGY_SCAN or len(snapshots) == 1:
            return {
                s.symbol: self.evaluate(s, bar_open_ms=bar_open_ms, timeframe=timeframe)
                for s in snapshots
            }

        ctx_map = {}
        if self.institutional is not None:
            ctx_map = self.institutional.evaluate_batch(snapshots)

        workers = max(min(Config.MAX_WORKERS, len(snapshots)), 1)
        output: dict[str, list[StrategyScore]] = {}

        def _run(snap: MarketSnapshot) -> tuple[str, list[StrategyScore]]:
            results = self._scan_all(snap)
            deduped = self.correlation_guard.deduplicate(results)
            ctx = ctx_map.get(snap.symbol)
            scores = self.confluence_scorer.score_results(
                snap,
                deduped,
                bar_open_ms=bar_open_ms,
                timeframe=timeframe,
                institutional_context=ctx,
            )
            return snap.symbol, scores

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_run, snap) for snap in snapshots]
            for future in as_completed(futures):
                try:
                    symbol, scores = future.result()
                    output[symbol] = scores
                except Exception as exc:
                    error_logger.error("Async strategy eval failed: %s", exc)
        return output

    def _scan_all(self, snapshot: MarketSnapshot) -> list[StrategyResult]:
        results: list[StrategyResult] = []
        for strategy in self.registry.enabled():
            if strategy.requires_top_volume() and not snapshot.is_top_volume:
                continue

            fit = strategy.regime_fit(snapshot)
            if fit <= 0:
                continue

            allowed = strategy.allowed_regimes()
            if allowed and snapshot.regime.value not in allowed:
                continue

            try:
                if hasattr(strategy, "scan"):
                    result = strategy.scan(
                        snapshot.symbol,
                        snapshot.candles,
                        snapshot,
                    )
                else:
                    signal = strategy.evaluate(snapshot)
                    if signal is None:
                        result = StrategyResult.neutral(snapshot.symbol, strategy.tag)
                    else:
                        result = StrategyResult.from_signal(signal)
            except Exception as exc:
                error_logger.error(
                    "Strategy %s scan failed on %s: %s",
                    strategy.tag,
                    snapshot.symbol,
                    exc,
                )
                continue

            if result.is_actionable:
                results.append(result)
        return results
