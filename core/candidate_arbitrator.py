"""Merge and rank candidates from multiple strategies."""

from __future__ import annotations

from typing import Optional

from config import Config
from core.types import SignalCandidate
from logger import signal_logger
from utils import safe_float


class CandidateArbitrator:
    """Pick per-symbol winners and rank global candidate batch."""

    @staticmethod
    def apply_regime_fit(candidate: SignalCandidate, regime_fit: float) -> SignalCandidate:
        candidate.regime_fit = max(0.0, min(1.0, regime_fit))
        candidate.adjusted_score = (
            candidate.score * candidate.regime_fit * candidate.priority_weight
        )
        return candidate

    @staticmethod
    def pick_symbol_winner(
        symbol: str,
        candidates: list[SignalCandidate],
    ) -> tuple[Optional[SignalCandidate], list[SignalCandidate]]:
        """
        Select best candidate for one symbol.
        Returns (winner, losers) for conflict logging.
        """
        valid = [c for c in candidates if c.action in ("LONG", "SHORT")]
        if not valid:
            return None, []

        if len(valid) == 1:
            return valid[0], []

        longs = [c for c in valid if c.action == "LONG"]
        shorts = [c for c in valid if c.action == "SHORT"]

        if longs and shorts:
            best_long = max(longs, key=CandidateArbitrator._sort_key)
            best_short = max(shorts, key=CandidateArbitrator._sort_key)
            if CandidateArbitrator._sort_key(best_long) >= CandidateArbitrator._sort_key(
                best_short
            ):
                winner = best_long
                losers = shorts + [c for c in longs if c is not winner]
            else:
                winner = best_short
                losers = longs + [c for c in shorts if c is not winner]
            signal_logger.info(
                "Arbitrator resolved %s conflict | winner=%s %s adj=%.1f | rejected=%s",
                symbol,
                winner.strategy,
                winner.action,
                winner.adjusted_score,
                len(losers),
            )
            return winner, losers

        winner = max(valid, key=CandidateArbitrator._sort_key)
        losers = [c for c in valid if c is not winner]
        return winner, losers

    @staticmethod
    def rank_global(
        candidates: list[SignalCandidate],
        max_results: int,
    ) -> list[SignalCandidate]:
        ranked = sorted(candidates, key=CandidateArbitrator._sort_key, reverse=True)
        return ranked[:max_results]

    @staticmethod
    def _sort_key(candidate: SignalCandidate) -> float:
        adjusted = safe_float(candidate.adjusted_score)
        if adjusted <= 0:
            adjusted = safe_float(candidate.score)
        volume_bonus = 0.0
        meta = candidate.structure_metadata or {}
        if meta.get("volume_24h"):
            volume_bonus = min(safe_float(meta["volume_24h"]) / 1_000_000_000.0, 0.5)
        return adjusted + volume_bonus
