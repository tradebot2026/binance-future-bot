"""Directional candidate resolution shared by strategy modules."""

from __future__ import annotations

from typing import Any, Callable, Optional

from config import Config
from core.types import SignalCandidate
from logger import signal_logger
from utils import safe_float


def pick_directional_candidate(
    symbol: str,
    long_candidate: Optional[SignalCandidate],
    short_candidate: Optional[SignalCandidate],
    *,
    win_margin: float,
    resolve_margin: float,
    min_score_fn: Callable[[str, str], float],
    enable_equilibrium_resolve: bool = True,
) -> Optional[SignalCandidate]:
    """Pick LONG/SHORT winner or return None for neutral."""
    long_score = safe_float(long_candidate.score) if long_candidate else 0.0
    short_score = safe_float(short_candidate.score) if short_candidate else 0.0

    if long_score <= 0 and short_score <= 0:
        return None

    if long_score > 0 and short_score <= 0:
        return long_candidate
    if short_score > 0 and long_score <= 0:
        return short_candidate

    gap = abs(long_score - short_score)
    winner = long_candidate if long_score >= short_score else short_candidate
    loser_score = min(long_score, short_score)
    winner_score = max(long_score, short_score)

    if gap >= win_margin:
        signal_logger.info(
            "Direction winner | %s LONG=%.1f SHORT=%.1f gap=%.1f",
            symbol,
            long_score,
            short_score,
            gap,
        )
        return long_candidate if long_score > short_score else short_candidate

    if (
        enable_equilibrium_resolve
        and gap >= resolve_margin
        and winner
        and winner.macro_trend == "NEUTRAL"
    ):
        min_required = min_score_fn(winner.confluence, "NEUTRAL")
        if winner_score >= min_required:
            signal_logger.info(
                "Equilibrium resolved | %s %s score=%.1f vs %.1f gap=%.1f min=%.1f",
                symbol,
                winner.action,
                winner_score,
                loser_score,
                gap,
                min_required,
            )
            return winner

    signal_logger.info(
        "Equilibrium skip | %s LONG=%.1f SHORT=%.1f gap=%.1f (<%.1f)",
        symbol,
        long_score,
        short_score,
        gap,
        resolve_margin,
    )
    return None
