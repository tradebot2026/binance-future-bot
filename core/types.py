"""Shared types for the modular strategy pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Optional

import pandas as pd


class RegimeLabel(str, Enum):
    """Market regime classification for strategy routing."""

    STRONG_TREND = "strong_trend"
    RANGE_CHOP = "range_chop"
    COMPRESSION = "compression"
    EXPANSION_SPIKE = "expansion_spike"
    UNCLEAR = "unclear"


Action = Literal["LONG", "SHORT", "NEUTRAL"]


@dataclass(frozen=True)
class MarketSnapshot:
    """Read-only WS-backed market view for a single symbol."""

    symbol: str
    price: float
    ticker: dict[str, Any]
    book: dict[str, Any]
    candles: dict[str, pd.DataFrame]
    spread_pct: float
    volume_24h: float
    regime: RegimeLabel
    timestamp_ms: int
    volume_rank: int = 0
    is_top_volume: bool = False
    derivatives: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalCandidate:
    """Unified strategy output consumed by arbitrator, risk, and executor."""

    symbol: str
    action: Literal["LONG", "SHORT"]
    strategy: str
    score: float
    price: float
    atr: float
    timeframe: str
    regime: str = ""
    confluence: str = ""
    macro_trend: str = "NEUTRAL"
    structure_metadata: dict[str, Any] = field(default_factory=dict)
    regime_fit: float = 1.0
    priority_weight: float = 1.0
    adjusted_score: float = 0.0

    def __post_init__(self) -> None:
        if self.adjusted_score <= 0:
            self.adjusted_score = self.score * self.regime_fit * self.priority_weight

    def to_dict(self) -> dict[str, Any]:
        """Backward-compatible dict for executor and legacy scanner consumers."""
        long_score = self.score if self.action == "LONG" else 0.0
        short_score = self.score if self.action == "SHORT" else 0.0
        return {
            "symbol": self.symbol,
            "action": self.action,
            "direction": self.action,
            "strategy": self.strategy,
            "score": self.score,
            "long_score": long_score,
            "short_score": short_score,
            "price": self.price,
            "atr": self.atr,
            "timeframe": self.timeframe,
            "regime": self.regime,
            "confluence": self.confluence,
            "macro_trend": self.macro_trend,
            "structure_metadata": self.structure_metadata,
            "regime_fit": self.regime_fit,
            "priority_weight": self.priority_weight,
            "adjusted_score": self.adjusted_score,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Optional["SignalCandidate"]:
        action = str(data.get("action", "NEUTRAL")).upper()
        if action not in ("LONG", "SHORT"):
            return None
        return cls(
            symbol=str(data.get("symbol", "")).upper(),
            action=action,  # type: ignore[arg-type]
            strategy=str(data.get("strategy", "")),
            score=float(data.get("score", 0.0)),
            price=float(data.get("price", 0.0)),
            atr=float(data.get("atr", 0.0)),
            timeframe=str(data.get("timeframe", "")),
            regime=str(data.get("regime", "")),
            confluence=str(data.get("confluence", "")),
            macro_trend=str(data.get("macro_trend", "NEUTRAL")),
            structure_metadata=dict(data.get("structure_metadata") or {}),
            regime_fit=float(data.get("regime_fit", 1.0)),
            adjusted_score=float(data.get("adjusted_score", 0.0)),
        )


@dataclass
class AllocationResult:
    """Portfolio allocator decision for a candidate."""

    approved: bool
    risk_budget_usdt: float = 0.0
    size_multiplier: float = 1.0
    reason: str = ""


@dataclass(frozen=True)
class CandleCloseEvent:
    """WS or catch-up candle close trigger."""

    symbol: str
    timeframe: str
    bar_open_ms: int
    source: Literal["ws", "catchup", "timer"] = "ws"
    due_at: float = 0.0


@dataclass
class StrategyResult:
    """
    Standard output from BaseStrategy.scan().
    direction NEUTRAL means no actionable setup.
    """

    symbol: str
    strategy: str
    direction: Action = "NEUTRAL"
    score: float = 0.0
    confidence: float = 0.0
    atr: float = 0.0
    timeframe: str = ""
    key_levels: list[float] = field(default_factory=list)
    level_tags: list[str] = field(default_factory=list)
    confluence: str = ""
    macro_trend: str = "NEUTRAL"
    structure_metadata: dict[str, Any] = field(default_factory=dict)
    correlated_with: list[str] = field(default_factory=list)

    @property
    def is_actionable(self) -> bool:
        return self.direction in ("LONG", "SHORT") and self.score > 0

    @classmethod
    def neutral(cls, symbol: str, strategy: str) -> "StrategyResult":
        return cls(symbol=symbol.upper(), strategy=strategy, direction="NEUTRAL")

    @classmethod
    def from_signal(cls, signal: "SignalCandidate") -> "StrategyResult":
        return cls(
            symbol=signal.symbol,
            strategy=signal.strategy,
            direction=signal.action,
            score=float(signal.score),
            confidence=min(max(float(signal.score) / 100.0, 0.0), 1.0),
            atr=float(signal.atr),
            timeframe=signal.timeframe,
            key_levels=list(
                safe_float_level(v)
                for v in (signal.structure_metadata or {}).get("key_levels", [])
                if safe_float_level(v) > 0
            ),
            level_tags=list((signal.structure_metadata or {}).get("level_tags", [])),
            confluence=signal.confluence,
            macro_trend=signal.macro_trend,
            structure_metadata=dict(signal.structure_metadata or {}),
        )


def safe_float_level(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class StrategyScore:
    """Normalized strategy evaluation for one symbol."""

    symbol: str
    strategy: str
    score: float
    adjusted_score: float
    min_score: float = 0.0
    normalized_score: float = 0.0
    regime_fit: float = 1.0
    priority_weight: float = 1.0
    action: Action = "NEUTRAL"
    bar_open_ms: int = 0
    timeframe: str = ""
    confluence_bonus: float = 0.0
    context_bonus: float = 0.0
    context_multiplier: float = 1.0
    final_score: float = 0.0


@dataclass
class CoinAssignment:
    """Tier-2 active assignment for a symbol."""

    symbol: str
    strategy: str
    score: float
    adjusted_score: float
    normalized_score: float = 0.0
    min_score: float = 0.0
    assigned_bar_open_ms: int = 0
    assigned_at_ms: int = 0
    frozen: bool = False


class CoinLifecycle(str, Enum):
    """Dynamic universe membership — scores decay; DORMANT is not a blacklist."""

    UNSEEN = "UNSEEN"
    DISCOVERED = "DISCOVERED"
    CANDIDATE = "CANDIDATE"
    WATCH = "WATCH"
    ACTIVE = "ACTIVE"
    HOT = "HOT"
    OPPORTUNITY = "OPPORTUNITY"
    WEAKENED = "WEAKENED"
    DORMANT = "DORMANT"


@dataclass(frozen=True)
class ChannelScores:
    """Three opportunity channels, each 0–100."""

    momentum: float = 0.0
    reversal: float = 0.0
    structure: float = 0.0

    @property
    def dominant(self) -> str:
        ranking = (
            ("momentum", self.momentum),
            ("reversal", self.reversal),
            ("structure", self.structure),
        )
        return max(ranking, key=lambda item: item[1])[0]


@dataclass
class CoinOpportunity:
    """WS-only coin ranking snapshot used by universe rotation."""

    symbol: str
    score: float = 0.0
    previous_score: float = 0.0
    velocity: float = 0.0
    relative_rank: int = 0
    channels: ChannelScores = field(default_factory=ChannelScores)
    lifecycle: CoinLifecycle = CoinLifecycle.UNSEEN
    peak_score: float = 0.0
    last_price: float = 0.0
    volume_24h: float = 0.0
    range_pct: float = 0.0
    updated_at: float = 0.0
    first_seen_at: float = 0.0
