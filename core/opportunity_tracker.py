"""WS-only coin opportunity scoring, velocity, rank, and lifecycle.

This is ranking state for UniverseBuilder / ScanPriorityQueue — not a scan loop.
All inputs come from local ticker/kline caches. No REST.

OI / open-interest proxy (miniTicker limitation)
------------------------------------------------
Binance ``!miniTicker@arr`` does **not** include open interest or funding.
The tracker therefore cannot detect true OI spikes from the WS ticker stream.

``OpportunityTracker._is_spike`` approximates an OI burst with the fields
that *are* on miniTicker (plus optional cached kline extras):

* **Price move** vs the previous ingest (``HOT_PRICE_MOVE_PCT``)
* **24h quote-volume ratio** vs the previous ingest (``HOT_VOLUME_RATIO``)
* **24h range expansion** vs the previous ingest (``HOT_RANGE_EXPANSION_PCT``)
* **Kline volume burst** from WS-cached bars when the coin is already HOT /
  OPPORTUNITY / ACTIVE / WATCH (``volume_burst >= 1.4``)

This is a volume/price/range proxy, not exchange OI. Real OI/funding still
comes from the separate cached derivatives path used by ``OI_FUNDING``
context — never from ranking/sub-scans, which stay REST-blocked.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

from config import Config
from core.types import ChannelScores, CoinLifecycle, CoinOpportunity
from utils import safe_float

HOT_STATES = {
    CoinLifecycle.HOT,
    CoinLifecycle.OPPORTUNITY,
}
SCANNABLE_STATES = {
    CoinLifecycle.CANDIDATE,
    CoinLifecycle.WATCH,
    CoinLifecycle.ACTIVE,
    CoinLifecycle.HOT,
    CoinLifecycle.OPPORTUNITY,
    CoinLifecycle.WEAKENED,
}
PRIORITY_STATES = {
    CoinLifecycle.HOT,
    CoinLifecycle.OPPORTUNITY,
}


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _unit(value: float, high: float) -> float:
    if high <= 0:
        return 0.0
    return _clamp((max(value, 0.0) / high) * 100.0)


def _percentile_rank(value: float, ordered: list[float]) -> float:
    """Percentile 0–100 of value within ordered sample (ascending)."""
    if not ordered:
        return 50.0
    n = len(ordered)
    below = 0
    for item in ordered:
        if item < value:
            below += 1
        else:
            break
    return _clamp(100.0 * below / max(n - 1, 1))


@dataclass(frozen=True)
class TickerFeatures:
    """Normalized WS ticker fields for one symbol."""

    symbol: str
    last_price: float
    open_price: float
    high_price: float
    low_price: float
    volume_24h: float
    spread_pct: float
    range_pct: float
    change_pct: float
    atr_pct: float = 0.0
    bar_range_ratio: float = 0.0
    wick_rejection: float = 0.0
    volume_burst: float = 0.0
    structure_consistency: float = 0.0
    compression: float = 0.0


@dataclass
class UniverseScoreContext:
    volume_values: list[float] = field(default_factory=list)
    range_values: list[float] = field(default_factory=list)
    median_volume: float = 1.0
    median_range: float = 1.0


def build_score_context(features: Iterable[TickerFeatures]) -> UniverseScoreContext:
    volumes = sorted(max(row.volume_24h, 0.0) for row in features)
    ranges = sorted(max(row.range_pct, 0.0) for row in features)
    mid_v = volumes[len(volumes) // 2] if volumes else 1.0
    mid_r = ranges[len(ranges) // 2] if ranges else 1.0
    return UniverseScoreContext(
        volume_values=volumes,
        range_values=ranges,
        median_volume=max(mid_v, 1.0),
        median_range=max(mid_r, 0.01),
    )


def score_channels(
    row: TickerFeatures,
    ctx: UniverseScoreContext,
) -> ChannelScores:
    """Three-channel 0–100 scores from WS ticker (+ optional cached kline extras)."""
    vol_pct = _percentile_rank(row.volume_24h, ctx.volume_values)
    range_pctile = _percentile_rank(row.range_pct, ctx.range_values)
    abs_change = abs(row.change_pct)
    near_high = 0.0
    near_low = 0.0
    if row.high_price > row.low_price > 0 and row.last_price > 0:
        span = row.high_price - row.low_price
        near_high = _clamp(100.0 * (1.0 - (row.high_price - row.last_price) / span))
        near_low = _clamp(100.0 * (1.0 - (row.last_price - row.low_price) / span))
    extreme = max(near_high, near_low)

    expansion = row.bar_range_ratio if row.bar_range_ratio > 0 else range_pctile
    momentum = (
        0.35 * _unit(abs_change, 6.0)
        + 0.25 * _unit(row.range_pct, 8.0)
        + 0.20 * vol_pct
        + 0.20 * _clamp(expansion)
    )

    absorption = _clamp(vol_pct - _unit(abs_change, 4.0))
    spread_tight = _clamp(100.0 - _unit(row.spread_pct, 0.12))
    wick = row.wick_rejection if row.wick_rejection > 0 else extreme * 0.6
    reversal = (
        0.35 * extreme
        + 0.25 * _clamp(wick)
        + 0.25 * absorption
        + 0.15 * spread_tight
    )

    compression = row.compression
    if compression <= 0:
        compression = _clamp(100.0 - range_pctile)
    mid_range = 100.0 - abs(extreme - 50.0) * 2.0
    structure_leg = (
        row.structure_consistency if row.structure_consistency > 0 else mid_range
    )
    structure = (
        0.40 * _clamp(compression)
        + 0.30 * _clamp(structure_leg)
        + 0.30 * _clamp(mid_range)
    )

    if row.atr_pct > 0:
        atr_boost = _unit(row.atr_pct, 1.2) * 0.08
        momentum = _clamp(momentum + atr_boost)
        structure = _clamp(structure + (100.0 - _unit(row.atr_pct, 1.2)) * 0.05)

    return ChannelScores(
        momentum=_clamp(momentum),
        reversal=_clamp(reversal),
        structure=_clamp(structure),
    )


def composite_opportunity_score(channels: ChannelScores) -> float:
    total_w = (
        Config.OPPORTUNITY_MOMENTUM_WEIGHT
        + Config.OPPORTUNITY_REVERSAL_WEIGHT
        + Config.OPPORTUNITY_STRUCTURE_WEIGHT
    )
    if total_w <= 0:
        total_w = 1.0
    raw = (
        Config.OPPORTUNITY_MOMENTUM_WEIGHT * channels.momentum
        + Config.OPPORTUNITY_REVERSAL_WEIGHT * channels.reversal
        + Config.OPPORTUNITY_STRUCTURE_WEIGHT * channels.structure
    ) / total_w
    return _clamp(raw)


def _decay_score(score: float, elapsed_seconds: float) -> float:
    if score <= 0 or elapsed_seconds <= 0:
        return score
    half_life_min = max(Config.OPPORTUNITY_SCORE_HALF_LIFE_MINUTES, 1.0)
    half_life_s = half_life_min * 60.0
    return score * math.pow(0.5, elapsed_seconds / half_life_s)


class OpportunityTracker:
    """Persistent score / velocity / lifecycle memory across universe refreshes."""

    def __init__(self) -> None:
        self._records: dict[str, CoinOpportunity] = {}
        self._prev_price: dict[str, float] = {}
        self._prev_volume: dict[str, float] = {}
        self._prev_range: dict[str, float] = {}

    @property
    def records(self) -> dict[str, CoinOpportunity]:
        return self._records

    def get(self, symbol: str) -> Optional[CoinOpportunity]:
        return self._records.get(symbol.upper())

    def opportunity_scores(self) -> dict[str, float]:
        return {sym: rec.score for sym, rec in self._records.items()}

    def score_velocity(self) -> dict[str, float]:
        return {sym: rec.velocity for sym, rec in self._records.items()}

    def relative_ranks(self) -> dict[str, int]:
        return {sym: rec.relative_rank for sym, rec in self._records.items()}

    def lifecycle_map(self) -> dict[str, str]:
        return {sym: rec.lifecycle.value for sym, rec in self._records.items()}

    def known_symbols(self) -> set[str]:
        """Symbols with live ranking state (excludes dormant)."""
        return {
            rec.symbol.upper()
            for rec in self._records.values()
            if rec.lifecycle != CoinLifecycle.DORMANT
        }

    def hot_symbols(self) -> list[str]:
        rows = [
            rec
            for rec in self._records.values()
            if rec.lifecycle in HOT_STATES
        ]
        rows.sort(key=lambda rec: (rec.score, rec.velocity), reverse=True)
        return [rec.symbol for rec in rows]

    def ingest(
        self,
        features: list[TickerFeatures],
        *,
        primary_symbols: Optional[set[str]] = None,
        now: Optional[float] = None,
    ) -> list[str]:
        """
        Update scores/lifecycle from a WS ticker snapshot.
        Returns symbols that should be fast-tracked this cycle.
        """
        now = time.monotonic() if now is None else now
        primary = {s.upper() for s in (primary_symbols or set())}
        if not features:
            self._decay_missing(set(), now)
            return []

        ctx = build_score_context(features)
        present: set[str] = set()
        newly_hot: list[tuple[float, str]] = []

        for row in features:
            symbol = row.symbol.upper()
            present.add(symbol)
            channels = score_channels(row, ctx)
            fresh = composite_opportunity_score(channels)
            spiked = self._is_spike(row)
            record = self._records.get(symbol)
            elapsed = 0.0 if record is None else max(0.0, now - record.updated_at)
            if record is not None and elapsed > 0:
                fresh = max(fresh, _decay_score(record.score, elapsed) * 0.15 + fresh * 0.85)

            previous = record.score if record is not None else 0.0
            velocity = fresh - previous
            peak = max(fresh, record.peak_score if record is not None else 0.0)
            first_seen = record.first_seen_at if record is not None else now
            prev_life = record.lifecycle if record is not None else CoinLifecycle.UNSEEN
            lifecycle = self._next_lifecycle(
                prev_life,
                score=fresh,
                velocity=velocity,
                peak=peak,
                spiked=spiked,
                in_primary=symbol in primary,
                stale_seconds=elapsed,
            )
            self._records[symbol] = CoinOpportunity(
                symbol=symbol,
                score=fresh,
                previous_score=previous,
                velocity=velocity,
                relative_rank=0,
                channels=channels,
                lifecycle=lifecycle,
                peak_score=peak,
                last_price=row.last_price,
                volume_24h=row.volume_24h,
                range_pct=row.range_pct,
                updated_at=now,
                first_seen_at=first_seen,
            )
            became_hot = lifecycle in HOT_STATES and (
                spiked
                or velocity >= Config.HOT_SCORE_VELOCITY
                or prev_life not in HOT_STATES
            )
            if became_hot:
                newly_hot.append((fresh + max(velocity, 0.0), symbol))

            self._prev_price[symbol] = row.last_price
            self._prev_volume[symbol] = row.volume_24h
            self._prev_range[symbol] = row.range_pct

        self._decay_missing(present, now)
        self._assign_relative_ranks()

        newly_hot.sort(reverse=True)
        cap = max(Config.HOT_FAST_TRACK_MAX_PER_CYCLE, 1)
        return [sym for _, sym in newly_hot[:cap]]

    def _is_spike(self, row: TickerFeatures) -> bool:
        """True when volume/price/range jumped enough to stand in for an OI burst.

        ``!miniTicker@arr`` has no OI. A spike is inferred from last-price delta,
        24h quote-volume ratio, 24h range expansion, or a cached-bar volume burst.
        """
        symbol = row.symbol.upper()
        prev_price = self._prev_price.get(symbol, 0.0)
        prev_volume = self._prev_volume.get(symbol, 0.0)
        prev_range = self._prev_range.get(symbol, 0.0)
        price_move = 0.0
        if prev_price > 0 and row.last_price > 0:
            price_move = abs(row.last_price - prev_price) / prev_price * 100.0
        volume_ratio = (
            row.volume_24h / prev_volume if prev_volume > 0 else 1.0
        )
        range_jump = row.range_pct - prev_range if prev_range > 0 else 0.0
        kline_burst = row.volume_burst >= 1.4
        return (
            price_move >= Config.HOT_PRICE_MOVE_PCT
            or volume_ratio >= Config.HOT_VOLUME_RATIO
            or range_jump >= Config.HOT_RANGE_EXPANSION_PCT
            or kline_burst
        )

    def _next_lifecycle(
        self,
        previous: CoinLifecycle,
        *,
        score: float,
        velocity: float,
        peak: float,
        spiked: bool,
        in_primary: bool,
        stale_seconds: float,
    ) -> CoinLifecycle:
        stale = stale_seconds >= Config.OPPORTUNITY_STALE_SECONDS
        if previous == CoinLifecycle.DORMANT and score < Config.OPPORTUNITY_CANDIDATE_MIN:
            return CoinLifecycle.DORMANT
        if (
            score <= Config.OPPORTUNITY_DORMANT_MAX
            and previous
            not in (CoinLifecycle.UNSEEN, CoinLifecycle.DISCOVERED)
        ) or (stale and score < Config.OPPORTUNITY_WATCH_MIN):
            return CoinLifecycle.DORMANT

        weaken = (
            peak - score >= Config.OPPORTUNITY_WEAKEN_DROP
            and previous
            in (
                CoinLifecycle.ACTIVE,
                CoinLifecycle.HOT,
                CoinLifecycle.OPPORTUNITY,
            )
            and not spiked
            and velocity < 0
        )
        if weaken:
            return CoinLifecycle.WEAKENED

        hot_event = spiked or velocity >= Config.HOT_SCORE_VELOCITY
        if score >= Config.OPPORTUNITY_SETUP_MIN and (hot_event or in_primary):
            return CoinLifecycle.OPPORTUNITY
        if hot_event and score >= Config.OPPORTUNITY_WATCH_MIN:
            return CoinLifecycle.HOT
        if score >= Config.OPPORTUNITY_HOT_MIN and in_primary:
            return CoinLifecycle.HOT
        if in_primary or score >= Config.OPPORTUNITY_ACTIVE_MIN:
            return CoinLifecycle.ACTIVE
        if score >= Config.OPPORTUNITY_WATCH_MIN:
            return CoinLifecycle.WATCH
        if score >= Config.OPPORTUNITY_CANDIDATE_MIN:
            return CoinLifecycle.CANDIDATE
        if previous == CoinLifecycle.UNSEEN:
            return CoinLifecycle.DISCOVERED
        return CoinLifecycle.DISCOVERED

    def _decay_missing(self, present: set[str], now: float) -> None:
        stale_cut = Config.OPPORTUNITY_STALE_SECONDS
        for symbol, rec in list(self._records.items()):
            if symbol in present:
                continue
            elapsed = max(0.0, now - rec.updated_at)
            decayed = _decay_score(rec.score, elapsed)
            life = rec.lifecycle
            if elapsed >= stale_cut or decayed <= Config.OPPORTUNITY_DORMANT_MAX:
                life = CoinLifecycle.DORMANT
            elif rec.peak_score - decayed >= Config.OPPORTUNITY_WEAKEN_DROP:
                life = CoinLifecycle.WEAKENED
            self._records[symbol] = CoinOpportunity(
                symbol=symbol,
                score=decayed,
                previous_score=rec.score,
                velocity=decayed - rec.score,
                relative_rank=rec.relative_rank,
                channels=rec.channels,
                lifecycle=life,
                peak_score=rec.peak_score,
                last_price=rec.last_price,
                volume_24h=rec.volume_24h,
                range_pct=rec.range_pct,
                updated_at=now,
                first_seen_at=rec.first_seen_at,
            )
            if life == CoinLifecycle.DORMANT and elapsed >= stale_cut * 2:
                del self._records[symbol]
                self._prev_price.pop(symbol, None)
                self._prev_volume.pop(symbol, None)
                self._prev_range.pop(symbol, None)

    def apply_primary_pool(self, symbols: Iterable[str]) -> None:
        """Promote/demote ACTIVE membership after the ranked pool is chosen."""
        primary = {str(s).upper() for s in symbols if s}
        for rec in self._records.values():
            if rec.lifecycle in HOT_STATES or rec.lifecycle == CoinLifecycle.DORMANT:
                continue
            if rec.symbol in primary:
                if rec.lifecycle in (
                    CoinLifecycle.UNSEEN,
                    CoinLifecycle.DISCOVERED,
                    CoinLifecycle.CANDIDATE,
                    CoinLifecycle.WATCH,
                    CoinLifecycle.WEAKENED,
                ):
                    rec.lifecycle = CoinLifecycle.ACTIVE
            elif rec.lifecycle == CoinLifecycle.ACTIVE:
                rec.lifecycle = (
                    CoinLifecycle.WATCH
                    if rec.score >= Config.OPPORTUNITY_WATCH_MIN
                    else CoinLifecycle.CANDIDATE
                )

    def _assign_relative_ranks(self) -> None:
        ranked = sorted(
            self._records.values(),
            key=lambda rec: rec.score,
            reverse=True,
        )
        for index, rec in enumerate(ranked, start=1):
            rec.relative_rank = index

    def snapshot_for_symbols(
        self, symbols: Iterable[str]
    ) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for symbol in symbols:
            rec = self._records.get(symbol.upper())
            if rec is None:
                continue
            out[rec.symbol] = {
                "opportunity_score": rec.score,
                "score_velocity": rec.velocity,
                "relative_rank": rec.relative_rank,
                "lifecycle": rec.lifecycle.value,
                "channel_momentum": rec.channels.momentum,
                "channel_reversal": rec.channels.reversal,
                "channel_structure": rec.channels.structure,
                "dominant_channel": rec.channels.dominant,
            }
        return out
