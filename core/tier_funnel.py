"""Strict 3-tier scan funnel: Normal (wide) → Hot (backtest) → Super (execution)."""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from config import Config
from core.pace_clock import MinuteWindow
from core.strategy_score_ranges import score_range_for
from core.types import StrategyScore
from logger import scanner_logger


NotifyFn = Callable[[str], None]


@dataclass
class HotRecord:
    symbol: str
    strategy: str
    backup_strategies: tuple[str, ...] = ()
    score: float = 0.0
    action: str = "NEUTRAL"
    backtest_passed: bool = False
    backup_index: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SuperRecord:
    symbol: str
    strategy: str
    backup_strategies: tuple[str, ...] = ()
    score: float = 0.0
    candidate: dict[str, Any] = field(default_factory=dict)


class TierFunnel:
    """
    Normal: paced ingest of 2 coins/min (1 every 30s) up to a 120-coin
    candidate lock, then 3 scan passes over 3 hours. Newest scan sits at
    the front of recently_scanned_queue. Hot/Super stay across flushes.
    """

    def __init__(self, *, notify: Optional[NotifyFn] = None) -> None:
        self._notify = notify
        self._candidates: list[str] = []
        self._normal: list[str] = []
        self._demoted_hold: list[str] = []
        self._normal_index: int = 0
        self._ingest_index: int = 0
        self._lock_started: float = 0.0
        self._lock_cycle: int = 0
        self._pass_number: int = 1
        self._scans_this_window: int = 0
        self._current_index: int = 0
        self._current_symbol: str = ""
        self._last_normal_take: float = 0.0
        self._last_flushed: list[str] = []
        self._flush_count: int = 0
        self._normal_scores: dict[str, float] = {}
        self._recently_scanned: deque[str] = deque(
            maxlen=max(int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1)
        )
        self._hot: dict[str, HotRecord] = {}
        self._hot_order: list[str] = []
        self._hot_index: int = 0
        self._super: dict[str, SuperRecord] = {}
        self._super_order: list[str] = []
        self._super_index: int = 0
        self._promoted: set[str] = set()
        self._normal_clock = MinuteWindow(Config.NORMAL_TIER_COINS_PER_MINUTE)
        self._hot_clock = MinuteWindow(Config.HOT_TIER_COINS_PER_MINUTE)
        self._super_clock = MinuteWindow(Config.SUPER_TIER_COINS_PER_MINUTE)
        self._pending_hot_rest: set[str] = set()

    @property
    def normal_symbols(self) -> list[str]:
        return list(self._normal)

    @property
    def candidate_symbols(self) -> list[str]:
        return list(self._candidates)

    @property
    def has_candidate_universe(self) -> bool:
        return bool(self._candidates)

    @property
    def recently_scanned(self) -> list[str]:
        return list(self._recently_scanned)

    @property
    def pass_number(self) -> int:
        return int(self._pass_number)

    @property
    def current_index(self) -> int:
        return int(self._current_index)

    @property
    def current_symbol(self) -> str:
        return self._current_symbol

    @property
    def ingested_count(self) -> int:
        return len(self._normal)

    @property
    def last_flushed(self) -> list[str]:
        return list(self._last_flushed)

    @property
    def flush_count(self) -> int:
        return int(self._flush_count)

    def note_normal_score(self, symbol: str, score: float) -> None:
        key = str(symbol or "").upper()
        if not key:
            return
        incoming = max(float(score or 0.0), 0.0)
        if incoming <= 0:
            self._normal_scores.setdefault(key, 0.0)
            return
        self._normal_scores[key] = incoming

    def record_normal_scan(
        self, symbol: str, scores: list[StrategyScore]
    ) -> Optional[HotRecord]:
        """Store the display score, then promote when Hot-band is met."""
        self.note_normal_score(str(symbol).upper(), _display_score(scores))
        if not scores:
            return None
        return self.promote_from_normal(symbol, scores)

    def normal_score(self, symbol: str) -> float:
        return float(self._normal_scores.get(str(symbol).upper(), 0.0))

    @property
    def hot_symbols(self) -> list[str]:
        return [self._hot[s].symbol for s in self._hot_order if s in self._hot]

    @property
    def super_symbols(self) -> list[str]:
        return [self._super[s].symbol for s in self._super_order if s in self._super]

    @property
    def lock_cycle(self) -> int:
        return self._lock_cycle

    def lock_age_seconds(self, now: float) -> float:
        if self._lock_started <= 0:
            return 0.0
        return max(now - self._lock_started, 0.0)

    def lock_expired(self, now: float) -> bool:
        if not self._candidates and self._lock_started <= 0:
            return True
        if self._lock_started <= 0:
            return True
        passes = max(int(Config.normal_tier_passes_per_lock()), 1)
        universe = max(len(self._candidates), 1)
        if self._candidates and self._scans_this_window >= passes * universe:
            return True
        hours = max(float(Config.NORMAL_TIER_LOCK_HOURS), 0.25)
        return self.lock_age_seconds(now) >= hours * 3600.0

    def flush_minutes_remaining(self, now: float) -> int:
        hours = max(float(Config.NORMAL_TIER_LOCK_HOURS), 0.25)
        remaining = max(hours * 3600.0 - self.lock_age_seconds(now), 0.0)
        if self.lock_expired(now):
            return 0
        return int(math.ceil(remaining / 60.0))

    def flush_non_setup(self, *, keep: Optional[set[str]] = None) -> list[str]:
        """Wipe Normal/candidate memory; keep Hot, Super, and active symbols."""
        keep_keys = {str(s).upper() for s in (keep or set()) if s}
        keep_keys.update(self._hot)
        keep_keys.update(self._super)
        outgoing: list[str] = []
        seen: set[str] = set()
        for raw in self._normal + self._demoted_hold + self._candidates:
            key = str(raw).upper()
            if not key or key in keep_keys or key in seen:
                continue
            seen.add(key)
            outgoing.append(key)
        self._normal = [s for s in self._normal if s in keep_keys]
        self._demoted_hold = [s for s in self._demoted_hold if s in keep_keys]
        self._candidates = []
        self._ingest_index = 0
        self._normal_index = 0
        self._scans_this_window = 0
        self._pass_number = 1
        self._current_index = 0
        self._current_symbol = ""
        self._last_normal_take = 0.0
        self._last_flushed = list(outgoing)
        self._recently_scanned.clear()
        self._normal_scores = {
            key: val for key, val in self._normal_scores.items() if key in keep_keys
        }
        if outgoing:
            scanner_logger.info(
                "[TIER_NORMAL] flushed %s non-setup symbol(s); kept hot=%s super=%s.",
                len(outgoing),
                len(self._hot),
                len(self._super),
            )
        return list(outgoing)

    def replace_normal_universe(
        self,
        symbols: list[str],
        *,
        now: float,
        keep: Optional[set[str]] = None,
    ) -> list[str]:
        """Seed a fresh top-N candidate pool. Ingest happens at 2 coins/min."""
        self.flush_non_setup(keep=keep)
        cap = max(int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1)
        blocked = set(self._hot) | set(self._super)
        seen: set[str] = set()
        candidates: list[str] = []
        for raw in symbols:
            key = str(raw or "").upper()
            if not key or key in seen or key in blocked:
                continue
            seen.add(key)
            candidates.append(key)
            if len(candidates) >= cap:
                break
        self._candidates = candidates
        self._ingest_index = 0
        self._normal_index = 0
        self._lock_started = now
        self._lock_cycle += 1
        self._flush_count = self._lock_cycle
        self._pass_number = 1
        self._scans_this_window = 0
        self._recently_scanned = deque(maxlen=cap)
        scanner_logger.info(
            "[TIER_NORMAL] seeded %s candidates for %.1fh paced ingest "
            "(2 coins/min, cycle=%s). Scan queue starts empty.",
            len(self._candidates),
            Config.NORMAL_TIER_LOCK_HOURS,
            self._lock_cycle,
        )
        return list(self._candidates)

    def is_in_hot(self, symbol: str) -> bool:
        return str(symbol).upper() in self._hot

    def is_in_super(self, symbol: str) -> bool:
        return str(symbol).upper() in self._super

    def is_promoted(self, symbol: str) -> bool:
        return str(symbol).upper() in self._promoted

    def scan_pool(self) -> list[str]:
        """Normal 3-hour memory plus demoted hold, excluding Hot/Super."""
        blocked = set(self._hot) | set(self._super)
        out: list[str] = []
        seen: set[str] = set()
        for raw in self._normal + self._demoted_hold:
            key = str(raw).upper()
            if not key or key in seen or key in blocked:
                continue
            seen.add(key)
            out.append(key)
        return out

    def take_normal(self, now: float | None = None) -> list[str]:
        stamp = time.monotonic() if now is None else now
        if self.lock_expired(stamp):
            return []
        interval = Config.normal_tier_ingest_interval_seconds()
        if self._last_normal_take > 0 and (stamp - self._last_normal_take) < interval:
            return []
        if self._normal_clock.take(1, now=stamp) < 1:
            return []
        symbol = self._next_normal_symbol()
        if not symbol:
            return []
        self._last_normal_take = stamp
        self._note_scanned(symbol)
        return [symbol]

    def _next_normal_symbol(self) -> str:
        if self._ingest_index < len(self._candidates):
            ingested = self._ingest_one()
            if ingested:
                return ingested
        pool = self.scan_pool()
        if not pool:
            return ""
        if self._normal_index >= len(pool):
            self._normal_index = 0
        symbol = pool[self._normal_index % len(pool)]
        self._normal_index = (self._normal_index + 1) % max(len(pool), 1)
        return symbol

    def _ingest_one(self) -> str:
        blocked = set(self._hot) | set(self._super)
        while self._ingest_index < len(self._candidates):
            key = self._candidates[self._ingest_index]
            self._ingest_index += 1
            if not key or key in blocked:
                continue
            if key not in self._normal:
                self._normal.append(key)
            return key
        return ""

    def _scan_universe_size(self) -> int:
        if self._candidates:
            return max(len(self._candidates), 1)
        return max(int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1)

    def _note_scanned(self, symbol: str) -> None:
        key = str(symbol).upper()
        if not key:
            return
        cap = self._recently_scanned.maxlen or max(
            int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1
        )
        self._recently_scanned = deque(
            [key, *[s for s in self._recently_scanned if s != key]],
            maxlen=cap,
        )
        self._current_symbol = key
        self._scans_this_window += 1
        universe = self._scan_universe_size()
        self._current_index = ((self._scans_this_window - 1) % universe) + 1
        passes = max(int(Config.normal_tier_passes_per_lock()), 1)
        self._pass_number = min(
            passes,
            ((self._scans_this_window - 1) // universe) + 1,
        )

    def watchlist_snapshot(self, now: float | None = None) -> dict[str, Any]:
        stamp = time.monotonic() if now is None else now
        universe = max(self._scan_universe_size(), int(Config.NORMAL_TIER_UNIVERSE_SIZE))
        return {
            "pass_number": self._pass_number,
            "passes_total": max(int(Config.normal_tier_passes_per_lock()), 1),
            "current_index": self._current_index,
            "universe_size": universe,
            "ingested_count": len(self._normal),
            "currently_scanning": self._current_symbol,
            "recently_scanned": list(self._recently_scanned),
            "normal_scores": {
                key: float(self._normal_scores.get(key, 0.0))
                for key in self._recently_scanned
            },
            "flush_minutes": self.flush_minutes_remaining(stamp),
            "flush_count": self._flush_count,
            "lock_cycle": self._lock_cycle,
        }

    def take_hot_for_rest(self, now: float | None = None) -> Optional[HotRecord]:
        """One Hot coin per minute that still needs the REST backtest fetch."""
        pending = [
            self._hot[s]
            for s in self._hot_order
            if s in self._hot
            and not self._hot[s].backtest_passed
            and s not in self._pending_hot_rest
        ]
        if not pending:
            return None
        if self._hot_clock.take(1, now=now) < 1:
            return None
        rec = pending[0]
        self._pending_hot_rest.add(rec.symbol)
        return rec

    def take_hot_for_rescore(self, now: float | None = None) -> Optional[HotRecord]:
        """
        One parked (backtest-passed) Hot coin per minute for live rescore.
        Shares the Hot 1/min clock with REST fetches — REST-needed coins go first.
        """
        parked = [
            self._hot[s]
            for s in self._hot_order
            if s in self._hot and self._hot[s].backtest_passed
        ]
        if not parked:
            return None
        if self._hot_clock.take(1, now=now) < 1:
            return None
        rec = parked[self._hot_index % len(parked)]
        self._hot_index = (self._hot_index + 1) % max(len(parked), 1)
        return rec

    def take_super(self, now: float | None = None) -> Optional[SuperRecord]:
        if not self._super_order:
            return None
        if self._super_clock.take(1, now=now) < 1:
            return None
        key = self._super_order[self._super_index % len(self._super_order)]
        self._super_index = (self._super_index + 1) % len(self._super_order)
        return self._super.get(key)

    def promote_from_normal(
        self,
        symbol: str,
        scores: list[StrategyScore],
        *,
        extra: Optional[dict[str, Any]] = None,
    ) -> Optional[HotRecord]:
        """Promote when primary score reaches Hot band. Remembers until Hot demotes."""
        key = str(symbol).upper()
        if not key or key in self._promoted or key in self._hot or key in self._super:
            return None
        ranked = _ranked_scores(scores)
        if not ranked:
            return None
        best = ranked[0]
        best_score = _effective_score(best)
        if not score_range_for(best.strategy).meets_hot(best_score):
            return None
        backups = tuple(row.strategy for row in ranked[1:3])
        rec = HotRecord(
            symbol=key,
            strategy=best.strategy,
            backup_strategies=backups,
            score=best_score,
            action=str(best.action or "NEUTRAL"),
            metadata=dict(extra or {}),
        )
        self._hot[key] = rec
        self._hot_order.append(key)
        self._promoted.add(key)
        backup_label = ", ".join(backups) if backups else "none"
        msg = (
            f"[TIER_HOT] promote {key} | {best.strategy} score={best_score:.1f} "
            f"(backups: {backup_label})"
        )
        scanner_logger.info(msg)
        self._emit(
            "🔥 <b>Hot Tier promotion</b>\n"
            f"🪙 {key}\n"
            f"🧠 {best.strategy} ({backup_label})\n"
            f"📊 score={best_score:.1f}"
        )
        return rec

    def note_hot_rest_started(self, symbol: str) -> None:
        self._pending_hot_rest.add(str(symbol).upper())

    def release_hot_rest(self, symbol: str) -> None:
        """Clear REST-in-flight so the coin can retry (e.g. -1003 deferred)."""
        self._pending_hot_rest.discard(str(symbol).upper())

    def on_backtest_failed(self, symbol: str, reason: str = "") -> None:
        key = str(symbol).upper()
        self._pending_hot_rest.discard(key)
        rec = self._hot.get(key)
        if rec is None:
            return
        scanner_logger.info(
            "[TIER_HOT] demote %s — backtest failed (%s)",
            key,
            reason or "rejected",
        )
        self._demote_hot(key, reason=reason or "backtest_failed")

    def on_backtest_passed(
        self,
        symbol: str,
        payload: Optional[dict[str, Any]] = None,
    ) -> Optional[HotRecord]:
        key = str(symbol).upper()
        self._pending_hot_rest.discard(key)
        rec = self._hot.get(key)
        if rec is None:
            return None
        rec.backtest_passed = True
        if payload:
            rec.metadata.update(payload)
        return rec

    def apply_hot_scores(
        self,
        symbol: str,
        scores: list[StrategyScore],
        *,
        candidate: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Score the current minute for a parked Hot coin.
        Super if primary or a backup reaches Super; stay Hot if any backup
        still meets Hot; otherwise demote to Normal.
        Returns SUPER / HOT / NORMAL (NORMAL means demoted).
        """
        key = str(symbol).upper()
        rec = self._hot.get(key)
        if rec is None:
            return "NORMAL"
        ranked = _ranked_scores(scores)
        by_tag = {row.strategy: row for row in ranked}
        primary = by_tag.get(rec.strategy)
        primary_score = _effective_score(primary) if primary is not None else 0.0
        if primary is not None and score_range_for(rec.strategy).meets_super(
            primary_score
        ):
            self._promote_super(rec, primary, candidate)
            return "SUPER"

        for backup in rec.backup_strategies:
            row = by_tag.get(backup)
            if row is None:
                continue
            backup_score = _effective_score(row)
            if score_range_for(backup).meets_super(backup_score):
                scanner_logger.info(
                    "[TIER_HOT] %s switch %s → %s score=%.1f (super)",
                    key,
                    rec.strategy,
                    backup,
                    backup_score,
                )
                rec.strategy = backup
                rec.score = backup_score
                rec.action = str(row.action or rec.action)
                rec.backup_index += 1
                self._promote_super(rec, row, candidate)
                return "SUPER"

        if primary is not None and score_range_for(rec.strategy).meets_hot(primary_score):
            rec.score = primary_score
            rec.action = str(primary.action or rec.action)
            return "HOT"

        for backup in rec.backup_strategies:
            row = by_tag.get(backup)
            if row is None:
                continue
            backup_score = _effective_score(row)
            if score_range_for(backup).meets_hot(backup_score):
                scanner_logger.info(
                    "[TIER_HOT] %s switch %s → %s score=%.1f",
                    key,
                    rec.strategy,
                    backup,
                    backup_score,
                )
                rec.strategy = backup
                rec.score = backup_score
                rec.action = str(row.action or rec.action)
                rec.backup_index += 1
                return "HOT"

        self._demote_hot(key, reason="scores_below_hot")
        return "NORMAL"

    def drop_super(self, symbol: str) -> None:
        key = str(symbol).upper()
        self._super.pop(key, None)
        self._super_order = [s for s in self._super_order if s != key]
        if self._super_index >= len(self._super_order):
            self._super_index = 0

    def note_filled(self, symbol: str) -> None:
        """Remove Super/Hot after an entry fill so the coin is not re-dispatched."""
        key = str(symbol).upper()
        was_super = key in self._super
        self.drop_super(key)
        self._hot.pop(key, None)
        self._hot_order = [s for s in self._hot_order if s != key]
        self._pending_hot_rest.discard(key)
        self._promoted.discard(key)
        if was_super:
            scanner_logger.info("[TIER_SUPER] cleared %s after fill", key)

    def demote_super(self, symbol: str, *, reason: str) -> None:
        key = str(symbol).upper()
        self.drop_super(key)
        self._promoted.discard(key)
        if key not in {s.upper() for s in self._demoted_hold}:
            self._demoted_hold.append(key)
        scanner_logger.info("[TIER_SUPER] demote %s — %s", key, reason)

    def hot_summary(self) -> list[tuple[str, str, float]]:
        rows = [
            (rec.symbol, rec.strategy, rec.score) for rec in self._hot.values()
        ]
        return sorted(rows, key=lambda row: row[2], reverse=True)

    def super_summary(self) -> list[tuple[str, str, float]]:
        rows = [
            (rec.symbol, rec.strategy, rec.score) for rec in self._super.values()
        ]
        return sorted(rows, key=lambda row: row[2], reverse=True)

    def _promote_super(
        self,
        rec: HotRecord,
        score: StrategyScore,
        candidate: Optional[dict[str, Any]],
    ) -> None:
        key = rec.symbol
        payload = dict(candidate or {})
        effective = _effective_score(score)
        payload.setdefault("symbol", key)
        payload.setdefault("strategy", score.strategy)
        payload.setdefault("score", effective)
        payload.setdefault("action", score.action)
        meta = dict(payload.get("structure_metadata") or {})
        meta["funnel_tier"] = "SUPER"
        meta["backup_strategies"] = list(rec.backup_strategies)
        meta["primary_strategy"] = rec.strategy
        if rec.metadata:
            meta.update({k: v for k, v in rec.metadata.items() if k not in meta})
        payload["structure_metadata"] = meta
        self._hot.pop(key, None)
        self._hot_order = [s for s in self._hot_order if s != key]
        self._pending_hot_rest.discard(key)
        if key not in self._super:
            self._super_order.append(key)
        self._super[key] = SuperRecord(
            symbol=key,
            strategy=score.strategy,
            backup_strategies=rec.backup_strategies,
            score=effective,
            candidate=payload,
        )
        scanner_logger.info(
            "[TIER_SUPER] promote %s | %s score=%.1f",
            key,
            score.strategy,
            effective,
        )
        self._emit(
            "⭐ <b>Super Tier promotion</b>\n"
            f"🪙 {key}\n"
            f"🧠 {score.strategy}\n"
            f"📊 score={effective:.1f}"
        )

    def _demote_hot(self, symbol: str, *, reason: str) -> None:
        key = symbol.upper()
        rec = self._hot.pop(key, None)
        self._hot_order = [s for s in self._hot_order if s != key]
        self._pending_hot_rest.discard(key)
        self._promoted.discard(key)
        if rec is None:
            return
        if key not in {s.upper() for s in self._demoted_hold}:
            self._demoted_hold.append(key)
        self._emit(
            "⬇️ <b>Hot Tier demotion</b>\n"
            f"🪙 {key}\n"
            f"<i>{reason}</i> — returned to Normal cycle"
        )

    def _emit(self, text: str) -> None:
        if self._notify is None:
            return
        try:
            self._notify(text)
        except Exception:
            pass


def _effective_score(row: StrategyScore) -> float:
    """Watchlist / funnel confidence: best of confluence final vs raw score.

    `final_score or score` treats a stored 0.0 final as missing, but also hid
    a positive raw score whenever callers ranked on `score > 0` only. Always
    take the max so /watchlist matches the Hot/Super gate.
    """
    return max(float(row.final_score or 0.0), float(row.score or 0.0), 0.0)


def _display_score(scores: list[StrategyScore]) -> float:
    ranked = _ranked_scores(scores)
    if ranked:
        return _effective_score(ranked[0])
    if not scores:
        return 0.0
    return _effective_score(max(scores, key=_effective_score))


def _ranked_scores(scores: list[StrategyScore]) -> list[StrategyScore]:
    valid = [row for row in scores if _effective_score(row) > 0]
    valid.sort(
        key=lambda row: (
            _effective_score(row),
            row.normalized_score,
            row.score,
            row.adjusted_score,
        ),
        reverse=True,
    )
    return valid


def pick_backup_strategies(scores: list[StrategyScore], primary: str) -> tuple[str, ...]:
    ranked = [row for row in _ranked_scores(scores) if row.strategy != primary]
    return tuple(row.strategy for row in ranked[:2])
