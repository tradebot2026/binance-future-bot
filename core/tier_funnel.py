"""Strict 3-tier scan funnel: Normal (wide) → Hot (backtest) → Super (execution)."""

from __future__ import annotations

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
    Normal: 120-coin lock for 3 hours, 2 coins/min, lightweight scores.
    Hot: promoted coins, 1 coin/min, 1 REST history fetch/min via validator.
    Super: 1 coin/min precise setup; execution is gated separately (4 orders/min).
    """

    def __init__(self, *, notify: Optional[NotifyFn] = None) -> None:
        self._notify = notify
        self._normal: list[str] = []
        self._demoted_hold: list[str] = []
        self._normal_index: int = 0
        self._lock_started: float = 0.0
        self._lock_cycle: int = 0
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
        hours = max(float(Config.NORMAL_TIER_LOCK_HOURS), 0.25)
        if not self._normal:
            return True
        return self.lock_age_seconds(now) >= hours * 3600.0

    def replace_normal_universe(self, symbols: list[str], *, now: float) -> list[str]:
        """Flush Normal memory and lock a new top-N set for the 3-hour cycle."""
        cap = max(int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1)
        seen: set[str] = set()
        locked: list[str] = []
        for raw in symbols:
            key = str(raw or "").upper()
            if not key or key in seen:
                continue
            seen.add(key)
            locked.append(key)
            if len(locked) >= cap:
                break
        self._normal = locked
        self._demoted_hold = []
        self._normal_index = 0
        self._lock_started = now
        self._lock_cycle += 1
        scanner_logger.info(
            "[TIER_NORMAL] locked %s symbols for %.1fh (cycle=%s).",
            len(self._normal),
            Config.NORMAL_TIER_LOCK_HOURS,
            self._lock_cycle,
        )
        return list(self._normal)

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
        pool = self.scan_pool()
        if not pool:
            return []
        n = self._normal_clock.take(Config.NORMAL_TIER_COINS_PER_MINUTE, now=now)
        if n <= 0:
            return []
        taken: list[str] = []
        for _ in range(n):
            if self._normal_index >= len(pool):
                self._normal_index = 0
            if not pool:
                break
            taken.append(pool[self._normal_index % len(pool)])
            self._normal_index = (self._normal_index + 1) % max(len(pool), 1)
        return taken

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
        if not score_range_for(best.strategy).meets_hot(best.score):
            return None
        backups = tuple(row.strategy for row in ranked[1:3])
        rec = HotRecord(
            symbol=key,
            strategy=best.strategy,
            backup_strategies=backups,
            score=best.score,
            action=str(best.action or "NEUTRAL"),
            metadata=dict(extra or {}),
        )
        self._hot[key] = rec
        self._hot_order.append(key)
        self._promoted.add(key)
        backup_label = ", ".join(backups) if backups else "none"
        msg = (
            f"[TIER_HOT] promote {key} | {best.strategy} score={best.score:.1f} "
            f"(backups: {backup_label})"
        )
        scanner_logger.info(msg)
        self._emit(
            "🔥 <b>Hot Tier promotion</b>\n"
            f"🪙 {key}\n"
            f"🧠 {best.strategy} ({backup_label})\n"
            f"📊 score={best.score:.1f}"
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
        primary_score = primary.score if primary else 0.0
        if primary is not None and score_range_for(rec.strategy).meets_super(
            primary_score
        ):
            self._promote_super(rec, primary, candidate)
            return "SUPER"

        for backup in rec.backup_strategies:
            row = by_tag.get(backup)
            if row is None:
                continue
            if score_range_for(backup).meets_super(row.score):
                scanner_logger.info(
                    "[TIER_HOT] %s switch %s → %s score=%.1f (super)",
                    key,
                    rec.strategy,
                    backup,
                    row.score,
                )
                rec.strategy = backup
                rec.score = row.score
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
            if score_range_for(backup).meets_hot(row.score):
                scanner_logger.info(
                    "[TIER_HOT] %s switch %s → %s score=%.1f",
                    key,
                    rec.strategy,
                    backup,
                    row.score,
                )
                rec.strategy = backup
                rec.score = row.score
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
        payload.setdefault("symbol", key)
        payload.setdefault("strategy", score.strategy)
        payload.setdefault("score", score.score)
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
            score=score.score,
            candidate=payload,
        )
        scanner_logger.info(
            "[TIER_SUPER] promote %s | %s score=%.1f",
            key,
            score.strategy,
            score.score,
        )
        self._emit(
            "⭐ <b>Super Tier promotion</b>\n"
            f"🪙 {key}\n"
            f"🧠 {score.strategy}\n"
            f"📊 score={score.score:.1f}"
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


def _ranked_scores(scores: list[StrategyScore]) -> list[StrategyScore]:
    valid = [row for row in scores if row.score > 0]
    valid.sort(
        key=lambda row: (
            row.final_score or row.score,
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
