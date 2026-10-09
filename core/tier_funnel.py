"""Strict 3-tier scan funnel: Normal (wide) → Hot (backtest) → Super (execution)."""

from __future__ import annotations

import math
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from config import Config
from core.candle_backtest import kline_load_percent
from core.pace_clock import MinuteWindow
from core.strategy_score_ranges import score_range_for
from core.types import StrategyScore
from logger import scanner_logger


NotifyFn = Callable[[str], None]
HistoryBarsFn = Callable[[str], int]

_HAVE_BARS_RE = re.compile(r"have\s+(\d+)", re.IGNORECASE)


def is_history_demote_reason(reason: str) -> bool:
    """True when Hot demotion is from thin 15m history / pending-pass exhaustion."""
    text = str(reason or "").lower()
    if "insufficient 15m history" in text:
        return True
    return "exceeded" in text and "pass" in text


@dataclass
class HotRecord:
    symbol: str
    strategy: str
    backup_strategies: tuple[str, ...] = ()
    score: float = 0.0
    action: str = "NEUTRAL"
    backtest_passed: bool = False
    backtest_pending: bool = False
    pending_reason: str = ""
    pending_passes: int = 0
    pending_pass_limit: int = 0
    history_bars: int = 0
    history_pct: int = 0
    last_history_pct: int = 0
    rest_cooldown_until: float = 0.0
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
        self._kline_pending: set[str] = set()
        self._recently_scanned: deque[str] = deque(
            maxlen=max(int(Config.NORMAL_TIER_UNIVERSE_SIZE), 1)
        )
        self._hot: dict[str, HotRecord] = {}
        self._hot_order: list[str] = []
        self._hot_index: int = 0
        self._hot_rest_index: int = 0
        self._super: dict[str, SuperRecord] = {}
        self._super_order: list[str] = []
        self._super_index: int = 0
        self._promoted: set[str] = set()
        self._normal_clock = MinuteWindow(Config.NORMAL_TIER_COINS_PER_MINUTE)
        self._hot_clock = MinuteWindow(Config.HOT_TIER_COINS_PER_MINUTE)
        self._super_clock = MinuteWindow(Config.SUPER_TIER_COINS_PER_MINUTE)
        self._pending_hot_rest: set[str] = set()
        self._history_bars_fn: Optional[HistoryBarsFn] = None
        self._demote_cooldown_until: dict[str, float] = {}
        self._digest_lines: list[str] = []
        self._digest_started: float = 0.0
        self._super_skip_tg_at: dict[str, float] = {}

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

    def note_kline_pending(self, symbol: str) -> None:
        """Watchlist shows [Pending] until klines are warm enough to score."""
        key = str(symbol or "").upper()
        if key:
            self._kline_pending.add(key)

    def clear_kline_pending(self, symbol: str) -> None:
        self._kline_pending.discard(str(symbol or "").upper())

    def is_kline_pending(self, symbol: str) -> bool:
        return str(symbol or "").upper() in self._kline_pending

    def pending_kline_symbols(self) -> list[str]:
        return [key for key in self._recently_scanned if key in self._kline_pending]

    def note_normal_score(self, symbol: str, score: float) -> None:
        key = str(symbol or "").upper()
        if not key:
            return
        incoming = max(float(score or 0.0), 0.0)
        if incoming <= 0:
            if key in self._kline_pending:
                return
            self._normal_scores.setdefault(key, 0.0)
            return
        self._normal_scores[key] = incoming
        self._kline_pending.discard(key)

    def record_normal_scan(
        self, symbol: str, scores: list[StrategyScore]
    ) -> Optional[HotRecord]:
        """Store a real evaluation, then promote when Hot-band is met.

        Callers must invoke this only after scan klines are complete.
        Incomplete snapshots stay in `_kline_pending` and must not land here.
        """
        key = str(symbol).upper()
        self.clear_kline_pending(key)
        self.note_normal_score(key, _display_score(scores))
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
        keep_keys.update(self._active_history_quarantine(time.monotonic()))
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
        self._kline_pending = {key for key in self._kline_pending if key in keep_keys}
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
                key: float(self._normal_scores[key])
                for key in self._recently_scanned
                if key in self._normal_scores
            },
            "kline_pending": [
                key
                for key in self._recently_scanned
                if key in self._kline_pending or key not in self._normal_scores
            ],
            "flush_minutes": self.flush_minutes_remaining(stamp),
            "flush_count": self._flush_count,
            "lock_cycle": self._lock_cycle,
            "hot_backtest_pending": self.pending_backtest_symbols(),
            "hot_backtest_progress": self.pending_backtest_progress(),
        }

    def take_hot_for_rest(self, now: float | None = None) -> Optional[HotRecord]:
        """One Hot coin per minute, round-robin. Cooling/pending coins are skipped."""
        stamp = time.monotonic() if now is None else now
        order = [s for s in self._hot_order if s in self._hot]
        if not order:
            return None
        n = len(order)
        start = self._hot_rest_index % n
        picked: Optional[HotRecord] = None
        cooling = 0
        for offset in range(n):
            key = order[(start + offset) % n]
            rec = self._hot[key]
            if rec.backtest_passed or key in self._pending_hot_rest:
                continue
            if rec.rest_cooldown_until > stamp:
                cooling += 1
                continue
            picked = rec
            self._hot_rest_index = (start + offset + 1) % n
            break
        if picked is None:
            return None
        if self._hot_clock.take(1, now=stamp) < 1:
            return None
        self._pending_hot_rest.add(picked.symbol)
        scanner_logger.info(
            "[TIER_HOT] rotate REST %s | cooling=%s remaining=%s",
            picked.symbol,
            cooling,
            sum(
                1
                for s in order
                if not self._hot[s].backtest_passed and s not in self._pending_hot_rest
            ),
        )
        return picked

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
        now: float | None = None,
    ) -> Optional[HotRecord]:
        """Promote when primary score reaches Hot band. Remembers until Hot demotes."""
        key = str(symbol).upper()
        stamp = time.monotonic() if now is None else now
        if not key or key in self._promoted or key in self._hot or key in self._super:
            return None
        if self.in_history_demote_cooldown(key, now=stamp):
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
            f"📊 score={best_score:.1f}",
            instant=False,
            digest=f"⬆️ Hot: {key} | {best.strategy} | {best_score:.0f}",
        )
        return rec

    def attach_kline_progress_fn(self, fn: Optional[HistoryBarsFn]) -> None:
        """Optional memory-only bar counter (WS/backtest buffer — no REST)."""
        self._history_bars_fn = fn

    def _memory_history_bars(self, symbol: str) -> int:
        fn = self._history_bars_fn
        if not callable(fn):
            return 0
        try:
            return max(int(fn(str(symbol).upper()) or 0), 0)
        except Exception:
            return 0

    def _resolve_history_bars(
        self, symbol: str, reason: str = "", history_bars: int | None = None
    ) -> int:
        bars = 0
        if history_bars is not None:
            bars = max(int(history_bars), 0)
        match = _HAVE_BARS_RE.search(str(reason or ""))
        if match:
            bars = max(bars, int(match.group(1)))
        bars = max(bars, self._memory_history_bars(symbol))
        rec = self._hot.get(str(symbol).upper())
        if rec is not None:
            bars = max(bars, int(rec.history_bars or 0))
        return bars

    def pending_backtest_progress(self) -> dict[str, int]:
        """Live 0–100 load % for Hot coins still waiting on 15m history."""
        out: dict[str, int] = {}
        for rec in (self._hot[s] for s in self._hot_order if s in self._hot):
            if not rec.backtest_pending or rec.backtest_passed:
                continue
            bars = self._resolve_history_bars(rec.symbol)
            if bars > rec.history_bars:
                rec.history_bars = bars
                rec.history_pct = kline_load_percent(bars)
            out[rec.symbol] = int(rec.history_pct)
        return out

    def note_hot_rest_started(self, symbol: str) -> None:
        self._pending_hot_rest.add(str(symbol).upper())

    def release_hot_rest(self, symbol: str) -> None:
        """Clear REST-in-flight so the coin can retry (e.g. -1003 deferred)."""
        self._pending_hot_rest.discard(str(symbol).upper())

    def note_hot_backtest_pending(
        self,
        symbol: str,
        reason: str = "",
        *,
        now: float | None = None,
        history_bars: int | None = None,
    ) -> None:
        """Park a thin-history Hot coin on cooldown and rotate; demote if progress is stuck."""
        key = str(symbol).upper()
        self._pending_hot_rest.discard(key)
        rec = self._hot.get(key)
        if rec is None or rec.backtest_passed:
            return
        stamp = time.monotonic() if now is None else now
        rec.backtest_pending = True
        rec.pending_reason = str(reason or "Need 200+ closed 15m bars")
        rec.pending_passes += 1
        rec.rest_cooldown_until = stamp + Config.hot_pending_cooldown_seconds()
        bars = self._resolve_history_bars(key, rec.pending_reason, history_bars)
        prev_pct = int(rec.history_pct)
        rec.last_history_pct = prev_pct
        rec.history_bars = bars
        rec.history_pct = kline_load_percent(bars)
        base_max = Config.hot_pending_max_passes()
        effective_max = rec.pending_pass_limit or base_max
        scanner_logger.info(
            "[TIER_HOT] %s [Pending Backtest %s%%] — %s | pass=%s/%s cooldown=%.0fs",
            key,
            rec.history_pct,
            rec.pending_reason,
            rec.pending_passes,
            effective_max,
            Config.hot_pending_cooldown_seconds(),
        )
        if rec.pending_passes < effective_max:
            return
        if rec.history_pct >= 100:
            rec.pending_pass_limit = rec.pending_passes + 1
            scanner_logger.info(
                "[TIER_HOT] %s extra pass — history 100%%, waiting on strategy validation",
                key,
            )
            return
        if rec.history_pct > prev_pct:
            rec.pending_pass_limit = rec.pending_passes + 1
            scanner_logger.info(
                "[TIER_HOT] %s extra pass granted — progress %s%% → %s%%",
                key,
                prev_pct,
                rec.history_pct,
            )
            return
        scanner_logger.info(
            "[TIER_HOT] %s progress stuck at %s%% — demote after %s passes",
            key,
            rec.history_pct,
            rec.pending_passes,
        )
        self._demote_hot(
            key,
            reason=(
                f"Pending Backtest exceeded {rec.pending_passes} passes "
                "— insufficient 15m history"
            ),
            now=stamp,
        )

    def pending_backtest_symbols(self) -> list[str]:
        return [
            rec.symbol
            for rec in (self._hot[s] for s in self._hot_order if s in self._hot)
            if rec.backtest_pending and not rec.backtest_passed
        ]

    def on_backtest_failed(self, symbol: str, reason: str = "") -> None:
        key = str(symbol).upper()
        self._pending_hot_rest.discard(key)
        rec = self._hot.get(key)
        if rec is None:
            return
        label = str(reason or "Backtest rejected").strip() or "Backtest rejected"
        scanner_logger.info(
            "[TIER_HOT] demote %s — %s",
            key,
            label,
        )
        self._demote_hot(key, reason=label)

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
        rec.backtest_pending = False
        rec.pending_reason = ""
        rec.pending_passes = 0
        rec.pending_pass_limit = 0
        rec.history_bars = 0
        rec.history_pct = 0
        rec.last_history_pct = 0
        rec.rest_cooldown_until = 0.0
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

        self._demote_hot(key, reason="Score dropped below Hot band")
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
            f"📊 score={effective:.1f}",
            instant=True,
            digest=f"⭐ Super: {key} | {score.strategy} | {effective:.0f}",
        )

    def in_history_demote_cooldown(
        self, symbol: str, *, now: float | None = None
    ) -> bool:
        """True while a thin-history demote still blocks Hot re-promotion."""
        key = str(symbol).upper()
        stamp = time.monotonic() if now is None else now
        return key in self._active_history_quarantine(stamp)

    def _active_history_quarantine(self, now: float) -> set[str]:
        live: set[str] = set()
        expired: list[str] = []
        for key, until in self._demote_cooldown_until.items():
            if now >= until:
                expired.append(key)
            else:
                live.add(key)
        for key in expired:
            self._demote_cooldown_until.pop(key, None)
        return live

    def _demote_hot(
        self, symbol: str, *, reason: str, now: float | None = None
    ) -> None:
        key = symbol.upper()
        rec = self._hot.pop(key, None)
        self._hot_order = [s for s in self._hot_order if s != key]
        self._pending_hot_rest.discard(key)
        self._promoted.discard(key)
        if rec is None:
            return
        if key not in {s.upper() for s in self._demoted_hold}:
            self._demoted_hold.append(key)
        if is_history_demote_reason(reason):
            stamp = time.monotonic() if now is None else now
            hold = Config.hot_history_demote_cooldown_seconds()
            self._demote_cooldown_until[key] = stamp + hold
            scanner_logger.info(
                "[TIER_HOT] %s history-demote cooldown %.0fs — skip Hot re-promote",
                key,
                hold,
            )
        self._emit(
            "⬇️ <b>Hot Tier demotion</b>\n"
            f"🪙 {key}\n"
            f"<i>{reason}</i> — returned to Normal cycle",
            instant=False,
            digest=f"⬇️ Demote {key} — {reason}",
        )

    def note_super_skip(self, symbol: str, reason: str, *, instant: bool = False) -> None:
        key = str(symbol or "").upper()
        label = str(reason or "setup not ready").strip()
        if not key:
            return
        self._emit(
            "⚠️ <b>Super setup skipped</b>\n"
            f"🪙 {key}\n"
            f"<i>{label}</i>",
            instant=instant,
            digest=f"⚠️ Super skip {key} — {label}",
            instant_cooldown_key=key if instant else "",
        )

    def maybe_flush_digest(self, now: float | None = None) -> Optional[str]:
        """Send the hourly Hot/pending summary. Super/live alerts stay instant."""
        stamp = time.monotonic() if now is None else now
        interval = max(float(getattr(Config, "FUNNEL_DIGEST_SECONDS", 3600.0)), 60.0)
        if self._digest_started <= 0:
            self._digest_started = stamp
            return None
        if (stamp - self._digest_started) < interval:
            return None
        pending_bt = self.pending_backtest_symbols()
        if not self._digest_lines and not pending_bt:
            self._digest_started = stamp
            return None
        lines = ["📊 <b>Hourly Funnel Digest</b>"]
        if self._digest_lines:
            lines.extend(self._digest_lines)
        else:
            lines.append("<i>No Hot promotions or demotions this hour.</i>")
        if pending_bt:
            progress = self.pending_backtest_progress()
            labels = [
                f"{sym} {int(progress.get(sym, 0))}%"
                for sym in pending_bt[:20]
            ]
            lines.append("⏳ Pending Backtest: " + ", ".join(labels))
        self._digest_lines = []
        self._digest_started = stamp
        text = "\n".join(lines)
        self._emit(text, instant=True)
        return text

    def _emit(
        self,
        text: str,
        *,
        instant: bool = False,
        digest: Optional[str] = None,
        instant_cooldown_key: str = "",
    ) -> None:
        if digest:
            self._digest_lines.append(digest)
        if not instant or not text or self._notify is None:
            return
        if instant_cooldown_key:
            now = time.monotonic()
            last = self._super_skip_tg_at.get(instant_cooldown_key, 0.0)
            if (now - last) < 600.0:
                return
            self._super_skip_tg_at[instant_cooldown_key] = now
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
