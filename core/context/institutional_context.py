"""Aggregate institutional context modules into confluence multipliers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd

from config import Config
from core.context.context_types import ContextModuleResult, InstitutionalContext
from core.context.mtf_alignment_engine import evaluate_mtf_alignment
from core.context.oi_funding_context_engine import evaluate_oi_funding_context
from core.context.order_flow_imbalance_engine import evaluate_order_flow_imbalance
from core.context.volume_profile_engine import evaluate_volume_profile_context
from core.types import MarketSnapshot
from indicators.market_analyzer import MarketAnalyzer
from logger import error_logger
from core.candle_prep import prepare_df, resolve_price
from utils import safe_float

if TYPE_CHECKING:
    from exchange import BinanceExchangeManager


class InstitutionalContextEvaluator:
    """
    Evaluate MTF, volume profile, order flow, and OI/funding context.
    Designed for thread-pool batch evaluation during multi-pair scans.
    """

    def __init__(
        self,
        exchange: Optional["BinanceExchangeManager"] = None,
    ) -> None:
        self.exchange = exchange
        self.analyzer = MarketAnalyzer()

    def evaluate(self, snapshot: MarketSnapshot) -> InstitutionalContext:
        if not Config.ENABLE_INSTITUTIONAL_CONTEXT:
            return InstitutionalContext.empty(snapshot.symbol)

        modules = self._run_modules(snapshot)
        return self._aggregate(snapshot.symbol, modules)

    def evaluate_batch(
        self,
        snapshots: list[MarketSnapshot],
    ) -> dict[str, InstitutionalContext]:
        if not snapshots:
            return {}
        if not Config.ENABLE_INSTITUTIONAL_CONTEXT:
            return {s.symbol: InstitutionalContext.empty(s.symbol) for s in snapshots}

        if not Config.ENABLE_ASYNC_CONTEXT_EVAL or len(snapshots) == 1:
            return {s.symbol: self.evaluate(s) for s in snapshots}

        workers = max(min(Config.MAX_WORKERS, len(snapshots)), 1)
        output: dict[str, InstitutionalContext] = {}

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self.evaluate, snap): snap.symbol for snap in snapshots}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    output[symbol] = future.result()
                except Exception as exc:
                    error_logger.error(
                        "Institutional context eval failed for %s: %s",
                        symbol,
                        exc,
                    )
                    output[symbol] = InstitutionalContext.empty(symbol)
        return output

    def _run_modules(self, snapshot: MarketSnapshot) -> list[ContextModuleResult]:
        candles = snapshot.candles
        entry_tf = Config.ENTRY_TIMEFRAME
        df_entry = prepare_df(candles.get(entry_tf), self.analyzer)
        price = resolve_price(snapshot, df_entry) if df_entry is not None else snapshot.price
        atr = self.analyzer.get_latest_atr(df_entry) if df_entry is not None else 0.0

        modules: list[ContextModuleResult] = []

        if Config.ENABLE_CONTEXT_MTF:
            modules.append(
                evaluate_mtf_alignment(
                    candles,
                    entry_tf=entry_tf,
                    confirm_tf=Config.CONFIRM_TIMEFRAME,
                    trend_tf=Config.TREND_TIMEFRAME,
                )
            )

        vp_result = ContextModuleResult(module="VP_KEYLEVEL")
        if Config.ENABLE_CONTEXT_VP and df_entry is not None and atr > 0:
            vp_result = evaluate_volume_profile_context(df_entry, price, atr)
            modules.append(vp_result)

        key_levels = list(vp_result.key_levels)
        if Config.ENABLE_CONTEXT_ORDER_FLOW and df_entry is not None and atr > 0:
            modules.append(
                evaluate_order_flow_imbalance(
                    df_entry,
                    price,
                    atr,
                    key_levels=key_levels,
                )
            )

        if Config.ENABLE_CONTEXT_OI_FUNDING:
            derivatives = self._resolve_derivatives(snapshot)
            price_change = self._price_change_pct(df_entry)
            modules.append(evaluate_oi_funding_context(derivatives, price_change))

        return modules

    def _resolve_derivatives(self, snapshot: MarketSnapshot) -> dict[str, Any]:
        if snapshot.derivatives:
            return dict(snapshot.derivatives)
        if self.exchange is not None:
            try:
                return self.exchange.fetch_derivatives_context(snapshot.symbol)
            except Exception as exc:
                error_logger.debug(
                    "Derivatives fetch skipped for %s: %s",
                    snapshot.symbol,
                    exc,
                )
        return {}

    @staticmethod
    def _price_change_pct(df: Optional[pd.DataFrame]) -> float:
        if df is None or len(df) < 6:
            return 0.0
        prev = safe_float(df.iloc[-6]["close"])
        last = safe_float(df.iloc[-1]["close"])
        if prev <= 0:
            return 0.0
        return (last - prev) / prev * 100.0

    def _aggregate(
        self,
        symbol: str,
        modules: list[ContextModuleResult],
    ) -> InstitutionalContext:
        ctx = InstitutionalContext(symbol=symbol, modules=modules)
        if not modules:
            return ctx

        long_bonus = 0.0
        short_bonus = 0.0
        long_mult = 1.0
        short_mult = 1.0

        for mod in modules:
            if not mod.is_actionable or mod.score < Config.CONTEXT_MODULE_MIN_SCORE:
                continue
            weight = mod.score / 100.0
            if mod.direction == "LONG":
                long_bonus += mod.bonus or (
                    weight * Config.INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE
                )
                long_mult *= mod.multiplier
            elif mod.direction == "SHORT":
                short_bonus += mod.bonus or (
                    weight * Config.INSTITUTIONAL_CONTEXT_BONUS_PER_MODULE
                )
                short_mult *= mod.multiplier

        ctx.long_bonus = min(long_bonus, Config.INSTITUTIONAL_CONTEXT_BONUS_MAX)
        ctx.short_bonus = min(short_bonus, Config.INSTITUTIONAL_CONTEXT_BONUS_MAX)
        ctx.long_multiplier = max(
            min(long_mult, Config.INSTITUTIONAL_CONTEXT_MULT_MAX),
            Config.INSTITUTIONAL_CONTEXT_MULT_MIN,
        )
        ctx.short_multiplier = max(
            min(short_mult, Config.INSTITUTIONAL_CONTEXT_MULT_MAX),
            Config.INSTITUTIONAL_CONTEXT_MULT_MIN,
        )
        return ctx
