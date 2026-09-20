"""Fixed system parameters, trade states, and shared literals."""

from typing import Final, Set

# ---------------- Trade lifecycle statuses ----------------
TRADE_STATUS_OPEN: Final[str] = "OPEN"
TRADE_STATUS_TP1_HIT: Final[str] = "TP1_HIT"
TRADE_STATUS_TP2_HIT: Final[str] = "TP2_HIT"
TRADE_STATUS_CLOSED: Final[str] = "CLOSED"

ACTIVE_TRADE_STATUSES: Final[tuple[str, ...]] = (
    TRADE_STATUS_OPEN,
    TRADE_STATUS_TP1_HIT,
    TRADE_STATUS_TP2_HIT,
)

# ---------------- Daily scheduler statuses ----------------
DAILY_STATUS_ACTIVE: Final[str] = "ACTIVE"
DAILY_STATUS_PAUSED: Final[str] = "PAUSED"

# ---------------- Partial take-profit ratios (original position) ----------------
TP1_PORTION: Final[float] = 0.30
TP2_PORTION: Final[float] = 0.30
TP3_PORTION: Final[float] = 0.40

# ---------------- Strategy identifiers (persisted on every trade) ----------------
STRATEGY_SMC_TREND: Final[str] = "SMC_TREND"
STRATEGY_SMC_LEGACY: Final[str] = "SMC_MULTITF"
STRATEGY_RANGE_REVERSION: Final[str] = "RANGE_REVERSION"
STRATEGY_LIQUIDITY_SWEEP: Final[str] = "LIQUIDITY_SWEEP_CONT"
STRATEGY_VWAP_PULLBACK: Final[str] = "VWAP_PULLBACK"
STRATEGY_VP_BREAKOUT: Final[str] = "VOLUME_PROFILE_BREAKOUT"
STRATEGY_VOL_EXPANSION: Final[str] = "VOL_EXPANSION_MR"

SMC_STRATEGY_TAGS: Final[tuple[str, ...]] = (
    STRATEGY_SMC_TREND,
    STRATEGY_SMC_LEGACY,
)

STRATEGY_BREAKOUT_RETEST: Final[str] = "BREAKOUT_RETEST"
STRATEGY_TREND_MOMENTUM: Final[str] = "TREND_MOMENTUM"
STRATEGY_VOL_SQUEEZE: Final[str] = "VOL_SQUEEZE"
STRATEGY_ORDER_FLOW: Final[str] = "ORDER_FLOW"
STRATEGY_OI_FUNDING: Final[str] = "OI_FUNDING"
STRATEGY_VWAP_MR: Final[str] = "VWAP_MEAN_REVERSION"
STRATEGY_VP_KEYLEVEL: Final[str] = "VP_KEYLEVEL"
STRATEGY_MTF_ALIGNMENT: Final[str] = "MTF_ALIGNMENT"
STRATEGY_FALSE_BREAKOUT_SFP: Final[str] = "FALSE_BREAKOUT_SFP"
STRATEGY_PRICE_ACTION_REVERSAL: Final[str] = "PRICE_ACTION_REVERSAL"

ALL_STRATEGY_TAGS: Final[tuple[str, ...]] = (
    STRATEGY_SMC_TREND,
    STRATEGY_RANGE_REVERSION,
    STRATEGY_LIQUIDITY_SWEEP,
    STRATEGY_VWAP_PULLBACK,
    STRATEGY_VP_BREAKOUT,
    STRATEGY_VOL_EXPANSION,
    STRATEGY_BREAKOUT_RETEST,
    STRATEGY_TREND_MOMENTUM,
    STRATEGY_VOL_SQUEEZE,
    STRATEGY_ORDER_FLOW,
    STRATEGY_OI_FUNDING,
    STRATEGY_VP_KEYLEVEL,
    STRATEGY_MTF_ALIGNMENT,
    STRATEGY_FALSE_BREAKOUT_SFP,
    STRATEGY_PRICE_ACTION_REVERSAL,
)


def is_range_strategy(strategy: str) -> bool:
    return str(strategy).upper() == STRATEGY_RANGE_REVERSION


def is_smc_strategy(strategy: str) -> bool:
    tag = str(strategy).upper()
    return tag in SMC_STRATEGY_TAGS or tag == STRATEGY_SMC_LEGACY


def strategy_display_label(strategy: str) -> str:
    tag = str(strategy).upper()
    labels = {
        "RANGE_REVERSION": "RANGE Mode",
        "SMC_TREND": "SMC Mode",
        "SMC_MULTITF": "SMC Mode",
        "LIQUIDITY_SWEEP_CONT": "LSC Mode",
        "VWAP_PULLBACK": "VWAP Mode",
        "VOLUME_PROFILE_BREAKOUT": "VPB Mode",
        "VOL_EXPANSION_MR": "VEMR Mode",
        "BREAKOUT_RETEST": "Breakout Retest",
        "FALSE_BREAKOUT_SFP": "SFP Mode",
        "VOL_SQUEEZE": "Squeeze Mode",
        "TREND_MOMENTUM": "Trend Cont.",
        "PRICE_ACTION_REVERSAL": "PA Reversal",
    }
    return labels.get(tag, str(strategy))

# ---------------- Database column whitelist ----------------
ALLOWED_TRADE_COLUMNS: Final[Set[str]] = {
    "symbol",
    "side",
    "entry_price",
    "quantity",
    "status",
    "take_profit_1",
    "take_profit_2",
    "take_profit_3",
    "stop_loss",
    "pnl",
    "realized_pnl",
    "exit_price",
    "opened_at",
    "closed_at",
    "strategy",
    "score",
    "leverage",
    "margin",
    "fee",
    "exit_reason",
    "duration",
    "metadata",
    "exchange_order_id",
}
