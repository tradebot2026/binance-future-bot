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
TP1_PORTION: Final[float] = 0.33
TP2_PORTION: Final[float] = 0.33
TP3_PORTION: Final[float] = 0.34

# ---------------- Strategy identifiers (persisted on every trade) ----------------
STRATEGY_SMC_TREND: Final[str] = "SMC_TREND"
STRATEGY_SMC_LEGACY: Final[str] = "SMC_MULTITF"
STRATEGY_RANGE_REVERSION: Final[str] = "RANGE_REVERSION"

SMC_STRATEGY_TAGS: Final[tuple[str, ...]] = (
    STRATEGY_SMC_TREND,
    STRATEGY_SMC_LEGACY,
)


def is_range_strategy(strategy: str) -> bool:
    return str(strategy).upper() == STRATEGY_RANGE_REVERSION


def is_smc_strategy(strategy: str) -> bool:
    tag = str(strategy).upper()
    return tag in SMC_STRATEGY_TAGS or tag == STRATEGY_SMC_LEGACY


def strategy_display_label(strategy: str) -> str:
    if is_range_strategy(strategy):
        return "RANGE Mode"
    if is_smc_strategy(strategy):
        return "SMC Mode"
    return str(strategy)

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
