"""
Reporting module.
Exports trade history, daily stats, and performance summaries to CSV.
Uses public DatabaseManager APIs only.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Optional

from config import Config

import pandas as pd

from config import Config
from utils import utc_now, safe_float
from database import DatabaseManager
from logger import error_logger, performance_logger, system_logger

if TYPE_CHECKING:
    from exchange import BinanceExchangeManager, LiveAccountSnapshot


def fetch_live_account_for_display(
    exchange: "BinanceExchangeManager",
) -> "LiveAccountSnapshot":
    """Live wallet/margin/unrealized from Binance fapi/v2/account (Telegram /status)."""
    return exchange.fetch_live_account_snapshot(include_today_income=True)


def _format_duration(seconds: float) -> str:
    if seconds < 0 or seconds == float("inf"):
        return "n/a"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def format_bot_health_message(
    *,
    exchange: Optional["BinanceExchangeManager"] = None,
    market_data: Any = None,
    scanner: Any = None,
    controller: Any = None,
    scheduler: Any = None,
) -> str:
    """Build /health and /pulse Telegram diagnostic report."""
    from core.bot_health import (
        get_monitor_stall_recoveries,
        get_scan_age_seconds,
        get_scan_counts,
        get_uptime_seconds,
        is_monitor_watchdog_active,
    )
    from core.ops_heartbeat import (
        get_main_stall_seconds,
        get_monitor_stall_seconds,
        read_heartbeat_payload,
    )
    from utils import escape_html, utc_now

    now = utc_now()
    refreshed = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    hb_payload = read_heartbeat_payload()
    hb_ts = safe_float(hb_payload.get("timestamp"))
    if hb_ts > 0:
        hb_age = max(now.timestamp() - hb_ts, 0.0)
    else:
        hb_age = get_main_stall_seconds()
        if hb_age == float("inf"):
            hb_age = -1.0

    main_stall = get_main_stall_seconds()
    monitor_stall = get_monitor_stall_seconds()
    heartbeat_ok = hb_age >= 0 and hb_age <= max(
        float(Config.HEARTBEAT_SECONDS) * 3, 90.0
    )
    if heartbeat_ok:
        hb_line = f"Alive (Last ping: {_format_duration(hb_age)} ago)"
    elif hb_age < 0:
        hb_line = "Unknown (no heartbeat yet)"
    else:
        hb_line = f"STALE (Last ping: {_format_duration(hb_age)} ago)"

    hub = market_data
    if hub is None and exchange is not None:
        hub = exchange.get_market_data_hub()
    if hub is not None and hasattr(hub, "get_ws_health_snapshot"):
        ws = hub.get_ws_health_snapshot()
        ws_state = str(ws.get("state", "UNKNOWN")).upper()
        feeds = int(ws.get("active_feeds", 0))
        tickers = int(ws.get("ticker_symbols", 0))
        if ws_state in {"HEALTHY", "CONNECTED"}:
            ws_line = f"HEALTHY ({feeds} active feeds | {tickers} tickers cached)"
        elif ws_state == "RECONNECTING":
            ws_line = f"RECONNECTING ({feeds} feeds scheduled)"
        elif ws_state == "WARMING":
            ws_line = (
                f"WARMING ({feeds} feeds | waiting for first tick, "
                f"age {_format_duration(safe_float(ws.get('ticker_age_seconds', -1)))})"
            )
        else:
            ws_line = f"{ws_state} ({feeds} feeds | ticker age {_format_duration(safe_float(ws.get('ticker_age_seconds', -1)))})"
    else:
        ws_line = "UNAVAILABLE (market data hub not attached)"

    engine_stalled = main_stall > max(float(Config.HEARTBEAT_SECONDS) * 2, 60.0)
    entries_paused = False
    pause_reason = ""
    if scheduler is not None:
        entries_paused, pause_reason = scheduler.is_entry_paused()
    elif controller is not None:
        entries_paused, pause_reason = controller.is_manually_paused()

    if engine_stalled:
        engine_line = "STALLED"
    elif entries_paused:
        engine_line = f"IDLE ({escape_html(pause_reason[:60])})" if pause_reason else "IDLE (entries paused)"
    else:
        engine_line = "RUNNING"

    scan_age = get_scan_age_seconds()
    scanned, universe_total = get_scan_counts()
    if scanner is not None and universe_total <= 0:
        pipeline = getattr(scanner, "_pipeline", None)
        if pipeline is not None:
            syms = getattr(pipeline, "_last_universe_symbols", []) or []
            universe_total = len(syms)
            scanned = universe_total
    if scan_age is None:
        scan_line = f"No scan yet | Universe: {universe_total} symbols"
    else:
        scan_line = (
            f"Last scan {_format_duration(scan_age)} ago | "
            f"Scanned: {scanned} / {universe_total} symbols"
        )

    watchdog_active = is_monitor_watchdog_active()
    stall_recoveries = get_monitor_stall_recoveries()
    uptime = _format_duration(get_uptime_seconds())
    if watchdog_active:
        watchdog_line = (
            f"ACTIVE (Monitor stall: {_format_duration(monitor_stall)} | "
            f"Stalls recovered: {stall_recoveries})"
        )
    else:
        watchdog_line = f"INACTIVE (Stalls recovered: {stall_recoveries})"

    mode = "TESTNET" if Config.USE_TESTNET else "MAINNET"
    cycle = hb_payload.get("cycle")
    cycle_line = f"🔄 <b>Main cycle:</b> {cycle}\n" if cycle is not None else ""

    return (
        f"🩺 <b>BOT HEALTH AND DIAGNOSTICS</b>\n"
        f"🌐 <b>Mode:</b> {escape_html(mode)}\n\n"
        f"⏱️ <b>Last Refreshed:</b> {refreshed}\n"
        f"💓 <b>Heartbeat:</b> {escape_html(hb_line)}\n"
        f"🌐 <b>WebSocket State:</b> {escape_html(ws_line)}\n"
        f"🧠 <b>Decision Engine:</b> {escape_html(engine_line)}\n"
        f"📊 <b>Scanner:</b> {escape_html(scan_line)}\n"
        f"🛡️ <b>Watchdog:</b> {escape_html(watchdog_line)}\n"
        f"⏳ <b>Uptime:</b> {uptime}\n"
        f"{cycle_line}"
        f"📡 <b>Response:</b> {now.strftime('%H:%M:%S UTC')}"
    )


class ReportGenerator:
    """Generates CSV reports for external analysis and VPS archival."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.reports_dir = Config.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)

    def live_account_summary(self, exchange: "BinanceExchangeManager") -> dict[str, float]:
        """Return live exchange balances for reporting dashboards."""
        snap = fetch_live_account_for_display(exchange)
        return {
            "wallet_balance": safe_float(snap.wallet_balance),
            "margin_balance": safe_float(snap.margin_balance),
            "unrealized_pnl": safe_float(snap.unrealized_pnl),
            "today_realized_pnl": safe_float(snap.today_realized_pnl),
        }

    def export_trades_history(self) -> str:
        """Export full trade history to a timestamped CSV file."""
        try:
            df = self.db.get_all_trades_df()
            if df.empty:
                system_logger.info("No trades available to export.")
                return ""

            filename = f"trade_history_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(self.reports_dir, filename)
            df.to_csv(filepath, index=False)
            system_logger.info("Trade history exported: %s", filepath)
            return filepath
        except Exception as exc:
            error_logger.error("Trade history export failed: %s", exc)
            return ""

    def export_daily_stats(self) -> str:
        """Export daily performance statistics to CSV."""
        try:
            df = self.db.get_all_daily_stats_df()
            if df.empty:
                system_logger.info("No daily stats available to export.")
                return ""

            filename = f"daily_performance_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(self.reports_dir, filename)
            df.to_csv(filepath, index=False)
            system_logger.info("Daily stats exported: %s", filepath)
            return filepath
        except Exception as exc:
            error_logger.error("Daily stats export failed: %s", exc)
            return ""

    def export_performance_summary(self) -> str:
        """
        Export closed-trade performance metrics:
        win rate, profit factor, expectancy, max drawdown proxy.
        """
        try:
            df = self.db.get_all_trades_df()
            closed = df[df["status"] == "CLOSED"].copy() if not df.empty else pd.DataFrame()
            if closed.empty:
                system_logger.info("No closed trades for performance summary.")
                return ""

            metrics = self._calculate_performance_metrics(closed)
            summary_df = pd.DataFrame([metrics])

            filename = f"performance_summary_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(self.reports_dir, filename)
            summary_df.to_csv(filepath, index=False)

            performance_logger.info(
                "Performance summary | trades=%s | win_rate=%.2f%% | pf=%.2f | expectancy=%.4f",
                metrics["total_trades"],
                metrics["win_rate_percent"],
                metrics["profit_factor"],
                metrics["expectancy"],
            )
            system_logger.info("Performance summary exported: %s", filepath)
            return filepath
        except Exception as exc:
            error_logger.error("Performance summary export failed: %s", exc)
            return ""

    def export_weekly_report(self) -> str:
        if not Config.ENABLE_WEEKLY_REPORT:
            return ""
        return self._export_period_trades(prefix="weekly", days=7)

    def export_monthly_report(self) -> str:
        if not Config.ENABLE_MONTHLY_REPORT:
            return ""
        return self._export_period_trades(prefix="monthly", days=30)

    def run_scheduled_exports(self) -> dict[str, str]:
        """
        Run all enabled exports. Intended for daily scheduler hooks in main.py.
        Returns mapping of report type to filepath.
        """
        outputs: dict[str, str] = {}

        if Config.ENABLE_DAILY_REPORT:
            path = self.export_daily_stats()
            if path:
                outputs["daily_stats"] = path
            path = self.export_performance_summary()
            if path:
                outputs["performance"] = path

        if Config.ENABLE_WEEKLY_REPORT:
            path = self.export_weekly_report()
            if path:
                outputs["weekly"] = path

        if Config.ENABLE_MONTHLY_REPORT:
            path = self.export_monthly_report()
            if path:
                outputs["monthly"] = path

        return outputs

    def _export_period_trades(self, prefix: str, days: int) -> str:
        try:
            df = self.db.get_all_trades_df()
            if df.empty or "closed_at" not in df.columns:
                return ""

            closed = df[df["status"] == "CLOSED"].copy()
            if closed.empty:
                return ""

            closed["closed_at_dt"] = pd.to_datetime(closed["closed_at"], errors="coerce", utc=True)
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
            period = closed[closed["closed_at_dt"] >= cutoff]
            if period.empty:
                return ""

            filename = f"{prefix}_trades_{utc_now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(self.reports_dir, filename)
            period.drop(columns=["closed_at_dt"], errors="ignore").to_csv(filepath, index=False)
            system_logger.info("%s report exported: %s", prefix.capitalize(), filepath)
            return filepath
        except Exception as exc:
            error_logger.error("%s report export failed: %s", prefix, exc)
            return ""

    @staticmethod
    def _calculate_performance_metrics(closed: pd.DataFrame) -> dict[str, Any]:
        pnl = pd.to_numeric(closed["pnl"], errors="coerce").fillna(0.0)
        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]

        total = len(pnl)
        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / total * 100.0) if total else 0.0

        gross_profit = float(wins.sum()) if not wins.empty else 0.0
        gross_loss = abs(float(losses.sum())) if not losses.empty else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

        avg_win = float(wins.mean()) if not wins.empty else 0.0
        avg_loss = abs(float(losses.mean())) if not losses.empty else 0.0
        win_prob = win_count / total if total else 0.0
        loss_prob = loss_count / total if total else 0.0
        expectancy = (win_prob * avg_win) - (loss_prob * avg_loss)

        equity = pnl.cumsum()
        peak = equity.cummax()
        drawdown = peak - equity
        max_drawdown = float(drawdown.max()) if not drawdown.empty else 0.0

        return {
            "generated_at_utc": utc_now().isoformat(),
            "total_trades": total,
            "wins": win_count,
            "losses": loss_count,
            "win_rate_percent": round(win_rate, 2),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else 9999.0,
            "expectancy": round(expectancy, 4),
            "gross_profit": round(gross_profit, 4),
            "gross_loss": round(gross_loss, 4),
            "max_drawdown_abs": round(max_drawdown, 4),
            "total_pnl": round(float(pnl.sum()), 4),
        }
