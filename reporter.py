"""
Reporting module.
Exports trade history, daily stats, and performance summaries to CSV.
Uses public DatabaseManager APIs only.
"""

from __future__ import annotations

import os
from typing import Any, Optional

import pandas as pd

from config import Config
from utils import utc_now
from database import DatabaseManager
from logger import error_logger, performance_logger, system_logger


class ReportGenerator:
    """Generates CSV reports for external analysis and VPS archival."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self.reports_dir = Config.REPORTS_DIR
        os.makedirs(self.reports_dir, exist_ok=True)

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
