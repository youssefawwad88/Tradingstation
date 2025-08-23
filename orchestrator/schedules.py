"""Job scheduling definitions for continuous minute scheduler.
"""

from datetime import datetime
from typing import Any, Dict, List

from orchestrator.modes import MarketMode


def get_schedule_for_mode(mode: MarketMode) -> List[Dict[str, Any]]:
    """Get the job schedule for a specific market mode (legacy function)."""
    if mode == MarketMode.PREMARKET:
        return get_premarket_schedule()
    elif mode == MarketMode.MARKET:
        return get_market_schedule()
    elif mode == MarketMode.POSTMARKET:
        return get_postmarket_schedule()
    elif mode == MarketMode.DAILY:
        return get_daily_schedule()
    else:
        return []


def due_every_minute(now_et: datetime) -> List[Dict[str, Any]]:
    """Return jobs due every minute between 04:00-20:00 ET."""
    jobs = []

    # Always include 1-min intraday fetch during extended hours (04:00-20:00 ET)
    if 4 <= now_et.hour < 20:
        jobs.append({
            "name": "Update 1min Intraday Data",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "intraday", "--interval", "1min"],
            "critical": False  # Don't kill the loop for data fetch failures
        })

    # Add screeners based on time and session
    # Gap & Go screener: every minute during premarket (7:00-9:30) and regular session (9:30-10:30)
    if (7 <= now_et.hour < 9) or (now_et.hour == 9 and now_et.minute < 30):
        # Premarket Gap & Go
        jobs.append({
            "name": "Gap & Go Screener (Premarket)",
            "module": "screeners.gapgo",
            "args": [],
            "critical": False
        })
    elif (now_et.hour == 9 and now_et.minute >= 30) or (now_et.hour == 10 and now_et.minute <= 30):
        # Regular session Gap & Go (only after 09:36 ET breakout guard)
        if now_et.hour > 9 or (now_et.hour == 9 and now_et.minute >= 36):
            jobs.append({
                "name": "Gap & Go Screener (Regular)",
                "module": "screeners.gapgo",
                "args": [],
                "critical": False
            })

    # ORB screener at specific time
    if now_et.hour == 9 and now_et.minute == 40:
        jobs.append({
            "name": "Opening Range Breakout Screener",
            "module": "screeners.orb",
            "args": [],
            "critical": False
        })

    # Market hours screeners (every minute during regular session)
    if 9 <= now_et.hour < 16:
        if not (now_et.hour == 9 and now_et.minute < 30):  # Skip before 9:30
            # Add other screeners that run during market hours
            jobs.extend([
                {
                    "name": "AVWAP Reclaim Screener",
                    "module": "screeners.avwap_reclaim",
                    "args": [],
                    "critical": False
                },
                {
                    "name": "Breakout Screener",
                    "module": "screeners.breakout",
                    "args": [],
                    "critical": False
                },
                {
                    "name": "EMA Pullback Screener",
                    "module": "screeners.ema_pullback",
                    "args": [],
                    "critical": False
                }
            ])

    return jobs


def due_every_quarter_hour(now_et: datetime) -> List[Dict[str, Any]]:
    """Return jobs due at :00, :15, :30, :45 during extended hours."""
    jobs = []

    # 30-min intraday fetch on quarter hours during extended hours (04:00-20:00 ET)
    if now_et.minute in [0, 15, 30, 45] and 4 <= now_et.hour < 20:
        jobs.append({
            "name": "Update 30min Intraday Data",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "intraday", "--interval", "30min"],
            "critical": False  # Don't kill the loop for data fetch failures
        })

    return jobs


def due_once_at(now_et: datetime) -> List[Dict[str, Any]]:
    """Return jobs that run once at specific times."""
    jobs = []

    # Daily full refresh at 06:30 ET (once per day)
    if now_et.hour == 6 and now_et.minute == 30:
        jobs.append({
            "name": "Daily Refresh",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "daily", "--force-full"],
            "critical": True,  # Daily refresh is critical
            "run_once_per_day": True
        })

    # AVWAP anchors at 06:35 ET (once per day)
    if now_et.hour == 6 and now_et.minute == 35:
        jobs.append({
            "name": "Find AVWAP Anchors",
            "module": "jobs.find_avwap_anchors",
            "args": ["--lookback-days", "5"],
            "critical": False,
            "run_once_per_day": True
        })

    # Postmarket jobs
    if now_et.hour == 16 and now_et.minute == 30:  # 4:30 PM ET
        jobs.append({
            "name": "Exhaustion Reversal Screener",
            "module": "screeners.exhaustion_reversal",
            "args": [],
            "critical": False,
            "run_once_per_day": True
        })

    if now_et.hour == 17 and now_et.minute == 0:  # 5:00 PM ET
        jobs.append({
            "name": "Generate Master Dashboard",
            "module": "dashboard.master_dashboard",
            "args": [],
            "critical": False,
            "run_once_per_day": True
        })

    # Health check every 6 hours
    if now_et.minute == 0 and now_et.hour in [2, 8, 14, 20]:
        jobs.append({
            "name": "Health Check",
            "module": "tools.health_check",
            "args": [],
            "critical": False
        })

    return jobs


def get_premarket_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during premarket (4:00 AM - 9:30 AM ET)."""
    return [
        {
            "name": "Daily Refresh",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "daily", "--force-full"],
            "critical": True
        },
        {
            "name": "Find AVWAP Anchors",
            "module": "jobs.find_avwap_anchors",
            "args": ["--lookback-days", "5"],
            "critical": False
        },
        {
            "name": "Gap & Go Screener",
            "module": "screeners.gapgo",
            "args": [],
            "critical": False,
            "delay": 2
        },
        {
            "name": "Opening Range Breakout Screener",
            "module": "screeners.orb",
            "args": [],
            "critical": False
        }
    ]


def get_market_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during market hours (9:30 AM - 4:00 PM ET)."""
    return [
        {
            "name": "Update 1min Intraday Data",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "intraday", "--interval", "1min"],
            "critical": True
        },
        {
            "name": "AVWAP Reclaim Screener",
            "module": "screeners.avwap_reclaim",
            "args": [],
            "critical": False
        },
        {
            "name": "Breakout Screener",
            "module": "screeners.breakout",
            "args": [],
            "critical": False
        },
        {
            "name": "EMA Pullback Screener",
            "module": "screeners.ema_pullback",
            "args": [],
            "critical": False
        }
    ]


def get_postmarket_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during postmarket (4:00 PM - 8:00 PM ET)."""
    return [
        {
            "name": "Update 30min Intraday Data",
            "module": "jobs.data_fetch_manager",
            "args": ["--job", "intraday", "--interval", "30min"],
            "critical": True
        },
        {
            "name": "Exhaustion Reversal Screener",
            "module": "screeners.exhaustion_reversal",
            "args": [],
            "critical": False
        },
        {
            "name": "Generate Master Dashboard",
            "module": "dashboard.master_dashboard",
            "args": [],
            "critical": False
        }
    ]


def get_daily_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during daily maintenance (8:00 PM - 4:00 AM ET)."""
    return [
        {
            "name": "Health Check",
            "module": "tools.health_check",
            "args": [],
            "critical": False
        },
        {
            "name": "Data Integrity Check",
            "module": "jobs.backfill_rebuilder",
            "args": ["--operation", "verify"],
            "critical": False
        }
    ]
