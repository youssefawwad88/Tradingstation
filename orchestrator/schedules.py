"""Job scheduling definitions for different market modes.
"""

from typing import Any, Dict, List

from orchestrator.modes import MarketMode


def get_schedule_for_mode(mode: MarketMode) -> List[Dict[str, Any]]:
    """Get the job schedule for a specific market mode."""
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
