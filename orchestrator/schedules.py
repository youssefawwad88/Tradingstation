"""
Job scheduling definitions for different market modes.
"""

from typing import List, Dict, Any
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
            "name": "Update Universe",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/data_fetch_manager.py --job universe",
            "critical": True
        },
        {
            "name": "Fetch Daily Data",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/data_fetch_manager.py --job daily",
            "critical": True
        },
        {
            "name": "Find AVWAP Anchors",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/find_avwap_anchors.py --lookback-days 5",
            "critical": False
        },
        {
            "name": "Gap & Go Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/gapgo.py",
            "critical": False,
            "delay": 2
        },
        {
            "name": "Opening Range Breakout Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/orb.py",
            "critical": False
        }
    ]


def get_market_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during market hours (9:30 AM - 4:00 PM ET)."""
    return [
        {
            "name": "Update 1min Intraday Data",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/data_fetch_manager.py --job intraday_1min",
            "critical": True
        },
        {
            "name": "AVWAP Reclaim Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/avwap_reclaim.py",
            "critical": False
        },
        {
            "name": "Breakout Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/breakout.py",
            "critical": False
        },
        {
            "name": "EMA Pullback Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/ema_pullback.py",
            "critical": False
        }
    ]


def get_postmarket_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during postmarket (4:00 PM - 8:00 PM ET)."""
    return [
        {
            "name": "Update 30min Intraday Data",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/data_fetch_manager.py --job intraday_30min",
            "critical": True
        },
        {
            "name": "Exhaustion Reversal Screener",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 screeners/exhaustion_reversal.py",
            "critical": False
        },
        {
            "name": "Generate Master Dashboard",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 dashboard/master_dashboard.py",
            "critical": False
        }
    ]


def get_daily_schedule() -> List[Dict[str, Any]]:
    """Jobs to run during daily maintenance (8:00 PM - 4:00 AM ET)."""
    return [
        {
            "name": "Health Check",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 tools/health_check.py",
            "critical": False
        },
        {
            "name": "Data Integrity Check",
            "command": "cd /home/runner/work/Tradingstation/Tradingstation && python3 jobs/backfill_rebuilder.py --operation verify",
            "critical": False
        }
    ]