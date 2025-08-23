"""Breakout Strategy Screener - Daily consolidation breakouts with volume.

This module identifies consolidation box breakouts on daily timeframe
with volume expansion and range compression criteria.
"""

import time
from typing import Dict, List, Optional

import pandas as pd

from utils.config import config
from utils.paths import daily_key
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import utc_now

logger = get_logger(__name__)


class BreakoutScreener:
    """Daily consolidation breakout screener."""

    def __init__(self) -> None:
        """Initialize the breakout screener."""
        self.universe_tickers: List[str] = []
        self.signals: List[Dict] = []
        self.load_universe()

    def load_universe(self) -> None:
        """Load the master ticker list from Spaces."""
        try:
            universe_key = config.get_spaces_path(*config.MASTER_TICKERLIST_PATH)
            df = spaces_io.download_dataframe(universe_key)

            if df is not None and not df.empty:
                active_tickers = df[
                    (df["active"] == 1) & (df["fetch_daily"] == 1)
                ]["symbol"].tolist()
                self.universe_tickers = active_tickers
                logger.info(f"Loaded {len(active_tickers)} tickers for breakout screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS

        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_breakout_screen(self) -> bool:
        """Run the breakout screening process."""
        logger.job_start("BreakoutScreener.run_breakout_screen")
        start_time = time.time()

        try:
            self.signals.clear()
            successful_tickers = 0

            for ticker in self.universe_tickers:
                try:
                    signals = self.screen_ticker(ticker)
                    if signals:
                        self.signals.extend(signals)
                        successful_tickers += 1

                except Exception as e:
                    logger.error(f"Error screening {ticker}: {e}")

            self.save_signals()

            duration = time.time() - start_time
            logger.job_complete(
                "BreakoutScreener.run_breakout_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_signals=len(self.signals),
            )

            return True

        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "BreakoutScreener.run_breakout_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """Screen a ticker for breakout setups."""
        try:
            data_key = daily_key(ticker)
            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty or len(df) < 25:
                return []

            df = self._prepare_data(df)
            recent_data = df.tail(25)  # Last 25 days

            # Find consolidation box
            box = self._find_consolidation_box(recent_data)
            if not box:
                return []

            # Check for breakout
            latest_bar = recent_data.iloc[-1]
            signal = self._check_breakout(ticker, box, latest_bar, recent_data)

            return [signal] if signal else []

        except Exception as e:
            logger.error(f"Error screening {ticker}: {e}")
            return []

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare daily data with indicators."""
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["range"] = df["high"] - df["low"]
        df["avg_range_20"] = df["range"].rolling(20, min_periods=10).mean()
        df["avg_volume_20"] = df["volume"].rolling(20, min_periods=10).mean()
        return df

    def _find_consolidation_box(self, df: pd.DataFrame) -> Optional[Dict]:
        """Find consolidation box in recent data."""
        try:
            # Look for 10-20 day consolidation
            lookback = min(20, len(df))
            consolidation_data = df.tail(lookback)

            box_high = consolidation_data["high"].max()
            box_low = consolidation_data["low"].min()
            box_height = box_high - box_low

            # Check for range compression
            avg_range = consolidation_data["avg_range_20"].iloc[-1]
            if box_height < avg_range * 1.5:  # Tight range
                return {
                    "box_high": box_high,
                    "box_low": box_low,
                    "box_height": box_height,
                    "consolidation_days": len(consolidation_data),
                }

            return None

        except Exception:
            return None

    def _check_breakout(self, ticker: str, box: Dict, bar: pd.Series, df: pd.DataFrame) -> Optional[Dict]:
        """Check for valid breakout."""
        try:
            # Check for breakout above box high
            if bar["close"] <= box["box_high"]:
                return None

            # Volume confirmation
            avg_volume = bar["avg_volume_20"]
            vol_ratio = bar["volume"] / avg_volume if avg_volume > 0 else 0

            if vol_ratio < 1.5:  # Need 150% of average volume
                return None

            # Calculate trade parameters
            entry = bar["close"]
            stop = box["box_low"]
            risk_per_share = entry - stop

            if risk_per_share <= 0:
                return None

            # Targets based on box height
            tp1 = entry + box["box_height"]
            tp2 = entry + (2 * box["box_height"])
            tp3 = entry + (3 * box["box_height"])

            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)

            signal = {
                "timestamp_utc": utc_now().isoformat(),
                "symbol": ticker,
                "direction": "long",
                "setup_name": "breakout",
                "score": min(10.0, 5.0 + vol_ratio),
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": (tp1 - entry) / risk_per_share,
                "r_multiple_at_tp2": (tp2 - entry) / risk_per_share,
                "r_multiple_at_tp3": (tp3 - entry) / risk_per_share,
                "notes": f"Box breakout, vol {vol_ratio:.1f}x",
                "box_high": box["box_high"],
                "box_low": box["box_low"],
                "box_height": box["box_height"],
                "vol_ratio": vol_ratio,
                "position_size": position_size,
            }

            return signal

        except Exception as e:
            logger.error(f"Error checking breakout: {e}")
            return None

    def save_signals(self) -> bool:
        """Save breakout signals to Spaces."""
        try:
            if not self.signals:
                return True

            df = pd.DataFrame(self.signals)
            df["generated_at"] = utc_now().isoformat()

            signals_key = config.get_spaces_path("data", "signals", "breakout.csv")
            success = spaces_io.upload_dataframe(df, signals_key)

            if success:
                logger.info(f"Saved {len(df)} breakout signals")

            return success

        except Exception as e:
            logger.error(f"Error saving breakout signals: {e}")
            return False


def main():
    """Main entry point for breakout screener."""
    screener = BreakoutScreener()

    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()

    logger.info(f"--- Running Breakout Screener --- {deployment_info}")

    success = screener.run_breakout_screen()
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
