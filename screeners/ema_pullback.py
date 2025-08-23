"""EMA Pullback Strategy Screener - Trend continuation pullbacks.

This module identifies pullbacks to EMA20 in uptrends with 
bullish reversal signals for continuation trades.
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


class EMAPullbackScreener:
    """EMA pullback strategy screener."""

    def __init__(self) -> None:
        """Initialize the EMA pullback screener."""
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
                logger.info(f"Loaded {len(active_tickers)} tickers for EMA pullback screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS

        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_ema_screen(self) -> bool:
        """Run the EMA pullback screening process."""
        logger.job_start("EMAPullbackScreener.run_ema_screen")
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
                "EMAPullbackScreener.run_ema_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_signals=len(self.signals),
            )

            return True

        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "EMAPullbackScreener.run_ema_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """Screen a ticker for EMA pullback setups."""
        try:
            data_key = daily_key(ticker)
            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty or len(df) < 60:
                return []

            df = self._prepare_data(df)

            # Check latest bar for setup
            latest_bar = df.iloc[-1]
            prev_bar = df.iloc[-2]

            signal = self._check_pullback_setup(ticker, latest_bar, prev_bar, df)

            return [signal] if signal else []

        except Exception as e:
            logger.error(f"Error screening {ticker}: {e}")
            return []

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare daily data with EMAs."""
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Calculate EMAs
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

        # Calculate volume average
        df["avg_volume_20"] = df["volume"].rolling(20, min_periods=10).mean()

        return df

    def _check_pullback_setup(self, ticker: str, bar: pd.Series, prev_bar: pd.Series, df: pd.DataFrame) -> Optional[Dict]:
        """Check for valid EMA pullback setup."""
        try:
            ema20 = bar["ema20"]
            ema50 = bar["ema50"]

            # Check trend: price above EMA20 > EMA50
            if not (bar["close"] > ema20 > ema50):
                return None

            # Check for pullback to EMA20 with reversal
            pullback_depth = abs(bar["low"] - ema20) / ema20

            # Must have touched or come close to EMA20
            if pullback_depth > 0.02:  # More than 2% away from EMA20
                return None

            # Check for bullish reversal candle
            is_bullish = bar["close"] > bar["open"]
            closes_above_ema = bar["close"] > ema20

            if not (is_bullish and closes_above_ema):
                return None

            # Calculate trade parameters
            entry = bar["close"]
            stop = min(bar["low"], ema20 * 0.99)  # Below pullback low or EMA20

            risk_per_share = entry - stop
            if risk_per_share <= 0:
                return None

            # Find resistance for target
            recent_highs = df.tail(20)["high"]
            resistance = recent_highs.max()

            # Targets
            tp1 = min(resistance, entry + (2 * risk_per_share))
            tp2 = entry + (3 * risk_per_share)
            tp3 = entry + (4 * risk_per_share)

            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)

            # Reversal candle quality
            body_size = abs(bar["close"] - bar["open"])
            candle_range = bar["high"] - bar["low"]
            rev_candle_type = "strong" if body_size / candle_range > 0.6 else "weak"

            signal = {
                "timestamp_utc": utc_now().isoformat(),
                "symbol": ticker,
                "direction": "long",
                "setup_name": "ema_pullback",
                "score": 7.0 if rev_candle_type == "strong" else 5.0,
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": (tp1 - entry) / risk_per_share,
                "r_multiple_at_tp2": (tp2 - entry) / risk_per_share,
                "r_multiple_at_tp3": (tp3 - entry) / risk_per_share,
                "notes": f"EMA20 pullback, {rev_candle_type} reversal",
                "ema20": ema20,
                "ema50": ema50,
                "pullback_depth": pullback_depth,
                "rev_candle_type": rev_candle_type,
                "position_size": position_size,
            }

            return signal

        except Exception as e:
            logger.error(f"Error checking pullback setup: {e}")
            return None

    def save_signals(self) -> bool:
        """Save EMA pullback signals to Spaces."""
        try:
            if not self.signals:
                return True

            df = pd.DataFrame(self.signals)
            df["generated_at"] = utc_now().isoformat()

            signals_key = config.get_spaces_path("data", "signals", "ema_pullback.csv")
            success = spaces_io.upload_dataframe(df, signals_key)

            if success:
                logger.info(f"Saved {len(df)} EMA pullback signals")

            return success

        except Exception as e:
            logger.error(f"Error saving EMA pullback signals: {e}")
            return False


def main():
    """Main entry point for EMA pullback screener."""
    screener = EMAPullbackScreener()

    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()

    logger.info(f"--- Running EMA Pullback Screener --- {deployment_info}")

    success = screener.run_ema_screen()
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
