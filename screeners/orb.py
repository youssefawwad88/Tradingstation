"""Opening Range Breakout (ORB) Strategy Screener.

This module implements the classic Opening Range Breakout strategy that identifies
breakouts from the first N minutes of trading with volume confirmation.
"""

import time
from datetime import timedelta
from typing import Dict, List, Optional

import pandas as pd

from utils.config import config
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import (
    convert_to_utc,
    get_market_time,
    get_session_window,
    is_in_session_window,
    utc_now,
)

logger = get_logger(__name__)


class ORBScreener:
    """Opening Range Breakout strategy screener."""

    def __init__(self, or_window_minutes: int = 15) -> None:
        """Initialize the ORB screener.
        
        Args:
            or_window_minutes: Opening range window in minutes (5, 15, or 30)
        """
        self.or_window_minutes = or_window_minutes
        self.universe_tickers: List[str] = []
        self.signals: List[Dict] = []
        self.load_universe()

    def load_universe(self) -> None:
        """Load the master ticker list from Spaces."""
        try:
            universe_key = config.get_spaces_path(*config.MASTER_TICKERLIST_PATH)
            df = spaces_io.download_dataframe(universe_key)

            if df is not None and not df.empty:
                # Filter for active tickers with 1min data enabled
                active_tickers = df[
                    (df["active"] == 1) & (df["fetch_1min"] == 1)
                ]["symbol"].tolist()
                self.universe_tickers = active_tickers
                logger.info(f"Loaded {len(active_tickers)} tickers for ORB screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS
                logger.warning("Universe not found, using fallback tickers")

        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_orb_screen(self) -> bool:
        """Run the complete ORB screening process.
        
        Returns:
            True if successful, False otherwise
        """
        logger.job_start("ORBScreener.run_orb_screen")
        start_time = time.time()

        try:
            current_time = get_market_time()
            self.signals.clear()

            # Check if we're in a valid screening window
            if not self._is_valid_screening_time():
                logger.info("Outside valid ORB screening window")
                return True

            # Screeners guardrail: Check if 1-min data is stale or provider degraded
            if self._is_data_stale_or_degraded():
                logger.info("ORB screening suppressed - stale 1-min data or degraded provider")
                return True

            successful_tickers = 0

            for ticker in self.universe_tickers:
                try:
                    signals = self.screen_ticker(ticker)
                    if signals:
                        self.signals.extend(signals)
                        successful_tickers += 1
                        logger.debug(f"Found {len(signals)} ORB signals for {ticker}")

                except Exception as e:
                    logger.error(f"Error screening {ticker}: {e}")

            # Save signals
            self.save_signals()

            duration = time.time() - start_time
            logger.job_complete(
                "ORBScreener.run_orb_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_tickers=len(self.universe_tickers),
                total_signals=len(self.signals),
                or_window=self.or_window_minutes,
            )

            return True

        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "ORBScreener.run_orb_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """Screen a specific ticker for ORB setups.
        
        Args:
            ticker: Stock symbol to screen
            
        Returns:
            List of signal dictionaries
        """
        try:
            # Load 1-minute data
            intraday_key = config.get_spaces_path("data", "intraday", "1min", f"{ticker}.csv")
            df = spaces_io.download_dataframe(intraday_key)

            if df is None or df.empty:
                logger.debug(f"No 1-minute data for {ticker}")
                return []

            # Prepare data
            df = self._prepare_intraday_data(df)

            # Get today's data
            current_date = get_market_time().date()
            today_data = df[df["date"] == current_date].copy()

            if today_data.empty:
                logger.debug(f"No today's data for {ticker}")
                return []

            # Get regular session data only
            session_data = today_data[today_data["is_session"]].copy()

            if session_data.empty:
                logger.debug(f"No session data for {ticker}")
                return []

            # Calculate opening range
            or_data = self._calculate_opening_range(session_data)
            if or_data is None:
                return []

            # Calculate session VWAP
            session_vwap = self._calculate_session_vwap(session_data)

            # Get data after opening range completion
            after_or_data = self._get_after_or_data(session_data, or_data)

            if after_or_data.empty:
                return []

            # Check for valid setups
            signals = []

            # Long setup (breakout above OR high)
            long_signal = self._check_long_setup(
                ticker, or_data, session_vwap, after_or_data, session_data
            )
            if long_signal:
                signals.append(long_signal)

            # Short setup (breakdown below OR low)
            short_signal = self._check_short_setup(
                ticker, or_data, session_vwap, after_or_data, session_data
            )
            if short_signal:
                signals.append(short_signal)

            return signals

        except Exception as e:
            logger.error(f"Error screening ticker {ticker}: {e}")
            return []

    def _is_valid_screening_time(self) -> bool:
        """Check if current time is valid for ORB screening."""
        current_time = get_market_time()

        # Must be a weekday
        if current_time.weekday() >= 5:
            return False

        # Must be during market hours or shortly after opening range
        market_open, market_close = get_session_window()
        or_completion_time = market_open + timedelta(minutes=self.or_window_minutes + 5)

        return market_open <= current_time <= market_close

    def _is_data_stale_or_degraded(self) -> bool:
        """Check if 1-min data is stale or provider is degraded.
        
        Per requirements: if latest 1-min age > 10 min (ET) or provider degraded
        → suppress new live signals for that cycle.
        
        Returns:
            True if data is stale or degraded, False otherwise
        """
        try:
            from utils.providers.router import health_check

            # Check provider health first
            is_healthy, status_msg = health_check()
            if not is_healthy:
                logger.warning(f"Provider degraded: {status_msg}")
                return True

            # Check 1-min data freshness for a sample ticker (use first from universe)
            if not self.universe_tickers:
                return False

            sample_ticker = self.universe_tickers[0]
            data_key = config.get_spaces_path("data", "intraday", "1min", f"{sample_ticker}.csv")
            df = spaces_io.download_dataframe(data_key)

            if df is None or df.empty:
                logger.warning("No 1-min data available for staleness check")
                return True

            # Get latest timestamp and convert to ET for age calculation
            latest_timestamp_utc = pd.to_datetime(df["timestamp"].max(), utc=True)

            from utils.time_utils import convert_utc_to_et
            latest_et = convert_utc_to_et(latest_timestamp_utc)
            now_et = convert_utc_to_et(utc_now())

            age_minutes = (now_et - latest_et).total_seconds() / 60

            # Check if age exceeds threshold (10 minutes)
            if age_minutes > 10:
                logger.warning(f"1-min data is stale: {age_minutes:.1f} minutes old (threshold: 10m)")
                return True

            return False

        except Exception as e:
            logger.error(f"Error checking data staleness: {e}")
            # On error, be conservative and suppress signals
            return True

    def _prepare_intraday_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare intraday data with calculated fields."""
        df = df.copy()

        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Add date column for grouping
        df["date"] = df["timestamp"].dt.date

        # Add market session flag
        df["is_session"] = df["timestamp"].apply(
            lambda x: is_in_session_window(x)
        )

        # Calculate VWAP components
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["vwap_numerator"] = df["typical_price"] * df["volume"]
        df["vwap_denominator"] = df["volume"]

        return df

    def _calculate_opening_range(self, session_data: pd.DataFrame) -> Optional[Dict]:
        """Calculate opening range high and low."""
        try:
            if session_data.empty:
                return None

            # Get first N minutes of data
            market_open = session_data.iloc[0]["timestamp"]
            or_end_time = market_open + timedelta(minutes=self.or_window_minutes)

            or_data = session_data[session_data["timestamp"] <= or_end_time]

            if or_data.empty:
                return None

            or_high = or_data["high"].max()
            or_low = or_data["low"].min()
            or_volume = or_data["volume"].sum()
            or_avg_volume = or_data["volume"].mean()

            # Calculate OR body percentage (close vs open of OR)
            or_open = or_data.iloc[0]["open"]
            or_close = or_data.iloc[-1]["close"]
            or_range = or_high - or_low
            or_body = abs(or_close - or_open)
            or_body_pct = (or_body / or_range) if or_range > 0 else 0

            return {
                "or_high": or_high,
                "or_low": or_low,
                "or_range": or_range,
                "or_open": or_open,
                "or_close": or_close,
                "or_volume": or_volume,
                "or_avg_volume": or_avg_volume,
                "or_body_pct": or_body_pct,
                "or_end_time": or_end_time,
                "or_bars": len(or_data),
            }

        except Exception as e:
            logger.error(f"Error calculating opening range: {e}")
            return None

    def _calculate_session_vwap(self, session_data: pd.DataFrame) -> Optional[float]:
        """Calculate regular session VWAP."""
        try:
            if session_data.empty:
                return None

            total_vwap_num = session_data["vwap_numerator"].sum()
            total_vwap_den = session_data["vwap_denominator"].sum()

            if total_vwap_den > 0:
                return total_vwap_num / total_vwap_den
            else:
                return session_data["close"].mean()

        except Exception as e:
            logger.error(f"Error calculating session VWAP: {e}")
            return None

    def _get_after_or_data(self, session_data: pd.DataFrame, or_data: Dict) -> pd.DataFrame:
        """Get data after opening range completion."""
        try:
            or_end_time = or_data["or_end_time"]
            after_or_data = session_data[session_data["timestamp"] > or_end_time].copy()
            return after_or_data

        except Exception as e:
            logger.error(f"Error getting after-OR data: {e}")
            return pd.DataFrame()

    def _check_long_setup(
        self,
        ticker: str,
        or_data: Dict,
        session_vwap: Optional[float],
        after_or_data: pd.DataFrame,
        session_data: pd.DataFrame,
    ) -> Optional[Dict]:
        """Check for valid long ORB setup."""
        try:
            if session_vwap is None or after_or_data.empty:
                return None

            or_high = or_data["or_high"]
            or_low = or_data["or_low"]

            # Find breakout above OR high
            breakout_bars = after_or_data[
                (after_or_data["high"] > or_high) &
                (after_or_data["close"] > or_high)
            ]

            if breakout_bars.empty:
                return None

            # Get first breakout bar
            first_breakout = breakout_bars.iloc[0]

            # Volume confirmation: breakout volume > OR average volume
            breakout_volume = first_breakout["volume"]
            vol_confirm = breakout_volume > or_data["or_avg_volume"]

            if not vol_confirm:
                return None

            # VWAP filter: price should be above VWAP for long
            vwap_filter = first_breakout["close"] > session_vwap

            if not vwap_filter:
                return None

            # Calculate trade parameters
            entry = first_breakout["close"]

            # Stop loss: OR low or just below VWAP (whichever is tighter)
            stop_candidates = [
                or_low,
                session_vwap * 0.995,  # Just below VWAP
            ]
            stop = max(stop_candidates)  # Use tightest (highest for long)

            # Ensure stop is below entry
            if stop >= entry:
                stop = entry * 0.99  # 1% below entry as fallback

            # Risk per share
            risk_per_share = entry - stop

            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share) if risk_per_share > 0 else 0

            # Targets based on OR height
            or_height = or_data["or_range"]
            tp1 = entry + or_height  # 1x OR height
            tp2 = entry + (2 * or_height)  # 2x OR height
            tp3 = entry + (3 * or_height)  # 3x OR height

            # Calculate R-multiples
            r1 = (tp1 - entry) / risk_per_share if risk_per_share > 0 else 0
            r2 = (tp2 - entry) / risk_per_share if risk_per_share > 0 else 0
            r3 = (tp3 - entry) / risk_per_share if risk_per_share > 0 else 0

            # Confidence score
            score = self._calculate_confidence_score(
                or_data, vol_confirm, vwap_filter, "long"
            )

            signal = {
                "timestamp_utc": convert_to_utc(first_breakout["timestamp"]).isoformat() + "Z",
                "symbol": ticker,
                "direction": "long",
                "setup_name": "orb",
                "score": score,
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": r1,
                "r_multiple_at_tp2": r2,
                "r_multiple_at_tp3": r3,
                "notes": f"ORB {self.or_window_minutes}min breakout above {or_high:.2f}",

                # ORB specific fields
                "or_high": or_high,
                "or_low": or_low,
                "or_window": self.or_window_minutes,
                "vol_confirm": vol_confirm,
                "vwap_filter": vwap_filter,
                "or_range": or_data["or_range"],
                "breakout_volume": breakout_volume,
                "or_avg_volume": or_data["or_avg_volume"],
                "volume_ratio": breakout_volume / or_data["or_avg_volume"],
                "risk_per_share": risk_per_share,
                "position_size": position_size,
            }

            return signal

        except Exception as e:
            logger.error(f"Error checking long ORB setup for {ticker}: {e}")
            return None

    def _check_short_setup(
        self,
        ticker: str,
        or_data: Dict,
        session_vwap: Optional[float],
        after_or_data: pd.DataFrame,
        session_data: pd.DataFrame,
    ) -> Optional[Dict]:
        """Check for valid short ORB setup."""
        try:
            if session_vwap is None or after_or_data.empty:
                return None

            or_high = or_data["or_high"]
            or_low = or_data["or_low"]

            # Find breakdown below OR low
            breakdown_bars = after_or_data[
                (after_or_data["low"] < or_low) &
                (after_or_data["close"] < or_low)
            ]

            if breakdown_bars.empty:
                return None

            # Get first breakdown bar
            first_breakdown = breakdown_bars.iloc[0]

            # Volume confirmation: breakdown volume > OR average volume
            breakdown_volume = first_breakdown["volume"]
            vol_confirm = breakdown_volume > or_data["or_avg_volume"]

            if not vol_confirm:
                return None

            # VWAP filter: price should be below VWAP for short
            vwap_filter = first_breakdown["close"] < session_vwap

            if not vwap_filter:
                return None

            # Calculate trade parameters
            entry = first_breakdown["close"]

            # Stop loss: OR high or just above VWAP (whichever is tighter)
            stop_candidates = [
                or_high,
                session_vwap * 1.005,  # Just above VWAP
            ]
            stop = min(stop_candidates)  # Use tightest (lowest for short)

            # Ensure stop is above entry
            if stop <= entry:
                stop = entry * 1.01  # 1% above entry as fallback

            # Risk per share
            risk_per_share = stop - entry

            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share) if risk_per_share > 0 else 0

            # Targets based on OR height
            or_height = or_data["or_range"]
            tp1 = entry - or_height  # 1x OR height
            tp2 = entry - (2 * or_height)  # 2x OR height
            tp3 = entry - (3 * or_height)  # 3x OR height

            # Calculate R-multiples
            r1 = (entry - tp1) / risk_per_share if risk_per_share > 0 else 0
            r2 = (entry - tp2) / risk_per_share if risk_per_share > 0 else 0
            r3 = (entry - tp3) / risk_per_share if risk_per_share > 0 else 0

            # Confidence score
            score = self._calculate_confidence_score(
                or_data, vol_confirm, vwap_filter, "short"
            )

            signal = {
                "timestamp_utc": convert_to_utc(first_breakdown["timestamp"]).isoformat() + "Z",
                "symbol": ticker,
                "direction": "short",
                "setup_name": "orb",
                "score": score,
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": r1,
                "r_multiple_at_tp2": r2,
                "r_multiple_at_tp3": r3,
                "notes": f"ORB {self.or_window_minutes}min breakdown below {or_low:.2f}",

                # ORB specific fields
                "or_high": or_high,
                "or_low": or_low,
                "or_window": self.or_window_minutes,
                "vol_confirm": vol_confirm,
                "vwap_filter": vwap_filter,
                "or_range": or_data["or_range"],
                "breakdown_volume": breakdown_volume,
                "or_avg_volume": or_data["or_avg_volume"],
                "volume_ratio": breakdown_volume / or_data["or_avg_volume"],
                "risk_per_share": risk_per_share,
                "position_size": position_size,
            }

            return signal

        except Exception as e:
            logger.error(f"Error checking short ORB setup for {ticker}: {e}")
            return None

    def _calculate_confidence_score(
        self,
        or_data: Dict,
        vol_confirm: bool,
        vwap_filter: bool,
        direction: str,
    ) -> float:
        """Calculate confidence score for the ORB setup."""
        score = 5.0  # Base score

        # Opening range quality (0-3 points)
        or_range_pct = (or_data["or_range"] / or_data["or_close"]) * 100
        if or_range_pct >= 2.0:
            score += 3.0
        elif or_range_pct >= 1.0:
            score += 2.0
        elif or_range_pct >= 0.5:
            score += 1.0

        # Volume confirmation (0-2 points)
        if vol_confirm:
            score += 2.0

        # VWAP filter (0-2 points)
        if vwap_filter:
            score += 2.0

        # OR body percentage (0-1 point) - prefer tighter ranges
        if or_data["or_body_pct"] <= 0.3:
            score += 1.0

        return min(10.0, score)

    def save_signals(self) -> bool:
        """Save ORB signals to Spaces."""
        try:
            if not self.signals:
                logger.info("No ORB signals to save")
                return True

            # Convert to DataFrame
            df = pd.DataFrame(self.signals)

            # Add metadata
            df["generated_at"] = utc_now().isoformat()
            df["deployment"] = config.DEPLOYMENT_TAG or "unknown"

            # Save to Spaces
            signals_key = config.get_spaces_path("data", "signals", "orb.csv")

            metadata = {
                "strategy": "orb",
                "or_window_minutes": str(self.or_window_minutes),
                "total_signals": str(len(df)),
                "long_signals": str(len(df[df["direction"] == "long"])),
                "short_signals": str(len(df[df["direction"] == "short"])),
                "generation_date": utc_now().isoformat(),
            }

            success = spaces_io.upload_dataframe(df, signals_key, metadata=metadata)

            if success:
                logger.info(f"Saved {len(df)} ORB signals (OR window: {self.or_window_minutes}min)")

                # Log trade signals
                for signal in self.signals:
                    logger.trade_signal(signal)

            return success

        except Exception as e:
            logger.error(f"Error saving ORB signals: {e}")
            return False


def main():
    """Main entry point for ORB screener."""
    import argparse

    parser = argparse.ArgumentParser(description="Opening Range Breakout (ORB) Screener")
    parser.add_argument(
        "--or-window",
        type=int,
        choices=[5, 15, 30],
        default=15,
        help="Opening range window in minutes",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers (overrides universe)",
    )

    args = parser.parse_args()

    screener = ORBScreener(or_window_minutes=args.or_window)

    # Override tickers if specified
    if args.tickers:
        screener.universe_tickers = [t.strip().upper() for t in args.tickers.split(",")]
        logger.info(f"Using custom ticker list: {screener.universe_tickers}")

    # Log deployment info
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()

    logger.info(f"--- Running ORB Screener ({args.or_window}min) --- {deployment_info}")

    success = screener.run_orb_screen()

    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
