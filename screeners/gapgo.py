"""
Gap & Go Strategy Screener - Umar Ashraf's momentum continuation strategy.

This module implements the Gap & Go strategy that identifies stocks gapping up
or down with strong premarket momentum and volume confirmation for continuation.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from utils.config import config
from utils.helpers import calculate_r_multiple
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import (
    convert_to_utc,
    get_breakout_guard_time,
    get_early_volume_window,
    get_market_time,
    get_premarket_window,
    get_session_window,
    is_in_premarket_window,
    is_in_session_window,
    utc_now,
)

logger = get_logger(__name__)


class GapAndGoScreener:
    """Gap & Go strategy screener with Umar Ashraf's methodology."""

    def __init__(self) -> None:
        """Initialize the Gap & Go screener."""
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
                logger.info(f"Loaded {len(active_tickers)} tickers for Gap & Go screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS
                logger.warning("Universe not found, using fallback tickers")
                
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_gap_and_go_screen(self) -> bool:
        """
        Run the complete Gap & Go screening process.
        
        Returns:
            True if successful, False otherwise
        """
        logger.job_start("GapAndGoScreener.run_gap_and_go_screen")
        start_time = time.time()
        
        try:
            current_time = get_market_time()
            self.signals.clear()
            
            # Check if we're in a valid screening window
            if not self._is_valid_screening_time():
                logger.info("Outside valid Gap & Go screening window")
                return True
            
            # Screeners guardrail: Check if 1-min data is stale or provider degraded
            if self._is_data_stale_or_degraded():
                logger.info("Gap & Go screening suppressed - stale 1-min data or degraded provider")
                return True
            
            successful_tickers = 0
            
            for ticker in self.universe_tickers:
                try:
                    signals = self.screen_ticker(ticker)
                    if signals:
                        self.signals.extend(signals)
                        successful_tickers += 1
                        logger.debug(f"Found {len(signals)} Gap & Go signals for {ticker}")
                
                except Exception as e:
                    logger.error(f"Error screening {ticker}: {e}")
            
            # Save signals
            self.save_signals()
            
            duration = time.time() - start_time
            logger.job_complete(
                "GapAndGoScreener.run_gap_and_go_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_tickers=len(self.universe_tickers),
                total_signals=len(self.signals),
            )
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "GapAndGoScreener.run_gap_and_go_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """
        Screen a specific ticker for Gap & Go setups.
        
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
            
            # Load daily data for gap calculation
            daily_key = config.get_spaces_path("data", "daily", f"{ticker}.csv")
            daily_df = spaces_io.download_dataframe(daily_key)
            
            if daily_df is None or daily_df.empty:
                logger.debug(f"No daily data for {ticker}")
                return []
            
            # Prepare data
            df = self._prepare_intraday_data(df)
            daily_df = self._prepare_daily_data(daily_df)
            
            # Get today's data
            current_date = get_market_time().date()
            today_data = df[df["date"] == current_date].copy()
            
            if today_data.empty:
                logger.debug(f"No today's data for {ticker}")
                return []
            
            # Calculate gap
            gap_pct = self._calculate_gap(daily_df, today_data)
            if gap_pct is None:
                return []
            
            # Calculate premarket levels
            premarket_levels = self._calculate_premarket_levels(today_data)
            if not premarket_levels:
                return []
            
            # Calculate regular session VWAP
            session_vwap = self._calculate_session_vwap(today_data)
            
            # Calculate early volume metrics
            volume_metrics = self._calculate_volume_metrics(ticker, today_data, daily_df)
            
            # Check for valid setups
            signals = []
            
            # Long setup
            long_signal = self._check_long_setup(
                ticker, gap_pct, premarket_levels, session_vwap, 
                volume_metrics, today_data
            )
            if long_signal:
                signals.append(long_signal)
            
            # Short setup
            short_signal = self._check_short_setup(
                ticker, gap_pct, premarket_levels, session_vwap, 
                volume_metrics, today_data
            )
            if short_signal:
                signals.append(short_signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error screening ticker {ticker}: {e}")
            return []

    def _is_valid_screening_time(self) -> bool:
        """Check if current time is valid for Gap & Go screening."""
        current_time = get_market_time()
        
        # Must be a weekday
        if current_time.weekday() >= 5:
            return False
        
        # Must be after premarket start and before market close
        premarket_start, _ = get_premarket_window()
        _, market_close = get_session_window()
        
        return premarket_start <= current_time <= market_close

    def _is_data_stale_or_degraded(self) -> bool:
        """
        Check if 1-min data is stale or provider is degraded.
        
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
        
        # Add market session flags
        df["is_premarket"] = df["timestamp"].apply(
            lambda x: is_in_premarket_window(x)
        )
        df["is_session"] = df["timestamp"].apply(
            lambda x: is_in_session_window(x)
        )
        
        # Calculate VWAP components
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["vwap_numerator"] = df["typical_price"] * df["volume"]
        df["vwap_denominator"] = df["volume"]
        
        return df

    def _prepare_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare daily data for gap calculations."""
        df = df.copy()
        
        # Ensure date is proper date type
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        
        # Sort by date
        df = df.sort_values("date").reset_index(drop=True)
        
        return df

    def _calculate_gap(self, daily_df: pd.DataFrame, today_data: pd.DataFrame) -> Optional[float]:
        """Calculate gap percentage."""
        try:
            # Get yesterday's close
            current_date = get_market_time().date()
            yesterday_data = daily_df[daily_df["date"] < current_date]
            
            if yesterday_data.empty:
                return None
            
            prev_close = yesterday_data.iloc[-1]["close"]
            
            # Get today's open (first bar of regular session)
            session_data = today_data[today_data["is_session"]]
            if session_data.empty:
                return None
            
            today_open = session_data.iloc[0]["open"]
            
            # Calculate gap percentage
            gap_pct = ((today_open - prev_close) / prev_close) * 100
            return gap_pct
            
        except Exception as e:
            logger.error(f"Error calculating gap: {e}")
            return None

    def _calculate_premarket_levels(self, today_data: pd.DataFrame) -> Optional[Dict]:
        """Calculate premarket high, low, and VWAP."""
        try:
            premarket_data = today_data[today_data["is_premarket"]]
            
            if premarket_data.empty:
                return None
            
            premarket_high = premarket_data["high"].max()
            premarket_low = premarket_data["low"].min()
            
            # Calculate premarket VWAP
            total_vwap_num = premarket_data["vwap_numerator"].sum()
            total_vwap_den = premarket_data["vwap_denominator"].sum()
            
            if total_vwap_den > 0:
                premarket_vwap = total_vwap_num / total_vwap_den
            else:
                premarket_vwap = premarket_data["close"].mean()
            
            return {
                "premarket_high": premarket_high,
                "premarket_low": premarket_low,
                "premarket_vwap": premarket_vwap,
            }
            
        except Exception as e:
            logger.error(f"Error calculating premarket levels: {e}")
            return None

    def _calculate_session_vwap(self, today_data: pd.DataFrame) -> Optional[float]:
        """Calculate regular session VWAP."""
        try:
            session_data = today_data[today_data["is_session"]]
            
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

    def _calculate_volume_metrics(
        self, 
        ticker: str, 
        today_data: pd.DataFrame, 
        daily_df: pd.DataFrame
    ) -> Dict:
        """Calculate early volume metrics."""
        try:
            current_time = get_market_time()
            current_date = current_time.date()
            
            # Get early volume window (09:30-09:44)
            early_window_start, early_window_end = get_early_volume_window(current_date)
            
            # Get today's early volume
            today_early = today_data[
                (today_data["timestamp"] >= early_window_start) &
                (today_data["timestamp"] <= early_window_end)
            ]
            
            today_early_vol = today_early["volume"].sum() if not today_early.empty else 0
            
            # Get average early volume from last 5 sessions
            recent_days = daily_df.tail(5)
            avg_early_vol_5d = 0
            
            if len(recent_days) > 0:
                # Estimate early volume as ~3% of daily volume (rough approximation)
                avg_daily_vol = recent_days["volume"].mean()
                avg_early_vol_5d = avg_daily_vol * 0.03  # 3% estimate
            
            # Calculate volume spike ratio
            vol_spike_ratio = (today_early_vol / avg_early_vol_5d) if avg_early_vol_5d > 0 else 0
            vol_spike_ok = vol_spike_ratio >= config.VOLUME_SPIKE_THRESHOLD
            
            return {
                "today_early_vol": today_early_vol,
                "avg_early_vol_5d": avg_early_vol_5d,
                "vol_spike_ratio": vol_spike_ratio,
                "vol_spike_ok": vol_spike_ok,
            }
            
        except Exception as e:
            logger.error(f"Error calculating volume metrics for {ticker}: {e}")
            return {
                "today_early_vol": 0,
                "avg_early_vol_5d": 0,
                "vol_spike_ratio": 0,
                "vol_spike_ok": False,
            }

    def _check_long_setup(
        self,
        ticker: str,
        gap_pct: float,
        premarket_levels: Dict,
        session_vwap: Optional[float],
        volume_metrics: Dict,
        today_data: pd.DataFrame,
    ) -> Optional[Dict]:
        """Check for valid long Gap & Go setup."""
        try:
            # Check gap requirement
            if gap_pct < config.MIN_GAP_LONG_PCT:
                return None
            
            # Check volume spike
            if not volume_metrics["vol_spike_ok"]:
                return None
            
            # Check if we have session VWAP
            if session_vwap is None:
                return None
            
            # Check for breakout after guard time
            current_time = get_market_time()
            current_date = current_time.date()
            guard_time = get_breakout_guard_time(current_date)
            
            # Get data after guard time
            after_guard_data = today_data[
                (today_data["timestamp"] >= guard_time) &
                (today_data["is_session"])
            ]
            
            if after_guard_data.empty:
                return None
            
            # Check for breakout above premarket high
            premarket_high = premarket_levels["premarket_high"]
            breakout_bars = after_guard_data[after_guard_data["close"] > premarket_high]
            
            if breakout_bars.empty:
                return None
            
            # Get first breakout bar
            first_breakout = breakout_bars.iloc[0]
            
            # Check VWAP reclaim (price above session VWAP)
            vwap_reclaimed = first_breakout["close"] > session_vwap
            if not vwap_reclaimed:
                return None
            
            # Calculate trade parameters
            entry = first_breakout["close"]
            
            # Stop loss: below VWAP or breakout candle low (tightest valid)
            stop_candidates = [
                session_vwap * 0.995,  # Just below VWAP with buffer
                first_breakout["low"],  # Breakout candle low
            ]
            stop = max(stop_candidates)  # Use tightest (highest for long)
            
            # Risk per share
            risk_per_share = entry - stop
            if risk_per_share <= 0:
                return None
            
            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)
            
            # Targets (2R, 3R, 4R)
            tp1 = entry + (2 * risk_per_share)
            tp2 = entry + (3 * risk_per_share)
            tp3 = entry + (4 * risk_per_share)
            
            # R-multiples
            r1 = 2.0
            r2 = 3.0
            r3 = 4.0
            
            # Confidence score (0-10)
            score = self._calculate_confidence_score(
                gap_pct, volume_metrics["vol_spike_ratio"], 
                vwap_reclaimed, "long"
            )
            
            signal = {
                "timestamp_utc": convert_to_utc(first_breakout["timestamp"]).isoformat() + "Z",
                "symbol": ticker,
                "direction": "long",
                "setup_name": "gap_and_go",
                "score": score,
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": r1,
                "r_multiple_at_tp2": r2,
                "r_multiple_at_tp3": r3,
                "notes": f"Gap up {gap_pct:.1f}%, vol spike {volume_metrics['vol_spike_ratio']:.1f}x",
                
                # Gap & Go specific fields
                "premarket_high": premarket_high,
                "gap_pct": gap_pct,
                "vol_spike_ratio": volume_metrics["vol_spike_ratio"],
                "breakout_time": first_breakout["timestamp"].strftime("%H:%M:%S"),
                "vwap_reclaimed": vwap_reclaimed,
                "stop_basis": "vwap_buffer",
                "risk_per_share": risk_per_share,
                "position_size": position_size,
                "confidence_score": score,
            }
            
            return signal
            
        except Exception as e:
            logger.error(f"Error checking long setup for {ticker}: {e}")
            return None

    def _check_short_setup(
        self,
        ticker: str,
        gap_pct: float,
        premarket_levels: Dict,
        session_vwap: Optional[float],
        volume_metrics: Dict,
        today_data: pd.DataFrame,
    ) -> Optional[Dict]:
        """Check for valid short Gap & Go setup."""
        try:
            # Check gap requirement (negative for short)
            if gap_pct > config.MIN_GAP_SHORT_PCT:
                return None
            
            # Check volume spike
            if not volume_metrics["vol_spike_ok"]:
                return None
            
            # Check if we have session VWAP
            if session_vwap is None:
                return None
            
            # Check for breakdown after guard time
            current_time = get_market_time()
            current_date = current_time.date()
            guard_time = get_breakout_guard_time(current_date)
            
            # Get data after guard time
            after_guard_data = today_data[
                (today_data["timestamp"] >= guard_time) &
                (today_data["is_session"])
            ]
            
            if after_guard_data.empty:
                return None
            
            # Check for breakdown below premarket low
            premarket_low = premarket_levels["premarket_low"]
            breakdown_bars = after_guard_data[after_guard_data["close"] < premarket_low]
            
            if breakdown_bars.empty:
                return None
            
            # Get first breakdown bar
            first_breakdown = breakdown_bars.iloc[0]
            
            # Check VWAP rejection (price below session VWAP)
            vwap_rejected = first_breakdown["close"] < session_vwap
            if not vwap_rejected:
                return None
            
            # Calculate trade parameters
            entry = first_breakdown["close"]
            
            # Stop loss: above VWAP or breakdown candle high (tightest valid)
            stop_candidates = [
                session_vwap * 1.005,  # Just above VWAP with buffer
                first_breakdown["high"],  # Breakdown candle high
            ]
            stop = min(stop_candidates)  # Use tightest (lowest for short)
            
            # Risk per share
            risk_per_share = stop - entry
            if risk_per_share <= 0:
                return None
            
            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)
            
            # Targets (2R, 3R, 4R)
            tp1 = entry - (2 * risk_per_share)
            tp2 = entry - (3 * risk_per_share)
            tp3 = entry - (4 * risk_per_share)
            
            # R-multiples
            r1 = 2.0
            r2 = 3.0
            r3 = 4.0
            
            # Confidence score (0-10)
            score = self._calculate_confidence_score(
                abs(gap_pct), volume_metrics["vol_spike_ratio"], 
                vwap_rejected, "short"
            )
            
            signal = {
                "timestamp_utc": convert_to_utc(first_breakdown["timestamp"]).isoformat() + "Z",
                "symbol": ticker,
                "direction": "short",
                "setup_name": "gap_and_go",
                "score": score,
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": r1,
                "r_multiple_at_tp2": r2,
                "r_multiple_at_tp3": r3,
                "notes": f"Gap down {gap_pct:.1f}%, vol spike {volume_metrics['vol_spike_ratio']:.1f}x",
                
                # Gap & Go specific fields
                "premarket_low": premarket_low,
                "gap_pct": gap_pct,
                "vol_spike_ratio": volume_metrics["vol_spike_ratio"],
                "breakdown_time": first_breakdown["timestamp"].strftime("%H:%M:%S"),
                "vwap_rejected": vwap_rejected,
                "stop_basis": "vwap_buffer",
                "risk_per_share": risk_per_share,
                "position_size": position_size,
                "confidence_score": score,
            }
            
            return signal
            
        except Exception as e:
            logger.error(f"Error checking short setup for {ticker}: {e}")
            return None

    def _calculate_confidence_score(
        self, 
        gap_pct: float, 
        vol_ratio: float, 
        vwap_condition: bool, 
        direction: str
    ) -> float:
        """Calculate confidence score for the setup."""
        score = 5.0  # Base score
        
        # Gap quality (0-3 points)
        if gap_pct >= 5.0:
            score += 3.0
        elif gap_pct >= 3.0:
            score += 2.0
        elif gap_pct >= 2.0:
            score += 1.0
        
        # Volume quality (0-3 points)
        if vol_ratio >= 2.0:
            score += 3.0
        elif vol_ratio >= 1.5:
            score += 2.0
        elif vol_ratio >= 1.15:
            score += 1.0
        
        # VWAP condition (0-2 points)
        if vwap_condition:
            score += 2.0
        
        return min(10.0, score)

    def save_signals(self) -> bool:
        """Save Gap & Go signals to Spaces."""
        try:
            if not self.signals:
                logger.info("No Gap & Go signals to save")
                return True
            
            # Convert to DataFrame
            df = pd.DataFrame(self.signals)
            
            # Add metadata
            df["generated_at"] = utc_now().isoformat()
            df["deployment"] = config.DEPLOYMENT_TAG or "unknown"
            
            # Save to Spaces
            signals_key = config.get_spaces_path("data", "signals", "gapgo.csv")
            
            metadata = {
                "strategy": "gap_and_go",
                "total_signals": str(len(df)),
                "long_signals": str(len(df[df["direction"] == "long"])),
                "short_signals": str(len(df[df["direction"] == "short"])),
                "generation_date": utc_now().isoformat(),
            }
            
            success = spaces_io.upload_dataframe(df, signals_key, metadata=metadata)
            
            if success:
                logger.info(f"Saved {len(df)} Gap & Go signals")
                
                # Log trade signals
                for signal in self.signals:
                    logger.trade_signal(signal)
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving Gap & Go signals: {e}")
            return False


def main():
    """Main entry point for Gap & Go screener."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Gap & Go Strategy Screener")
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers (overrides universe)",
    )
    
    args = parser.parse_args()
    
    screener = GapAndGoScreener()
    
    # Override tickers if specified
    if args.tickers:
        screener.universe_tickers = [t.strip().upper() for t in args.tickers.split(",")]
        logger.info(f"Using custom ticker list: {screener.universe_tickers}")
    
    # Log deployment info
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()
    
    logger.info(f"--- Running Gap & Go Screener --- {deployment_info}")
    
    success = screener.run_gap_and_go_screen()
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)