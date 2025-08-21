"""
AVWAP Anchor Detection Job.

This module automatically detects and validates AVWAP anchors based on
gap days, power candles, and volume spikes for use in AVWAP reclaim strategies.
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from utils.config import config
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import get_market_time, get_trading_days_back, utc_now

logger = get_logger(__name__)


class AVWAPAnchorDetector:
    """Detector for AVWAP anchors based on technical analysis criteria."""

    def __init__(self) -> None:
        """Initialize the AVWAP anchor detector."""
        self.universe_tickers: List[str] = []
        self.anchors: Dict[str, List[Dict]] = {}
        self.load_universe()

    def load_universe(self) -> None:
        """Load the master ticker list from Spaces."""
        try:
            universe_key = config.get_spaces_path(*config.MASTER_TICKERLIST_PATH)
            df = spaces_io.download_dataframe(universe_key)
            
            if df is not None and not df.empty:
                # Filter for active tickers
                active_tickers = df[df["active"] == 1]["symbol"].tolist()
                self.universe_tickers = active_tickers
                logger.info(f"Loaded {len(active_tickers)} active tickers for anchor detection")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS
                logger.warning("Universe not found, using fallback tickers")
                
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def detect_all_anchors(self, lookback_days: int = 30) -> bool:
        """
        Detect AVWAP anchors for all tickers.
        
        Args:
            lookback_days: Number of days to look back for anchors
            
        Returns:
            True if successful, False otherwise
        """
        logger.job_start("AVWAPAnchorDetector.detect_all_anchors")
        start_time = time.time()
        
        try:
            successful_tickers = 0
            
            for ticker in self.universe_tickers:
                try:
                    anchors = self.detect_ticker_anchors(ticker, lookback_days)
                    if anchors:
                        self.anchors[ticker] = anchors
                        successful_tickers += 1
                        logger.debug(f"Found {len(anchors)} anchors for {ticker}")
                    
                except Exception as e:
                    logger.error(f"Error detecting anchors for {ticker}: {e}")
            
            # Save detected anchors
            self.save_anchors()
            
            duration = time.time() - start_time
            logger.job_complete(
                "AVWAPAnchorDetector.detect_all_anchors",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_tickers=len(self.universe_tickers),
                total_anchors=sum(len(anchors) for anchors in self.anchors.values()),
            )
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "AVWAPAnchorDetector.detect_all_anchors",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def detect_ticker_anchors(self, ticker: str, lookback_days: int = 30) -> List[Dict]:
        """
        Detect AVWAP anchors for a specific ticker.
        
        Args:
            ticker: Stock symbol
            lookback_days: Number of days to analyze
            
        Returns:
            List of anchor dictionaries
        """
        try:
            # Load daily data
            daily_key = config.get_spaces_path("data", "daily", f"{ticker}.csv")
            daily_df = spaces_io.download_dataframe(daily_key)
            
            if daily_df is None or daily_df.empty:
                logger.warning(f"No daily data found for {ticker}")
                return []
            
            # Ensure we have enough data
            if len(daily_df) < lookback_days:
                logger.warning(f"Insufficient daily data for {ticker}: {len(daily_df)} < {lookback_days}")
                lookback_days = len(daily_df)
            
            # Get recent data for analysis
            recent_df = daily_df.tail(lookback_days).copy()
            
            # Prepare data
            recent_df = self._prepare_daily_data(recent_df)
            
            # Detect different types of anchors
            anchors = []
            
            # Gap anchors
            gap_anchors = self._detect_gap_anchors(recent_df, ticker)
            anchors.extend(gap_anchors)
            
            # Power candle anchors
            power_anchors = self._detect_power_candle_anchors(recent_df, ticker)
            anchors.extend(power_anchors)
            
            # Volume spike anchors
            volume_anchors = self._detect_volume_spike_anchors(recent_df, ticker)
            anchors.extend(volume_anchors)
            
            # Remove duplicates and sort by date
            anchors = self._deduplicate_anchors(anchors)
            anchors = sorted(anchors, key=lambda x: x["date"], reverse=True)
            
            logger.debug(f"Detected {len(anchors)} anchors for {ticker}")
            return anchors
            
        except Exception as e:
            logger.error(f"Error detecting anchors for {ticker}: {e}")
            return []

    def _prepare_daily_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare daily data with calculated indicators."""
        df = df.copy()
        
        # Ensure date column
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        else:
            logger.error("No date column in daily data")
            return df
        
        # Calculate metrics
        df["range"] = df["high"] - df["low"]
        df["body"] = abs(df["close"] - df["open"])
        df["body_pct"] = df["body"] / df["range"]
        
        # Calculate average range and volume for comparison
        df["avg_range_20"] = df["range"].rolling(20, min_periods=5).mean()
        df["avg_volume_20"] = df["volume"].rolling(20, min_periods=5).mean()
        
        # Gap calculations
        df["prev_close"] = df["close"].shift(1)
        df["gap_pct"] = ((df["open"] - df["prev_close"]) / df["prev_close"]) * 100
        
        return df

    def _detect_gap_anchors(self, df: pd.DataFrame, ticker: str) -> List[Dict]:
        """Detect gap-based anchors."""
        anchors = []
        
        # Define gap thresholds
        min_gap_up = 2.0  # 2% gap up
        min_gap_down = -2.0  # 2% gap down
        
        for idx, row in df.iterrows():
            gap_pct = row.get("gap_pct", 0)
            
            # Gap up anchor
            if gap_pct >= min_gap_up:
                anchor = {
                    "symbol": ticker,
                    "date": row["date"],
                    "type": "gap_up",
                    "gap_pct": gap_pct,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "anchor_confirmed": False,  # Will be confirmed later
                    "notes": f"{gap_pct:.1f}% gap up",
                }
                anchors.append(anchor)
            
            # Gap down anchor
            elif gap_pct <= min_gap_down:
                anchor = {
                    "symbol": ticker,
                    "date": row["date"],
                    "type": "gap_down",
                    "gap_pct": gap_pct,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "anchor_confirmed": False,
                    "notes": f"{gap_pct:.1f}% gap down",
                }
                anchors.append(anchor)
        
        return anchors

    def _detect_power_candle_anchors(self, df: pd.DataFrame, ticker: str) -> List[Dict]:
        """Detect power candle anchors (wide range candles with strong bodies)."""
        anchors = []
        
        for idx, row in df.iterrows():
            range_val = row.get("range", 0)
            avg_range = row.get("avg_range_20", 0)
            body_pct = row.get("body_pct", 0)
            volume = row.get("volume", 0)
            avg_volume = row.get("avg_volume_20", 0)
            
            # Skip if insufficient data for comparison
            if avg_range == 0 or avg_volume == 0:
                continue
            
            # Power candle criteria
            range_multiple = range_val / avg_range if avg_range > 0 else 0
            volume_multiple = volume / avg_volume if avg_volume > 0 else 0
            
            # Power candle thresholds
            min_range_multiple = 1.5  # 150% of average range
            min_body_pct = 0.6  # 60% body
            min_volume_multiple = 1.2  # 120% of average volume
            
            if (range_multiple >= min_range_multiple and 
                body_pct >= min_body_pct and 
                volume_multiple >= min_volume_multiple):
                
                # Determine direction
                direction = "bullish" if row["close"] > row["open"] else "bearish"
                
                anchor = {
                    "symbol": ticker,
                    "date": row["date"],
                    "type": f"power_candle_{direction}",
                    "range_multiple": range_multiple,
                    "body_pct": body_pct,
                    "volume_multiple": volume_multiple,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "anchor_confirmed": False,
                    "notes": f"Power candle {direction}: {range_multiple:.1f}x range, {body_pct:.1f} body%",
                }
                anchors.append(anchor)
        
        return anchors

    def _detect_volume_spike_anchors(self, df: pd.DataFrame, ticker: str) -> List[Dict]:
        """Detect volume spike anchors."""
        anchors = []
        
        for idx, row in df.iterrows():
            volume = row.get("volume", 0)
            avg_volume = row.get("avg_volume_20", 0)
            
            if avg_volume == 0:
                continue
            
            volume_multiple = volume / avg_volume
            min_volume_spike = 2.0  # 200% of average volume
            
            if volume_multiple >= min_volume_spike:
                # Determine if it's an accumulation or distribution day
                close_position = (row["close"] - row["low"]) / (row["high"] - row["low"])
                volume_type = "accumulation" if close_position > 0.6 else "distribution"
                
                anchor = {
                    "symbol": ticker,
                    "date": row["date"],
                    "type": f"volume_spike_{volume_type}",
                    "volume_multiple": volume_multiple,
                    "close_position": close_position,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "anchor_confirmed": False,
                    "notes": f"Volume spike {volume_type}: {volume_multiple:.1f}x average",
                }
                anchors.append(anchor)
        
        return anchors

    def _deduplicate_anchors(self, anchors: List[Dict]) -> List[Dict]:
        """Remove duplicate anchors for the same date."""
        if not anchors:
            return anchors
        
        # Group by date
        date_groups = {}
        for anchor in anchors:
            date_key = anchor["date"]
            if date_key not in date_groups:
                date_groups[date_key] = []
            date_groups[date_key].append(anchor)
        
        # Keep best anchor per date (priority: gap > power_candle > volume_spike)
        deduped_anchors = []
        type_priority = {"gap_up": 1, "gap_down": 1, "power_candle_bullish": 2, 
                        "power_candle_bearish": 2, "volume_spike_accumulation": 3,
                        "volume_spike_distribution": 3}
        
        for date_key, date_anchors in date_groups.items():
            if len(date_anchors) == 1:
                deduped_anchors.append(date_anchors[0])
            else:
                # Sort by priority and take the best
                sorted_anchors = sorted(
                    date_anchors,
                    key=lambda x: type_priority.get(x["type"], 99)
                )
                deduped_anchors.append(sorted_anchors[0])
        
        return deduped_anchors

    def save_anchors(self) -> bool:
        """Save detected anchors to Spaces."""
        try:
            if not self.anchors:
                logger.info("No anchors to save")
                return True
            
            # Convert to DataFrame
            all_anchors = []
            for ticker, ticker_anchors in self.anchors.items():
                all_anchors.extend(ticker_anchors)
            
            if not all_anchors:
                logger.info("No anchors detected across all tickers")
                return True
            
            df = pd.DataFrame(all_anchors)
            
            # Add metadata
            df["detected_at"] = utc_now().isoformat()
            df["deployment"] = config.DEPLOYMENT_TAG or "unknown"
            
            # Save to Spaces
            anchors_key = config.get_spaces_path("data", "signals", "avwap_anchors.csv")
            
            metadata = {
                "type": "avwap_anchors",
                "total_anchors": str(len(df)),
                "unique_tickers": str(df["symbol"].nunique()),
                "detection_date": utc_now().isoformat(),
            }
            
            success = spaces_io.upload_dataframe(df, anchors_key, metadata=metadata)
            
            if success:
                logger.info(
                    f"Saved {len(df)} AVWAP anchors for {df['symbol'].nunique()} tickers"
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving anchors: {e}")
            return False

    def load_existing_anchors(self) -> bool:
        """Load existing anchors from Spaces."""
        try:
            anchors_key = config.get_spaces_path("data", "signals", "avwap_anchors.csv")
            df = spaces_io.download_dataframe(anchors_key)
            
            if df is None or df.empty:
                logger.info("No existing anchors found")
                return True
            
            # Group by ticker
            self.anchors = {}
            for ticker in df["symbol"].unique():
                ticker_df = df[df["symbol"] == ticker]
                self.anchors[ticker] = ticker_df.to_dict("records")
            
            logger.info(f"Loaded {len(df)} existing anchors for {len(self.anchors)} tickers")
            return True
            
        except Exception as e:
            logger.error(f"Error loading existing anchors: {e}")
            return False


def main():
    """Main entry point for AVWAP anchor detection."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AVWAP Anchor Detection")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Number of days to look back for anchors",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated list of tickers (overrides universe)",
    )
    
    args = parser.parse_args()
    
    detector = AVWAPAnchorDetector()
    
    # Override tickers if specified
    if args.tickers:
        detector.universe_tickers = [t.strip().upper() for t in args.tickers.split(",")]
        logger.info(f"Using custom ticker list: {detector.universe_tickers}")
    
    # Log deployment info
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()
    
    logger.info(f"--- Running AVWAP Anchor Detection --- {deployment_info}")
    
    # Load existing anchors first
    detector.load_existing_anchors()
    
    # Detect new anchors
    success = detector.detect_all_anchors(args.lookback_days)
    
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)