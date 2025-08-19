"""
Intelligent Data Manager Module for Strategic Trading System

This module contains the core business logic for intelligent data management.
It implements market calendar awareness, file size checks, decision logic for
full vs compact fetches, and self-healing logic to fix data gaps.

Strategic Architecture Features:
- Single public interface: update_data(ticker, interval, data_type)
- Market calendar integration
- Intelligent full vs compact fetch decisions
- Self-healing data gap detection and repair
- File size monitoring and optimization
- Comprehensive error handling and recovery
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandas_market_calendars as mcal
import pytz

# Strategic imports
from core.data_fetcher import UnifiedDataFetcher
from utils.config import (
    BASE_DATA_DIR,
    INTRADAY_DATA_DIR,
    DAILY_DATA_DIR,
    INTRADAY_30MIN_DATA_DIR,
    INTRADAY_TRIM_DAYS,
    INTRADAY_EXCLUDE_TODAY,
    TIMEZONE,
    DEBUG_MODE
)
from utils.data_storage import save_df_to_s3, read_df_from_s3
from utils.helpers import apply_data_retention, is_today_present_enhanced

logger = logging.getLogger(__name__)


class IntelligentDataManager:
    """
    Professional-grade intelligent data management system.
    
    This class replaces all individual data management scripts with a single,
    intelligent system that makes optimal decisions about data fetching,
    storage, and maintenance.
    """
    
    def __init__(self, data_fetcher: UnifiedDataFetcher = None):
        """
        Initialize the intelligent data manager.
        
        Args:
            data_fetcher: Data fetcher instance. If None, creates a new one.
        """
        self.data_fetcher = data_fetcher or UnifiedDataFetcher()
        self.market_calendar = mcal.get_calendar('NYSE')
        self.timezone = pytz.timezone(TIMEZONE)
        
        # Intelligence thresholds and parameters
        self.file_size_threshold_mb = 50.0  # Threshold for full vs compact decision
        self.max_data_gap_days = 3  # Maximum gap before triggering self-healing
        self.required_data_coverage = 0.95  # Required data coverage ratio
        
        # Ensure data directories exist
        self._ensure_directories_exist()
        
        logger.info("Intelligent Data Manager initialized")
    
    def update_data(
        self,
        ticker: str,
        interval: str = "1min",
        data_type: str = "INTRADAY",
        force_full: bool = False
    ) -> bool:
        """
        Main entry point for intelligent data management.
        
        This single function implements all the intelligent logic developed:
        - File size checks
        - Market calendar awareness  
        - Decision to run full or compact fetch
        - Self-healing logic to fix data gaps
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval (1min, 30min, daily)
            data_type: Type of data (INTRADAY, DAILY, QUOTE)
            force_full: Force a full fetch regardless of file size
            
        Returns:
            True if update was successful, False otherwise
        """
        logger.info(f"🧠 Starting intelligent data update for {ticker} ({data_type}, {interval})")
        
        try:
            # Phase 1: Market calendar awareness
            if not self._should_update_data(ticker, data_type):
                logger.info(f"⏰ Market calendar check: No update needed for {ticker}")
                return True
            
            # Phase 2: Intelligent fetch decision
            fetch_type = self._determine_fetch_type(ticker, interval, data_type, force_full)
            logger.info(f"🎯 Intelligent decision: {fetch_type} fetch for {ticker}")
            
            # Phase 3: Execute data fetch
            success = self._execute_data_fetch(ticker, interval, data_type, fetch_type)
            
            if not success:
                logger.error(f"❌ Data fetch failed for {ticker}")
                return False
            
            # Phase 4: Self-healing data gap analysis
            if self._detect_data_gaps(ticker, interval, data_type):
                logger.info(f"🔧 Self-healing: Detected data gaps for {ticker}, initiating repair")
                success = self._heal_data_gaps(ticker, interval, data_type)
                
                if not success:
                    logger.warning(f"⚠️ Self-healing partially failed for {ticker}")
            
            # Phase 5: Data integrity validation
            if self._validate_data_integrity(ticker, interval, data_type):
                logger.info(f"✅ Data integrity validated for {ticker}")
                return True
            else:
                logger.error(f"❌ Data integrity check failed for {ticker}")
                return False
                
        except Exception as e:
            logger.error(f"💥 Unexpected error in intelligent data update for {ticker}: {e}")
            return False
    
    def _should_update_data(self, ticker: str, data_type: str) -> bool:
        """
        Market calendar awareness check.
        
        Determines if data should be updated based on market schedule,
        weekends, holidays, and current market session.
        """
        now = datetime.now(self.timezone)
        today = now.date()
        
        # Always update on market days during market hours
        if self._is_market_open(now):
            logger.debug(f"Market is open - update approved for {ticker}")
            return True
        
        # Check if today is a market day
        market_days = self.market_calendar.valid_days(
            start_date=today,
            end_date=today
        )
        
        if len(market_days) == 0:
            logger.debug(f"Today is not a market day - limiting updates for {ticker}")
            # On non-market days, only update if data is stale
            return self._is_data_stale(ticker, data_type)
        
        # Market day but outside hours - check if we need catch-up
        return self._needs_catchup_update(ticker, data_type)
    
    def _determine_fetch_type(
        self,
        ticker: str,
        interval: str,
        data_type: str,
        force_full: bool
    ) -> str:
        """
        Intelligent decision logic for full vs compact fetch.
        
        Considers file size, data gaps, and update frequency to make
        optimal fetch decisions.
        """
        if force_full:
            return "full"
        
        # Check current file size
        file_path = self._get_data_file_path(ticker, interval, data_type)
        file_size_mb = self._get_file_size_mb(file_path)
        
        logger.debug(f"Current file size for {ticker}: {file_size_mb:.2f} MB")
        
        # File size decision logic
        if file_size_mb > self.file_size_threshold_mb:
            logger.info(f"📊 File size {file_size_mb:.2f} MB exceeds threshold - using compact fetch")
            return "compact"
        
        # Check data coverage and gaps
        coverage_ratio = self._calculate_data_coverage(ticker, interval, data_type)
        
        if coverage_ratio < self.required_data_coverage:
            logger.info(f"📈 Data coverage {coverage_ratio:.2%} below threshold - using full fetch")
            return "full"
        
        # Default to compact for regular updates
        return "compact"
    
    def _execute_data_fetch(
        self,
        ticker: str,
        interval: str,
        data_type: str,
        fetch_type: str
    ) -> bool:
        """
        Execute the determined fetch strategy.
        
        Args:
            ticker: Stock ticker symbol
            interval: Time interval
            data_type: Type of data
            fetch_type: 'full' or 'compact'
            
        Returns:
            True if fetch was successful
        """
        outputsize = fetch_type  # 'full' or 'compact'
        
        # Fetch data using unified fetcher
        df, success = self.data_fetcher.fetch_data(
            ticker=ticker,
            data_type=data_type,
            interval=interval,
            outputsize=outputsize
        )
        
        if not success or df is None:
            logger.error(f"Failed to fetch {fetch_type} data for {ticker}")
            return False
        
        # Apply data retention policies
        df = self._apply_retention_policies(df, data_type)
        
        # Save to local storage
        local_success = self._save_to_local_storage(df, ticker, interval, data_type)
        
        # Save to cloud storage
        cloud_success = self._save_to_cloud_storage(df, ticker, interval, data_type)
        
        if local_success and cloud_success:
            logger.info(f"💾 Data successfully saved for {ticker} ({len(df)} rows)")
            return True
        else:
            logger.warning(f"⚠️ Partial save failure for {ticker}")
            return local_success  # Local storage is minimum requirement
    
    def _detect_data_gaps(
        self,
        ticker: str,
        interval: str,
        data_type: str
    ) -> bool:
        """
        Self-healing: Detect data gaps that need repair.
        
        Returns:
            True if gaps were detected
        """
        try:
            df = self._load_existing_data(ticker, interval, data_type)
            
            if df is None or len(df) == 0:
                logger.info(f"No existing data for gap analysis: {ticker}")
                return False
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            # Calculate time differences
            if data_type.upper() == "INTRADAY":
                expected_freq = "1min" if interval == "1min" else "30min"
                max_gap = timedelta(minutes=30)  # Allow for market gaps
            else:
                expected_freq = "D"
                max_gap = timedelta(days=3)  # Allow for weekends
            
            # Find gaps larger than expected
            time_diffs = df['timestamp'].diff()
            large_gaps = time_diffs > max_gap
            
            gap_count = large_gaps.sum()
            
            if gap_count > 0:
                logger.info(f"🔍 Detected {gap_count} data gaps for {ticker}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error detecting data gaps for {ticker}: {e}")
            return False
    
    def _heal_data_gaps(
        self,
        ticker: str,
        interval: str,
        data_type: str
    ) -> bool:
        """
        Self-healing: Repair detected data gaps.
        
        Returns:
            True if healing was successful
        """
        try:
            logger.info(f"🔧 Starting self-healing process for {ticker}")
            
            # Load current data
            current_df = self._load_existing_data(ticker, interval, data_type)
            
            if current_df is None:
                logger.warning(f"No current data to heal for {ticker}")
                return False
            
            # Fetch fresh full data to fill gaps
            fresh_df, success = self.data_fetcher.fetch_data(
                ticker=ticker,
                data_type=data_type,
                interval=interval,
                outputsize="full"
            )
            
            if not success or fresh_df is None:
                logger.error(f"Failed to fetch healing data for {ticker}")
                return False
            
            # Merge and deduplicate
            combined_df = pd.concat([current_df, fresh_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=['timestamp', 'ticker'],
                keep='last'
            ).sort_values('timestamp')
            
            # Apply retention policies
            healed_df = self._apply_retention_policies(combined_df, data_type)
            
            # Save healed data
            local_success = self._save_to_local_storage(healed_df, ticker, interval, data_type)
            cloud_success = self._save_to_cloud_storage(healed_df, ticker, interval, data_type)
            
            if local_success:
                gap_reduction = len(healed_df) - len(current_df)
                logger.info(f"✨ Self-healing completed for {ticker}: +{gap_reduction} records")
                return True
            else:
                logger.error(f"Failed to save healed data for {ticker}")
                return False
                
        except Exception as e:
            logger.error(f"Error in self-healing process for {ticker}: {e}")
            return False
    
    def _validate_data_integrity(
        self,
        ticker: str,
        interval: str,
        data_type: str
    ) -> bool:
        """
        Final data integrity validation.
        
        Ensures timestamps are in order and no data is corrupted.
        """
        try:
            df = self._load_existing_data(ticker, interval, data_type)
            
            if df is None or len(df) == 0:
                logger.warning(f"No data to validate for {ticker}")
                return False
            
            # Check required columns
            required_columns = ['timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns for {ticker}: {missing_columns}")
                return False
            
            # Check timestamp ordering
            df_sorted = df.sort_values('timestamp')
            if not df['timestamp'].equals(df_sorted['timestamp']):
                logger.error(f"Timestamps not in order for {ticker}")
                return False
            
            # Check for null values in critical columns
            critical_nulls = df[['open', 'high', 'low', 'close']].isnull().any().any()
            if critical_nulls:
                logger.error(f"Null values found in price data for {ticker}")
                return False
            
            # Check for reasonable price ranges
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if (df[col] <= 0).any():
                    logger.error(f"Invalid price values (<=0) found for {ticker}")
                    return False
            
            logger.debug(f"Data integrity validated for {ticker}: {len(df)} records")
            return True
            
        except Exception as e:
            logger.error(f"Error validating data integrity for {ticker}: {e}")
            return False
    
    # Helper methods
    
    def _is_market_open(self, dt: datetime) -> bool:
        """Check if market is currently open."""
        try:
            schedule = self.market_calendar.schedule(
                start_date=dt.date(),
                end_date=dt.date()
            )
            
            if len(schedule) == 0:
                return False
            
            market_open = schedule.iloc[0]['market_open'].tz_convert(self.timezone)
            market_close = schedule.iloc[0]['market_close'].tz_convert(self.timezone)
            
            return market_open <= dt <= market_close
            
        except Exception:
            return False
    
    def _is_data_stale(self, ticker: str, data_type: str, hours: int = 24) -> bool:
        """Check if data is stale and needs updating."""
        file_path = self._get_data_file_path(ticker, "1min", data_type)
        
        if not os.path.exists(file_path):
            return True
        
        file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(file_path))
        return file_age > timedelta(hours=hours)
    
    def _needs_catchup_update(self, ticker: str, data_type: str) -> bool:
        """Check if we need a catch-up update."""
        # Always allow updates for now - can be enhanced with more sophisticated logic
        return True
    
    def _get_data_file_path(self, ticker: str, interval: str, data_type: str) -> str:
        """Get the file path for storing data."""
        if data_type.upper() == "DAILY":
            return os.path.join(DAILY_DATA_DIR, f"{ticker}.csv")
        elif interval == "30min":
            return os.path.join(INTRADAY_30MIN_DATA_DIR, f"{ticker}.csv")
        else:
            return os.path.join(INTRADAY_DATA_DIR, f"{ticker}.csv")
    
    def _get_file_size_mb(self, file_path: str) -> float:
        """Get file size in MB."""
        if not os.path.exists(file_path):
            return 0.0
        
        return os.path.getsize(file_path) / (1024 * 1024)
    
    def _calculate_data_coverage(
        self, ticker: str, interval: str, data_type: str
    ) -> float:
        """Calculate data coverage ratio."""
        try:
            df = self._load_existing_data(ticker, interval, data_type)
            
            if df is None or len(df) == 0:
                return 0.0
            
            # Simple coverage calculation based on recent data
            recent_days = 7
            end_date = datetime.now(self.timezone)
            start_date = end_date - timedelta(days=recent_days)
            
            recent_data = df[df['timestamp'] >= start_date]
            expected_records = recent_days * 390 if data_type.upper() == "INTRADAY" else recent_days
            
            return min(len(recent_data) / expected_records, 1.0)
            
        except Exception:
            return 0.0
    
    def _apply_retention_policies(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Apply data retention policies."""
        if data_type.upper() == "INTRADAY":
            return apply_data_retention(
                df,
                trim_days=INTRADAY_TRIM_DAYS
            )
        return df
    
    def _load_existing_data(
        self, ticker: str, interval: str, data_type: str
    ) -> Optional[pd.DataFrame]:
        """Load existing data from local storage."""
        file_path = self._get_data_file_path(ticker, interval, data_type)
        
        try:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            return None
        except Exception as e:
            logger.error(f"Error loading existing data for {ticker}: {e}")
            return None
    
    def _save_to_local_storage(
        self, df: pd.DataFrame, ticker: str, interval: str, data_type: str
    ) -> bool:
        """Save data to local storage."""
        try:
            file_path = self._get_data_file_path(ticker, interval, data_type)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_csv(file_path, index=False)
            return True
        except Exception as e:
            logger.error(f"Error saving to local storage for {ticker}: {e}")
            return False
    
    def _save_to_cloud_storage(
        self, df: pd.DataFrame, ticker: str, interval: str, data_type: str
    ) -> bool:
        """Save data to cloud storage."""
        try:
            if data_type.upper() == "DAILY":
                spaces_path = f"data/daily/{ticker}.csv"
            elif interval == "30min":
                spaces_path = f"data/intraday_30min/{ticker}.csv"
            else:
                spaces_path = f"data/intraday/{ticker}.csv"
            
            return save_df_to_s3(df, spaces_path)
        except Exception as e:
            logger.error(f"Error saving to cloud storage for {ticker}: {e}")
            return False
    
    def _ensure_directories_exist(self):
        """Ensure all required directories exist."""
        directories = [BASE_DATA_DIR, INTRADAY_DATA_DIR, DAILY_DATA_DIR, INTRADAY_30MIN_DATA_DIR]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)


# Global instance for convenient access
intelligent_manager = IntelligentDataManager()


def update_data(
    ticker: str,
    interval: str = "1min",
    data_type: str = "INTRADAY",
    force_full: bool = False
) -> bool:
    """
    Convenience function for the main data update interface.
    
    This provides the single public function interface as specified
    in the strategic architecture.
    """
    return intelligent_manager.update_data(ticker, interval, data_type, force_full)