"""
AVWAP anchor finder job for Trading Station.
Scans daily data to identify power candles for AVWAP anchor points.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time

from utils.config import DAILY_DIR, UNIVERSE_DIR, StrategyDefaults
from utils.logging_setup import get_logger, log_job_start, log_job_complete, log_ticker_result
from utils.storage import get_storage
from utils.ticker_management import load_master_tickerlist
from utils.time_utils import now_et, prev_trading_day
from utils.helpers import calculate_avwap_anchor_score, calculate_wick_body_ratios, calculate_gap_percentage

logger = get_logger(__name__)

class AvwapAnchorFinder:
    """Finds AVWAP anchor points (power candles) from daily data."""
    
    def __init__(self):
        self.storage = get_storage()
        
        # AVWAP anchor criteria from strategy defaults
        self.min_body_ratio = StrategyDefaults.AVWAP_MIN_BODY_TO_RANGE_RATIO  # 0.6
        self.min_volume_percentile = StrategyDefaults.AVWAP_MIN_VOLUME_PERCENTILE  # 80
        self.min_gap_percent = 2.0  # Minimum gap for consideration
        self.min_range_percent = 2.0  # Minimum daily range
        self.min_score = 50.0  # Minimum anchor score
        
        # Lookback period for anchor identification
        self.lookback_days = 100  # Analyze last 100 trading days
        
    def load_daily_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Load daily data for a ticker."""
        file_path = f"{DAILY_DIR}/{ticker}_daily.csv"
        return self.storage.read_df(file_path)
    
    def calculate_anchor_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate metrics needed for anchor identification."""
        if df.empty:
            return df
        
        df = df.copy()
        
        # Ensure date column is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)
        
        # Calculate previous close for gap analysis
        df['prev_close'] = df['close'].shift(1)
        
        # Calculate wick and body ratios
        df = calculate_wick_body_ratios(df)
        
        # Calculate gap percentage
        df['gap_pct'] = df.apply(
            lambda row: calculate_gap_percentage(row['open'], row['prev_close']) 
            if pd.notna(row['prev_close']) else 0.0, 
            axis=1
        )
        
        # Calculate daily range percentage
        df['range_pct'] = ((df['high'] - df['low']) / df['close']) * 100
        
        # Calculate volume percentile (rolling 50-day window)
        df['volume_percentile'] = df['volume'].rolling(window=50, min_periods=10).rank(pct=True) * 100
        
        # Calculate anchor score using helper function
        df = calculate_avwap_anchor_score(
            df, 
            body_threshold=self.min_body_ratio,
            volume_percentile=self.min_volume_percentile
        )
        
        return df
    
    def identify_anchors(self, df: pd.DataFrame, ticker: str) -> List[Dict[str, Any]]:
        """Identify anchor points from daily data."""
        if df.empty:
            return []
        
        anchors = []
        
        # Filter for potential anchors
        anchor_candidates = df[
            (df['anchor_score'] >= self.min_score) &
            (df['body_to_range_ratio'] >= self.min_body_ratio) &
            (df['volume_percentile'] >= self.min_volume_percentile)
        ].copy()
        
        for _, row in anchor_candidates.iterrows():
            # Determine anchor price (typically the close of the power candle)
            anchor_price = row['close']
            
            # Create rationale string
            rationale_parts = []
            
            if row['body_to_range_ratio'] >= self.min_body_ratio:
                rationale_parts.append(f"Strong body ({row['body_to_range_ratio']:.2f})")
            
            if row['volume_percentile'] >= self.min_volume_percentile:
                rationale_parts.append(f"High volume ({row['volume_percentile']:.0f}th percentile)")
            
            if abs(row['gap_pct']) >= self.min_gap_percent:
                gap_direction = "up" if row['gap_pct'] > 0 else "down"
                rationale_parts.append(f"Gap {gap_direction} ({row['gap_pct']:.1f}%)")
            
            if row['range_pct'] >= self.min_range_percent:
                rationale_parts.append(f"Wide range ({row['range_pct']:.1f}%)")
            
            rationale = "; ".join(rationale_parts)
            
            anchor = {
                'ticker': ticker,
                'anchor_date': row['date'],
                'anchor_price': round(anchor_price, 2),
                'anchor_score': round(row['anchor_score'], 1),
                'rationale': rationale,
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'body_ratio': round(row['body_to_range_ratio'], 3),
                'volume_percentile': round(row['volume_percentile'], 1),
                'gap_pct': round(row['gap_pct'], 2),
                'range_pct': round(row['range_pct'], 2)
            }
            
            anchors.append(anchor)
        
        # Sort by score descending
        anchors.sort(key=lambda x: x['anchor_score'], reverse=True)
        
        return anchors
    
    def filter_recent_anchors(self, anchors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter anchors to keep only recent ones."""
        if not anchors:
            return []
        
        cutoff_date = now_et() - timedelta(days=self.lookback_days)
        
        recent_anchors = [
            anchor for anchor in anchors
            if pd.to_datetime(anchor['anchor_date']) >= cutoff_date
        ]
        
        return recent_anchors
    
    def process_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """Process a single ticker to find anchor points."""
        start_time = time.time()
        
        try:
            # Load daily data
            df = self.load_daily_data(ticker)
            
            if df is None or df.empty:
                log_ticker_result(logger, ticker, "LOAD", False, "No daily data available")
                return []
            
            # Filter to recent data only
            cutoff_date = now_et() - timedelta(days=self.lookback_days)
            df['date'] = pd.to_datetime(df['date'])
            df = df[df['date'] >= cutoff_date].copy()
            
            if df.empty:
                log_ticker_result(logger, ticker, "FILTER", False, "No recent daily data")
                return []
            
            # Calculate anchor metrics
            df_with_metrics = self.calculate_anchor_metrics(df)
            
            # Identify anchors
            anchors = self.identify_anchors(df_with_metrics, ticker)
            
            # Filter for recent anchors only
            recent_anchors = self.filter_recent_anchors(anchors)
            
            # Log result
            elapsed_ms = (time.time() - start_time) * 1000
            
            if recent_anchors:
                top_score = recent_anchors[0]['anchor_score']
                log_ticker_result(
                    logger, ticker, "ANALYZE", True,
                    f"Found {len(recent_anchors)} anchors, top score: {top_score}",
                    elapsed_ms
                )
            else:
                log_ticker_result(logger, ticker, "ANALYZE", True, "No anchors found", elapsed_ms)
            
            return recent_anchors
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            log_ticker_result(logger, ticker, "ANALYZE", False, str(e), elapsed_ms)
            return []
    
    def save_anchor_results(self, all_anchors: List[Dict[str, Any]]) -> str:
        """Save anchor results to storage."""
        try:
            if not all_anchors:
                # Save empty file
                empty_df = pd.DataFrame(columns=[
                    'ticker', 'anchor_date', 'anchor_price', 'anchor_score', 'rationale',
                    'high', 'low', 'close', 'volume', 'body_ratio', 'volume_percentile',
                    'gap_pct', 'range_pct'
                ])
                output_path = f"{UNIVERSE_DIR}/avwap_anchors.csv"
                self.storage.save_df(empty_df, output_path)
                logger.info("Saved empty anchor file - no anchors found")
                return output_path
            
            # Create DataFrame from anchors
            anchors_df = pd.DataFrame(all_anchors)
            
            # Sort by score descending, then by date descending
            anchors_df = anchors_df.sort_values(['anchor_score', 'anchor_date'], ascending=[False, False])
            
            # Save to storage
            output_path = f"{UNIVERSE_DIR}/avwap_anchors.csv"
            self.storage.save_df(anchors_df, output_path)
            
            # Log summary
            ticker_count = anchors_df['ticker'].nunique()
            avg_score = anchors_df['anchor_score'].mean()
            
            logger.info(
                f"Saved {len(anchors_df)} anchors for {ticker_count} tickers. "
                f"Average score: {avg_score:.1f}"
            )
            
            # Log top 5 anchors
            top_anchors = anchors_df.head(5)
            for _, anchor in top_anchors.iterrows():
                logger.info(
                    f"Top anchor: {anchor['ticker']} @ ${anchor['anchor_price']:.2f} "
                    f"({anchor['anchor_date'].strftime('%Y-%m-%d')}) - "
                    f"Score: {anchor['anchor_score']:.1f}"
                )
            
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save anchor results: {e}")
            raise
    
    def run_anchor_finding(self, ticker_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Run anchor finding for all tickers."""
        start_time = datetime.now()
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found in master list")
            return []
        
        log_job_start(logger, "find_avwap_anchors", len(ticker_list))
        
        all_anchors = []
        processed_count = 0
        
        # Process each ticker
        for i, ticker in enumerate(ticker_list, 1):
            logger.info(f"Processing {ticker} ({i}/{len(ticker_list)})")
            
            ticker_anchors = self.process_ticker(ticker)
            all_anchors.extend(ticker_anchors)
            processed_count += 1
            
            # Small delay to be kind to storage
            time.sleep(0.1)
        
        # Save results
        self.save_anchor_results(all_anchors)
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "find_avwap_anchors", elapsed_ms, processed_count)
        
        anchor_count = len(all_anchors)
        ticker_with_anchors = len(set(anchor['ticker'] for anchor in all_anchors))
        
        logger.info(
            f"AVWAP anchor finding complete: {anchor_count} anchors found "
            f"across {ticker_with_anchors} tickers"
        )
        
        return all_anchors

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for AVWAP anchor finding."""
    try:
        finder = AvwapAnchorFinder()
        anchors = finder.run_anchor_finding(ticker_list)
        
        # Print summary
        print(f"AVWAP anchor finding complete: {len(anchors)} anchors found")
        
        if anchors:
            # Group by ticker and show counts
            ticker_counts = {}
            for anchor in anchors:
                ticker = anchor['ticker']
                ticker_counts[ticker] = ticker_counts.get(ticker, 0) + 1
            
            top_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            print(f"Top tickers by anchor count: {dict(top_tickers)}")
            
            # Show top anchors by score
            top_anchors = sorted(anchors, key=lambda x: x['anchor_score'], reverse=True)[:5]
            print("\nTop 5 anchors by score:")
            for anchor in top_anchors:
                print(f"  {anchor['ticker']}: ${anchor['anchor_price']:.2f} "
                      f"({pd.to_datetime(anchor['anchor_date']).strftime('%Y-%m-%d')}) "
                      f"- Score: {anchor['anchor_score']:.1f}")
        
        return anchors
        
    except Exception as e:
        logger.error(f"AVWAP anchor finder failed: {e}")
        return []

if __name__ == "__main__":
    import sys
    
    # Allow passing specific tickers as command line arguments
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)