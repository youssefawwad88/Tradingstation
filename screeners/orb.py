"""
Opening Range Breakout (ORB) screener for Trading Station.
Implements ORB strategy based on 9:30-9:39 range as per Umar Ashraf's methodology.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import List, Dict, Any, Optional

from utils.config import SIGNALS_DIR, StrategyDefaults
from utils.logging_setup import get_logger, log_job_start, log_job_complete
from utils.storage import get_storage
from utils.time_utils import now_et, is_orb_range_time, parse_market_time, ORB_RANGE_START, ORB_RANGE_END
from utils.ticker_management import load_master_tickerlist
from utils.helpers import extract_opening_range, calculate_r_multiple_targets
from utils.validators import validate_signal_schema

logger = get_logger(__name__)

class ORBScreener:
    """Opening Range Breakout screener."""
    
    def __init__(self):
        self.storage = get_storage()
        
        # ORB criteria
        self.orb_start_time = ORB_RANGE_START  # 9:30 AM ET
        self.orb_end_time = ORB_RANGE_END      # 9:39 AM ET
        self.min_volume_multiple = StrategyDefaults.ORB_MIN_VOLUME_MULTIPLE  # 1.5x
        
        # Minimum range requirements
        self.min_range_dollars = 0.50  # $0.50 minimum range
        self.min_range_percent = 1.0   # 1% minimum range
    
    def load_data_for_ticker(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """Load required data for a ticker."""
        data = {}
        
        # Load 1-minute intraday data
        intraday_path = f"data/intraday/{ticker}_1min.csv"
        df_intraday = self.storage.read_df(intraday_path)
        if df_intraday is not None and not df_intraday.empty:
            # Filter to today's data only
            today = now_et().strftime('%Y-%m-%d')
            if 'day_id' in df_intraday.columns:
                data['intraday'] = df_intraday[df_intraday['day_id'] == today].copy()
            else:
                # Fallback: use timestamp
                df_intraday['timestamp'] = pd.to_datetime(df_intraday['timestamp'])
                today_data = df_intraday[df_intraday['timestamp'].dt.date == pd.to_datetime(today).date()]
                data['intraday'] = today_data.copy()
        
        # Load daily data for volume comparison
        daily_path = f"data/daily/{ticker}_daily.csv"
        df_daily = self.storage.read_df(daily_path)
        if df_daily is not None and not df_daily.empty:
            data['daily'] = df_daily.tail(5)  # Last 5 days
        
        return data
    
    def calculate_opening_range(self, intraday_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate opening range (9:30-9:39) metrics."""
        if intraday_df.empty:
            return {}
        
        # Extract opening range using helper function
        or_metrics = extract_opening_range(
            intraday_df,
            start_time=self.orb_start_time.strftime('%H:%M:%S'),
            end_time=self.orb_end_time.strftime('%H:%M:%S')
        )
        
        if not all(key in or_metrics and or_metrics[key] is not None for key in ['or_high', 'or_low', 'or_range']):
            return {}
        
        or_high = or_metrics['or_high']
        or_low = or_metrics['or_low']
        or_range = or_metrics['or_range']
        
        # Calculate additional metrics
        or_midpoint = (or_high + or_low) / 2
        or_range_percent = (or_range / or_midpoint) * 100 if or_midpoint > 0 else 0
        
        # Filter to opening range data for volume analysis
        df_analysis = intraday_df.copy()
        df_analysis['timestamp'] = pd.to_datetime(df_analysis['timestamp'])
        df_analysis['time_et'] = df_analysis['timestamp'].dt.tz_convert('US/Eastern').dt.time
        
        or_data = df_analysis[
            (df_analysis['time_et'] >= self.orb_start_time) &
            (df_analysis['time_et'] <= self.orb_end_time)
        ]
        
        or_volume = or_data['volume'].sum() if not or_data.empty else 0
        
        return {
            'or_high': or_high,
            'or_low': or_low,
            'or_range': or_range,
            'or_midpoint': or_midpoint,
            'or_range_percent': or_range_percent,
            'or_volume': or_volume,
            'or_bars': len(or_data)
        }
    
    def check_breakout_conditions(self, intraday_df: pd.DataFrame, or_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Check for breakout conditions after opening range."""
        if intraday_df.empty or not or_metrics:
            return {}
        
        df_analysis = intraday_df.copy()
        df_analysis['timestamp'] = pd.to_datetime(df_analysis['timestamp'])
        df_analysis['time_et'] = df_analysis['timestamp'].dt.tz_convert('US/Eastern').dt.time
        
        # Filter to post-opening-range data (after 9:39)
        post_or_data = df_analysis[df_analysis['time_et'] > self.orb_end_time]
        
        if post_or_data.empty:
            return {
                'breakout_direction': None,
                'breakout_time': None,
                'breakout_price': None,
                'breakout_volume': 0
            }
        
        or_high = or_metrics['or_high']
        or_low = or_metrics['or_low']
        
        # Check for breakouts
        upside_breakouts = post_or_data[post_or_data['high'] > or_high]
        downside_breakouts = post_or_data[post_or_data['low'] < or_low]
        
        breakout_info = {
            'breakout_direction': None,
            'breakout_time': None,
            'breakout_price': None,
            'breakout_volume': 0
        }
        
        # Find first breakout (upside takes precedence if both occur)
        if not upside_breakouts.empty:
            first_upside = upside_breakouts.iloc[0]
            breakout_info.update({
                'breakout_direction': 'long',
                'breakout_time': first_upside['timestamp'],
                'breakout_price': first_upside['high'],
                'breakout_volume': first_upside['volume']
            })
        elif not downside_breakouts.empty:
            first_downside = downside_breakouts.iloc[0]
            breakout_info.update({
                'breakout_direction': 'short',
                'breakout_time': first_downside['timestamp'],
                'breakout_price': first_downside['low'],
                'breakout_volume': first_downside['volume']
            })
        
        return breakout_info
    
    def calculate_volume_metrics(self, or_metrics: Dict[str, Any], daily_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate volume metrics for ORB validation."""
        if not or_metrics or daily_df.empty:
            return {}
        
        or_volume = or_metrics.get('or_volume', 0)
        
        # Calculate average daily volume (last 5 days)
        avg_daily_volume = daily_df['volume'].tail(5).mean()
        
        # ORB volume is for ~10 minutes, so compare to proportional daily volume
        expected_10min_volume = avg_daily_volume * (10 / 390)  # 390 minutes in trading day
        volume_multiple = or_volume / expected_10min_volume if expected_10min_volume > 0 else 0
        
        return {
            'or_volume': or_volume,
            'avg_daily_volume': avg_daily_volume,
            'expected_10min_volume': expected_10min_volume,
            'volume_multiple': volume_multiple
        }
    
    def generate_signal(self, ticker: str, data: Dict[str, pd.DataFrame]) -> Optional[Dict[str, Any]]:
        """Generate ORB signal for a ticker."""
        
        # Check if we have required data
        if 'intraday' not in data:
            return None
        
        # Calculate opening range
        or_metrics = self.calculate_opening_range(data['intraday'])
        if not or_metrics:
            return None
        
        # Check minimum range requirements
        or_range = or_metrics['or_range']
        or_range_percent = or_metrics['or_range_percent']
        
        if or_range < self.min_range_dollars or or_range_percent < self.min_range_percent:
            return None
        
        # Calculate volume metrics
        volume_metrics = {}
        if 'daily' in data:
            volume_metrics = self.calculate_volume_metrics(or_metrics, data['daily'])
        
        # Check volume requirement
        volume_multiple = volume_metrics.get('volume_multiple', 0)
        if volume_multiple < self.min_volume_multiple:
            return None
        
        # Check for breakout conditions
        breakout_info = self.check_breakout_conditions(data['intraday'], or_metrics)
        
        # Determine if setup is valid
        has_breakout = breakout_info['breakout_direction'] is not None
        setup_valid = (
            or_range >= self.min_range_dollars and
            or_range_percent >= self.min_range_percent and
            volume_multiple >= self.min_volume_multiple and
            has_breakout
        )
        
        # Calculate entry, stop, and targets
        if has_breakout:
            direction = breakout_info['breakout_direction']
            entry_price = breakout_info['breakout_price']
            
            if direction == 'long':
                stop_price = or_metrics['or_low']
            else:  # short
                stop_price = or_metrics['or_high']
            
        else:
            # No breakout yet - set up for potential breakout
            # Default to long bias
            direction = 'long'
            entry_price = or_metrics['or_high']
            stop_price = or_metrics['or_low']
        
        # Calculate R-multiple targets
        targets = calculate_r_multiple_targets(entry_price, stop_price, direction)
        
        # Calculate confidence score
        confidence = 0.0
        
        # Range quality (0-0.3)
        if or_range_percent >= 2.0:
            confidence += 0.3
        elif or_range_percent >= self.min_range_percent:
            confidence += 0.2
        
        # Volume confirmation (0-0.3)
        if volume_multiple >= 2.5:
            confidence += 0.3
        elif volume_multiple >= self.min_volume_multiple:
            confidence += 0.2
        
        # Breakout confirmation (0-0.4)
        if has_breakout:
            confidence += 0.4
        
        confidence = min(confidence, 1.0)
        
        # Create signal
        signal = {
            'as_of': now_et().isoformat(),
            'ticker': ticker,
            'direction': direction,
            'setup_valid': setup_valid,
            'entry': entry_price,
            'stop': stop_price,
            'r_multiple': targets.get('r_multiple', 0),
            't1_2R': targets.get('t1_2R', entry_price),
            't2_3R': targets.get('t2_3R', entry_price),
            'confidence': confidence,
            
            # Audit fields
            'or_high': or_metrics['or_high'],
            'or_low': or_metrics['or_low'],
            'or_range': or_range,
            'or_range_percent': or_range_percent,
            'volume_multiple': volume_multiple,
            'breakout_time': breakout_info.get('breakout_time', '').strftime('%H:%M:%S') if breakout_info.get('breakout_time') else '',
            'has_breakout': has_breakout,
            'strategy': 'ORB'
        }
        
        return signal
    
    def run_screening(self, ticker_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Run ORB screening for all tickers."""
        start_time = datetime.now()
        log_job_start(logger, "orb")
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found for ORB screening")
            return []
        
        signals = []
        processed_count = 0
        
        # Process each ticker
        for ticker in ticker_list:
            try:
                # Load data
                data = self.load_data_for_ticker(ticker)
                
                # Generate signal
                signal = self.generate_signal(ticker, data)
                
                if signal:
                    signals.append(signal)
                    logger.debug(f"ORB signal generated for {ticker}")
                
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"Error processing {ticker} for ORB: {e}")
        
        # Save signals
        if signals:
            signals_df = pd.DataFrame(signals)
            
            # Validate schema
            validation_result = validate_signal_schema(signals_df)
            if not validation_result.valid:
                logger.error(f"Signal schema validation failed: {validation_result.message}")
            else:
                # Save to storage
                output_path = f"{SIGNALS_DIR}/orb.csv"
                self.storage.save_df(signals_df, output_path)
                logger.info(f"Saved {len(signals)} ORB signals to {output_path}")
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "orb", elapsed_ms, len(signals))
        
        logger.info(f"ORB screening complete: {len(signals)} signals from {processed_count} tickers")
        return signals

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for ORB screener."""
    try:
        screener = ORBScreener()
        signals = screener.run_screening(ticker_list)
        
        print(f"ORB screening complete: {len(signals)} signals generated")
        
        if signals:
            # Show top signals by confidence
            top_signals = sorted(signals, key=lambda x: x['confidence'], reverse=True)[:5]
            print("\nTop 5 ORB signals by confidence:")
            for signal in top_signals:
                print(f"  {signal['ticker']}: {signal['confidence']:.2f} confidence, "
                      f"range: ${signal['or_range']:.2f} ({signal['or_range_percent']:.1f}%), "
                      f"entry: ${signal['entry']:.2f}")
        
        return signals
        
    except Exception as e:
        logger.error(f"ORB screener failed: {e}")
        return []

if __name__ == "__main__":
    import sys
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)