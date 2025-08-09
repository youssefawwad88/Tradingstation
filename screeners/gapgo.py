"""
Gap & Go screener for Trading Station.
Implements Umar Ashraf's Gap & Go strategy with 9:30-9:44 volume and first break ≥ 9:36 rule.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import List, Dict, Any, Optional

from utils.config import SIGNALS_DIR, StrategyDefaults
from utils.logging_setup import get_logger, log_job_start, log_job_complete
from utils.storage import get_storage
from utils.time_utils import now_et, is_valid_gapgo_time, parse_market_time
from utils.ticker_management import load_master_tickerlist
from utils.helpers import calculate_gap_percentage, calculate_r_multiple_targets, extract_premarket_levels
from utils.validators import validate_signal_schema

logger = get_logger(__name__)

class GapGoScreener:
    """Gap & Go strategy screener."""
    
    def __init__(self):
        self.storage = get_storage()
        
        # Gap & Go criteria
        self.min_gap_percent = StrategyDefaults.GAPGO_MIN_GAP_PERCENT  # 2.0%
        self.min_volume_multiple = StrategyDefaults.GAPGO_MIN_VOLUME_MULTIPLE  # 2.0x
        self.first_valid_time = parse_market_time(StrategyDefaults.GAPGO_FIRST_VALID_TIME)  # 9:36 AM ET
        
        # Volume analysis window (9:30-9:44)
        self.volume_window_start = parse_market_time("09:30:00")
        self.volume_window_end = parse_market_time("09:44:00")
    
    def load_data_for_ticker(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """Load required data for a ticker."""
        data = {}
        
        # Load daily data for gap calculation
        daily_path = f"data/daily/{ticker}_daily.csv"
        df_daily = self.storage.read_df(daily_path)
        if df_daily is not None and not df_daily.empty:
            data['daily'] = df_daily.tail(5)  # Last 5 days
        
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
        
        return data
    
    def calculate_gap_metrics(self, daily_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate gap metrics from daily data."""
        if daily_df.empty or len(daily_df) < 2:
            return {}
        
        # Get today's open and yesterday's close
        today = daily_df.iloc[-1]
        yesterday = daily_df.iloc[-2]
        
        gap_pct = calculate_gap_percentage(today['open'], yesterday['close'])
        
        return {
            'gap_percent': gap_pct,
            'prev_close': yesterday['close'],
            'current_open': today['open'],
            'current_high': today['high'],
            'current_low': today['low'],
            'current_volume': today['volume']
        }
    
    def analyze_volume_pattern(self, intraday_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze volume pattern in the 9:30-9:44 window."""
        if intraday_df.empty:
            return {}
        
        # Convert timestamp to time for filtering
        df_analysis = intraday_df.copy()
        df_analysis['timestamp'] = pd.to_datetime(df_analysis['timestamp'])
        df_analysis['time_et'] = df_analysis['timestamp'].dt.tz_convert('US/Eastern').dt.time
        
        # Filter to volume analysis window (9:30-9:44)
        volume_window = df_analysis[
            (df_analysis['time_et'] >= self.volume_window_start) &
            (df_analysis['time_et'] <= self.volume_window_end)
        ]
        
        if volume_window.empty:
            return {}
        
        # Calculate volume metrics
        total_volume_window = volume_window['volume'].sum()
        avg_volume_per_minute = total_volume_window / len(volume_window) if len(volume_window) > 0 else 0
        
        # Calculate volume vs. average (simplified - using current day as proxy)
        daily_avg_volume = intraday_df['volume'].mean() if not intraday_df.empty else 0
        volume_multiple = avg_volume_per_minute / daily_avg_volume if daily_avg_volume > 0 else 0
        
        return {
            'volume_9_30_to_9_44': total_volume_window,
            'avg_volume_per_minute': avg_volume_per_minute,
            'volume_multiple': volume_multiple,
            'window_bars': len(volume_window)
        }
    
    def find_first_break(self, intraday_df: pd.DataFrame, gap_metrics: Dict[str, float]) -> Dict[str, Any]:
        """Find the first break above premarket high >= 9:36."""
        if intraday_df.empty or not gap_metrics:
            return {}
        
        df_analysis = intraday_df.copy()
        df_analysis['timestamp'] = pd.to_datetime(df_analysis['timestamp'])
        df_analysis['time_et'] = df_analysis['timestamp'].dt.tz_convert('US/Eastern').dt.time
        
        # Filter to valid time window (>= 9:36)
        valid_time_data = df_analysis[df_analysis['time_et'] >= self.first_valid_time]
        
        if valid_time_data.empty:
            return {}
        
        # Get premarket high (use current day's high as proxy if no premarket data)
        premkt_levels = extract_premarket_levels(df_analysis)
        premkt_high = premkt_levels.get('premkt_high', gap_metrics.get('current_high', 0))
        
        # Find first break above premarket high
        breaks = valid_time_data[valid_time_data['high'] > premkt_high]
        
        if breaks.empty:
            return {}
        
        first_break = breaks.iloc[0]
        
        return {
            'first_break_time': first_break['timestamp'],
            'first_break_price': first_break['high'],
            'premkt_high': premkt_high,
            'break_valid': first_break['time_et'] >= self.first_valid_time
        }
    
    def generate_signal(self, ticker: str, data: Dict[str, pd.DataFrame]) -> Optional[Dict[str, Any]]:
        """Generate Gap & Go signal for a ticker."""
        
        # Check if we have required data
        if 'daily' not in data or 'intraday' not in data:
            return None
        
        # Calculate gap metrics
        gap_metrics = self.calculate_gap_metrics(data['daily'])
        if not gap_metrics:
            return None
        
        # Check minimum gap requirement
        gap_pct = gap_metrics['gap_percent']
        if abs(gap_pct) < self.min_gap_percent:
            return None
        
        # Analyze volume pattern
        volume_metrics = self.analyze_volume_pattern(data['intraday'])
        if not volume_metrics:
            return None
        
        # Check volume requirement
        volume_multiple = volume_metrics.get('volume_multiple', 0)
        if volume_multiple < self.min_volume_multiple:
            return None
        
        # Find first break
        break_metrics = self.find_first_break(data['intraday'], gap_metrics)
        
        # Determine setup validity
        setup_valid = (
            abs(gap_pct) >= self.min_gap_percent and
            volume_multiple >= self.min_volume_multiple and
            break_metrics.get('break_valid', False)
        )
        
        # Calculate entry, stop, and targets
        if setup_valid and break_metrics:
            entry_price = break_metrics['first_break_price']
            # Stop below premarket low (or use gap as proxy)
            stop_price = max(
                break_metrics.get('premkt_high', gap_metrics['current_open']) * 0.98,  # 2% below premkt high
                gap_metrics['current_open'] * 0.95  # 5% below open as fallback
            )
            direction = 'long' if gap_pct > 0 else 'short'
            
            # Calculate R-multiple targets
            targets = calculate_r_multiple_targets(entry_price, stop_price, direction)
        else:
            entry_price = gap_metrics['current_open']
            stop_price = entry_price * 0.95  # 5% stop as fallback
            direction = 'long' if gap_pct > 0 else 'short'
            targets = calculate_r_multiple_targets(entry_price, stop_price, direction)
        
        # Calculate confidence score
        confidence = 0.0
        if abs(gap_pct) >= 3.0:
            confidence += 0.3
        elif abs(gap_pct) >= self.min_gap_percent:
            confidence += 0.2
        
        if volume_multiple >= 3.0:
            confidence += 0.3
        elif volume_multiple >= self.min_volume_multiple:
            confidence += 0.2
        
        if break_metrics.get('break_valid', False):
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
            'gap_percent': gap_pct,
            'volume_multiple': volume_multiple,
            'premkt_high': break_metrics.get('premkt_high', 0),
            'first_break_time': break_metrics.get('first_break_time', '').strftime('%H:%M:%S') if break_metrics.get('first_break_time') else '',
            'volume_15min': volume_metrics.get('volume_9_30_to_9_44', 0),
            'strategy': 'Gap_and_Go'
        }
        
        return signal
    
    def run_screening(self, ticker_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Run Gap & Go screening for all tickers."""
        start_time = datetime.now()
        log_job_start(logger, "gapgo")
        
        # Check if it's valid time for Gap & Go
        if not is_valid_gapgo_time():
            logger.info("Not valid time for Gap & Go screening")
            return []
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found for Gap & Go screening")
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
                    logger.debug(f"Gap & Go signal generated for {ticker}")
                
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"Error processing {ticker} for Gap & Go: {e}")
        
        # Save signals
        if signals:
            signals_df = pd.DataFrame(signals)
            
            # Validate schema
            validation_result = validate_signal_schema(signals_df)
            if not validation_result.valid:
                logger.error(f"Signal schema validation failed: {validation_result.message}")
            else:
                # Save to storage
                output_path = f"{SIGNALS_DIR}/gapgo.csv"
                self.storage.save_df(signals_df, output_path)
                logger.info(f"Saved {len(signals)} Gap & Go signals to {output_path}")
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "gapgo", elapsed_ms, len(signals))
        
        logger.info(f"Gap & Go screening complete: {len(signals)} signals from {processed_count} tickers")
        return signals

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for Gap & Go screener."""
    try:
        screener = GapGoScreener()
        signals = screener.run_screening(ticker_list)
        
        print(f"Gap & Go screening complete: {len(signals)} signals generated")
        
        if signals:
            # Show top signals by confidence
            top_signals = sorted(signals, key=lambda x: x['confidence'], reverse=True)[:5]
            print("\nTop 5 signals by confidence:")
            for signal in top_signals:
                print(f"  {signal['ticker']}: {signal['confidence']:.2f} confidence, "
                      f"{signal['gap_percent']:.1f}% gap, entry: ${signal['entry']:.2f}")
        
        return signals
        
    except Exception as e:
        logger.error(f"Gap & Go screener failed: {e}")
        return []

if __name__ == "__main__":
    import sys
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)