"""
AVWAP reclaim screener for Trading Station.
Implements Brian Shannon's AVWAP methodology with anchor-based volume-weighted average prices.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from utils.config import SIGNALS_DIR, UNIVERSE_DIR
from utils.logging_setup import get_logger, log_job_start, log_job_complete
from utils.storage import get_storage
from utils.time_utils import now_et
from utils.ticker_management import load_master_tickerlist
from utils.helpers import calculate_r_multiple_targets
from utils.markets import calculate_avwap, identify_avwap_reclaim
from utils.validators import validate_signal_schema

logger = get_logger(__name__)

class AVWAPScreener:
    """AVWAP reclaim screener."""
    
    def __init__(self):
        self.storage = get_storage()
        
        # AVWAP criteria
        self.min_volume_multiple = 1.5  # Minimum volume for reclaim confirmation
        self.max_anchor_age_days = 30   # Maximum age of anchor points to consider
        
    def load_avwap_anchors(self) -> pd.DataFrame:
        """Load AVWAP anchor points."""
        anchors_path = f"{UNIVERSE_DIR}/avwap_anchors.csv"
        df_anchors = self.storage.read_df(anchors_path)
        
        if df_anchors is None or df_anchors.empty:
            logger.warning("No AVWAP anchors found")
            return pd.DataFrame()
        
        # Filter to recent anchors
        cutoff_date = now_et() - timedelta(days=self.max_anchor_age_days)
        df_anchors['anchor_date'] = pd.to_datetime(df_anchors['anchor_date'])
        recent_anchors = df_anchors[df_anchors['anchor_date'] >= cutoff_date]
        
        logger.info(f"Loaded {len(recent_anchors)} recent AVWAP anchors")
        return recent_anchors
    
    def load_data_for_ticker(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """Load required data for a ticker."""
        data = {}
        
        # Load 30-minute intraday data (AVWAP works better on 30m timeframe)
        intraday_path = f"data/intraday_30min/{ticker}_30min.csv"
        df_intraday = self.storage.read_df(intraday_path)
        if df_intraday is not None and not df_intraday.empty:
            # Filter to recent data (last 30 days)
            df_intraday['timestamp'] = pd.to_datetime(df_intraday['timestamp'])
            cutoff = now_et() - timedelta(days=30)
            data['intraday_30min'] = df_intraday[df_intraday['timestamp'] >= cutoff].copy()
        
        return data
    
    def calculate_avwap_for_anchors(self, ticker: str, intraday_df: pd.DataFrame, anchors_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Calculate AVWAP for all relevant anchor points."""
        ticker_anchors = anchors_df[anchors_df['ticker'] == ticker]
        
        if ticker_anchors.empty:
            return []
        
        avwap_calculations = []
        
        for _, anchor in ticker_anchors.iterrows():
            try:
                # Calculate AVWAP from this anchor
                df_with_avwap = calculate_avwap(
                    intraday_df,
                    anchor['anchor_price'],
                    anchor['anchor_date']
                )
                
                if 'avwap' not in df_with_avwap.columns or df_with_avwap['avwap'].isna().all():
                    continue
                
                # Identify reclaim signals
                df_with_signals = identify_avwap_reclaim(df_with_avwap)
                
                # Get latest data point
                latest = df_with_signals.iloc[-1] if not df_with_signals.empty else None
                
                if latest is not None:
                    avwap_calc = {
                        'anchor_date': anchor['anchor_date'],
                        'anchor_price': anchor['anchor_price'],
                        'anchor_score': anchor.get('anchor_score', 0),
                        'current_avwap': latest.get('avwap', 0),
                        'current_price': latest.get('close', 0),
                        'above_avwap': latest.get('above_avwap', False),
                        'recent_reclaim': df_with_signals['avwap_reclaim'].tail(10).any(),  # Reclaim in last 10 bars
                        'recent_rejection': df_with_signals['avwap_rejection'].tail(10).any(),
                        'df_with_signals': df_with_signals
                    }
                    
                    avwap_calculations.append(avwap_calc)
                    
            except Exception as e:
                logger.debug(f"Error calculating AVWAP for {ticker} anchor {anchor['anchor_date']}: {e}")
                continue
        
        return avwap_calculations
    
    def find_best_avwap_setup(self, avwap_calculations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the best AVWAP setup from multiple anchor calculations."""
        if not avwap_calculations:
            return None
        
        # Score each AVWAP setup
        scored_setups = []
        
        for calc in avwap_calculations:
            score = 0
            
            # Anchor quality (higher score = better anchor)
            score += calc['anchor_score'] * 0.1
            
            # Recency of reclaim (recent reclaim = better)
            if calc['recent_reclaim']:
                score += 30
            
            # Current position relative to AVWAP
            current_price = calc['current_price']
            current_avwap = calc['current_avwap']
            
            if current_avwap > 0:
                price_distance = abs(current_price - current_avwap) / current_avwap
                # Prefer prices close to AVWAP (better entry)
                if price_distance < 0.02:  # Within 2%
                    score += 20
                elif price_distance < 0.05:  # Within 5%
                    score += 10
            
            # Penalize recent rejections
            if calc['recent_rejection']:
                score -= 15
            
            scored_setups.append((score, calc))
        
        # Return the highest-scored setup
        best_setup = max(scored_setups, key=lambda x: x[0])
        return best_setup[1] if best_setup[0] > 0 else None
    
    def generate_signal(self, ticker: str, data: Dict[str, pd.DataFrame], anchors_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """Generate AVWAP signal for a ticker."""
        
        # Check if we have required data
        if 'intraday_30min' not in data:
            return None
        
        intraday_df = data['intraday_30min']
        if intraday_df.empty:
            return None
        
        # Calculate AVWAP for all relevant anchors
        avwap_calculations = self.calculate_avwap_for_anchors(ticker, intraday_df, anchors_df)
        
        if not avwap_calculations:
            return None
        
        # Find the best AVWAP setup
        best_setup = self.find_best_avwap_setup(avwap_calculations)
        
        if not best_setup:
            return None
        
        # Analyze the signals DataFrame for volume confirmation
        df_signals = best_setup['df_with_signals']
        latest_bars = df_signals.tail(5)  # Last 5 bars
        
        # Check for volume confirmation on recent reclaims
        volume_confirmed = False
        if 'volume_ratio' in df_signals.columns:
            recent_reclaims = latest_bars[latest_bars['avwap_reclaim'] == True]
            if not recent_reclaims.empty:
                max_volume_ratio = recent_reclaims['volume_ratio'].max()
                volume_confirmed = max_volume_ratio >= self.min_volume_multiple
        
        # Determine setup validity
        current_price = best_setup['current_price']
        current_avwap = best_setup['current_avwap']
        above_avwap = best_setup['above_avwap']
        recent_reclaim = best_setup['recent_reclaim']
        
        setup_valid = (
            recent_reclaim and
            above_avwap and
            abs(current_price - current_avwap) / current_avwap < 0.05  # Within 5% of AVWAP
        )
        
        # Calculate entry, stop, and targets
        if above_avwap:
            direction = 'long'
            entry_price = current_price
            stop_price = current_avwap * 0.98  # 2% below AVWAP
        else:
            direction = 'short'
            entry_price = current_price
            stop_price = current_avwap * 1.02  # 2% above AVWAP
        
        # Calculate R-multiple targets
        targets = calculate_r_multiple_targets(entry_price, stop_price, direction)
        
        # Calculate confidence score
        confidence = 0.0
        
        # Anchor quality (0-0.3)
        anchor_score = best_setup['anchor_score']
        if anchor_score >= 70:
            confidence += 0.3
        elif anchor_score >= 50:
            confidence += 0.2
        elif anchor_score >= 30:
            confidence += 0.1
        
        # Reclaim signal (0-0.4)
        if recent_reclaim:
            confidence += 0.4
        
        # Volume confirmation (0-0.2)
        if volume_confirmed:
            confidence += 0.2
        
        # Position relative to AVWAP (0-0.1)
        if current_avwap > 0:
            price_distance = abs(current_price - current_avwap) / current_avwap
            if price_distance < 0.02:
                confidence += 0.1
        
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
            'anchor_date': best_setup['anchor_date'].strftime('%Y-%m-%d'),
            'anchor_price': best_setup['anchor_price'],
            'anchor_score': best_setup['anchor_score'],
            'current_avwap': current_avwap,
            'above_avwap': above_avwap,
            'recent_reclaim': recent_reclaim,
            'volume_confirmed': volume_confirmed,
            'strategy': 'AVWAP_Reclaim'
        }
        
        return signal
    
    def run_screening(self, ticker_list: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Run AVWAP screening for all tickers."""
        start_time = datetime.now()
        log_job_start(logger, "avwap")
        
        # Load AVWAP anchors
        anchors_df = self.load_avwap_anchors()
        if anchors_df.empty:
            logger.warning("No AVWAP anchors available for screening")
            return []
        
        # Load ticker list
        if ticker_list is None:
            ticker_list = load_master_tickerlist()
        
        if not ticker_list:
            logger.warning("No tickers found for AVWAP screening")
            return []
        
        # Filter to tickers that have anchors
        available_tickers = anchors_df['ticker'].unique()
        relevant_tickers = [ticker for ticker in ticker_list if ticker in available_tickers]
        
        logger.info(f"AVWAP screening {len(relevant_tickers)} tickers with anchors")
        
        signals = []
        processed_count = 0
        
        # Process each ticker
        for ticker in relevant_tickers:
            try:
                # Load data
                data = self.load_data_for_ticker(ticker)
                
                # Generate signal
                signal = self.generate_signal(ticker, data, anchors_df)
                
                if signal:
                    signals.append(signal)
                    logger.debug(f"AVWAP signal generated for {ticker}")
                
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"Error processing {ticker} for AVWAP: {e}")
        
        # Save signals
        if signals:
            signals_df = pd.DataFrame(signals)
            
            # Validate schema
            validation_result = validate_signal_schema(signals_df)
            if not validation_result.valid:
                logger.error(f"Signal schema validation failed: {validation_result.message}")
            else:
                # Save to storage
                output_path = f"{SIGNALS_DIR}/avwap.csv"
                self.storage.save_df(signals_df, output_path)
                logger.info(f"Saved {len(signals)} AVWAP signals to {output_path}")
        
        # Log completion
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        log_job_complete(logger, "avwap", elapsed_ms, len(signals))
        
        logger.info(f"AVWAP screening complete: {len(signals)} signals from {processed_count} tickers")
        return signals

def main(ticker_list: Optional[List[str]] = None):
    """Main entry point for AVWAP screener."""
    try:
        screener = AVWAPScreener()
        signals = screener.run_screening(ticker_list)
        
        print(f"AVWAP screening complete: {len(signals)} signals generated")
        
        if signals:
            # Show top signals by confidence
            top_signals = sorted(signals, key=lambda x: x['confidence'], reverse=True)[:5]
            print("\nTop 5 AVWAP signals by confidence:")
            for signal in top_signals:
                print(f"  {signal['ticker']}: {signal['confidence']:.2f} confidence, "
                      f"anchor: {signal['anchor_date']} @ ${signal['anchor_price']:.2f}, "
                      f"entry: ${signal['entry']:.2f}")
        
        return signals
        
    except Exception as e:
        logger.error(f"AVWAP screener failed: {e}")
        return []

if __name__ == "__main__":
    import sys
    ticker_list = sys.argv[1:] if len(sys.argv) > 1 else None
    main(ticker_list)