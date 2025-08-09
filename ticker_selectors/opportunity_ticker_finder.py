"""
Opportunity ticker finder for Trading Station.
Filters S&P 500 universe based on market cap, volume, and breakout criteria.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import yaml
from pathlib import Path

from utils.config import get_strategy_config, UNIVERSE_DIR
from utils.logging_setup import get_logger, log_job_start, log_job_complete
from utils.storage import get_storage
from utils.validators import validate_ticker_symbol
from utils.alpha_vantage_api import get_api
from utils.time_utils import now_et, prev_trading_day
from utils.helpers import calculate_volume_metrics, calculate_gap_percentage

logger = get_logger(__name__)

class OpportunityTickerFinder:
    """Finds trading opportunities from S&P 500 universe."""
    
    def __init__(self):
        self.storage = get_storage()
        self.api = get_api()
        self.config = get_strategy_config()
        
        # Default filtering criteria
        self.min_market_cap = 1_000_000_000  # $1B
        self.min_avg_volume = 500_000        # 500k daily
        self.min_price = 5.0                 # $5 minimum
        self.max_price = 500.0               # $500 maximum
        
        # Load custom config if available
        universe_config = self.config.get('universe', {})
        filters = universe_config.get('filters', {})
        
        self.min_market_cap = filters.get('min_market_cap', self.min_market_cap)
        self.min_avg_volume = filters.get('min_avg_volume', self.min_avg_volume)
        
    def load_sp500_universe(self) -> List[str]:
        """Load S&P 500 ticker symbols from config."""
        try:
            universe_config = self.config.get('universe', {})
            symbols_list = universe_config.get('symbols', [])
            
            if not symbols_list:
                # Fallback to hardcoded list
                symbols_list = self._get_default_sp500_symbols()
                logger.warning("Using default S&P 500 symbol list")
            
            tickers = [symbol['symbol'] for symbol in symbols_list if 'symbol' in symbol]
            
            # Add forced includes
            forced_includes = universe_config.get('manual_tickers', {}).get('force_include', [])
            tickers.extend(forced_includes)
            
            # Remove duplicates and validate
            unique_tickers = []
            for ticker in set(tickers):
                if validate_ticker_symbol(ticker).valid:
                    unique_tickers.append(ticker.upper())
            
            logger.info(f"Loaded {len(unique_tickers)} symbols from S&P 500 universe")
            return sorted(unique_tickers)
            
        except Exception as e:
            logger.error(f"Failed to load S&P 500 universe: {e}")
            return self._get_default_sp500_symbols()
    
    def _get_default_sp500_symbols(self) -> List[str]:
        """Get default S&P 500 symbols as fallback."""
        # Top 50 S&P 500 by market cap (sample)
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'UNH', 'JNJ',
            'JPM', 'V', 'PG', 'XOM', 'HD', 'CVX', 'MA', 'BAC', 'ABBV', 'PFE',
            'AVGO', 'KO', 'COST', 'MRK', 'DIS', 'TMO', 'ACN', 'WMT', 'ABT', 'NEE',
            'DHR', 'VZ', 'NKE', 'ADBE', 'CRM', 'TXN', 'PM', 'MDT', 'BMY', 'RTX',
            'QCOM', 'HON', 'UPS', 'SCHW', 'LOW', 'AMGN', 'IBM', 'ELV', 'SPGI', 'INTU'
        ]
    
    def get_daily_data_for_screening(self, tickers: List[str]) -> Dict[str, pd.DataFrame]:
        """Get daily data for all tickers for screening purposes."""
        daily_data = {}
        failed_tickers = []
        
        logger.info(f"Fetching daily data for {len(tickers)} tickers for screening")
        
        for ticker in tickers:
            try:
                # Try to load from storage first
                daily_path = f"data/daily/{ticker}_daily.csv"
                df = self.storage.read_df(daily_path)
                
                # If not available or outdated, fetch from API
                if df is None or self._is_data_stale(df):
                    logger.debug(f"Fetching fresh daily data for {ticker}")
                    df = self.api.get_daily_data(ticker, outputsize="compact")
                    
                    if df is not None and not df.empty:
                        # Save for future use
                        self.storage.save_df(df, daily_path)
                
                if df is not None and not df.empty:
                    daily_data[ticker] = df
                else:
                    failed_tickers.append(ticker)
                    
            except Exception as e:
                logger.warning(f"Failed to get daily data for {ticker}: {e}")
                failed_tickers.append(ticker)
        
        if failed_tickers:
            logger.warning(f"Failed to get data for {len(failed_tickers)} tickers: {failed_tickers[:5]}...")
        
        logger.info(f"Successfully loaded daily data for {len(daily_data)} tickers")
        return daily_data
    
    def _is_data_stale(self, df: pd.DataFrame, max_age_days: int = 1) -> bool:
        """Check if daily data is stale."""
        if df.empty or 'date' not in df.columns:
            return True
        
        latest_date = pd.to_datetime(df['date']).max()
        cutoff_date = now_et() - timedelta(days=max_age_days)
        
        return latest_date < cutoff_date
    
    def apply_basic_filters(self, daily_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Apply basic filtering criteria."""
        filtered_tickers = []
        
        for ticker, df in daily_data.items():
            try:
                if df.empty:
                    continue
                
                # Get latest data
                latest = df.iloc[-1]
                
                # Price filter
                if not (self.min_price <= latest['close'] <= self.max_price):
                    continue
                
                # Volume filter (average over last 20 days)
                if len(df) >= 20:
                    avg_volume = df['volume'].tail(20).mean()
                    if avg_volume < self.min_avg_volume:
                        continue
                
                # Add more sophisticated filters here based on requirements
                filtered_tickers.append(ticker)
                
            except Exception as e:
                logger.warning(f"Error filtering {ticker}: {e}")
                continue
        
        return filtered_tickers
    
    def apply_ashraf_breakout_logic(self, daily_data: Dict[str, pd.DataFrame]) -> List[str]:
        """Apply Umar Ashraf's breakout logic for screening."""
        breakout_candidates = []
        
        for ticker, df in daily_data.items():
            try:
                if len(df) < 50:  # Need enough history
                    continue
                
                # Calculate technical indicators
                df_analysis = df.copy()
                
                # Calculate volume metrics
                df_analysis = calculate_volume_metrics(df_analysis, lookback_days=20)
                
                # Calculate 20-day high
                df_analysis['high_20d'] = df_analysis['high'].rolling(20).max()
                
                # Get latest data
                latest = df_analysis.iloc[-1]
                
                # Ashraf breakout criteria:
                # 1. Close near 20-day high (within 5%)
                high_20d = latest['high_20d']
                if latest['close'] < high_20d * 0.95:
                    continue
                
                # 2. Volume above average
                if latest['volume_ratio'] < 1.5:  # 1.5x average volume
                    continue
                
                # 3. Not extended (within 15% of 50-day moving average)
                if len(df_analysis) >= 50:
                    sma50 = df_analysis['close'].tail(50).mean()
                    if latest['close'] > sma50 * 1.15:
                        continue
                
                # 4. Minimum absolute volume
                if latest['volume'] < self.min_avg_volume:
                    continue
                
                breakout_candidates.append(ticker)
                
            except Exception as e:
                logger.warning(f"Error applying breakout logic to {ticker}: {e}")
                continue
        
        return breakout_candidates
    
    def score_opportunities(self, candidates: List[str], daily_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """Score and rank opportunity candidates."""
        scored_opportunities = []
        
        for ticker in candidates:
            try:
                df = daily_data[ticker]
                latest = df.iloc[-1]
                
                score = 0
                reasons = []
                
                # Volume score (0-30 points)
                if len(df) >= 20:
                    avg_volume = df['volume'].tail(20).mean()
                    volume_ratio = latest['volume'] / avg_volume
                    
                    if volume_ratio >= 3.0:
                        score += 30
                        reasons.append("Very high volume")
                    elif volume_ratio >= 2.0:
                        score += 20
                        reasons.append("High volume")
                    elif volume_ratio >= 1.5:
                        score += 10
                        reasons.append("Above average volume")
                
                # Price action score (0-25 points)
                if len(df) >= 10:
                    recent_high = df['high'].tail(10).max()
                    if latest['close'] >= recent_high * 0.98:
                        score += 25
                        reasons.append("Near recent high")
                    elif latest['close'] >= recent_high * 0.95:
                        score += 15
                        reasons.append("Close to recent high")
                
                # Range expansion (0-20 points)
                if len(df) >= 20:
                    avg_range = ((df['high'] - df['low']) / df['close']).tail(20).mean()
                    current_range = (latest['high'] - latest['low']) / latest['close']
                    
                    if current_range >= avg_range * 2.0:
                        score += 20
                        reasons.append("Wide range day")
                    elif current_range >= avg_range * 1.5:
                        score += 10
                        reasons.append("Above average range")
                
                # Gap up (0-15 points)
                if len(df) >= 2:
                    prev_close = df.iloc[-2]['close']
                    gap_pct = calculate_gap_percentage(latest['open'], prev_close)
                    
                    if gap_pct >= 3.0:
                        score += 15
                        reasons.append(f"Gap up {gap_pct:.1f}%")
                    elif gap_pct >= 1.5:
                        score += 10
                        reasons.append(f"Small gap up {gap_pct:.1f}%")
                
                # Market cap bonus (0-10 points) - prefer liquid names
                # This would require market cap data from API
                if latest['volume'] * latest['close'] > 50_000_000:  # $50M+ daily dollar volume
                    score += 10
                    reasons.append("High dollar volume")
                
                opportunity = {
                    'ticker': ticker,
                    'score': score,
                    'price': latest['close'],
                    'volume': latest['volume'],
                    'reasons': reasons,
                    'analysis_date': latest.get('date', datetime.now().date())
                }
                
                scored_opportunities.append(opportunity)
                
            except Exception as e:
                logger.warning(f"Error scoring {ticker}: {e}")
                continue
        
        # Sort by score descending
        scored_opportunities.sort(key=lambda x: x['score'], reverse=True)
        return scored_opportunities
    
    def save_results(self, opportunities: List[Dict[str, Any]]) -> str:
        """Save screening results and return ticker list."""
        try:
            # Save detailed analysis
            analysis_df = pd.DataFrame(opportunities)
            analysis_path = f"{UNIVERSE_DIR}/opportunity_analysis.csv"
            self.storage.save_df(analysis_df, analysis_path)
            
            # Extract just the ticker symbols for master list
            tickers = [opp['ticker'] for opp in opportunities]
            
            # Save master ticker list
            output_path = f"{UNIVERSE_DIR}/selector_tickerlist.txt"
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                for ticker in tickers:
                    f.write(f"{ticker}\n")
            
            logger.info(f"Saved {len(tickers)} opportunity tickers to {output_path}")
            
            # Log top opportunities
            if opportunities:
                top_5 = opportunities[:5]
                for i, opp in enumerate(top_5, 1):
                    reasons_str = ", ".join(opp['reasons'][:3])  # Top 3 reasons
                    logger.info(f"#{i} {opp['ticker']}: {opp['score']} pts - {reasons_str}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to save screening results: {e}")
            raise
    
    def run_screening(self) -> List[str]:
        """Run complete screening process."""
        start_time = datetime.now()
        log_job_start(logger, "opportunity_ticker_finder")
        
        try:
            # Load universe
            sp500_tickers = self.load_sp500_universe()
            logger.info(f"Starting screening with {len(sp500_tickers)} S&P 500 tickers")
            
            # Get daily data
            daily_data = self.get_daily_data_for_screening(sp500_tickers)
            
            # Apply basic filters
            basic_filtered = self.apply_basic_filters(daily_data)
            logger.info(f"After basic filters: {len(basic_filtered)} tickers")
            
            # Apply Ashraf breakout logic
            breakout_candidates = self.apply_ashraf_breakout_logic(
                {ticker: daily_data[ticker] for ticker in basic_filtered if ticker in daily_data}
            )
            logger.info(f"After breakout logic: {len(breakout_candidates)} candidates")
            
            # Score and rank opportunities
            opportunities = self.score_opportunities(breakout_candidates, daily_data)
            
            # Take top 50 opportunities
            top_opportunities = opportunities[:50]
            
            # Save results
            self.save_results(top_opportunities)
            
            # Return ticker list
            result_tickers = [opp['ticker'] for opp in top_opportunities]
            
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            log_job_complete(logger, "opportunity_ticker_finder", elapsed_ms, len(result_tickers))
            
            return result_tickers
            
        except Exception as e:
            elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
            logger.error(f"Opportunity screening failed: {e}")
            log_job_complete(logger, "opportunity_ticker_finder", elapsed_ms, 0)
            raise

def main():
    """Main entry point for opportunity ticker finder."""
    try:
        finder = OpportunityTickerFinder()
        tickers = finder.run_screening()
        
        print(f"Opportunity screening complete: {len(tickers)} tickers selected")
        if tickers:
            print(f"Top 10: {', '.join(tickers[:10])}")
        
        return tickers
        
    except Exception as e:
        logger.error(f"Opportunity ticker finder failed: {e}")
        return []

if __name__ == "__main__":
    main()