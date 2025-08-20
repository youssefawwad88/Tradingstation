"""
Signal scoring and ranking system for trade selection.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timezone


class SignalScorer:
    """Scores and ranks trading signals based on multiple criteria."""
    
    def __init__(self):
        """Initialize the signal scorer with default weights."""
        self.weights = {
            "signal_quality": 0.30,      # Original signal score
            "risk_reward": 0.25,         # R-multiple quality
            "setup_strength": 0.20,      # Setup-specific factors
            "timing_quality": 0.15,      # Time-based factors
            "market_context": 0.10       # Market environment
        }
    
    def score_signal_quality(self, signal: pd.Series) -> float:
        """
        Score the raw signal quality (0-10).
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Normalized quality score (0-1)
        """
        raw_score = float(signal.get('score', 0))
        return min(raw_score / 10.0, 1.0)  # Normalize to 0-1
    
    def score_risk_reward(self, signal: pd.Series) -> float:
        """
        Score the risk/reward profile (0-1).
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Risk/reward score (0-1)
        """
        entry = float(signal.get('entry', 0))
        stop = float(signal.get('stop', 0))
        tp1 = float(signal.get('tp1', 0))
        tp2 = float(signal.get('tp2', 0))
        tp3 = float(signal.get('tp3', 0))
        direction = signal.get('direction', 'long')
        
        if entry <= 0 or stop <= 0:
            return 0.0
        
        # Calculate R-multiples
        risk_per_share = abs(entry - stop)
        if risk_per_share <= 0:
            return 0.0
        
        r_multiples = []
        for tp in [tp1, tp2, tp3]:
            if tp > 0:
                if direction == 'long':
                    r_mult = (tp - entry) / risk_per_share
                else:  # short
                    r_mult = (entry - tp) / risk_per_share
                r_multiples.append(r_mult)
        
        if not r_multiples:
            return 0.0
        
        # Score based on best R-multiple
        max_r = max(r_multiples)
        
        # Scoring curve: excellent above 3R, good above 2R, fair above 1.5R
        if max_r >= 3.0:
            return 1.0
        elif max_r >= 2.0:
            return 0.8
        elif max_r >= 1.5:
            return 0.6
        elif max_r >= 1.0:
            return 0.4
        else:
            return 0.2 * max_r  # Linear below 1R
    
    def score_setup_strength(self, signal: pd.Series) -> float:
        """
        Score setup-specific strength factors (0-1).
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Setup strength score (0-1)
        """
        screener = signal.get('screener', '')
        setup_name = signal.get('setup_name', '')
        
        # Base score by screener type (some setups are inherently stronger)
        screener_scores = {
            'gapgo': 0.8,           # High probability momentum
            'orb': 0.7,             # Reliable breakout pattern
            'avwap_reclaim': 0.9,   # Strong institutional flow
            'breakout': 0.6,        # General breakout
            'ema_pullback': 0.7,    # Trend continuation
            'exhaustion_reversal': 0.5  # Counter-trend (higher risk)
        }
        
        base_score = screener_scores.get(screener, 0.5)
        
        # Adjust based on setup-specific factors
        setup_adjustments = 0.0
        
        # Volume confirmation adds strength
        if 'volume' in setup_name.lower() or 'confirmation' in signal.get('notes', '').lower():
            setup_adjustments += 0.1
        
        # Gap size for gap plays
        if screener == 'gapgo':
            gap_size = signal.get('gap_percent', 0)
            if gap_size >= 5:  # Large gap
                setup_adjustments += 0.1
            elif gap_size >= 3:  # Medium gap
                setup_adjustments += 0.05
        
        # AVWAP reclaim quality
        if screener == 'avwap_reclaim':
            reclaim_quality = signal.get('reclaim_quality', 0)
            setup_adjustments += (reclaim_quality - 5) * 0.02  # Center around 5
        
        return min(base_score + setup_adjustments, 1.0)
    
    def score_timing_quality(self, signal: pd.Series) -> float:
        """
        Score timing-related factors (0-1).
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Timing quality score (0-1)
        """
        # Check if signal has timestamp
        if 'timestamp_utc' not in signal or pd.isna(signal['timestamp_utc']):
            return 0.5  # Neutral score for missing timestamp
        
        try:
            signal_time = pd.to_datetime(signal['timestamp_utc'])
            if signal_time.tz is None:
                signal_time = signal_time.tz_localize('UTC')
            
            current_time = datetime.now(timezone.utc)
            age_hours = (current_time - signal_time).total_seconds() / 3600
            
            # Fresher signals score higher
            if age_hours <= 1:
                freshness_score = 1.0
            elif age_hours <= 4:
                freshness_score = 0.8
            elif age_hours <= 12:
                freshness_score = 0.6
            elif age_hours <= 24:
                freshness_score = 0.4
            else:
                freshness_score = 0.2
            
            # Time of day factors (market hours are better)
            signal_hour_et = (signal_time - pd.Timedelta(hours=4)).hour  # Convert to ET
            
            if 9 <= signal_hour_et <= 16:  # Market hours
                time_score = 1.0
            elif 4 <= signal_hour_et <= 9 or 16 <= signal_hour_et <= 20:  # Extended hours
                time_score = 0.7
            else:  # After hours
                time_score = 0.3
            
            return (freshness_score + time_score) / 2
            
        except Exception:
            return 0.5  # Neutral score for timestamp parsing errors
    
    def score_market_context(self, signal: pd.Series) -> float:
        """
        Score market context factors (0-1).
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Market context score (0-1)
        """
        # For now, use a simplified market context score
        # This could be enhanced with actual market data
        
        direction = signal.get('direction', 'long')
        screener = signal.get('screener', '')
        
        # General market bias (could be fed from market indicators)
        market_bias = 0.6  # Neutral-slightly bullish default
        
        # Trend following strategies work better in trending markets
        if screener in ['ema_pullback', 'breakout'] and direction == 'long':
            trend_adjustment = 0.1
        elif screener == 'exhaustion_reversal':
            trend_adjustment = -0.1  # Counter-trend is harder
        else:
            trend_adjustment = 0.0
        
        return min(market_bias + trend_adjustment, 1.0)
    
    def calculate_composite_score(self, signal: pd.Series) -> float:
        """
        Calculate the weighted composite score.
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Composite score (0-10)
        """
        # Calculate individual component scores
        quality_score = self.score_signal_quality(signal)
        rr_score = self.score_risk_reward(signal)
        setup_score = self.score_setup_strength(signal)
        timing_score = self.score_timing_quality(signal)
        market_score = self.score_market_context(signal)
        
        # Calculate weighted composite
        composite = (
            quality_score * self.weights["signal_quality"] +
            rr_score * self.weights["risk_reward"] +
            setup_score * self.weights["setup_strength"] +
            timing_score * self.weights["timing_quality"] +
            market_score * self.weights["market_context"]
        )
        
        # Scale to 0-10
        return round(composite * 10, 2)
    
    def score_signals(self, signals_df: pd.DataFrame) -> pd.DataFrame:
        """
        Score all signals in the DataFrame.
        
        Args:
            signals_df: DataFrame of signals
            
        Returns:
            DataFrame with additional scoring columns
        """
        if signals_df.empty:
            return signals_df
        
        scored_df = signals_df.copy()
        
        # Calculate component scores
        scored_df['quality_score'] = signals_df.apply(self.score_signal_quality, axis=1)
        scored_df['rr_score'] = signals_df.apply(self.score_risk_reward, axis=1)
        scored_df['setup_score'] = signals_df.apply(self.score_setup_strength, axis=1)
        scored_df['timing_score'] = signals_df.apply(self.score_timing_quality, axis=1)
        scored_df['market_score'] = signals_df.apply(self.score_market_context, axis=1)
        
        # Calculate composite score
        scored_df['composite_score'] = signals_df.apply(self.calculate_composite_score, axis=1)
        
        # Add ranking
        scored_df['rank'] = scored_df['composite_score'].rank(method='dense', ascending=False)
        
        return scored_df
    
    def get_top_signals(self, signals_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
        """
        Get the top N signals by composite score.
        
        Args:
            signals_df: DataFrame of signals
            top_n: Number of top signals to return
            
        Returns:
            DataFrame with top signals
        """
        if signals_df.empty:
            return signals_df
        
        scored_df = self.score_signals(signals_df)
        return scored_df.nlargest(top_n, 'composite_score')
    
    def generate_score_breakdown(self, signal: pd.Series) -> Dict[str, float]:
        """
        Generate detailed score breakdown for a single signal.
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Dictionary with score breakdown
        """
        return {
            "signal_quality": self.score_signal_quality(signal),
            "risk_reward": self.score_risk_reward(signal),
            "setup_strength": self.score_setup_strength(signal),
            "timing_quality": self.score_timing_quality(signal),
            "market_context": self.score_market_context(signal),
            "composite_score": self.calculate_composite_score(signal)
        }