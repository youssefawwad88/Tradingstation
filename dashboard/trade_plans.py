"""
Trade plan generator for position sizing and risk management.
"""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone


class TradePlanGenerator:
    """Generates standardized trade plans with position sizing and risk management."""
    
    def __init__(self, account_size: float = 100000, risk_per_trade: float = 0.02):
        """
        Initialize trade plan generator.
        
        Args:
            account_size: Total account size for position sizing
            risk_per_trade: Risk per trade as percentage (0.02 = 2%)
        """
        self.account_size = account_size
        self.risk_per_trade = risk_per_trade
    
    def calculate_position_size(self, entry_price: float, stop_price: float) -> Dict[str, float]:
        """
        Calculate position size based on risk management rules.
        
        Args:
            entry_price: Entry price for the trade
            stop_price: Stop loss price
            
        Returns:
            Dictionary with position sizing details
        """
        if entry_price <= 0 or stop_price <= 0:
            return {"shares": 0, "position_value": 0, "risk_amount": 0}
        
        # Calculate risk per share
        risk_per_share = abs(entry_price - stop_price)
        
        if risk_per_share <= 0:
            return {"shares": 0, "position_value": 0, "risk_amount": 0}
        
        # Calculate risk amount in dollars
        risk_amount = self.account_size * self.risk_per_trade
        
        # Calculate shares based on risk
        shares = int(risk_amount / risk_per_share)
        
        # Calculate position value
        position_value = shares * entry_price
        
        return {
            "shares": shares,
            "position_value": position_value,
            "risk_amount": risk_amount,
            "risk_per_share": risk_per_share,
            "position_size_percent": (position_value / self.account_size) * 100
        }
    
    def generate_trade_plan(self, signal: pd.Series) -> Dict[str, Any]:
        """
        Generate a complete trade plan from a signal.
        
        Args:
            signal: Signal data as pandas Series
            
        Returns:
            Complete trade plan dictionary
        """
        # Extract signal data
        symbol = signal.get('symbol', '')
        direction = signal.get('direction', 'long')
        entry = float(signal.get('entry', 0))
        stop = float(signal.get('stop', 0))
        score = float(signal.get('score', 0))
        setup_name = signal.get('setup_name', 'Unknown')
        screener = signal.get('screener', 'Unknown')
        
        # Calculate position sizing
        position_data = self.calculate_position_size(entry, stop)
        
        # Extract target prices
        tp1 = float(signal.get('tp1', 0))
        tp2 = float(signal.get('tp2', 0))
        tp3 = float(signal.get('tp3', 0))
        
        # Calculate R-multiples for targets
        risk_per_share = position_data.get('risk_per_share', 0)
        r_multiples = []
        
        if risk_per_share > 0:
            for tp in [tp1, tp2, tp3]:
                if tp > 0:
                    if direction == 'long':
                        r_mult = (tp - entry) / risk_per_share
                    else:  # short
                        r_mult = (entry - tp) / risk_per_share
                    r_multiples.append(round(r_mult, 2))
                else:
                    r_multiples.append(0)
        else:
            r_multiples = [0, 0, 0]
        
        # Generate trade plan
        trade_plan = {
            "symbol": symbol,
            "screener": screener,
            "setup_name": setup_name,
            "direction": direction,
            "signal_score": score,
            "timestamp": signal.get('timestamp_utc', datetime.now(timezone.utc)),
            
            # Entry and exit levels
            "entry_price": entry,
            "stop_loss": stop,
            "target_1": tp1,
            "target_2": tp2,
            "target_3": tp3,
            
            # Position sizing
            "shares": position_data['shares'],
            "position_value": position_data['position_value'],
            "position_size_percent": position_data['position_size_percent'],
            "risk_amount": position_data['risk_amount'],
            "risk_per_share": position_data['risk_per_share'],
            
            # Risk/reward
            "r_multiple_tp1": r_multiples[0],
            "r_multiple_tp2": r_multiples[1], 
            "r_multiple_tp3": r_multiples[2],
            "max_r_multiple": max(r_multiples) if r_multiples else 0,
            
            # Trade quality assessment
            "trade_quality": self.assess_trade_quality(score, r_multiples),
            
            # Execution notes
            "notes": self.generate_execution_notes(signal, position_data, r_multiples)
        }
        
        return trade_plan
    
    def assess_trade_quality(self, score: float, r_multiples: List[float]) -> str:
        """
        Assess overall trade quality based on score and R-multiples.
        
        Args:
            score: Signal score (1-10)
            r_multiples: List of R-multiples for targets
            
        Returns:
            Trade quality assessment string
        """
        max_r = max(r_multiples) if r_multiples else 0
        
        # Quality matrix based on score and R-multiple
        if score >= 8 and max_r >= 3:
            return "EXCELLENT"
        elif score >= 7 and max_r >= 2:
            return "GOOD"
        elif score >= 6 and max_r >= 1.5:
            return "FAIR"
        elif score >= 5 and max_r >= 1:
            return "MARGINAL"
        else:
            return "POOR"
    
    def generate_execution_notes(self, signal: pd.Series, position_data: Dict, r_multiples: List[float]) -> str:
        """Generate execution notes for the trade."""
        notes = []
        
        # Position size warnings
        if position_data['position_size_percent'] > 10:
            notes.append("⚠️ Large position size (>10% of account)")
        elif position_data['position_size_percent'] < 1:
            notes.append("ℹ️ Small position size (<1% of account)")
        
        # R-multiple assessment
        max_r = max(r_multiples) if r_multiples else 0
        if max_r < 1:
            notes.append("⚠️ Poor risk/reward ratio (<1R)")
        elif max_r >= 3:
            notes.append("✅ Excellent risk/reward ratio (≥3R)")
        
        # Entry timing
        if signal.get('screener') == 'gapgo':
            notes.append("🕘 Best entry: 09:36-10:00 ET")
        elif signal.get('screener') == 'orb':
            notes.append("🕘 Best entry: Opening range breakout")
        
        return " | ".join(notes) if notes else "Standard execution"
    
    def generate_plans(self, signals_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Generate trade plans for all signals.
        
        Args:
            signals_df: DataFrame of signals
            
        Returns:
            List of trade plan dictionaries
        """
        if signals_df.empty:
            return []
        
        trade_plans = []
        
        for _, signal in signals_df.iterrows():
            try:
                plan = self.generate_trade_plan(signal)
                trade_plans.append(plan)
            except Exception as e:
                # Log error but continue processing other signals
                print(f"Error generating plan for {signal.get('symbol', 'Unknown')}: {e}")
        
        # Sort by trade quality and score
        quality_order = {"EXCELLENT": 5, "GOOD": 4, "FAIR": 3, "MARGINAL": 2, "POOR": 1}
        trade_plans.sort(
            key=lambda x: (quality_order.get(x['trade_quality'], 0), x['signal_score']),
            reverse=True
        )
        
        return trade_plans
    
    def calculate_portfolio_risk(self, trade_plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate aggregate portfolio risk across all planned trades.
        
        Args:
            trade_plans: List of trade plan dictionaries
            
        Returns:
            Portfolio risk summary
        """
        if not trade_plans:
            return {"total_risk_percent": 0, "position_count": 0, "total_exposure": 0}
        
        total_risk = sum(plan['risk_amount'] for plan in trade_plans)
        total_exposure = sum(plan['position_value'] for plan in trade_plans)
        position_count = len(trade_plans)
        
        risk_percent = (total_risk / self.account_size) * 100
        exposure_percent = (total_exposure / self.account_size) * 100
        
        # Risk assessment
        risk_level = "LOW"
        if risk_percent > 20:
            risk_level = "EXTREME"
        elif risk_percent > 15:
            risk_level = "HIGH"
        elif risk_percent > 10:
            risk_level = "MODERATE"
        
        return {
            "total_risk_percent": round(risk_percent, 2),
            "total_exposure_percent": round(exposure_percent, 2),
            "position_count": position_count,
            "risk_level": risk_level,
            "average_risk_per_trade": round(risk_percent / position_count, 2) if position_count > 0 else 0,
            "recommended_max_positions": max(1, int(25 / self.risk_per_trade * 100))  # 25% max portfolio risk
        }