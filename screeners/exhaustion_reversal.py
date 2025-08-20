"""
Exhaustion Reversal Strategy Screener - Mean reversion after climactic moves.

This module identifies exhaustion patterns after extended moves away from 
AVWAP/EMA with climactic volume for mean reversion trades.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from utils.config import config
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io

logger = get_logger(__name__)


class ExhaustionReversalScreener:
    """Exhaustion reversal strategy screener."""

    def __init__(self) -> None:
        """Initialize the exhaustion reversal screener."""
        self.universe_tickers: List[str] = []
        self.signals: List[Dict] = []
        self.load_universe()

    def load_universe(self) -> None:
        """Load the master ticker list from Spaces."""
        try:
            universe_key = config.get_spaces_path(*config.MASTER_TICKERLIST_PATH)
            df = spaces_io.download_dataframe(universe_key)
            
            if df is not None and not df.empty:
                active_tickers = df[
                    (df["active"] == 1) & (df["fetch_30min"] == 1)
                ]["symbol"].tolist()
                self.universe_tickers = active_tickers
                logger.info(f"Loaded {len(active_tickers)} tickers for exhaustion screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS
                
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def run_exhaustion_screen(self) -> bool:
        """Run the exhaustion reversal screening process."""
        logger.job_start("ExhaustionReversalScreener.run_exhaustion_screen")
        start_time = time.time()
        
        try:
            self.signals.clear()
            successful_tickers = 0
            
            for ticker in self.universe_tickers:
                try:
                    signals = self.screen_ticker(ticker)
                    if signals:
                        self.signals.extend(signals)
                        successful_tickers += 1
                
                except Exception as e:
                    logger.error(f"Error screening {ticker}: {e}")
            
            self.save_signals()
            
            duration = time.time() - start_time
            logger.job_complete(
                "ExhaustionReversalScreener.run_exhaustion_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_signals=len(self.signals),
            )
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "ExhaustionReversalScreener.run_exhaustion_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """Screen a ticker for exhaustion reversal setups."""
        try:
            intraday_key = config.get_spaces_path("data", "intraday", "30min", f"{ticker}.csv")
            df = spaces_io.download_dataframe(intraday_key)
            
            if df is None or df.empty or len(df) < 50:
                return []
            
            df = self._prepare_data(df)
            
            # Look for exhaustion in recent bars
            recent_data = df.tail(20)
            signals = []
            
            for idx in range(1, len(recent_data)):  # Skip first bar
                bar = recent_data.iloc[idx]
                prev_bar = recent_data.iloc[idx-1]
                
                signal = self._check_exhaustion_setup(ticker, bar, prev_bar, df)
                if signal:
                    signals.append(signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error screening {ticker}: {e}")
            return []

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare 30-minute data with indicators."""
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        
        # Calculate VWAP (session-based)
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]
        
        # Simple rolling VWAP for 30min data
        window = 20  # 20 periods ~ 10 hours
        df["vwap"] = df["pv"].rolling(window).sum() / df["volume"].rolling(window).sum()
        
        # Calculate EMA20
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        
        # Volume and range metrics
        df["avg_volume"] = df["volume"].rolling(20, min_periods=10).mean()
        df["range"] = df["high"] - df["low"]
        df["avg_range"] = df["range"].rolling(20, min_periods=10).mean()
        
        return df

    def _check_exhaustion_setup(self, ticker: str, bar: pd.Series, prev_bar: pd.Series, df: pd.DataFrame) -> Optional[Dict]:
        """Check for valid exhaustion reversal setup."""
        try:
            vwap = bar["vwap"]
            ema20 = bar["ema20"]
            
            if pd.isna(vwap) or pd.isna(ema20):
                return None
            
            # Check for extended move away from VWAP
            distance_from_vwap = abs(bar["close"] - vwap) / vwap
            
            if distance_from_vwap < 0.02:  # Must be at least 2% away
                return None
            
            # Check for climactic volume
            avg_volume = bar["avg_volume"]
            vol_ratio = bar["volume"] / avg_volume if avg_volume > 0 else 0
            
            if vol_ratio < 2.0:  # Need 200% of average volume
                return None
            
            # Check for large range candle
            avg_range = bar["avg_range"]
            range_ratio = bar["range"] / avg_range if avg_range > 0 else 0
            
            if range_ratio < 1.5:  # Need 150% of average range
                return None
            
            # Determine direction and reversal signal
            direction = "short" if bar["close"] > vwap else "long"
            
            # Check for reversal pattern
            if direction == "long":
                # After selling climax, look for hammer/doji
                reversal_signal = self._check_bullish_reversal(bar, prev_bar)
                if not reversal_signal:
                    return None
                
                entry = bar["close"]
                stop = bar["low"] * 0.995  # Just below climax low
                
                # Target: VWAP or EMA20 (whichever is closer)
                target_vwap = vwap
                target_ema = ema20
                target = min(target_vwap, target_ema) if entry < min(target_vwap, target_ema) else max(target_vwap, target_ema)
                
            else:  # short
                # After buying climax, look for shooting star/doji
                reversal_signal = self._check_bearish_reversal(bar, prev_bar)
                if not reversal_signal:
                    return None
                
                entry = bar["close"]
                stop = bar["high"] * 1.005  # Just above climax high
                
                # Target: VWAP or EMA20 (whichever is closer)
                target_vwap = vwap
                target_ema = ema20
                target = max(target_vwap, target_ema) if entry > max(target_vwap, target_ema) else min(target_vwap, target_ema)
            
            risk_per_share = abs(entry - stop)
            if risk_per_share <= 0:
                return None
            
            # Targets
            tp1 = target
            if direction == "long":
                tp2 = entry + (2 * risk_per_share)
                tp3 = entry + (3 * risk_per_share)
            else:
                tp2 = entry - (2 * risk_per_share)
                tp3 = entry - (3 * risk_per_share)
            
            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)
            
            # Calculate R-multiples
            if direction == "long":
                r1 = (tp1 - entry) / risk_per_share
                r2 = (tp2 - entry) / risk_per_share
                r3 = (tp3 - entry) / risk_per_share
            else:
                r1 = (entry - tp1) / risk_per_share
                r2 = (entry - tp2) / risk_per_share
                r3 = (entry - tp3) / risk_per_share
            
            signal = {
                "timestamp_utc": bar["timestamp"].isoformat() + "Z",
                "symbol": ticker,
                "direction": direction,
                "setup_name": "exhaustion_reversal",
                "score": min(10.0, 5.0 + vol_ratio + range_ratio),
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": r1,
                "r_multiple_at_tp2": r2,
                "r_multiple_at_tp3": r3,
                "notes": f"Exhaustion {direction} after climax",
                "climax_range_pct": range_ratio,
                "climax_vol_ratio": vol_ratio,
                "distance_from_vwap": distance_from_vwap,
                "vwap_price": vwap,
                "position_size": position_size,
            }
            
            return signal
            
        except Exception as e:
            logger.error(f"Error checking exhaustion setup: {e}")
            return None

    def _check_bullish_reversal(self, bar: pd.Series, prev_bar: pd.Series) -> bool:
        """Check for bullish reversal pattern."""
        # Hammer or doji after decline
        body_size = abs(bar["close"] - bar["open"])
        lower_wick = bar["open"] if bar["close"] > bar["open"] else bar["close"]
        lower_wick_size = lower_wick - bar["low"]
        
        candle_range = bar["high"] - bar["low"]
        
        # Hammer: small body, long lower wick
        if candle_range > 0:
            body_ratio = body_size / candle_range
            wick_ratio = lower_wick_size / candle_range
            
            return body_ratio < 0.3 and wick_ratio > 0.5
        
        return False

    def _check_bearish_reversal(self, bar: pd.Series, prev_bar: pd.Series) -> bool:
        """Check for bearish reversal pattern."""
        # Shooting star or doji after advance
        body_size = abs(bar["close"] - bar["open"])
        upper_wick = bar["close"] if bar["close"] < bar["open"] else bar["open"]
        upper_wick_size = bar["high"] - upper_wick
        
        candle_range = bar["high"] - bar["low"]
        
        # Shooting star: small body, long upper wick
        if candle_range > 0:
            body_ratio = body_size / candle_range
            wick_ratio = upper_wick_size / candle_range
            
            return body_ratio < 0.3 and wick_ratio > 0.5
        
        return False

    def save_signals(self) -> bool:
        """Save exhaustion reversal signals to Spaces."""
        try:
            if not self.signals:
                return True
            
            df = pd.DataFrame(self.signals)
            df["generated_at"] = datetime.utcnow().isoformat() + "Z"
            
            signals_key = config.get_spaces_path("data", "signals", "exhaustion_reversal.csv")
            success = spaces_io.upload_dataframe(df, signals_key)
            
            if success:
                logger.info(f"Saved {len(df)} exhaustion reversal signals")
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving exhaustion reversal signals: {e}")
            return False


def main():
    """Main entry point for exhaustion reversal screener."""
    screener = ExhaustionReversalScreener()
    
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()
    
    logger.info(f"--- Running Exhaustion Reversal Screener --- {deployment_info}")
    
    success = screener.run_exhaustion_screen()
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)