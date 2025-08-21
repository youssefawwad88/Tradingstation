"""
AVWAP Reclaim Strategy Screener - Brian Shannon's anchor reclaim methodology.

This module implements AVWAP reclaim setups based on validated anchors
and reclaim quality for trend continuation trades.
"""

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from utils.config import config
from utils.logging_setup import get_logger
from utils.spaces_io import spaces_io
from utils.time_utils import get_market_time, is_in_session_window, utc_now

logger = get_logger(__name__)


class AVWAPReclaimScreener:
    """AVWAP Reclaim strategy screener with Brian Shannon's methodology."""

    def __init__(self) -> None:
        """Initialize the AVWAP Reclaim screener."""
        self.universe_tickers: List[str] = []
        self.signals: List[Dict] = []
        self.anchors: Dict[str, List[Dict]] = {}
        self.load_universe()
        self.load_anchors()

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
                logger.info(f"Loaded {len(active_tickers)} tickers for AVWAP screening")
            else:
                self.universe_tickers = config.FALLBACK_TICKERS
                logger.warning("Universe not found, using fallback tickers")
                
        except Exception as e:
            logger.error(f"Error loading universe: {e}")
            self.universe_tickers = config.FALLBACK_TICKERS

    def load_anchors(self) -> None:
        """Load AVWAP anchors from Spaces."""
        try:
            anchors_key = config.get_spaces_path("data", "signals", "avwap_anchors.csv")
            df = spaces_io.download_dataframe(anchors_key)
            
            if df is not None and not df.empty:
                # Group by ticker
                for ticker in df["symbol"].unique():
                    ticker_anchors = df[df["symbol"] == ticker].to_dict("records")
                    self.anchors[ticker] = ticker_anchors
                
                logger.info(f"Loaded anchors for {len(self.anchors)} tickers")
            else:
                logger.warning("No AVWAP anchors found")
                self.anchors = {}
                
        except Exception as e:
            logger.error(f"Error loading anchors: {e}")
            self.anchors = {}

    def run_avwap_screen(self) -> bool:
        """Run the AVWAP reclaim screening process."""
        logger.job_start("AVWAPReclaimScreener.run_avwap_screen")
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
                        logger.debug(f"Found {len(signals)} AVWAP signals for {ticker}")
                
                except Exception as e:
                    logger.error(f"Error screening {ticker}: {e}")
            
            self.save_signals()
            
            duration = time.time() - start_time
            logger.job_complete(
                "AVWAPReclaimScreener.run_avwap_screen",
                duration_seconds=duration,
                success=True,
                successful_tickers=successful_tickers,
                total_signals=len(self.signals),
            )
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            logger.job_complete(
                "AVWAPReclaimScreener.run_avwap_screen",
                duration_seconds=duration,
                success=False,
                error=str(e),
            )
            return False

    def screen_ticker(self, ticker: str) -> List[Dict]:
        """Screen a ticker for AVWAP reclaim setups."""
        try:
            # Check if we have anchors for this ticker
            if ticker not in self.anchors:
                return []
            
            # Load 30-minute data
            intraday_key = config.get_spaces_path("data", "intraday", "30min", f"{ticker}.csv")
            df = spaces_io.download_dataframe(intraday_key)
            
            if df is None or df.empty:
                return []
            
            # Prepare data
            df = self._prepare_data(df)
            
            signals = []
            
            # Check each anchor for reclaim setups
            for anchor in self.anchors[ticker]:
                anchor_signals = self._check_anchor_reclaim(ticker, anchor, df)
                signals.extend(anchor_signals)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error screening {ticker}: {e}")
            return []

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare 30-minute data."""
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"] = df["timestamp"].dt.date
        return df

    def _check_anchor_reclaim(self, ticker: str, anchor: Dict, df: pd.DataFrame) -> List[Dict]:
        """Check for AVWAP reclaim setup from an anchor."""
        try:
            anchor_date = anchor["date"]
            if isinstance(anchor_date, str):
                anchor_date = datetime.strptime(anchor_date, "%Y-%m-%d").date()
            
            # Get data from anchor date onwards
            anchor_data = df[df["date"] >= anchor_date].copy()
            
            if anchor_data.empty:
                return []
            
            # Calculate AVWAP from anchor
            anchor_data = self._calculate_avwap(anchor_data, anchor)
            
            # Find recent reclaim opportunities
            recent_data = anchor_data.tail(20)  # Last 20 bars
            
            signals = []
            for idx, row in recent_data.iterrows():
                signal = self._check_reclaim_bar(ticker, anchor, row, anchor_data)
                if signal:
                    signals.append(signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error checking anchor reclaim: {e}")
            return []

    def _calculate_avwap(self, df: pd.DataFrame, anchor: Dict) -> pd.DataFrame:
        """Calculate AVWAP from anchor point."""
        df = df.copy()
        
        # Calculate typical price and VWAP components
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["pv"] = df["typical_price"] * df["volume"]
        
        # Calculate cumulative AVWAP
        df["cum_pv"] = df["pv"].cumsum()
        df["cum_vol"] = df["volume"].cumsum()
        df["avwap"] = df["cum_pv"] / df["cum_vol"]
        
        return df

    def _check_reclaim_bar(self, ticker: str, anchor: Dict, bar: pd.Series, df: pd.DataFrame) -> Optional[Dict]:
        """Check if a bar represents a valid AVWAP reclaim."""
        try:
            avwap = bar["avwap"]
            
            # Check for reclaim (close above AVWAP for long)
            if bar["close"] <= avwap:
                return None
            
            # Check reclaim quality
            avwap_distance = (bar["close"] - avwap) / avwap
            body_size = abs(bar["close"] - bar["open"])
            candle_range = bar["high"] - bar["low"]
            body_pct = body_size / candle_range if candle_range > 0 else 0
            
            # Quality thresholds
            min_quality = 0.6  # 60% body
            min_distance = 0.002  # 0.2% above AVWAP
            
            if body_pct < min_quality or avwap_distance < min_distance:
                return None
            
            # Calculate trade parameters
            entry = bar["close"]
            stop = avwap * 0.995  # Just below AVWAP
            
            risk_per_share = entry - stop
            if risk_per_share <= 0:
                return None
            
            # Targets: 2R and 3R
            tp1 = entry + (2 * risk_per_share)
            tp2 = entry + (3 * risk_per_share)
            tp3 = entry + (4 * risk_per_share)
            
            # Position sizing
            account_size = config.ACCOUNT_SIZE
            risk_pct = config.MAX_RISK_PER_TRADE_PCT / 100
            risk_amount = account_size * risk_pct
            position_size = int(risk_amount / risk_per_share)
            
            # Confidence score
            score = 5.0 + (body_pct * 3) + (min(avwap_distance * 1000, 2))
            
            signal = {
                "timestamp_utc": bar["timestamp"].isoformat() + "Z",
                "symbol": ticker,
                "direction": "long",
                "setup_name": "avwap_reclaim",
                "score": min(10.0, score),
                "entry": entry,
                "stop": stop,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "r_multiple_at_tp1": 2.0,
                "r_multiple_at_tp2": 3.0,
                "r_multiple_at_tp3": 4.0,
                "notes": f"AVWAP reclaim from {anchor['type']} anchor",
                
                # AVWAP specific fields
                "anchor_date": str(anchor["date"]),
                "anchor_type": anchor["type"],
                "avwap_price": avwap,
                "avwap_distance": avwap_distance,
                "reclaim_quality": body_pct,
                "position_size": position_size,
            }
            
            return signal
            
        except Exception as e:
            logger.error(f"Error checking reclaim bar: {e}")
            return None

    def save_signals(self) -> bool:
        """Save AVWAP reclaim signals to Spaces."""
        try:
            if not self.signals:
                logger.info("No AVWAP reclaim signals to save")
                return True
            
            df = pd.DataFrame(self.signals)
            df["generated_at"] = utc_now().isoformat()
            df["deployment"] = config.DEPLOYMENT_TAG or "unknown"
            
            signals_key = config.get_spaces_path("data", "signals", "avwap_reclaim.csv")
            
            metadata = {
                "strategy": "avwap_reclaim",
                "total_signals": str(len(df)),
                "generation_date": utc_now().isoformat(),
            }
            
            success = spaces_io.upload_dataframe(df, signals_key, metadata=metadata)
            
            if success:
                logger.info(f"Saved {len(df)} AVWAP reclaim signals")
                for signal in self.signals:
                    logger.trade_signal(signal)
            
            return success
            
        except Exception as e:
            logger.error(f"Error saving AVWAP reclaim signals: {e}")
            return False


def main():
    """Main entry point for AVWAP reclaim screener."""
    screener = AVWAPReclaimScreener()
    
    from utils.config import get_deployment_info
    deployment_info = get_deployment_info()
    
    logger.info(f"--- Running AVWAP Reclaim Screener --- {deployment_info}")
    
    success = screener.run_avwap_screen()
    return success


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)