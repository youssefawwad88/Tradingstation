"""
Trading Station Dashboard - Main Streamlit Application
Professional trading platform for intraday and swing strategies
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from utils.config import (
        SIGNALS_DIR, TIMEZONE, DEBUG_MODE, 
        get_strategy_config, validate_config
    )
    from utils.storage import get_storage
    from utils.logging_setup import get_logger
    from utils.time_utils import now_et
    from utils.ticker_management import load_master_tickerlist
    UTILS_AVAILABLE = True
except ImportError as e:
    st.warning(f"Some utilities not available: {e}")
    st.info("Running in basic mode. Some features may be limited.")
    UTILS_AVAILABLE = False
    
    # Fallback constants
    SIGNALS_DIR = "data/signals"
    TIMEZONE = "America/New_York"
    DEBUG_MODE = False
    
    # Fallback functions
    def get_storage():
        return None
    
    def get_logger(name):
        import logging
        return logging.getLogger(name)
    
    def now_et():
        return datetime.now()
    
    def load_master_tickerlist():
        return ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL"]  # Sample tickers
    
    def validate_config():
        return False

# Configure Streamlit page
st.set_page_config(
    page_title="Trading Station",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize logger
if UTILS_AVAILABLE:
    logger = get_logger(__name__)
else:
    import logging
    logger = logging.getLogger(__name__)

class TradingDashboard:
    """Main trading dashboard class."""
    
    def __init__(self):
        if UTILS_AVAILABLE:
            self.storage = get_storage()
        else:
            self.storage = None
        
    def load_signals(self, strategy: str = None) -> pd.DataFrame:
        """Load trading signals from storage."""
        if not UTILS_AVAILABLE or self.storage is None:
            # Return sample data for demo
            sample_data = {
                'ticker': ['AAPL', 'TSLA', 'NVDA'],
                'strategy': ['orb', 'gapgo', 'avwap'],
                'direction': ['long', 'short', 'long'],
                'entry': [150.0, 200.0, 400.0],
                'stop': [145.0, 210.0, 390.0],
                'confidence': [0.85, 0.72, 0.91],
                'setup_valid': [True, False, True]
            }
            return pd.DataFrame(sample_data)
        
        try:
            if strategy:
                signals_path = f"{SIGNALS_DIR}/{strategy}.csv"
                df = self.storage.read_df(signals_path)
                if df is not None:
                    return df
                else:
                    return pd.DataFrame()
            else:
                # Load all signals
                all_signals = []
                strategies = ['orb', 'gapgo', 'avwap', 'breakout', 'exhaustion', 'ema_pullback']
                
                for strat in strategies:
                    signals_path = f"{SIGNALS_DIR}/{strat}.csv"
                    df = self.storage.read_df(signals_path)
                    if df is not None and not df.empty:
                        df['strategy'] = strat
                        all_signals.append(df)
                
                if all_signals:
                    return pd.concat(all_signals, ignore_index=True)
                else:
                    return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error loading signals: {e}")
            return pd.DataFrame()
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status."""
        health = {
            'status': 'healthy',
            'last_update': now_et().strftime('%Y-%m-%d %H:%M:%S ET'),
            'api_status': 'unknown',
            'data_freshness': 'unknown',
            'signal_count': 0
        }
        
        try:
            # Check API configuration
            if UTILS_AVAILABLE:
                config_valid = validate_config()
                health['api_status'] = 'configured' if config_valid else 'missing_keys'
            else:
                health['api_status'] = 'utilities_unavailable'
            
            # Check signal freshness
            signals = self.load_signals()
            if not signals.empty:
                health['signal_count'] = len(signals)
                if 'as_of' in signals.columns:
                    try:
                        latest_signal = pd.to_datetime(signals['as_of']).max()
                        hours_old = (now_et() - latest_signal.tz_localize(None)).total_seconds() / 3600
                        health['data_freshness'] = f"{hours_old:.1f} hours ago"
                    except:
                        health['data_freshness'] = "demo_data"
                else:
                    health['data_freshness'] = "demo_data"
            
            # Check ticker list
            if UTILS_AVAILABLE:
                ticker_count = len(load_master_tickerlist())
            else:
                ticker_count = 5  # Sample ticker count
            health['ticker_count'] = ticker_count
            
        except Exception as e:
            health['status'] = 'error'
            health['error'] = str(e)
            logger.error(f"Health check failed: {e}")
        
        return health

def main():
    """Main dashboard application."""
    
    # Header
    st.title("🧠 Trading Station Dashboard")
    st.markdown("*Professional-grade trading platform for intraday and swing strategies*")
    
    # Initialize dashboard
    dashboard = TradingDashboard()
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["🏠 Overview", "📊 Active Signals", "📈 Strategy Performance", "🔧 System Health", "📚 Documentation"]
    )
    
    if page == "🏠 Overview":
        show_overview(dashboard)
    elif page == "📊 Active Signals":
        show_active_signals(dashboard)
    elif page == "📈 Strategy Performance":
        show_strategy_performance(dashboard)
    elif page == "🔧 System Health":
        show_system_health(dashboard)
    elif page == "📚 Documentation":
        show_documentation()

def show_overview(dashboard):
    """Show overview page."""
    st.header("System Overview")
    
    # Quick stats
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Load current signals
        signals = dashboard.load_signals()
        if UTILS_AVAILABLE:
            ticker_count = len(load_master_tickerlist())
        else:
            ticker_count = 5  # Sample count
        
        with col1:
            st.metric("Active Signals", len(signals))
        
        with col2:
            valid_signals = len(signals[signals.get('setup_valid', False)]) if not signals.empty else 0
            st.metric("Valid Setups", valid_signals)
        
        with col3:
            st.metric("Watched Tickers", ticker_count)
        
        with col4:
            market_status = "Open" if now_et().hour >= 9 and now_et().hour < 16 else "Closed"
            st.metric("Market Status", market_status)
        
        # Recent signals table
        if not signals.empty:
            st.subheader("Recent Signals")
            
            # Sort by confidence and timestamp
            if 'confidence' in signals.columns:
                recent_signals = signals.nlargest(10, 'confidence')
            else:
                recent_signals = signals.head(10)
            
            # Display table
            display_cols = ['ticker', 'strategy', 'direction', 'entry', 'stop', 'confidence']
            available_cols = [col for col in display_cols if col in recent_signals.columns]
            
            if available_cols:
                st.dataframe(
                    recent_signals[available_cols],
                    use_container_width=True
                )
            else:
                st.dataframe(recent_signals, use_container_width=True)
        else:
            st.info("No signals available. System may be initializing or markets may be closed.")
    
    except Exception as e:
        st.error(f"Error loading overview data: {e}")

def show_active_signals(dashboard):
    """Show active signals page."""
    st.header("Active Trading Signals")
    
    # Strategy filter
    strategies = st.multiselect(
        "Filter by strategy:",
        ["orb", "gapgo", "avwap", "breakout", "exhaustion", "ema_pullback"],
        default=[]
    )
    
    # Load signals
    signals = dashboard.load_signals()
    
    if not signals.empty:
        # Apply filters
        if strategies:
            signals = signals[signals.get('strategy', '').isin(strategies)]
        
        # Display signals
        if not signals.empty:
            st.subheader(f"Found {len(signals)} signals")
            
            # Enhanced table display
            if st.checkbox("Show detailed view"):
                st.dataframe(signals, use_container_width=True)
            else:
                # Simplified view
                display_cols = ['ticker', 'strategy', 'direction', 'entry', 'stop', 'confidence', 'setup_valid']
                available_cols = [col for col in display_cols if col in signals.columns]
                if available_cols:
                    st.dataframe(signals[available_cols], use_container_width=True)
                else:
                    st.dataframe(signals, use_container_width=True)
        else:
            st.info("No signals match the selected filters.")
    else:
        st.info("No signals available.")

def show_strategy_performance(dashboard):
    """Show strategy performance page."""
    st.header("Strategy Performance")
    
    signals = dashboard.load_signals()
    
    if not signals.empty and 'strategy' in signals.columns:
        # Strategy breakdown
        strategy_counts = signals['strategy'].value_counts()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Signals by Strategy")
            st.bar_chart(strategy_counts)
        
        with col2:
            st.subheader("Strategy Statistics")
            for strategy in strategy_counts.index:
                strategy_signals = signals[signals['strategy'] == strategy]
                valid_setups = len(strategy_signals[strategy_signals.get('setup_valid', False)])
                avg_confidence = strategy_signals.get('confidence', pd.Series([0])).mean()
                
                st.write(f"**{strategy.upper()}**")
                st.write(f"- Total signals: {len(strategy_signals)}")
                st.write(f"- Valid setups: {valid_setups}")
                st.write(f"- Avg confidence: {avg_confidence:.2f}")
                st.write("---")
    else:
        st.info("No performance data available.")

def show_system_health(dashboard):
    """Show system health page."""
    st.header("System Health & Status")
    
    health = dashboard.get_system_health()
    
    # Status indicator
    status_color = {
        'healthy': '🟢',
        'degraded': '🟡', 
        'error': '🔴'
    }
    
    st.subheader(f"{status_color.get(health['status'], '⚪')} System Status: {health['status'].upper()}")
    
    # Health metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Last Update", health['last_update'])
        st.metric("API Status", health['api_status'])
    
    with col2:
        st.metric("Data Freshness", health['data_freshness'])
        st.metric("Active Signals", health['signal_count'])
    
    # Additional health info
    if 'ticker_count' in health:
        st.metric("Monitored Tickers", health['ticker_count'])
    
    if 'error' in health:
        st.error(f"System Error: {health['error']}")
    
    # Environment info
    st.subheader("Environment Information")
    env_info = {
        "Debug Mode": DEBUG_MODE,
        "Timezone": TIMEZONE,
        "Current Time": now_et().strftime('%Y-%m-%d %H:%M:%S ET')
    }
    
    for key, value in env_info.items():
        st.write(f"**{key}**: {value}")

def show_documentation():
    """Show documentation page."""
    st.header("Trading Station Documentation")
    
    st.markdown("""
    ## 🎯 Trading Strategies
    
    ### Opening Range Breakout (ORB)
    - **Mentor**: Umar Ashraf
    - **Timeframe**: 1-minute
    - **Setup**: Break of 9:30-9:39 AM range with volume confirmation
    - **Entry**: Above/below opening range
    - **Stop**: Opposite end of opening range
    
    ### Gap & Go
    - **Mentor**: Umar Ashraf  
    - **Timeframe**: 1-minute
    - **Setup**: Pre-market gap + high breakout + VWAP reclaim
    - **Entry**: Break of pre-market high
    - **Stop**: Below VWAP or gap fill level
    
    ### AVWAP Reclaim
    - **Mentor**: Brian Shannon
    - **Timeframe**: 30-minute / Daily
    - **Setup**: Anchor VWAP reclaim with volume spike
    - **Entry**: Above AVWAP with momentum
    - **Stop**: Below AVWAP or anchor low
    
    ## 🔧 System Features
    
    - ✅ **Automated Data Pipeline**: Live daily + 1-min intraday data
    - ✅ **Modular Signal Engines**: Independent strategy scripts
    - ✅ **Risk-Aware Trade Plans**: Entry, Stop, 2R/3R targets
    - ✅ **Cloud-Native**: DigitalOcean Spaces integration
    - ✅ **Real-time Dashboard**: Live signal monitoring
    
    ## 📊 Data Sources
    
    - **Market Data**: Alpha Vantage API
    - **Storage**: DigitalOcean Spaces / AWS S3
    - **Universe**: S&P 500 + Manual ticker list
    
    ## 🚀 Deployment
    
    The system auto-deploys from the `main` branch to DigitalOcean App Platform.
    Development work happens in the `dev` branch.
    """)

if __name__ == "__main__":
    main()