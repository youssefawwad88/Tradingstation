import json
import os
import sys
import time

import pandas as pd
import streamlit as st

# --- System Path Setup ---
# This ensures the app can find the 'utils' module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    # Import unified ticker and config management functions
    from utils.helpers import (
        read_config_from_s3,
        read_df_from_s3,
        read_master_tickerlist,
        save_config_to_s3,
        save_list_to_s3,
    )
except ImportError:
    st.error(
        "Fatal Error: Could not import helper functions from `utils.helpers`. The app cannot function without them."
    )

# Create cached wrapper for data loading functions to improve performance
@st.cache_data(ttl=300)  # Cache for 5 minutes
def cached_read_df_from_s3(object_name: str) -> pd.DataFrame:
    """
    Cached wrapper for read_df_from_s3 to improve dashboard performance.
    
    Args:
        object_name: Object name/path in S3
        
    Returns:
        DataFrame if successful, empty DataFrame otherwise
    """
    return read_df_from_s3(object_name)
    st.stop()

# --- Page Configuration ---
st.set_page_config(page_title="System Settings", layout="wide")

# --- Load Custom CSS from the main app ---
try:
    from streamlit_app import load_css

    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")

# --- Load Configuration ---
CONFIG_FILE_PATH = "config.json"
DEFAULT_CONFIG = {
    "risk_per_trade": 100.0,
    "enable_shorts": True,
    "volume_spike_multiplier": 2.5,
    "avwap_anchor_keywords": "earnings,guidance,fda,approval,contract,acquisition",
}
config = read_config_from_s3(CONFIG_FILE_PATH)
if not config:
    st.info("No configuration file found in cloud storage. Using default settings.")
    config = DEFAULT_CONFIG

# --- Page Title ---
st.title("🔧 System Settings Panel")
st.write(
    "Manage the core configuration of your trading engine. All changes are saved to `master_tickerlist.csv` and `config.json` in the cloud."
)

# --- Ticker List & Engine Config in one form ---
with st.expander("Master Configuration", expanded=True):

    # --- Ticker List Management ---
    st.subheader("Master Ticker List")
    st.write(
        "Manage the `master_tickerlist.csv` file. This is the **single source of truth** for all data fetching jobs and screeners."
    )
    try:
        current_tickers = read_master_tickerlist()
        current_tickers_str = "\n".join(current_tickers)
    except Exception as e:
        st.error(f"Could not load ticker list from master_tickerlist.csv: {e}")
        current_tickers = []
        current_tickers_str = "Error loading ticker list."

    new_tickers_input = st.text_area(
        "Ticker List (one per line):",
        value=current_tickers_str,
        height=250,
        key="ticker_list_input",
    )

    # --- Quick Add New Ticker ---
    st.markdown("##### ➕ Quick Add New Ticker")
    col_add1, col_add2 = st.columns([3, 1])
    
    with col_add1:
        new_ticker = st.text_input(
            "Add a single ticker:",
            placeholder="e.g., AAPL",
            key="new_ticker_input",
            help="Enter a stock ticker symbol to add to the master list"
        )
    
    with col_add2:
        st.write("")  # Add some spacing
        if st.button("💾 Add Ticker", type="secondary", use_container_width=True):
            if new_ticker and new_ticker.strip():
                ticker_upper = new_ticker.strip().upper()
                if ticker_upper not in current_tickers:
                    # Add ticker to the list
                    updated_tickers = current_tickers + [ticker_upper]
                    success = save_list_to_s3(updated_tickers, "master_tickerlist.csv")
                    if success:
                        st.success(f"✅ Successfully added **{ticker_upper}** to the master list!")
                        with st.spinner("Refreshing..."):
                            time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"❌ Failed to add {ticker_upper}. Check the application logs.")
                else:
                    st.warning(f"⚠️ **{ticker_upper}** is already in the master list.")
            else:
                st.warning("⚠️ Please enter a valid ticker symbol.")

    # --- Remove Tickers ---
    st.markdown("##### ➖ Remove Selected Tickers")
    
    if current_tickers:
        col_remove1, col_remove2 = st.columns([3, 1])
        
        with col_remove1:
            tickers_to_remove = st.multiselect(
                "Select tickers to remove:",
                options=current_tickers,
                placeholder="Choose one or more tickers to remove...",
                help="Select multiple tickers to remove them from the master list"
            )
        
        with col_remove2:
            st.write("")  # Add some spacing
            if st.button("🗑️ Remove Selected", type="secondary", use_container_width=True):
                if tickers_to_remove:
                    # Remove selected tickers from the list
                    updated_tickers = [ticker for ticker in current_tickers if ticker not in tickers_to_remove]
                    success = save_list_to_s3(updated_tickers, "master_tickerlist.csv")
                    if success:
                        removed_count = len(tickers_to_remove)
                        if removed_count == 1:
                            st.success(f"✅ Successfully removed **{tickers_to_remove[0]}** from the master list!")
                        else:
                            st.success(f"✅ Successfully removed **{removed_count}** tickers from the master list!")
                        with st.spinner("Refreshing..."):
                            time.sleep(1)
                        st.rerun()
                    else:
                        st.error("❌ Failed to remove tickers. Check the application logs.")
                else:
                    st.warning("⚠️ Please select at least one ticker to remove.")
    else:
        st.info("ℹ️ No tickers available to remove. Add some tickers first.")

    st.divider()

    # --- Engine Configuration ---
    st.subheader("⚙️ Engine & Strategy Configuration")
    col1, col2 = st.columns(2)

    with col1:
        st.write("#### Risk & Execution")
        st.number_input(
            "Risk per Trade ($)",
            min_value=1.0,
            value=float(config.get("risk_per_trade", 100.0)),
            step=10.0,
            key="risk_per_trade",
        )
        st.toggle(
            "Enable Short Setups",
            value=config.get("enable_shorts", True),
            key="enable_shorts",
        )

    with col2:
        st.write("#### Volume & Anchors")
        st.number_input(
            "Minimum Volume Spike (Multiplier)",
            min_value=1.0,
            value=float(config.get("volume_spike_multiplier", 2.5)),
            step=0.5,
            key="volume_spike_multiplier",
        )
        st.text_input(
            "AVWAP Anchor Keywords (comma-separated)",
            value=config.get("avwap_anchor_keywords", ""),
            key="avwap_anchor_keywords",
        )

    st.divider()

    # --- Save Button ---
    if st.button(
        "💾 Save Full Configuration", use_container_width=True, type="primary"
    ):
        # 1. Save the Ticker List to master_tickerlist.csv
        new_tickers_list = sorted(
            list(
                set(
                    [
                        t.strip().upper()
                        for t in st.session_state.ticker_list_input.split("\n")
                        if t.strip()
                    ]
                )
            )
        )
        list_saved = save_list_to_s3(new_tickers_list, "master_tickerlist.csv")

        # 2. Save the Engine Configuration
        new_config = {
            "risk_per_trade": st.session_state.risk_per_trade,
            "enable_shorts": st.session_state.enable_shorts,
            "volume_spike_multiplier": st.session_state.volume_spike_multiplier,
            "avwap_anchor_keywords": st.session_state.avwap_anchor_keywords,
        }
        config_saved = save_config_to_s3(new_config, CONFIG_FILE_PATH)

        if list_saved and config_saved:
            st.success(
                f"Successfully saved ticker list ({len(new_tickers_list)} tickers) and configuration!"
            )
            with st.spinner("Refreshing..."):
                time.sleep(1)
            st.rerun()
        else:
            st.error(
                "Failed to save one or more configurations. Check the application logs."
            )


# --- Raw Data Viewer ---
st.markdown("---")
with st.expander("🔬 Raw Data Viewer", expanded=False):
    st.write(
        "Select a ticker to view the latest raw daily and intraday data stored in the cloud."
    )

    if not current_tickers:
        st.warning(
            "Cannot display data viewer because the ticker list is empty or could not be loaded."
        )
    else:
        selected_ticker = st.selectbox(
            "Choose a ticker from the master list:",
            options=current_tickers,
            index=None,
            placeholder="Select a ticker...",
        )

        if selected_ticker:
            st.write(f"#### Displaying data for: **{selected_ticker}**")
            daily_file_path = f"data/daily/{selected_ticker}_daily.csv"
            intraday_file_path = f"data/intraday/{selected_ticker}_1min.csv"

            # Daily Data Section with enhanced error handling
            st.subheader("📈 Daily Data")
            with st.spinner(f"Loading daily data for {selected_ticker}..."):
                daily_df = cached_read_df_from_s3(daily_file_path)
                
            if daily_df is not None and not daily_df.empty:
                st.success(f"✅ Found daily data: {len(daily_df)} records")
                st.dataframe(daily_df, use_container_width=True, hide_index=True)
                
                # Show data summary
                if len(daily_df) > 0:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Records", len(daily_df))
                    with col2:
                        if 'Date' in daily_df.columns:
                            latest_date = pd.to_datetime(daily_df['Date']).max()
                            st.metric("Latest Date", latest_date.strftime('%Y-%m-%d'))
                    with col3:
                        if 'Close' in daily_df.columns:
                            latest_close = daily_df['Close'].iloc[-1] if len(daily_df) > 0 else "N/A"
                            st.metric("Latest Close", f"${latest_close:.2f}" if isinstance(latest_close, (int, float)) else latest_close)
            else:
                st.warning(f"⚠️ No daily data found for **{selected_ticker}** at `{daily_file_path}`")
                st.info("💡 Daily data is typically generated by running `fetch_daily.py` or the data fetching jobs.")

            st.divider()

            # Intraday Data Section with enhanced error handling  
            st.subheader("⚡ Intraday (1-min) Data")
            with st.spinner(f"Loading intraday data for {selected_ticker}..."):
                intraday_df = cached_read_df_from_s3(intraday_file_path)
                
            if intraday_df is not None and not intraday_df.empty:
                st.success(f"✅ Found intraday data: {len(intraday_df)} records") 
                st.dataframe(intraday_df, use_container_width=True, hide_index=True)
                
                # Show data summary
                if len(intraday_df) > 0:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Records", len(intraday_df))
                    with col2:
                        if 'datetime' in intraday_df.columns:
                            latest_time = pd.to_datetime(intraday_df['datetime']).max()
                            st.metric("Latest Time", latest_time.strftime('%Y-%m-%d %H:%M'))
                        elif 'Date' in intraday_df.columns:
                            latest_time = pd.to_datetime(intraday_df['Date']).max()
                            st.metric("Latest Time", latest_time.strftime('%Y-%m-%d %H:%M'))
                    with col3:
                        if 'close' in intraday_df.columns:
                            latest_close = intraday_df['close'].iloc[-1] if len(intraday_df) > 0 else "N/A"
                            st.metric("Latest Close", f"${latest_close:.2f}" if isinstance(latest_close, (int, float)) else latest_close)
                        elif 'Close' in intraday_df.columns:
                            latest_close = intraday_df['Close'].iloc[-1] if len(intraday_df) > 0 else "N/A"
                            st.metric("Latest Close", f"${latest_close:.2f}" if isinstance(latest_close, (int, float)) else latest_close)
            else:
                st.warning(f"⚠️ No intraday data found for **{selected_ticker}** at `{intraday_file_path}`")
                st.info("💡 Intraday data is typically generated by running `fetch_intraday_compact.py` or the data fetching jobs.")
