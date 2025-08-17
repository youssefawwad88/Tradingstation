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

            st.subheader("Daily Data")
            daily_df = read_df_from_s3(daily_file_path)
            if not daily_df.empty:
                st.dataframe(daily_df, use_container_width=True, hide_index=True)
            else:
                st.info(f"No daily data file found for **{selected_ticker}**.")

            st.subheader("Intraday (1-min) Data")
            intraday_df = read_df_from_s3(intraday_file_path)
            if not intraday_df.empty:
                st.dataframe(intraday_df, use_container_width=True, hide_index=True)
            else:
                st.info(f"No intraday data file found for **{selected_ticker}**.")
