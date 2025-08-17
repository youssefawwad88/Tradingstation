import os
import sys
from datetime import datetime

import pandas as pd
import streamlit as st

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    from utils.data_storage import read_df_from_s3
    from utils.helpers import list_files_in_s3_dir
except ImportError:
    st.error(
        "Fatal Error: Could not import helper functions. The app cannot function without them."
    )
    st.stop()

# --- Page Configuration ---
st.set_page_config(page_title="Master Screener Hub", layout="wide")

# --- Load Custom CSS from the main app ---
try:
    from streamlit_app import load_css

    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")


# --- Helper Functions ---
@st.cache_data(ttl=300)  # Cache data for 5 minutes
def load_all_signals():
    """
    Scans the S3 signals directory, loads all CSVs, and concatenates them
    into a single master DataFrame.
    """
    signals_dir = "data/signals/"
    signal_files = list_files_in_s3_dir(signals_dir)

    if not signal_files:
        return pd.DataFrame()

    all_signals_list = []
    for file_name in signal_files:
        df = read_df_from_s3(f"{signals_dir}{file_name}")
        if not df.empty:
            # Extract strategy name from filename, e.g., "gapgo_signals.csv" -> "Gap & Go"
            strategy_name = (
                file_name.replace("_signals.csv", "").replace("_", " ").title()
            )
            df["strategy"] = strategy_name
            all_signals_list.append(df)

    if not all_signals_list:
        return pd.DataFrame()

    master_df = pd.concat(all_signals_list, ignore_index=True)

    # Standardize column order for consistency
    cols_to_show = [
        "ticker",
        "strategy",
        "direction",
        "entry",
        "stop",
        "target_2r",
        "target_3r",
        "risk_per_share",
        "timestamp",
    ]
    # Add other columns if they exist, but ensure the core ones are first
    existing_cols = [col for col in cols_to_show if col in master_df.columns]
    other_cols = [col for col in master_df.columns if col not in existing_cols]
    master_df = master_df[existing_cols + other_cols]

    # Convert timestamp to datetime and sort
    if "timestamp" in master_df.columns:
        master_df["timestamp"] = pd.to_datetime(master_df["timestamp"])
        master_df = master_df.sort_values(by="timestamp", ascending=False)

    return master_df


def style_direction(direction):
    """Applies color styling to the 'direction' column."""
    direction = direction.lower()
    if direction == "long":
        return "background-color: #2E7D32; color: white;"
    elif direction == "short":
        return "background-color: #C62828; color: white;"
    return ""


# --- Main Page ---
st.title("📈 Master Screener Hub")
st.write(
    "All trading signals from all active screeners, consolidated in one view. Data is refreshed automatically."
)

# --- Load Data ---
master_signals_df = load_all_signals()

# --- Filters ---
if not master_signals_df.empty:
    st.sidebar.header("Screener Filters")

    # Strategy Filter
    unique_strategies = sorted(master_signals_df["strategy"].unique())
    selected_strategies = st.sidebar.multiselect(
        "Filter by Strategy:", options=unique_strategies, default=unique_strategies
    )

    # Direction Filter
    selected_direction = st.sidebar.radio(
        "Filter by Direction:",
        options=["All", "Long", "Short"],
        index=0,
        horizontal=True,
    )

    # Apply Filters
    filtered_df = master_signals_df.copy()
    if selected_strategies:
        filtered_df = filtered_df[filtered_df["strategy"].isin(selected_strategies)]
    if selected_direction != "All":
        filtered_df = filtered_df[
            filtered_df["direction"].str.lower() == selected_direction.lower()
        ]

else:
    filtered_df = pd.DataFrame()

# --- Display Table ---
st.markdown("---")
st.header("Consolidated Signals")

if not filtered_df.empty:
    st.dataframe(
        filtered_df.style.applymap(style_direction, subset=["direction"]),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(
        f"Displaying {len(filtered_df)} of {len(master_signals_df)} total signals."
    )
else:
    st.info(
        "No trading signals found. This could be because the market is closed, or no setups have met the criteria yet."
    )
    st.write(
        "Check the `Scheduler Monitor` to ensure the screener jobs are running successfully."
    )
