import streamlit as st
import pandas as pd
import os
import sys

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from utils.helpers import read_df_from_s3, list_files_in_s3_dir
except ImportError:
    st.error("Fatal Error: Could not import helper functions. The app cannot function without them.")
    st.stop()

# --- Page Configuration ---
st.set_page_config(page_title="Strategy Tabs", layout="wide")

# --- Load Custom CSS from the main app ---
try:
    from streamlit_app import load_css
    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")

# --- Main Page ---
st.title("🎯 Individual Strategy Outputs")
st.write("This section shows the raw, unfiltered output for each screener job. Each tab represents a specific signal file found in your cloud storage.")
st.markdown("---")

# --- Load Data and Create Tabs ---
@st.cache_data(ttl=300) # Cache the list of files for 5 minutes
def get_screener_files():
    """Gets the list of available screener signal files from S3."""
    signals_dir = 'data/signals/'
    return list_files_in_s3_dir(signals_dir)

signal_files = get_screener_files()

if not signal_files:
    st.info("No screener signal files were found in the `data/signals/` directory in your cloud storage.")
    st.write("Once your screener jobs run successfully, their results will appear here automatically.")
else:
    # Create clean names for tabs, e.g., "gapgo_signals.csv" -> "Gapgo"
    tab_names = [s.replace('_signals.csv', '').replace('_', ' ').capitalize() for s in signal_files]
    
    # Create the tabs
    screener_tabs = st.tabs(tab_names)

    # Populate each tab with its corresponding data
    for i, tab in enumerate(screener_tabs):
        with tab:
            file_name = signal_files[i]
            file_path = f"data/signals/{file_name}"
            
            # Use a unique key for each button to prevent state issues
            if st.button("🔄 Refresh", key=f"refresh_{file_name}"):
                # Clear the cache for this specific function to force a reload
                st.cache_data.clear()
                st.rerun()

            screener_df = read_df_from_s3(file_path)
            
            if not screener_df.empty:
                st.dataframe(screener_df, use_container_width=True, hide_index=True)
            else:
                st.info(f"The {tab_names[i]} screener has not generated any signals yet, or the signal file is empty.")

