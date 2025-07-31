import streamlit as st
import pandas as pd
from datetime import datetime
import os
import sys

# --- System Path Setup ---
# This ensures the app can find the 'utils' module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    # We will use the same helper function you defined in your script
    from utils.helpers import load_from_spaces
except ImportError:
    # A fallback for graceful error handling if the helper isn't found
    st.error("Could not import `load_from_spaces` from `utils.helpers`. Please ensure the file and function exist.")
    # Define a dummy function to prevent the app from crashing completely
    def load_from_spaces(path):
        return None
    
# --- Page Configuration ---
st.set_page_config(page_title="Scheduler Monitor", layout="wide")

# --- Load Custom CSS from the main app ---
# This is a small trick to apply the main CSS to sub-pages
try:
    from streamlit_app import load_css
    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")


def style_status(s):
    """
    Applies color styling to the 'status' column based on its value.
    - Green for 'Success'
    - Orange for 'Running' or 'Pending'
    - Red for 'Fail'
    """
    if s.lower() == 'success':
        return 'background-color: #2E7D32; color: white;'
    elif s.lower() == 'fail':
        return 'background-color: #C62828; color: white;'
    elif s.lower() in ['running', 'pending']:
        return 'background-color: #F9A825; color: black;'
    return ''

# --- Main Page Content ---
st.title("⏱️ Scheduler Monitor")
st.write("Live status of all backend data jobs and screeners. The table below automatically checks for the latest status log from the cloud.")

if st.button("🔄 Refresh Now"):
    st.rerun()

st.markdown("---")

# --- Status Table ---
LOG_FILE_PATH = "data/logs/scheduler_status.csv"
status_df = load_from_spaces(LOG_FILE_PATH)

if status_df is not None and not status_df.empty:
    st.write("### Job Status Overview")
    
    # Sort by timestamp to show the most recent jobs first
    status_df['last_run_timestamp'] = pd.to_datetime(status_df['last_run_timestamp'])
    status_df = status_df.sort_values(by='last_run_timestamp', ascending=False)
    
    # Apply styling
    styled_df = status_df.style.applymap(style_status, subset=['status'])
    
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
else:
    st.warning(f"Could not find the scheduler log file at `{LOG_FILE_PATH}`.")
    st.info("This is normal if the backend engine has not completed its first run yet. Once it runs, a status file will be generated and will appear here.")

