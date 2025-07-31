import streamlit as st
import pandas as pd
import os
import sys
from datetime import datetime

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

try:
    from utils.helpers import read_df_from_s3, save_df_to_s3, read_tickerlist_from_s3
except ImportError:
    st.error("Fatal Error: Could not import helper functions. The app cannot function without them.")
    st.stop()

# --- Page Configuration ---
st.set_page_config(page_title="Trade Journal", layout="wide")

# --- Load Custom CSS from the main app ---
try:
    from streamlit_app import load_css
    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")

# --- Constants ---
JOURNAL_FILE_PATH = 'data/journal/trade_journal.csv'

# --- Helper Functions ---
@st.cache_data(ttl=60)
def load_journal():
    """Loads the trade journal from S3."""
    journal_df = read_df_from_s3(JOURNAL_FILE_PATH)
    if 'timestamp' in journal_df.columns:
        journal_df['timestamp'] = pd.to_datetime(journal_df['timestamp'])
    return journal_df

# --- Main Page ---
st.title("📋 Trade Journal")
st.write("Log your executions, add notes, and track outcomes. Discipline is the bridge between goals and accomplishment.")

# --- Load Data ---
journal_df = load_journal()
tickers = read_tickerlist_from_s3('tickerlist.txt')

# --- Trade Entry Form ---
with st.form("trade_entry_form", clear_on_submit=True):
    st.subheader("Log a New Trade")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        trade_ticker = st.selectbox("Ticker", options=tickers if tickers else ["Enter Ticker Manually"], index=None)
        trade_direction = st.radio("Direction", ["Long", "Short"], horizontal=True)
    with c2:
        trade_entry = st.number_input("Entry Price", step=0.01, format="%.2f")
        trade_exit = st.number_input("Exit Price", step=0.01, format="%.2f")
    with c3:
        trade_outcome = st.radio("Outcome", ["✅ Win", "❌ Loss", "⚪️ Break-Even"], horizontal=True)
        trade_r_multiple = st.number_input("R-Multiple", step=0.1, format="%.1f")

    trade_notes = st.text_area("Notes & Observations")
    
    submitted = st.form_submit_button("💾 Save Trade to Journal")
    
    if submitted:
        if not all([trade_ticker, trade_entry > 0, trade_exit > 0]):
            st.warning("Please fill in at least the Ticker, Entry Price, and Exit Price.")
        else:
            new_trade = pd.DataFrame([{
                "timestamp": datetime.now(),
                "ticker": trade_ticker,
                "direction": trade_direction,
                "entry": trade_entry,
                "exit": trade_exit,
                "outcome": trade_outcome.split(" ")[1], # Store just "Win", "Loss", "Break-Even"
                "r_multiple": trade_r_multiple,
                "notes": trade_notes
            }])
            
            updated_journal = pd.concat([journal_df, new_trade], ignore_index=True)
            
            if save_df_to_s3(updated_journal, JOURNAL_FILE_PATH):
                st.success("Trade successfully logged!")
                st.cache_data.clear() # Clear cache to show the new entry
            else:
                st.error("Failed to save trade. Check logs.")

# --- Journal Display ---
st.markdown("---")
st.header("Trade History")

if not journal_df.empty:
    # Sort by timestamp to show the most recent trades first
    journal_df = journal_df.sort_values(by='timestamp', ascending=False)
    st.dataframe(journal_df, use_container_width=True, hide_index=True)
else:
    st.info("Your trade journal is empty. Log your first trade using the form above to get started.")

