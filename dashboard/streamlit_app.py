import streamlit as st
import sys
import os
import pandas as pd
from datetime import datetime

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_list_to_s3, read_df_from_s3

# --- Page Configuration ---
st.set_page_config(page_title="Trading System Dashboard", layout="wide")
st.html("<meta http-equiv='refresh' content='60'>")
st.title("Trading System Control Panel & Dashboard")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# --- Ticker List Management (in Sidebar) ---
with st.sidebar:
    st.header("Master Ticker List")
    
    try:
        current_tickers = read_tickerlist_from_s3('tickerlist.txt')
        current_tickers_str = "\n".join(current_tickers)
    except Exception as e:
        st.error(f"Could not load ticker list: {e}")
        current_tickers_str = ""

    st.write("Enter tickers for the day (one per line). This will overwrite the current list.")
    new_tickers_input = st.text_area("Tickers:", value=current_tickers_str, height=300, label_visibility="collapsed")

    if st.button("Update Master Ticker List", use_container_width=True):
        if new_tickers_input:
            new_tickers_list = [ticker.strip().upper() for ticker in new_tickers_input.split('\n') if ticker.strip()]
            
            # --- MODIFICATION ---
            # Now we check if the save was successful and show a detailed message
            save_successful = save_list_to_s3(new_tickers_list, 'tickerlist.txt')
            
            if save_successful:
                st.success(f"Successfully updated the ticker list with {len(new_tickers_list)} tickers!")
                # Use a little spinner to give time for the rerun to feel natural
                with st.spinner('Refreshing dashboard...'):
                    time.sleep(2)
                st.rerun()
            else:
                st.error("CRITICAL ERROR: Failed to save the new ticker list to cloud storage. Check the application logs for details.")
        else:
            st.warning("The ticker list cannot be empty.")

# --- Main Display Area ---
st.header("✅ Actionable Trade Plans")
trade_signals_df = read_df_from_s3('data/trade_signals.csv')

if not trade_signals_df.empty:
    st.dataframe(trade_signals_df, use_container_width=True, hide_index=True)
else:
    st.info("No valid trade plans found yet.")

st.divider()

st.header("🔍 Raw Screener Outputs")
screener_list = ['gapgo', 'orb', 'avwap', 'breakout', 'ema_pullback', 'exhaustion']
screener_tabs = st.tabs([s.capitalize() for s in screener_list])

for i, screener_name in enumerate(screener_list):
    with screener_tabs[i]:
        screener_df = read_df_from_s3(f'data/signals/{screener_name}_signals.csv')
        if not screener_df.empty:
            st.dataframe(screener_df, use_container_width=True, hide_index=True)
        else:
            st.info(f"The {screener_name} screener has not generated any signals yet.")
