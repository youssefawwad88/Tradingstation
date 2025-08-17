import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

# --- System Path Setup ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    from utils.data_storage import read_df_from_s3
except ImportError:
    st.error(
        "Fatal Error: Could not import helper functions. The app cannot function without them."
    )
    st.stop()

# --- Page Configuration ---
st.set_page_config(page_title="Performance Dashboard", layout="wide")

# --- Load Custom CSS from the main app ---
try:
    from streamlit_app import load_css

    load_css()
except ImportError:
    st.warning("Could not load custom CSS styles.")

# --- Constants ---
JOURNAL_FILE_PATH = "data/journal/trade_journal.csv"


# --- Helper Functions ---
@st.cache_data(ttl=60)
def load_performance_data():
    """Loads and processes the trade journal for performance analysis."""
    journal_df = read_df_from_s3(JOURNAL_FILE_PATH)
    if journal_df.empty:
        return pd.DataFrame()

    # Ensure timestamp is datetime and r_multiple is numeric
    journal_df["timestamp"] = pd.to_datetime(journal_df["timestamp"])
    journal_df["r_multiple"] = pd.to_numeric(journal_df["r_multiple"], errors="coerce")
    journal_df = journal_df.sort_values(by="timestamp").reset_index(drop=True)

    # Calculate equity curve
    journal_df["equity_curve"] = journal_df["r_multiple"].cumsum()

    return journal_df


# --- Main Page ---
st.title("📊 Performance Dashboard")
st.write(
    "A high-level overview of your trading performance, automatically updated from your journal."
)

# --- Load Data ---
perf_df = load_performance_data()

if perf_df.empty:
    st.info(
        "Your trade journal is empty. Log some trades to see your performance metrics here."
    )
    st.stop()

# --- Key Performance Indicators (KPIs) ---
st.markdown("---")
st.header("Key Performance Indicators")

total_trades = len(perf_df)
wins = perf_df[perf_df["outcome"] == "Win"]
losses = perf_df[perf_df["outcome"] == "Loss"]
total_r = perf_df["r_multiple"].sum()
win_rate = (len(wins) / total_trades) * 100 if total_trades > 0 else 0
avg_r = perf_df["r_multiple"].mean()

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric(label="Total Trades", value=total_trades)
kpi2.metric(label="Total R-Multiple", value=f"{total_r:.2f} R")
kpi3.metric(label="Win Rate", value=f"{win_rate:.2f}%")
kpi4.metric(label="Average R-Multiple", value=f"{avg_r:.2f} R")

# --- Visualizations ---
st.markdown("---")
st.header("Performance Charts")

# --- Equity Curve Chart ---
st.subheader("Equity Curve (R-Multiple)")
fig_equity = px.line(
    perf_df,
    x="timestamp",
    y="equity_curve",
    title="Cumulative R-Multiple Over Time",
    labels={"timestamp": "Date", "equity_curve": "Cumulative R"},
)
fig_equity.update_layout(template="plotly_dark")
st.plotly_chart(fig_equity, use_container_width=True)


# --- R-Multiple Distribution ---
st.subheader("R-Multiple Distribution per Trade")
fig_dist = px.bar(
    perf_df,
    x=perf_df.index,
    y="r_multiple",
    color="r_multiple",
    color_continuous_scale=px.colors.diverging.RdYlGn,
    color_continuous_midpoint=0,
    title="Profit/Loss per Trade in R-Multiples",
    labels={"x": "Trade Number", "r_multiple": "R-Multiple"},
)
fig_dist.update_layout(template="plotly_dark")
st.plotly_chart(fig_dist, use_container_width=True)
