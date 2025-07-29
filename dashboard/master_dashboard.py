"""
Master Dashboard

This script consolidates all valid signals from the individual screeners,
applies a specific trade plan (Entry, Stop, Targets) for each strategy,
and generates the final, actionable 'trade_signals.csv' and 
'execution_tracker.csv' outputs.
"""

import pandas as pd
import sys
import os
from datetime import datetime
import pytz
import numpy as np

# --- System Path Setup ---
PROJECT_ROOT = '/content/drive/MyDrive/trading-system'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils import config, helpers

# --- Trade Plan Logic ---

def calculate_execution_zone(current_price, entry_price):
    """Calculates the execution zone based on price proximity to entry."""
    if not all(helpers.is_valid_number(v) for v in [current_price, entry_price]) or entry_price == 0:
        return "N/A"
    
    distance_pct = abs(current_price - entry_price) / entry_price * 100
    if distance_pct <= 1.5:
        return "Optimal Zone"
    elif 1.5 < distance_pct <= 5.0:
        return "Chased"
    else:
        # A pullback to the entry level could be considered a reclaim entry.
        # This logic can be enhanced if intraday price action is analyzed.
        return "Far"

def calculate_gapgo_trade_plan(row: pd.Series, intraday_df: pd.DataFrame) -> dict:
    """Calculates trade plan for a Gap & Go setup."""
    plan = {'Trade Type': 'Intraday', 'Holding Days': '0-1'}
    # The breakout time is now in the 'Breakout Time Valid?' column from the screener
    breakout_time_str = row.get('Breakout Time Valid?')
    if not breakout_time_str or breakout_time_str == 'N/A': return {}

    breakout_datetime = pd.to_datetime(row['Date'] + ' ' + breakout_time_str)
    
    # Find the specific breakout candle in the intraday data
    breakout_candle = intraday_df.loc[breakout_datetime]
    
    if row['Direction'] == 'Long':
        plan['Entry'] = breakout_candle[config.HIGH_COL]
        plan['Stop Loss'] = breakout_candle[config.LOW_COL]
    else: # Short
        plan['Entry'] = breakout_candle[config.LOW_COL]
        plan['Stop Loss'] = breakout_candle[config.HIGH_COL]
        
    return plan

def calculate_orb_trade_plan(row: pd.Series) -> dict:
    """Calculates trade plan for an ORB setup."""
    plan = {'Trade Type': 'Intraday', 'Holding Days': '0-1'}
    if row['Direction'] == 'Long':
        plan['Entry'] = row['Opening Range High']
        plan['Stop Loss'] = row['Opening Range Low']
    else: # Short
        plan['Entry'] = row['Opening Range Low']
        plan['Stop Loss'] = row['Opening Range High']
    return plan
    
def calculate_avwap_trade_plan(row: pd.Series) -> dict:
    """Calculates trade plan for an AVWAP Reclaim setup."""
    plan = {'Trade Type': 'Swing', 'Holding Days': '2-5'}
    plan['Entry'] = row['Current Price']
    
    avwap1 = row.get('AVWAP 1')
    avwap2 = row.get('AVWAP 2')
    
    if row['Direction'] == 'Long':
        # Stop loss is below the closest (highest) AVWAP line
        plan['Stop Loss'] = max(avwap1, avwap2) if helpers.is_valid_number(avwap2) else avwap1
    else: # Short
        # Stop loss is above the closest (lowest) AVWAP line
        plan['Stop Loss'] = min(avwap1, avwap2) if helpers.is_valid_number(avwap2) else avwap1
    return plan

def calculate_ema_pullback_trade_plan(row: pd.Series, daily_df: pd.DataFrame) -> dict:
    """Calculates trade plan for an EMA Pullback setup."""
    plan = {'Trade Type': 'Swing', 'Holding Days': '2-10'}
    signal_candle = daily_df.loc[pd.to_datetime(row['Date'])]
    
    plan['Entry'] = signal_candle[config.CLOSE_COL]
    if row['Direction'] == 'Long':
        plan['Stop Loss'] = signal_candle[config.LOW_COL]
    else: # Short
        plan['Stop Loss'] = signal_candle[config.HIGH_COL]
    return plan

def calculate_breakout_trade_plan(row: pd.Series, daily_df: pd.DataFrame) -> dict:
    """Calculates trade plan for a Breakout Squeeze setup."""
    plan = {'Trade Type': 'Swing', 'Holding Days': '1-10'}
    signal_candle = daily_df.loc[pd.to_datetime(row['Date'])]
    
    plan['Entry'] = signal_candle[config.HIGH_COL] if row['Direction'] == 'Long' else signal_candle[config.LOW_COL]
    if row['Direction'] == 'Long':
        plan['Stop Loss'] = signal_candle[config.LOW_COL]
    else: # Short
        plan['Stop Loss'] = signal_candle[config.HIGH_COL]
    return plan

def calculate_exhaustion_trade_plan(row: pd.Series, daily_df: pd.DataFrame) -> dict:
    """Calculates trade plan for an Exhaustion Reversal setup."""
    plan = {'Trade Type': 'Swing', 'Holding Days': '1-5'}
    signal_candle = daily_df.loc[pd.to_datetime(row['Date'])]

    plan['Entry'] = signal_candle[config.CLOSE_COL]
    if row['Direction'] == 'Long':
        plan['Stop Loss'] = signal_candle[config.LOW_COL]
    else: # Short
        plan['Stop Loss'] = signal_candle[config.HIGH_COL]
    return plan

# --- Main Dashboard Logic ---
def run_master_dashboard():
    """Main function to consolidate signals and generate trade plans."""
    print("\n--- Running Master Dashboard ---")
    
    all_valid_signals = []
    
    for screener_file in os.listdir(config.SIGNALS_DIR):
        if screener_file.endswith('_signals.csv') and 'opportunities' not in screener_file:
            try:
                strategy_name = screener_file.replace('_signals.csv', '').capitalize()
                print(f"  -> Processing signals for: {strategy_name}")
                
                file_path = config.SIGNALS_DIR / screener_file
                df = pd.read_csv(file_path)
                
                valid_df = df[df['Setup Valid?'] == 'TRUE'].copy()
                if valid_df.empty:
                    print(f"     - No valid setups found.")
                    continue
                
                valid_df['Strategy'] = strategy_name
                all_valid_signals.append(valid_df)
            except Exception as e:
                print(f"   - ERROR processing signal file {screener_file}: {e}")

    if not all_valid_signals:
        print("--- No valid signals found across all screeners. ---")
        return

    consolidated_df = pd.concat(all_valid_signals, ignore_index=True)
    trade_plans = []

    for index, row in consolidated_df.iterrows():
        try:
            plan = {}
            ticker = row['Ticker']
            strategy = row['Strategy']
            
            daily_df = pd.read_csv(config.DAILY_DIR / f"{ticker}_daily.csv", index_col=0, parse_dates=True)
            intraday_df = pd.read_csv(config.INTRADAY_1MIN_DIR / f"{ticker}_1min.csv", index_col=0, parse_dates=True)

            if strategy == 'Gapgo': plan = calculate_gapgo_trade_plan(row, intraday_df)
            elif strategy == 'Orb': plan = calculate_orb_trade_plan(row)
            elif strategy == 'Avwap': plan = calculate_avwap_trade_plan(row)
            elif strategy == 'Ema_pullback': plan = calculate_ema_pullback_trade_plan(row, daily_df)
            elif strategy == 'Breakout': plan = calculate_breakout_trade_plan(row, daily_df)
            elif strategy == 'Exhaustion': plan = calculate_exhaustion_trade_plan(row, daily_df)
            
            if plan.get('Entry') and plan.get('Stop Loss'):
                risk_per_share = abs(plan['Entry'] - plan['Stop Loss'])
                if risk_per_share > 0:
                    plan['Risk/Share'] = risk_per_share
                    if row['Direction'] == 'Long':
                        plan['Target 1 (2R)'] = plan['Entry'] + (risk_per_share * 2)
                        plan['Target 2 (3R)'] = plan['Entry'] + (risk_per_share * 3)
                    else: # Short
                        plan['Target 1 (2R)'] = plan['Entry'] - (risk_per_share * 2)
                        plan['Target 2 (3R)'] = plan['Entry'] - (risk_per_share * 3)
                    
                    current_price = intraday_df[config.CLOSE_COL].iloc[-1]
                    plan['Current Price'] = current_price
                    plan['Execution Zone'] = calculate_execution_zone(current_price, plan['Entry'])
                    plan['Tradeable?'] = 'TRUE' if plan['Execution Zone'] != 'Far' else 'FALSE'
                    
                    full_signal_data = {**row.to_dict(), **plan}
                    trade_plans.append(full_signal_data)
        except Exception as e:
            print(f"   - ERROR generating trade plan for {row['Ticker']} ({row['Strategy']}): {e}")

    if not trade_plans:
        print("--- No actionable trade plans could be generated. ---")
        return

    final_df = pd.DataFrame(trade_plans)
    
    trade_signals_cols = [
        "Date", "Ticker", "Strategy", "Direction", "Current Price", "Entry", 
        "Stop Loss", "Risk/Share", "Target 1 (2R)", "Target 2 (3R)", 
        "Execution Zone", "Tradeable?", "Trade Type", "Holding Days"
    ]
    execution_tracker_cols = ["Date", "Ticker", "Strategy", "Entry", "Stop Loss", "Target 1 (2R)", "Target 2 (3R)"]

    trade_signals_df = final_df.reindex(columns=trade_signals_cols).fillna("N/A")
    execution_tracker_df = final_df.reindex(columns=execution_tracker_cols).fillna("N/A")

    helpers.save_signal_to_csv(trade_signals_df, 'trade_signals')
    helpers.save_signal_to_csv(execution_tracker_df, 'execution_tracker')

if __name__ == "__main__":
    run_master_dashboard()
