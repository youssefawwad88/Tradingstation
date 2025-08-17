import sys
import os
import pandas as pd
from tqdm import tqdm
import logging

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import save_df_to_s3, update_scheduler_status
from utils.data_storage import read_df_from_s3
from utils.spaces_manager import spaces_manager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_master_dashboard_consolidation():
    """
    Consolidates signals from all individual screener files into a single,
    master trade signals file for the dashboard to display.
    This script is designed to be robust and handle missing files gracefully.
    """
    logger.info("Starting Master Dashboard Signal Consolidation")
    
    signal_dir = 'data/signals/'
    
    logger.info(f"Scanning for signal files in '{signal_dir}'")
    try:
        signal_files = spaces_manager.list_objects(signal_dir)
        if not signal_files:
            logger.warning("No signal files found")
            return
    except Exception as e:
        logger.error(f"Could not list files in S3 directory '{signal_dir}': {e}")
        return

    logger.info(f"Found {len(signal_files)} signal files to process.")
    
    all_signals = []

    for file_name in tqdm(signal_files, desc="Consolidating Signals"):
        try:
            file_path = f"{signal_dir}{file_name}"
            signal_df = read_df_from_s3(file_path)

            if signal_df.empty:
                tqdm.write(f"Skipping empty signal file: {file_name}")
                continue
            
            # --- Standardize and add strategy name ---
            # Extract strategy name from filename (e.g., 'gapgo_signals.csv' -> 'Gap & Go')
            strategy_name = file_name.replace('_signals.csv', '').replace('_', ' ').title()
            signal_df['Strategy'] = strategy_name
            
            all_signals.append(signal_df)
            
        except Exception as e:
            tqdm.write(f"Error processing file {file_name}: {e}")

    if not all_signals:
        logger.info("No valid signals found after processing all files.")
        # Create an empty placeholder file so the dashboard doesn't error
        empty_df = pd.DataFrame(columns=['Ticker', 'Strategy', 'Direction', 'Entry', 'Stop', 'Valid?'])
        save_df_to_s3(empty_df, 'data/trade_signals.csv')
        return

    # Concatenate all dataframes into one
    master_df = pd.concat(all_signals, ignore_index=True)

    # --- Data Cleaning and Filtering ---
    # We only want to see signals that are marked as valid
    if 'Setup Valid?' in master_df.columns:
        master_df = master_df[master_df['Setup Valid?'] == True].copy()
    
    # Define a standard column order for the final output
    final_columns = [
        'Ticker', 'Strategy', 'Direction', 'Entry', 'Stop', 
        'Target 2R', 'Target 3R', 'Risk/Share', 'Close', 'Volume'
    ]
    
    # Ensure all required columns exist, adding them with None if they don't
    for col in final_columns:
        if col not in master_df.columns:
            master_df[col] = None
            
    # Reorder and select only the final columns
    master_df = master_df[final_columns]

    logger.info(f"\nConsolidation complete. Found {len(master_df)} valid trade plans.")
    
    # Save the final, consolidated file
    save_path = 'data/trade_signals.csv'
    save_df_to_s3(master_df, save_path)
    logger.info(f"Successfully saved master trade signals to '{save_path}'.")

    logger.info("--- Master Dashboard Signal Consolidation Finished ---")


if __name__ == "__main__":
    job_name = "master_dashboard"
    update_scheduler_status(job_name, "Running")
    try:
        run_master_dashboard_consolidation()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.info(error_message)
        update_scheduler_status(job_name, "Fail", str(e))
