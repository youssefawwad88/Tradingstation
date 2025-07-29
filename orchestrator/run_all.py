import schedule
import time
import subprocess
import sys
from datetime import datetime
import os

# Adjust the path to include the parent directory (trading-system)
# This allows imports from utils, screeners, etc.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import upload_initial_data_to_s3, read_df_from_s3

def run_script(script_path):
    """Runs a Python script as a subprocess."""
    try:
        print(f"--- Running {script_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        # We pass the python executable path to ensure it uses the same environment
        result = subprocess.run([sys.executable, script_path], check=True, capture_output=True, text=True)
        print(f"Output for {script_path}:\n{result.stdout}")
        if result.stderr:
            print(f"Errors for {script_path}:\n{result.stderr}")
        print(f"--- Finished {script_path} ---")
    except subprocess.CalledProcessError as e:
        print(f"!!! Error running {script_path}: {e}")
        print(f"!!! STDOUT: {e.stdout}")
        print(f"!!! STDERR: {e.stderr}")
    except FileNotFoundError:
        print(f"!!! Script not found at {script_path}. Please check the path.")
    except Exception as e:
        print(f"!!! An unexpected error occurred while running {script_path}: {e}")

def check_if_first_run():
    """
    Checks if a marker file exists in the S3 bucket to determine if this is the first run.
    """
    # We check for a file that only the initial upload function creates.
    # If sp500.csv exists, it means the initial upload has been done.
    sp500_path = 'universe/sp500.csv'
    df = read_df_from_s3(sp500_path)
    if df.empty:
        print("First run detected. Seeding database...")
        return True
    else:
        print("Database already seeded. Skipping initial upload.")
        return False

def main():
    """
    Main function to schedule and run all trading system jobs and screeners.
    """
    print("--- Starting Master Orchestrator ---")

    # --- Initial Setup on First Run ---
    if check_if_first_run():
        upload_initial_data_to_s3()

    # --- Define Paths to Scripts ---
    # Note: These paths are relative to the root of the repository.
    opportunity_finder_script = 'ticker_selectors/opportunity_ticker_finder.py'
    avwap_anchor_script = 'jobs/find_avwap_anchors.py'
    update_daily_data_script = 'jobs/update_all_data.py'
    update_intraday_script = 'jobs/update_intraday_compact.py'
    
    gapgo_screener = 'screeners/gapgo.py'
    orb_screener = 'screeners/orb.py'
    avwap_screener = 'screeners/avwap.py'
    breakout_screener = 'screeners/breakout.py'
    ema_pullback_screener = 'screeners/ema_pullback.py'
    exhaustion_screener = 'screeners/exhaustion.py'

    master_dashboard_script = 'dashboard/master_dashboard.py'

    # --- Schedule Tasks ---
    # Note: All times are in the server's timezone (likely UTC on DigitalOcean).
    # You may need to adjust these for your target market time (e.g., ET).
    
    # Pre-Market
    schedule.every().day.at("09:00").do(run_script, opportunity_finder_script) # ~5:00 AM ET
    schedule.every().day.at("09:05").do(run_script, avwap_anchor_script)
    schedule.every().day.at("09:10").do(run_script, update_daily_data_script)

    # Market Hours - High Frequency
    schedule.every(1).minutes.until("15:00").do(run_script, update_intraday_script) # Run every minute until ~11:00 AM ET
    schedule.every(1).minutes.until("14:30").do(run_script, gapgo_screener) # Run every minute in first hour

    # Market Hours - Standard Frequency
    schedule.every(15).minutes.until("21:00").do(run_script, avwap_screener) # Run every 15 mins until market close
    schedule.every(15).minutes.until("21:00").do(run_script, breakout_screener)
    schedule.every(15).minutes.until("21:00").do(run_script, ema_pullback_screener)
    schedule.every(15).minutes.until("21:00").do(run_script, exhaustion_screener)
    
    schedule.every().day.at("13:40").do(run_script, orb_screener) # Run once at 9:40 AM ET

    # Dashboard Updates
    schedule.every(5).minutes.until("14:30").do(run_script, master_dashboard_script) # High frequency in first hour
    schedule.every(15).minutes.until("21:00").do(run_script, master_dashboard_script) # Standard frequency after

    print("--- All jobs scheduled. Waiting for scheduled tasks to run... ---")
    print(f"Current server time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
