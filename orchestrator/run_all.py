import schedule
import time
import subprocess
import sys
from datetime import datetime, time as dt_time
import os

# Adjust the path to include the parent directory (trading-system)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_df_from_s3, upload_initial_data_to_s3

def run_script(script_path):
    """Runs a Python script as a subprocess."""
    try:
        full_path = os.path.join('/workspace', script_path)
        print(f"--- Running {full_path} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
        result = subprocess.run([sys.executable, full_path], check=True, capture_output=True, text=True)
        print(f"Output for {script_path}:\n{result.stdout}")
        if result.stderr:
            print(f"Errors for {script_path}:\n{result.stderr}")
        print(f"--- Finished {script_path} ---")
    except subprocess.CalledProcessError as e:
        print(f"!!! Error running {script_path}: {e}\n!!! STDOUT: {e.stdout}\n!!! STDERR: {e.stderr}")
    except Exception as e:
        print(f"!!! An unexpected error occurred while running {script_path}: {e}")

def main():
    """Main function to schedule and run all trading system jobs and screeners."""
    print("--- Starting Master Orchestrator ---")

    # This function is conceptual for a first-time setup. 
    # In a long-running app, you'd manage this differently.
    # For now, we run it once on startup.
    upload_initial_data_to_s3()

    # --- Define Paths to Scripts ---
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

    # --- Schedule Tasks (Times are UTC on the server) ---
    # To convert from ET to UTC, add 4 hours (EDT)
    # Example: 9:30 AM ET is 13:30 UTC
    
    # Pre-Market (ET: ~6:30 AM)
    schedule.every().day.at("10:30").do(run_script, opportunity_finder_script)
    schedule.every().day.at("10:35").do(run_script, avwap_anchor_script)
    schedule.every().day.at("10:40").do(run_script, update_daily_data_script)

    # Market Hours Jobs
    schedule.every(1).minutes.do(run_script, update_intraday_script)
    schedule.every(1).minutes.do(run_script, gapgo_screener)
    schedule.every().day.at("13:40").do(run_script, orb_screener) # 9:40 AM ET
    schedule.every(15).minutes.do(run_script, avwap_screener)
    schedule.every(15).minutes.do(run_script, breakout_screener)
    schedule.every(15).minutes.do(run_script, ema_pullback_screener)
    schedule.every(15).minutes.do(run_script, exhaustion_screener)
    schedule.every(5).minutes.do(run_script, master_dashboard_script)

    print("--- All jobs scheduled. ---")
    
    # --- Main Loop ---
    heartbeat_counter = 0
    while True:
        schedule.run_pending()
        time.sleep(1)
        heartbeat_counter += 1
        # Print a heartbeat message every 60 seconds to show the app is alive
        if heartbeat_counter % 60 == 0:
            print(f"Heartbeat: Orchestrator is alive. Waiting for next job. Current UTC time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
