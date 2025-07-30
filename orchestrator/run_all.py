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
        print(f"--- Running {full_path} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        result = subprocess.run([sys.executable, full_path], check=True, capture_output=True, text=True)
        print(f"Output for {script_path}:\n{result.stdout}")
        if result.stderr:
            print(f"Errors for {script_path}:\n{result.stderr}")
        print(f"--- Finished {script_path} ---")
    except subprocess.CalledProcessError as e:
        print(f"!!! Error running {script_path}: {e}\n!!! STDOUT: {e.stdout}\n!!! STDERR: {e.stderr}")
    except Exception as e:
        print(f"!!! An unexpected error occurred while running {script_path}: {e}")

def check_if_first_run():
    """Checks if a marker file exists in the S3 bucket to determine if this is the first run."""
    sp500_path = 'data/universe/sp500.csv'
    df = read_df_from_s3(sp500_path)
    if df.empty:
        print("First run detected. Seeding database...")
        return True
    else:
        print("Database already seeded. Skipping initial upload.")
        return False

def main():
    """Main function to schedule and run all trading system jobs and screeners."""
    print("--- Starting Master Orchestrator ---")

    if check_if_first_run():
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
    # To convert from ET to UTC, add 4 hours (EDT) or 5 hours (EST)
    # Example: 6:30 AM ET is 10:30 UTC
    
    # Pre-Market (ET: ~6:30 AM - 7:00 AM)
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

    print("--- All jobs scheduled. Waiting for scheduled tasks to run... ---")
    print(f"Current server time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
