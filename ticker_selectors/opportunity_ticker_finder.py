import sys
import os

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import update_scheduler_status

def run_opportunity_finder():
    """
    DEPRECATED: This function has been disabled per new requirements.
    Tickers now come only from manually maintained tickerlist.txt file.
    Automated ticker discovery via opportunity finder has been removed.
    """
    print("--- Opportunity Finder Job DISABLED ---")
    print("As per new requirements, this automated ticker source has been removed.")
    print("All tickers now come from manually maintained tickerlist.txt file.")
    print("Job completed (no operation performed).")
    return True

if __name__ == "__main__":
    job_name = "opportunity_ticker_finder"
    update_scheduler_status(job_name, "Running")
    try:
        run_opportunity_finder()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        update_scheduler_status(job_name, "Fail", str(e))
        # Re-raise the exception to ensure the script exits with an error code
        raise e
