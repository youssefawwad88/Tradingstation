import pandas as pd
import sys
import os

# Add project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import read_tickerlist_from_s3, save_df_to_s3, update_scheduler_status
from utils.data_storage import read_df_from_s3

def find_and_save_avwap_anchors():
    """
    Identifies significant candles to be used as AVWAP anchor points.
    - Reads daily data for each ticker.
    - Identifies candles with high volume and large range ("power candles").
    - Saves these anchor points to a single CSV file.
    """
    print("--- Starting Find AVWAP Anchors Job (Robust Version) ---")
    
    tickers = read_tickerlist_from_s3()
    if not tickers:
        print("No tickers found in tickerlist.txt. Exiting job.")
        return

    all_anchors = []

    for ticker in tickers:
        try:
            daily_df_path = f'data/daily/{ticker}_daily.csv'
            daily_df = read_df_from_s3(daily_df_path)

            if daily_df.empty or 'timestamp' not in daily_df.columns:
                print(f"No valid daily data for {ticker}, skipping anchor search.")
                continue

            daily_df['timestamp'] = pd.to_datetime(daily_df['timestamp'])
            daily_df.sort_values('timestamp', inplace=True)

            avg_volume = daily_df['volume'].rolling(window=20).mean()
            candle_range = daily_df['high'] - daily_df['low']
            avg_range = candle_range.rolling(window=20).mean()

            # Identify "power candles"
            power_candles = daily_df[
                (daily_df['volume'] > avg_volume * 1.5) &
                (candle_range > avg_range * 1.5)
            ].copy() # Use .copy() to avoid SettingWithCopyWarning

            if not power_candles.empty:
                for _, row in power_candles.iterrows():
                    all_anchors.append({
                        'ticker': ticker,
                        'anchor_date': row['timestamp'].strftime('%Y-%m-%d'),
                        'anchor_price': row['close'],
                        'reason': f"Power candle (Vol: {row['volume']:.0f}, Range: {candle_range.loc[row.name]:.2f})"
                    })
                print(f"Found {len(power_candles)} AVWAP anchor(s) for {ticker}.")

        except KeyError as e:
             print(f"ERROR finding anchors for {ticker}: A required column is missing - {e}")
        except Exception as e:
            print(f"An unexpected error occurred while finding anchors for {ticker}: {e}")

    if all_anchors:
        anchors_df = pd.DataFrame(all_anchors)
        save_path = 'data/avwap_anchors.csv'
        save_df_to_s3(anchors_df, save_path)
        print(f"\nSuccessfully saved a total of {len(anchors_df)} anchors to {save_path}.")
    else:
        print("\nNo new AVWAP anchors found across all tickers.")

    print("--- Find AVWAP Anchors Job Finished ---")

if __name__ == "__main__":
    job_name = "find_avwap_anchors"
    update_scheduler_status(job_name, "Running")
    try:
        find_and_save_avwap_anchors()
        update_scheduler_status(job_name, "Success")
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        update_scheduler_status(job_name, "Fail", error_message)
