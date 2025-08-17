import logging
import os
import sys

import numpy as np
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- System Path Setup ---
# This makes sure the script can find the 'utils' directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.helpers import (
    format_to_two_decimal,
    read_df_from_s3,
    read_tickerlist_from_s3,
    save_df_to_s3,
)

# --- Screener-Specific Configuration ---
MIN_DROP_PCT = -7.0
MIN_GAP_DOWN_PCT = -1.5
VOLUME_SPIKE_RATIO = 1.3  # 130%
MIN_BODY_PCT = 65.0
MIN_RED_DAYS_IN_PREVIOUS_5 = 3


def calculate_atr(df, period=14):
    """Calculate Average True Range"""
    df = df.copy()
    df["prev_close"] = df["close"].shift(1)
    df["tr1"] = df["high"] - df["low"]
    df["tr2"] = abs(df["high"] - df["prev_close"])
    df["tr3"] = abs(df["low"] - df["prev_close"])
    df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
    return df["tr"].rolling(window=period).mean()


def run_exhaustion_screener():
    """
    Exhaustion Reversal Screener per specification:
    Daily reversal after an exhaustion move and large wick indicating capitulation and reversal intent.
    """
    logger.info("Running Exhaustion Reversal Screener")

    # --- 1. Load tickers ---
    tickers = read_tickerlist_from_s3("tickerlist.txt")
    if not tickers:
        logger.warning("Ticker list from cloud is empty")
        return

    all_results = []

    for ticker in tickers:
        try:
            # --- 2. Load Data and Calculate Indicators ---
            df = read_df_from_s3(f"data/daily/{ticker}_daily.csv")
            if df is None or len(df) < 21:  # Need at least 21 days for indicators
                continue

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp")

            # Calculate ATR for gauging large moves
            df["ATR_14"] = calculate_atr(df, 14)

            # Volume metrics (avoid lookahead)
            df["Avg_Vol_20D"] = df["volume"].shift(1).rolling(window=20).mean()
            df["Volume_vs_Avg_Pct"] = (df["volume"] / df["Avg_Vol_20D"]) * 100

            latest = df.iloc[-1]
            previous = df.iloc[-2] if len(df) >= 2 else None

            # --- 3. Candle body and wick calculations ---
            candle_range = latest["high"] - latest["low"]
            body_size = abs(latest["close"] - latest["open"])
            upper_wick = latest["high"] - max(latest["open"], latest["close"])
            lower_wick = min(latest["open"], latest["close"]) - latest["low"]

            body_percent = (body_size / candle_range * 100) if candle_range > 0 else 0
            upper_wick_percent = (
                (upper_wick / candle_range * 100) if candle_range > 0 else 0
            )
            lower_wick_percent = (
                (lower_wick / candle_range * 100) if candle_range > 0 else 0
            )

            # --- 4. Direction determination ---
            direction = "None"
            large_move_vs_atr = "No"
            reversal_into_range = "No"

            # Check for large move vs ATR
            if pd.notna(latest["ATR_14"]) and latest["ATR_14"] > 0:
                price_move = abs(latest["close"] - latest["open"])
                if price_move >= 1.5 * latest["ATR_14"]:
                    large_move_vs_atr = "Yes"

            # Long exhaustion candidate: Large down move, large lower wick, close back inside range
            if latest["close"] < latest["open"]:  # Down day
                if (
                    lower_wick_percent >= 150
                ):  # Lower wick >= 1.5x body (converted to percentage)
                    if (
                        previous is not None and latest["close"] > previous["low"]
                    ):  # Close back inside prior range
                        direction = "Long"
                        reversal_into_range = "Yes"

            # Short exhaustion candidate: Large up move, large upper wick, close back inside range
            elif latest["close"] > latest["open"]:  # Up day
                if (
                    upper_wick_percent >= 150
                ):  # Upper wick >= 1.5x body (converted to percentage)
                    if (
                        previous is not None and latest["close"] < previous["high"]
                    ):  # Close back inside prior range
                        direction = "Short"
                        reversal_into_range = "Yes"

            # --- 5. Validation conditions ---
            volume_condition = latest["Volume_vs_Avg_Pct"] >= 130

            setup_valid = False
            reasons = []

            if direction != "None":
                if not volume_condition:
                    reasons.append("Volume vs Avg % < 130")
                if large_move_vs_atr != "Yes":
                    reasons.append("Move not large vs ATR")
                if reversal_into_range != "Yes":
                    reasons.append("No reversal into range")

                setup_valid = len(reasons) == 0
            else:
                reasons.append("No valid exhaustion pattern")

            # --- 6. Populate result ---
            result = {
                "Date": (
                    latest["timestamp"].strftime("%Y-%m-%d")
                    if hasattr(latest["timestamp"], "strftime")
                    else str(latest["timestamp"])
                ),
                "Ticker": ticker,
                "Direction": direction,
                "Close": format_to_two_decimal(latest["close"]),
                "High": format_to_two_decimal(latest["high"]),
                "Low": format_to_two_decimal(latest["low"]),
                "Open": format_to_two_decimal(latest["open"]),
                "Volume": f"{int(latest['volume']):,}",
                "Avg 20D Volume": (
                    f"{int(latest['Avg_Vol_20D']):,}"
                    if pd.notna(latest["Avg_Vol_20D"])
                    else "N/A"
                ),
                "Volume vs Avg %": format_to_two_decimal(latest["Volume_vs_Avg_Pct"]),
                "Body % of Candle": format_to_two_decimal(body_percent),
                "Upper Wick %": format_to_two_decimal(upper_wick_percent),
                "Lower Wick %": format_to_two_decimal(lower_wick_percent),
                "Large Move vs ATR?": large_move_vs_atr,
                "Reversal Into Range?": reversal_into_range,
                "Setup Valid?": "TRUE" if setup_valid else "FALSE",
                "Notes": "; ".join(reasons) if reasons else "N/A",
            }
            all_results.append(result)

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")

    # --- 7. Final Processing & Save to Cloud ---
    if not all_results:
        logger.info("No Exhaustion Reversal signals were generated.")
        return

    final_df = pd.DataFrame(all_results)
    final_df.sort_values(
        by=["Setup Valid?", "Ticker"], ascending=[False, True], inplace=True
    )

    save_df_to_s3(final_df, "data/signals/exhaustion_signals.csv")
    logger.info("Exhaustion Reversal Screener finished. Results saved to cloud.")


if __name__ == "__main__":
    run_exhaustion_screener()
