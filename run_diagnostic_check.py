import logging
import sys

# Ensure the 'jobs' directory is in the Python path
# This might be needed depending on the project structure
sys.path.append('.')

from jobs import data_health_check, compact_update

def run_diagnostics():
    """
    Runs a sequential, standalone diagnostic test of the two main data engines.
    """
    # Configure a basic, guaranteed-to-work logger for this test
    logging.basicConfig(
        level=logging.INFO,
        format='[DIAGNOSTIC] %(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout  # Force logs to print to the console
    )

    logging.info("--- MASTER DIAGNOSTIC SCRIPT STARTING ---")

    # --- Step 1: Test the Data Health & Recovery Engine ---
    logging.info("\n>>> STEP 1: EXECUTING data_health_check.run_health_check()...\n")
    try:
        # Assuming the main function is named run_health_check
        data_health_check.run_health_check()
        logging.info("\n>>> STEP 1: FINISHED data_health_check.run_health_check().\n")
    except Exception as e:
        logging.error(f"Data Health Check failed with an error: {e}", exc_info=True)

    # --- Step 2: Test the Compact Update Engine ---
    logging.info("\n>>> STEP 2: EXECUTING compact_update.run_compact_update()...\n")
    try:
        # Assuming the main function is named run_compact_update
        # You may need to update this name if it's different in the actual file
        compact_update.run_compact_update()
        logging.info("\n>>> STEP 2: FINISHED compact_update.run_compact_update().\n")
    except Exception as e:
        logging.error(f"Compact Update failed with an error: {e}", exc_info=True)

    logging.info("--- MASTER DIAGNOSTIC SCRIPT COMPLETE ---")

if __name__ == "__main__":
    run_diagnostics()