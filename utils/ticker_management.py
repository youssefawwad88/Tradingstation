"""
Ticker management utilities for Trading Station.
Handles manual ticker loading, merging with selector outputs, and persistence.
"""

import os
import logging
from pathlib import Path
from typing import List, Set, Optional
import pandas as pd
from .config import MANUAL_TICKER_SEARCH_PATHS, PROCESS_MANUAL_TICKERS, UNIVERSE_DIR
from .logging_setup import get_logger

logger = get_logger(__name__)

def load_manual_tickers() -> List[str]:
    """
    Load manual tickers from search paths.
    
    Returns:
        List of ticker symbols (uppercase, deduplicated)
    """
    if not PROCESS_MANUAL_TICKERS:
        logger.info("Manual ticker processing disabled (PROCESS_MANUAL_TICKERS=false)")
        return []
    
    all_tickers = set()
    files_found = []
    
    for search_path in MANUAL_TICKER_SEARCH_PATHS:
        path = Path(search_path)
        if path.exists() and path.is_file():
            try:
                with open(path, 'r') as f:
                    content = f.read().strip()
                    if content:
                        # Split by lines and clean up
                        tickers = [
                            ticker.strip().upper() 
                            for ticker in content.replace(',', '\n').split('\n')
                            if ticker.strip() and ticker.strip().upper().isalpha()
                        ]
                        all_tickers.update(tickers)
                        files_found.append(str(path))
                        logger.debug(f"Loaded {len(tickers)} tickers from {path}")
            except Exception as e:
                logger.warning(f"Failed to read manual tickers from {path}: {e}")
    
    ticker_list = sorted(list(all_tickers))
    
    if files_found:
        logger.info(
            f"Manual tickers loaded: {len(ticker_list)} from {len(files_found)} files. "
            f"Sources: {', '.join(files_found)}"
        )
        if ticker_list:
            sample = ticker_list[:5]
            logger.info(f"Sample manual tickers: {', '.join(sample)}")
    else:
        logger.error(
            f"No manual ticker files found in search paths: {MANUAL_TICKER_SEARCH_PATHS}. "
            "Proceeding with selector output only."
        )
    
    return ticker_list

def merge_ticker_lists(manual_tickers: List[str], selector_tickers: List[str]) -> List[str]:
    """
    Merge manual and selector ticker lists with deduplication.
    
    Args:
        manual_tickers: List of manual tickers
        selector_tickers: List of tickers from opportunity finder
        
    Returns:
        Merged and deduplicated list of tickers
    """
    # Combine and deduplicate
    all_tickers = set()
    
    # Add manual tickers (always included)
    for ticker in manual_tickers:
        if ticker and ticker.strip():
            all_tickers.add(ticker.strip().upper())
    
    # Add selector tickers
    for ticker in selector_tickers:
        if ticker and ticker.strip():
            all_tickers.add(ticker.strip().upper())
    
    merged_list = sorted(list(all_tickers))
    
    logger.info(
        f"Ticker merge complete: {len(manual_tickers)} manual + "
        f"{len(selector_tickers)} selector = {len(merged_list)} total unique"
    )
    
    return merged_list

def persist_master_tickerlist(tickers: List[str], output_path: Optional[str] = None) -> str:
    """
    Persist master ticker list to file.
    
    Args:
        tickers: List of ticker symbols
        output_path: Optional custom output path
        
    Returns:
        Path where the file was saved
    """
    if output_path is None:
        output_path = f"{UNIVERSE_DIR}/master_tickerlist.txt"
    
    # Ensure directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(output_path, 'w') as f:
            for ticker in tickers:
                f.write(f"{ticker}\n")
        
        logger.info(f"Master ticker list saved to {output_path}: {len(tickers)} symbols")
        
        # Also save as CSV for compatibility
        csv_path = output_path.replace('.txt', '.csv')
        df = pd.DataFrame({'symbol': tickers})
        df.to_csv(csv_path, index=False)
        logger.debug(f"Also saved as CSV: {csv_path}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Failed to save master ticker list to {output_path}: {e}")
        raise

def load_master_tickerlist(file_path: Optional[str] = None) -> List[str]:
    """
    Load master ticker list from file.
    
    Args:
        file_path: Optional custom file path
        
    Returns:
        List of ticker symbols
    """
    if file_path is None:
        file_path = f"{UNIVERSE_DIR}/master_tickerlist.txt"
    
    if not Path(file_path).exists():
        logger.warning(f"Master ticker list not found at {file_path}")
        return []
    
    try:
        with open(file_path, 'r') as f:
            tickers = [
                line.strip().upper() 
                for line in f 
                if line.strip() and line.strip().upper().isalpha()
            ]
        
        logger.info(f"Loaded {len(tickers)} tickers from master list: {file_path}")
        return tickers
        
    except Exception as e:
        logger.error(f"Failed to load master ticker list from {file_path}: {e}")
        return []

def validate_ticker_format(ticker: str) -> bool:
    """
    Validate ticker format.
    
    Args:
        ticker: Ticker symbol to validate
        
    Returns:
        True if valid format
    """
    if not ticker or not isinstance(ticker, str):
        return False
    
    ticker = ticker.strip().upper()
    
    # Basic validation: 1-5 characters, all letters
    if not (1 <= len(ticker) <= 5 and ticker.isalpha()):
        return False
    
    return True

def filter_valid_tickers(tickers: List[str]) -> List[str]:
    """
    Filter out invalid ticker symbols.
    
    Args:
        tickers: List of ticker symbols
        
    Returns:
        List of valid ticker symbols
    """
    valid_tickers = []
    invalid_count = 0
    
    for ticker in tickers:
        if validate_ticker_format(ticker):
            valid_tickers.append(ticker.strip().upper())
        else:
            invalid_count += 1
            logger.debug(f"Invalid ticker format filtered out: '{ticker}'")
    
    if invalid_count > 0:
        logger.warning(f"Filtered out {invalid_count} invalid ticker symbols")
    
    return valid_tickers

def get_ticker_stats(tickers: List[str]) -> dict:
    """
    Get statistics about the ticker list.
    
    Args:
        tickers: List of ticker symbols
        
    Returns:
        Dictionary with ticker statistics
    """
    if not tickers:
        return {
            'total_count': 0,
            'unique_count': 0,
            'duplicates': 0,
            'sample': []
        }
    
    unique_tickers = list(set(tickers))
    duplicates = len(tickers) - len(unique_tickers)
    sample = unique_tickers[:10]  # First 10 for display
    
    return {
        'total_count': len(tickers),
        'unique_count': len(unique_tickers),
        'duplicates': duplicates,
        'sample': sample
    }

def update_master_tickerlist(selector_output: List[str]) -> List[str]:
    """
    Complete ticker list update workflow.
    
    Args:
        selector_output: Tickers from opportunity selector
        
    Returns:
        Final merged ticker list
    """
    logger.info("Starting ticker list update workflow")
    
    # Load manual tickers
    manual_tickers = load_manual_tickers()
    
    # Filter valid tickers
    manual_tickers = filter_valid_tickers(manual_tickers)
    selector_output = filter_valid_tickers(selector_output)
    
    # Merge lists
    merged_tickers = merge_ticker_lists(manual_tickers, selector_output)
    
    # Persist master list
    persist_master_tickerlist(merged_tickers)
    
    # Log statistics
    stats = get_ticker_stats(merged_tickers)
    logger.info(
        f"Ticker list update complete: {stats['unique_count']} unique symbols. "
        f"Sample: {', '.join(stats['sample'][:5])}"
    )
    
    return merged_tickers

def echo_ticker_summary() -> None:
    """
    Echo ticker loading summary for orchestrator startup.
    """
    try:
        master_tickers = load_master_tickerlist()
        manual_tickers = load_manual_tickers()
        
        total_count = len(master_tickers)
        manual_count = len(manual_tickers)
        
        logger.info(
            f"Manual tickers ({manual_count}/{total_count}) loaded. "
            f"Sample: {', '.join(master_tickers[:5]) if master_tickers else 'None'}"
        )
        
    except Exception as e:
        logger.error(f"Failed to echo ticker summary: {e}")

# Export functions
__all__ = [
    'load_manual_tickers',
    'merge_ticker_lists', 
    'persist_master_tickerlist',
    'load_master_tickerlist',
    'validate_ticker_format',
    'filter_valid_tickers',
    'get_ticker_stats',
    'update_master_tickerlist',
    'echo_ticker_summary'
]