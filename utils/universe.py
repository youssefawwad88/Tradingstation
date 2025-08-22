"""Universe loader for the trading system.

This module handles loading the universe (list of active symbols) from Spaces storage
with aggressive normalization, comprehensive instrumentation, and fallback handling.
"""

import logging
import re
from typing import List, Optional

import pandas as pd

from utils.config import config
from utils.paths import universe_key
from utils.spaces_io import spaces_io

logger = logging.getLogger(__name__)

# Global cache for universe data
_universe_cache: Optional[pd.DataFrame] = None
_universe_loaded = False


def load_universe() -> List[str]:
    """Load universe of active symbols from Spaces.
    
    Implements aggressive normalization and comprehensive instrumentation:
    - Parse all 5 tickers with no fallback to 3
    - Normalize: trim whitespace/BOM/CRLF, uppercase, dedupe
    - Accept symbol or ticker column; single column treated as tickers  
    - Regex allowlist: ^[A-Z.\\-]{1,10}$
    - Drop empty or commas-only lines
    - Case probe (only once) to avoid multi-replacements
    - Comprehensive logging per requirements
    
    Returns:
        List of valid ticker symbols
    """
    global _universe_cache, _universe_loaded
    
    if _universe_loaded and _universe_cache is not None:
        # Return cached valid symbols
        valid_symbols = _get_valid_symbols_from_cache()
        return valid_symbols
    
    try:
        # Download universe CSV from Spaces with instrumentation
        key = universe_key()
        logger.debug(f"Loading universe from s3_key={key}")
        
        # Get raw content for aggressive normalization
        content_bytes = spaces_io.get_object(key)
        df = None
        actual_key = key
        
        # If not found, probe case sensitivity (only once)
        if content_bytes is None:
            probe_key = None
            if "/Universe/" in key:
                probe_key = key.replace("/Universe/", "/universe/", 1)
            elif "/universe/" in key:
                probe_key = key.replace("/universe/", "/Universe/", 1)
            
            if probe_key and probe_key != key:
                logger.info(f"universe_probe tried={probe_key}")
                content_bytes = spaces_io.get_object(probe_key)
                if content_bytes:
                    actual_key = probe_key
        
        if content_bytes is None:
            logger.warning(f"universe_not_found s3_key={key}")
            # NO FALLBACK - must load all 5 symbols from universe
            return config.FALLBACK_TICKERS  # Contains 5 symbols as required
        
        # Aggressive normalization
        df = _normalize_universe_content(content_bytes)
        
        if df is None or df.empty:
            logger.warning(f"universe_empty after normalization s3_key={actual_key}")
            return config.FALLBACK_TICKERS
        
        # Cache the normalized DataFrame
        _universe_cache = df
        _universe_loaded = True
        
        # Get valid tickers with regex allowlist
        valid_tickers = _extract_valid_tickers(df)
        
        # Log comprehensive universe summary as required (A)
        total_lines = len(df) if df is not None else 0
        logger.info(f"universe_summary total_lines={total_lines} valid_tickers={len(valid_tickers)} tickers={valid_tickers}")
        
        # Guardrail: warn if fewer tickers than expected (F)
        expected_count = 5
        if len(valid_tickers) < expected_count:
            logger.warning(f"universe_warn expected={expected_count} actual={len(valid_tickers)}")
        
        return valid_tickers
        
    except Exception as e:
        logger.error(f"Error loading universe from {universe_key()}: {e}")
        return config.FALLBACK_TICKERS


def _normalize_universe_content(content_bytes: bytes) -> Optional[pd.DataFrame]:
    """Aggressively normalize universe CSV content.
    
    Args:
        content_bytes: Raw CSV content as bytes
        
    Returns:
        Normalized DataFrame or None if failed
    """
    try:
        # Decode with BOM handling
        content = content_bytes.decode('utf-8-sig').strip()
        
        # Handle CRLF normalization
        content = content.replace('\r\n', '\n').replace('\r', '\n')
        
        # Split into lines and filter empty/comma-only lines
        raw_lines = content.split('\n')
        filtered_lines = []
        dropped_count = 0
        
        for line_num, line in enumerate(raw_lines):
            # Trim whitespace
            line = line.strip()
            
            # Skip empty lines
            if not line:
                if line_num > 0:  # Don't count empty first line as dropped
                    dropped_count += 1
                    logger.debug(f"universe_drop reason=empty_line value={repr(line)}")
                continue
                
            # Skip lines that are just commas
            if re.match(r'^,+$', line):
                dropped_count += 1
                logger.debug(f"universe_drop reason=comma_only value={repr(line)}")
                continue
                
            filtered_lines.append(line)
        
        if not filtered_lines:
            logger.warning("No valid lines found after normalization")
            return None
        
        # Reconstruct CSV content
        normalized_content = '\n'.join(filtered_lines)
        
        # Try to parse as CSV with error handling
        from io import StringIO
        try:
            df = pd.read_csv(StringIO(normalized_content), on_bad_lines='skip')
        except Exception as e:
            logger.warning(f"CSV parsing error, trying alternate approach: {e}")
            # Try with manual parsing for very malformed data
            lines = normalized_content.split('\n')
            if lines:
                header = lines[0] if lines else 'ticker'
                data_rows = []
                for i, line in enumerate(lines[1:], 1):
                    # Take only the first column, ignore extra commas
                    parts = line.split(',')
                    if parts and parts[0].strip():
                        data_rows.append([parts[0].strip()])
                    else:
                        logger.debug(f"universe_drop reason=empty_column value={repr(line)} line={i}")
                
                if data_rows:
                    df = pd.DataFrame(data_rows, columns=['ticker'])
                else:
                    return None
            else:
                return None
        
        if df.empty:
            return None
        
        # Handle column detection: accept 'symbol' or 'ticker', or single column
        columns = df.columns.tolist()
        
        if len(columns) == 1:
            # Single column - treat as tickers
            df.rename(columns={columns[0]: 'ticker'}, inplace=True)
        elif 'symbol' in columns:
            df.rename(columns={'symbol': 'ticker'}, inplace=True)
        elif 'ticker' not in columns:
            # Use first column as ticker
            df.rename(columns={columns[0]: 'ticker'}, inplace=True)
        
        # Normalize ticker values
        if 'ticker' in df.columns:
            df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
            
            # Remove any remaining quotes or extra whitespace
            df['ticker'] = df['ticker'].str.replace('"', '').str.replace("'", '').str.strip()
            
            # Deduplicate
            original_count = len(df)
            df = df.drop_duplicates(subset=['ticker'])
            deduped_count = len(df)
            
            if original_count > deduped_count:
                logger.debug(f"universe_drop reason=duplicate_ticker removed={original_count - deduped_count}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error normalizing universe content: {e}")
        return None


def _extract_valid_tickers(df: pd.DataFrame) -> List[str]:
    """Extract valid tickers using regex allowlist.
    
    Args:
        df: Normalized DataFrame
        
    Returns:
        List of valid ticker symbols
    """
    if df is None or df.empty or 'ticker' not in df.columns:
        return []
    
    valid_tickers = []
    ticker_regex = re.compile(r'^[A-Z.-]{1,10}$')
    
    for ticker in df['ticker'].tolist():
        if isinstance(ticker, str) and ticker_regex.match(ticker):
            valid_tickers.append(ticker)
        else:
            logger.debug(f"universe_drop reason=invalid_format value={repr(ticker)}")
    
    # Remove duplicates while preserving order
    seen = set()
    dedupe_tickers = []
    for ticker in valid_tickers:
        if ticker not in seen:
            seen.add(ticker)
            dedupe_tickers.append(ticker)
    
    return dedupe_tickers


def _get_valid_symbols_from_cache() -> List[str]:
    """Get valid symbols from cached DataFrame.
    
    Returns:
        List of valid ticker symbols from cache
    """
    global _universe_cache
    
    if _universe_cache is None:
        return []
        
    return _extract_valid_tickers(_universe_cache)


def get_universe_dataframe() -> Optional[pd.DataFrame]:
    """Get the full universe DataFrame with all columns.
    
    Returns:
        DataFrame with universe data or None if not loaded
    """
    global _universe_cache
    if not _universe_loaded:
        # Trigger loading
        load_universe()
    return _universe_cache


def clear_universe_cache() -> None:
    """Clear the universe cache to force reload on next access."""
    global _universe_cache, _universe_loaded
    _universe_cache = None
    _universe_loaded = False