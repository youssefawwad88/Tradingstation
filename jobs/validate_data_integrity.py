#!/usr/bin/env python3
"""
Data Integrity Validation Utility - Phase 2 Hardening
====================================================

This script implements a comprehensive data sanitization system that can scan
all data files in DigitalOcean Spaces and validate their integrity.

Addresses Pillar 3: Reliability requirements from Phase 2 instructions.

Key Features:
1. Scans all Parquet/CSV files for given tickers and intervals in Spaces
2. Validates timestamp column format (UTC standard)
3. Checks for NaN/NULL values in critical columns
4. Flags obvious data corruption (artificial precision, suspicious patterns)
5. Outputs detailed summary report without deleting data

This ensures the system can identify and report bad data systematically.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime, timedelta
import re
from typing import List, Dict, Tuple, Optional
import io
import pytz

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import core utilities
from utils.config import SPACES_BUCKET_NAME, SPACES_ACCESS_KEY_ID, SPACES_SECRET_ACCESS_KEY
from utils.helpers import read_master_tickerlist
from utils.spaces_manager import SpacesManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataIntegrityValidator:
    """
    Comprehensive data integrity validation system.
    
    Scans and validates all data files without making modifications.
    """
    
    def __init__(self):
        """Initialize the validator with Spaces connection."""
        self.spaces_manager = SpacesManager()
        self.validation_results = {
            'scanned_files': 0,
            'valid_files': 0,
            'corrupted_files': 0,
            'missing_files': 0,
            'issues_found': []
        }
        
    def validate_timestamp_format(self, df: pd.DataFrame, file_path: str) -> List[str]:
        """
        Validate that timestamps are in the standard UTC format.
        
        Args:
            df: DataFrame to validate
            file_path: Path of the file being validated
            
        Returns:
            List of issues found
        """
        issues = []
        
        # Find timestamp column
        timestamp_col = None
        for col in ['timestamp', 'Date', 'datetime']:
            if col in df.columns:
                timestamp_col = col
                break
                
        if not timestamp_col:
            issues.append(f"No timestamp column found (expected: timestamp, Date, or datetime)")
            return issues
            
        try:
            # Check if timestamps are strings in UTC format
            sample_timestamps = df[timestamp_col].head(10)
            
            for i, ts in enumerate(sample_timestamps):
                if pd.isna(ts):
                    issues.append(f"NULL timestamp found at row {i}")
                    continue
                    
                ts_str = str(ts)
                
                # Check for proper UTC format ending
                if not ts_str.endswith('+00:00'):
                    issues.append(f"Non-UTC timestamp format: {ts_str} (should end with +00:00)")
                    
                # Check for suspicious microsecond precision patterns
                if '.970407' in ts_str:
                    issues.append(f"Suspicious artificial microsecond pattern detected: {ts_str}")
                    
                # Check for overly precise microseconds (more than 3 decimal places)
                microsecond_pattern = r'\.\d{4,}-'
                if re.search(microsecond_pattern, ts_str):
                    issues.append(f"Excessive timestamp precision detected: {ts_str}")
                    
                # Try to parse the timestamp
                try:
                    parsed = pd.to_datetime(ts, utc=True)  # Parse as UTC to avoid timezone comparison issues
                    
                    # Check for future dates (potential corruption) - use timezone-aware comparison
                    now_utc = datetime.now(pytz.UTC)
                    if parsed > now_utc + timedelta(days=1):
                        issues.append(f"Future timestamp detected: {ts_str}")
                        
                    # Check for very old dates (potential corruption)
                    old_date_utc = datetime(2000, 1, 1, tzinfo=pytz.UTC)
                    if parsed < old_date_utc:
                        issues.append(f"Suspiciously old timestamp: {ts_str}")
                        
                except Exception as e:
                    issues.append(f"Invalid timestamp format: {ts_str} (parse error: {e})")
                    
        except Exception as e:
            issues.append(f"Error validating timestamps: {e}")
            
        return issues
        
    def validate_critical_columns(self, df: pd.DataFrame, file_path: str) -> List[str]:
        """
        Check for NaN/NULL values in critical columns.
        
        Args:
            df: DataFrame to validate
            file_path: Path of the file being validated
            
        Returns:
            List of issues found
        """
        issues = []
        
        # Define critical columns that should not have NaN values
        critical_columns = ['open', 'high', 'low', 'close', 'volume']
        
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    issues.append(f"Column '{col}' has {null_count} NULL/NaN values")
                    
                # Check for zero or negative values in price columns
                if col in ['open', 'high', 'low', 'close']:
                    invalid_prices = (df[col] <= 0).sum()
                    if invalid_prices > 0:
                        issues.append(f"Column '{col}' has {invalid_prices} zero or negative price values")
                        
                # Check for zero volume
                if col == 'volume':
                    zero_volume = (df[col] == 0).sum()
                    if zero_volume > 0:
                        issues.append(f"Column 'volume' has {zero_volume} zero volume entries")
            else:
                issues.append(f"Missing critical column: '{col}'")
                
        return issues
        
    def validate_price_precision(self, df: pd.DataFrame, file_path: str) -> List[str]:
        """
        Check for artificial or excessive price precision.
        
        Args:
            df: DataFrame to validate
            file_path: Path of the file being validated
            
        Returns:
            List of issues found
        """
        issues = []
        
        price_columns = ['open', 'high', 'low', 'close']
        
        for col in price_columns:
            if col in df.columns:
                try:
                    # Check for excessive decimal precision (more than 8 decimal places)
                    sample_prices = df[col].head(20)
                    
                    for i, price in enumerate(sample_prices):
                        if pd.isna(price):
                            continue
                            
                        price_str = str(price)
                        
                        # Check for excessive decimal places
                        if '.' in price_str:
                            decimal_part = price_str.split('.')[1]
                            if len(decimal_part) > 8:
                                issues.append(f"Excessive price precision in {col}: {price_str} ({len(decimal_part)} decimal places)")
                                
                        # Check for specific artificial patterns we've seen before
                        if '99.88885801779249' in price_str or '99.56018769783935' in price_str:
                            issues.append(f"Known artificial price pattern detected in {col}: {price_str}")
                            
                except Exception as e:
                    issues.append(f"Error validating price precision for {col}: {e}")
                    
        return issues
        
    def validate_data_patterns(self, df: pd.DataFrame, file_path: str) -> List[str]:
        """
        Check for suspicious data patterns that indicate corruption.
        
        Args:
            df: DataFrame to validate
            file_path: Path of the file being validated
            
        Returns:
            List of issues found
        """
        issues = []
        
        try:
            # Check for duplicate timestamps
            if 'timestamp' in df.columns:
                duplicate_count = df['timestamp'].duplicated().sum()
                if duplicate_count > 0:
                    issues.append(f"Found {duplicate_count} duplicate timestamps")
                    
            # Check for data that's too uniform (potential artificial generation)
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in df.columns:
                    # Check if all values are identical (suspicious)
                    unique_values = df[col].nunique()
                    if unique_values == 1 and len(df) > 10:
                        issues.append(f"All values in {col} are identical - possible artificial data")
                        
                    # Check for impossible price relationships
                    if col == 'high' and 'low' in df.columns:
                        invalid_highs = (df['high'] < df['low']).sum()
                        if invalid_highs > 0:
                            issues.append(f"Found {invalid_highs} rows where high < low (impossible)")
                            
            # Check for suspicious volume patterns
            if 'volume' in df.columns:
                # Check if volume values are all identical (suspicious)
                unique_volumes = df['volume'].nunique()
                if unique_volumes == 1 and len(df) > 10:
                    issues.append("All volume values are identical - possible artificial data")
                    
                # Check for extremely high volume values (potential data error)
                max_volume = df['volume'].max()
                if max_volume > 1e10:  # 10 billion shares
                    issues.append(f"Extremely high volume detected: {max_volume:,.0f}")
                    
        except Exception as e:
            issues.append(f"Error validating data patterns: {e}")
            
        return issues
        
    def validate_single_file(self, file_path: str) -> Dict:
        """
        Validate a single data file.
        
        Args:
            file_path: Path to the file in Spaces
            
        Returns:
            Dictionary with validation results
        """
        result = {
            'file_path': file_path,
            'status': 'unknown',
            'issues': [],
            'file_size': 0,
            'row_count': 0,
            'column_count': 0,
            'readable': False
        }
        
        try:
            logger.debug(f"Validating file: {file_path}")
            
            # Try to read the file
            df = self.spaces_manager.read_dataframe_from_spaces(file_path)
            
            if df is None or df.empty:
                result['status'] = 'missing_or_empty'
                result['issues'].append("File is missing or empty")
                return result
                
            result['readable'] = True
            result['row_count'] = len(df)
            result['column_count'] = len(df.columns)
            
            # Get file size (approximate from DataFrame)
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            result['file_size'] = len(buffer.getvalue())
            
            # Run all validation checks
            issues = []
            
            # Timestamp validation
            issues.extend(self.validate_timestamp_format(df, file_path))
            
            # Critical columns validation
            issues.extend(self.validate_critical_columns(df, file_path))
            
            # Price precision validation
            issues.extend(self.validate_price_precision(df, file_path))
            
            # Data patterns validation
            issues.extend(self.validate_data_patterns(df, file_path))
            
            result['issues'] = issues
            
            if len(issues) == 0:
                result['status'] = 'valid'
            else:
                result['status'] = 'corrupted'
                
        except Exception as e:
            result['status'] = 'error'
            result['issues'].append(f"Error reading file: {e}")
            logger.error(f"Error validating {file_path}: {e}")
            
        return result
        
    def scan_ticker_data(self, ticker: str) -> Dict:
        """
        Scan all data files for a specific ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dictionary with scan results for the ticker
        """
        ticker_results = {
            'ticker': ticker,
            'files_scanned': 0,
            'files_valid': 0,
            'files_corrupted': 0,
            'files_missing': 0,
            'file_results': []
        }
        
        # Define expected file paths for each ticker
        expected_files = [
            f'data/daily/{ticker}_daily.csv',
            f'data/intraday_30min/{ticker}_30min.csv',
            f'data/intraday/{ticker}_1min.csv'
        ]
        
        logger.info(f"🔍 Scanning data files for ticker: {ticker}")
        
        for file_path in expected_files:
            self.validation_results['scanned_files'] += 1
            ticker_results['files_scanned'] += 1
            
            result = self.validate_single_file(file_path)
            ticker_results['file_results'].append(result)
            
            # Update counters
            if result['status'] == 'valid':
                self.validation_results['valid_files'] += 1
                ticker_results['files_valid'] += 1
                logger.debug(f"✅ {file_path}: VALID")
            elif result['status'] == 'corrupted':
                self.validation_results['corrupted_files'] += 1
                ticker_results['files_corrupted'] += 1
                self.validation_results['issues_found'].extend([
                    f"{file_path}: {issue}" for issue in result['issues']
                ])
                logger.warning(f"⚠️ {file_path}: CORRUPTED ({len(result['issues'])} issues)")
            else:  # missing_or_empty or error
                self.validation_results['missing_files'] += 1
                ticker_results['files_missing'] += 1
                logger.warning(f"❌ {file_path}: MISSING/ERROR")
                
        return ticker_results
        
    def generate_summary_report(self, ticker_results: List[Dict]) -> str:
        """
        Generate a comprehensive summary report.
        
        Args:
            ticker_results: List of validation results for each ticker
            
        Returns:
            Formatted summary report string
        """
        report = []
        report.append("=" * 80)
        report.append("DATA INTEGRITY VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        report.append("")
        
        # Overall summary
        report.append("OVERALL SUMMARY")
        report.append("-" * 40)
        report.append(f"Total files scanned: {self.validation_results['scanned_files']}")
        report.append(f"Valid files: {self.validation_results['valid_files']}")
        report.append(f"Corrupted files: {self.validation_results['corrupted_files']}")
        report.append(f"Missing/Error files: {self.validation_results['missing_files']}")
        
        # Calculate percentages
        if self.validation_results['scanned_files'] > 0:
            valid_pct = (self.validation_results['valid_files'] / self.validation_results['scanned_files']) * 100
            corrupted_pct = (self.validation_results['corrupted_files'] / self.validation_results['scanned_files']) * 100
            missing_pct = (self.validation_results['missing_files'] / self.validation_results['scanned_files']) * 100
            
            report.append(f"Valid: {valid_pct:.1f}%, Corrupted: {corrupted_pct:.1f}%, Missing: {missing_pct:.1f}%")
        
        report.append("")
        
        # Per-ticker breakdown
        report.append("PER-TICKER BREAKDOWN")
        report.append("-" * 40)
        
        for ticker_result in ticker_results:
            ticker = ticker_result['ticker']
            valid = ticker_result['files_valid']
            corrupted = ticker_result['files_corrupted']
            missing = ticker_result['files_missing']
            total = ticker_result['files_scanned']
            
            status_icon = "✅" if corrupted == 0 and missing == 0 else "⚠️" if corrupted > 0 else "❌"
            report.append(f"{status_icon} {ticker}: {valid}/{total} valid, {corrupted} corrupted, {missing} missing")
            
        report.append("")
        
        # Detailed issues
        if self.validation_results['issues_found']:
            report.append("DETAILED ISSUES FOUND")
            report.append("-" * 40)
            
            # Group issues by type
            issue_groups = {}
            for issue in self.validation_results['issues_found']:
                file_path, issue_desc = issue.split(': ', 1)
                if file_path not in issue_groups:
                    issue_groups[file_path] = []
                issue_groups[file_path].append(issue_desc)
                
            for file_path, issues in issue_groups.items():
                report.append(f"\n📁 {file_path}:")
                for issue in issues:
                    report.append(f"   • {issue}")
        else:
            report.append("🎉 NO ISSUES FOUND - All data files are clean!")
            
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)
        
    def run_full_validation(self, tickers: Optional[List[str]] = None) -> str:
        """
        Run complete data integrity validation.
        
        Args:
            tickers: List of tickers to validate (if None, uses master list)
            
        Returns:
            Summary report string
        """
        logger.info("🚀 STARTING DATA INTEGRITY VALIDATION")
        
        # Get ticker list
        if tickers is None:
            tickers = read_master_tickerlist()
            
        if not tickers:
            logger.error("❌ No tickers found to validate")
            return "ERROR: No tickers found"
            
        logger.info(f"📋 Validating data for {len(tickers)} tickers: {tickers}")
        
        # Validate each ticker
        ticker_results = []
        
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"📍 Processing ticker {i}/{len(tickers)}: {ticker}")
            
            try:
                ticker_result = self.scan_ticker_data(ticker)
                ticker_results.append(ticker_result)
                
            except Exception as e:
                logger.error(f"❌ Error validating ticker {ticker}: {e}")
                # Add failed ticker to results
                ticker_results.append({
                    'ticker': ticker,
                    'files_scanned': 0,
                    'files_valid': 0,
                    'files_corrupted': 0,
                    'files_missing': 3,  # Assume all 3 files missing on error
                    'file_results': []
                })
                self.validation_results['missing_files'] += 3
                
        # Generate final report
        report = self.generate_summary_report(ticker_results)
        
        logger.info("✅ DATA INTEGRITY VALIDATION COMPLETE")
        return report


def main():
    """Main function to run data integrity validation."""
    try:
        # Check Spaces configuration
        if not SPACES_BUCKET_NAME:
            logger.error("❌ DigitalOcean Spaces not configured")
            return False
            
        # Initialize validator
        validator = DataIntegrityValidator()
        
        # Run validation
        report = validator.run_full_validation()
        
        # Print report to console
        print(report)
        
        # Save report to file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_filename = f"data_integrity_report_{timestamp}.txt"
        
        with open(report_filename, 'w') as f:
            f.write(report)
            
        logger.info(f"📄 Report saved to: {report_filename}")
        
        # Return success based on validation results
        return validator.validation_results['corrupted_files'] == 0
        
    except Exception as e:
        logger.error(f"❌ Critical error in data integrity validation: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)