#!/usr/bin/env python3
"""One-time migration tool to repair double-prefix and misplaced S3 objects.

This tool scans for objects with incorrect prefixes and safely migrates them:
- trading-system/trading-system/ → trading-system/ (remove double prefix)
- data/ → trading-system/data/ (add missing base prefix)

The tool is idempotent and safe, using copy-verify-delete pattern.
"""

import argparse
import logging
import sys
import time
from typing import List, Tuple

# Add project root to Python path
sys.path.append("/".join(__file__.split("/")[:-2]))

from utils.config import config
from utils.spaces_io import spaces_io

logger = logging.getLogger(__name__)


def scan_problematic_objects() -> List[Tuple[str, str, str]]:
    """Scan for objects that need repair.
    
    Returns:
        List of tuples: (old_key, new_key, reason)
    """
    repairs = []
    base = config.SPACES_BASE_PREFIX

    if not base:
        logger.error("SPACES_BASE_PREFIX not set - cannot proceed")
        return repairs

    logger.info(f"Scanning for objects needing repair with base prefix: {base}")

    # Scan for double-prefix pattern: trading-system/trading-system/
    double_prefix = f"{base}/{base}/"
    logger.info(f"Scanning for double-prefix pattern: {double_prefix}")
    double_prefix_objects = spaces_io.list_objects(double_prefix)

    for obj in double_prefix_objects:
        old_key = obj["key"]
        new_key = old_key.replace(double_prefix, f"{base}/", 1)
        repairs.append((old_key, new_key, "double_prefix"))
        logger.debug(f"Found double-prefix: {old_key} → {new_key}")

    # Scan for top-level data/ pattern (but not under base prefix)
    logger.info("Scanning for top-level data/ pattern")
    data_objects = spaces_io.list_objects("data/")

    for obj in data_objects:
        old_key = obj["key"]
        # Skip if already under base prefix
        if old_key.startswith(f"{base}/"):
            continue
        new_key = f"{base}/{old_key}"
        repairs.append((old_key, new_key, "missing_base"))
        logger.debug(f"Found missing base: {old_key} → {new_key}")

    logger.info(f"Found {len(repairs)} objects needing repair")
    return repairs


def verify_migration(old_key: str, new_key: str) -> bool:
    """Verify that migration was successful by comparing size and etag.
    
    Args:
        old_key: Original object key
        new_key: New object key
        
    Returns:
        True if migration verified successfully
    """
    try:
        old_metadata = spaces_io.object_metadata(old_key)
        new_metadata = spaces_io.object_metadata(new_key)

        if not old_metadata or not new_metadata:
            logger.error(f"Cannot verify migration - missing metadata for {old_key} → {new_key}")
            return False

        old_size = old_metadata.get("size", 0)
        new_size = new_metadata.get("size", 0)
        old_etag = old_metadata.get("etag", "")
        new_etag = new_metadata.get("etag", "")

        if old_size != new_size:
            logger.error(f"Size mismatch for {old_key} → {new_key}: {old_size} != {new_size}")
            return False

        if old_etag != new_etag:
            logger.error(f"ETag mismatch for {old_key} → {new_key}: {old_etag} != {new_etag}")
            return False

        return True

    except Exception as e:
        logger.error(f"Error verifying migration {old_key} → {new_key}: {e}")
        return False


def migrate_object(old_key: str, new_key: str, dry_run: bool = True) -> bool:
    """Migrate a single object using safe copy-verify-delete pattern.
    
    Args:
        old_key: Original object key
        new_key: New object key  
        dry_run: If True, only log what would be done
        
    Returns:
        True if successful (or would be successful in dry run)
    """
    if not spaces_io.is_available:
        logger.error("Spaces client not available")
        return False

    try:
        # Get original object metadata for logging
        old_metadata = spaces_io.object_metadata(old_key)
        if not old_metadata:
            logger.error(f"Object not found: {old_key}")
            return False

        old_size = old_metadata.get("size", 0)
        old_etag = old_metadata.get("etag", "unknown")

        if dry_run:
            logger.info(f"migrate_plan old={old_key} new={new_key} size={old_size} etag={old_etag}")
            return True

        # Check if target already exists
        if spaces_io.object_exists(new_key):
            logger.warning(f"Target already exists, skipping: {new_key}")
            return False

        # Step 1: Copy object to new location
        copy_source = {
            "Bucket": config.SPACES_BUCKET_NAME,
            "Key": old_key,
        }

        spaces_io._client.copy_object(
            Bucket=config.SPACES_BUCKET_NAME,
            Key=new_key,
            CopySource=copy_source,
            MetadataDirective="COPY",
        )

        # Step 2: Verify copy was successful
        if not verify_migration(old_key, new_key):
            logger.error(f"Migration verification failed for {old_key} → {new_key}")
            # Try to clean up the failed copy
            try:
                spaces_io._client.delete_object(
                    Bucket=config.SPACES_BUCKET_NAME,
                    Key=new_key,
                )
            except Exception:
                pass
            return False

        # Step 3: Delete original object
        spaces_io._client.delete_object(
            Bucket=config.SPACES_BUCKET_NAME,
            Key=old_key,
        )

        logger.info(f"migrate_ok old={old_key} new={new_key} size={old_size} etag={old_etag}")
        return True

    except Exception as e:
        logger.error(f"Error migrating {old_key} → {new_key}: {e}")
        return False


def repair_paths(dry_run: bool = True) -> bool:
    """Main repair function.
    
    Args:
        dry_run: If True, only show what would be done
        
    Returns:
        True if successful
    """
    logger.info(f"Starting path repair (dry_run={dry_run})")

    # Scan for problematic objects
    repairs = scan_problematic_objects()

    if not repairs:
        logger.info("No objects found that need repair")
        return True

    logger.info(f"Found {len(repairs)} objects needing repair")

    # Group repairs by reason for reporting
    double_prefix_count = sum(1 for _, _, reason in repairs if reason == "double_prefix")
    missing_base_count = sum(1 for _, _, reason in repairs if reason == "missing_base")

    logger.info(f"  - Double prefix fixes: {double_prefix_count}")
    logger.info(f"  - Missing base fixes: {missing_base_count}")

    if dry_run:
        logger.info("DRY RUN - showing planned changes:")
    else:
        logger.info("APPLYING changes:")

    # Process each repair
    success_count = 0
    for old_key, new_key, reason in repairs:
        if migrate_object(old_key, new_key, dry_run):
            success_count += 1
        else:
            logger.error(f"Failed to migrate {old_key} → {new_key}")

        # Small delay to avoid overwhelming the API
        time.sleep(0.1)

    logger.info(f"Repair complete: {success_count}/{len(repairs)} successful")
    return success_count == len(repairs)


def main():
    """Main entry point for the repair paths tool."""
    parser = argparse.ArgumentParser(description="Repair S3 object paths")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default is dry-run only)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned changes without applying (default)"
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Determine mode
    dry_run = not args.apply
    if args.dry_run:
        dry_run = True

    try:
        # Validate configuration
        if not config.SPACES_BASE_PREFIX:
            logger.error("SPACES_BASE_PREFIX environment variable must be set")
            return 1

        if not spaces_io.is_available:
            logger.error("Spaces client not available - check credentials")
            return 1

        # Run repair
        success = repair_paths(dry_run=dry_run)
        return 0 if success else 1

    except Exception as e:
        logger.error(f"Error in repair_paths: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
