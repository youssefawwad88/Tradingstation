#!/usr/bin/env python3
"""
Phase 3: Migration & Cleanup Strategy

One-time migration script that:
1. Copies any objects from top-level intraday/ and 30 minutes/ to the corresponding data/ paths
2. Preserves the existing objects (doesn't delete them yet)
3. Sets metadata on the old objects to mark them as "archived"
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Tuple

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.spaces_manager import get_spaces_client
from utils.config import SPACES_BUCKET_NAME, DEBUG_MODE

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def list_objects_with_prefix(client, prefix: str) -> List[str]:
    """
    List objects in the Spaces bucket with the given prefix.
    
    Args:
        client: boto3 S3 client
        prefix: Object prefix to filter by
        
    Returns:
        List of object keys
    """
    try:
        objects = []
        response = client.list_objects_v2(
            Bucket=SPACES_BUCKET_NAME,
            Prefix=prefix
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append(obj['Key'])
                
        logger.info(f"Found {len(objects)} objects with prefix '{prefix}'")
        return objects
        
    except Exception as e:
        logger.error(f"Error listing objects with prefix '{prefix}': {e}")
        return []

def extract_ticker_from_path(object_key: str) -> str:
    """
    Extract ticker symbol from object path.
    
    Args:
        object_key: S3 object key
        
    Returns:
        Ticker symbol or empty string if not found
    """
    # Handle different path formats:
    # intraday/AAPL_1min.csv -> AAPL
    # 30 minutes/AAPL_30min.csv -> AAPL
    # intraday/AAPL.csv -> AAPL
    
    filename = os.path.basename(object_key)
    
    # Remove file extension
    name_without_ext = os.path.splitext(filename)[0]
    
    # Extract ticker (everything before first underscore)
    if '_' in name_without_ext:
        ticker = name_without_ext.split('_')[0]
    else:
        ticker = name_without_ext
    
    return ticker.upper()

def get_standard_path(old_path: str) -> str:
    """
    Convert old path to standardized data/ path.
    
    Args:
        old_path: Original object path
        
    Returns:
        Standardized path under data/
    """
    ticker = extract_ticker_from_path(old_path)
    
    if old_path.startswith("intraday/"):
        # Move to data/intraday/
        return f"data/intraday/{ticker}_1min.csv"
    elif old_path.startswith("30 minutes/"):
        # Move to data/intraday_30min/
        return f"data/intraday_30min/{ticker}_30min.csv"
    else:
        # Unknown pattern, keep under data/
        filename = os.path.basename(old_path)
        return f"data/unknown/{filename}"

def copy_object(client, source_key: str, dest_key: str) -> bool:
    """
    Copy object from source to destination within the same bucket.
    
    Args:
        client: boto3 S3 client
        source_key: Source object key
        dest_key: Destination object key
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if destination already exists
        try:
            client.head_object(Bucket=SPACES_BUCKET_NAME, Key=dest_key)
            logger.info(f"Destination already exists, skipping: {dest_key}")
            return True
        except client.exceptions.NoSuchKey:
            pass  # Destination doesn't exist, proceed with copy
            
        # Copy the object
        copy_source = {'Bucket': SPACES_BUCKET_NAME, 'Key': source_key}
        client.copy_object(
            CopySource=copy_source,
            Bucket=SPACES_BUCKET_NAME,
            Key=dest_key
        )
        logger.info(f"Copied: {source_key} -> {dest_key}")
        return True
        
    except Exception as e:
        logger.error(f"Error copying {source_key} to {dest_key}: {e}")
        return False

def set_object_metadata(client, object_key: str, metadata: dict) -> bool:
    """
    Set metadata on an object to mark it as archived.
    
    Args:
        client: boto3 S3 client
        object_key: Object key to update
        metadata: Metadata dictionary to set
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get existing object metadata
        response = client.head_object(Bucket=SPACES_BUCKET_NAME, Key=object_key)
        existing_metadata = response.get('Metadata', {})
        
        # Merge new metadata with existing
        updated_metadata = {**existing_metadata, **metadata}
        
        # Copy object to itself with new metadata
        copy_source = {'Bucket': SPACES_BUCKET_NAME, 'Key': object_key}
        client.copy_object(
            CopySource=copy_source,
            Bucket=SPACES_BUCKET_NAME,
            Key=object_key,
            Metadata=updated_metadata,
            MetadataDirective='REPLACE'
        )
        
        logger.info(f"Updated metadata for: {object_key}")
        return True
        
    except Exception as e:
        logger.error(f"Error setting metadata for {object_key}: {e}")
        return False

def migrate_objects() -> Tuple[int, int, int]:
    """
    Main migration function.
    
    Returns:
        Tuple of (total_found, successfully_migrated, failed_migrations)
    """
    logger.info("Starting Spaces path migration...")
    
    # Get Spaces client
    client = get_spaces_client()
    if not client:
        logger.error("Cannot connect to Spaces - check credentials")
        return 0, 0, 0
        
    total_found = 0
    successfully_migrated = 0
    failed_migrations = 0
    
    # Define old prefixes to migrate
    old_prefixes = [
        "intraday/",
        "30 minutes/"
    ]
    
    for prefix in old_prefixes:
        logger.info(f"\nProcessing prefix: {prefix}")
        
        # List objects with this prefix
        objects = list_objects_with_prefix(client, prefix)
        total_found += len(objects)
        
        for obj_key in objects:
            logger.info(f"Migrating: {obj_key}")
            
            # Get standard path
            standard_path = get_standard_path(obj_key)
            
            # Copy to standard path
            if copy_object(client, obj_key, standard_path):
                # Mark original as archived
                archive_metadata = {
                    "Status": "Archived",
                    "MigratedTo": standard_path,
                    "MigrationDate": datetime.now().isoformat(),
                    "MigrationScript": "migrate_spaces_paths.py"
                }
                
                if set_object_metadata(client, obj_key, archive_metadata):
                    successfully_migrated += 1
                    logger.info(f"✅ Successfully migrated: {obj_key} -> {standard_path}")
                else:
                    failed_migrations += 1
                    logger.warning(f"⚠️ Copied but failed to set archive metadata: {obj_key}")
            else:
                failed_migrations += 1
                logger.error(f"❌ Failed to migrate: {obj_key}")
    
    return total_found, successfully_migrated, failed_migrations

def dry_run_migration() -> None:
    """
    Perform a dry run of the migration to show what would be migrated.
    """
    logger.info("Starting DRY RUN of Spaces path migration...")
    
    # Get Spaces client
    client = get_spaces_client()
    if not client:
        logger.error("Cannot connect to Spaces - check credentials")
        return
        
    # Define old prefixes to migrate
    old_prefixes = [
        "intraday/",
        "30 minutes/"
    ]
    
    total_found = 0
    
    for prefix in old_prefixes:
        logger.info(f"\nChecking prefix: {prefix}")
        
        # List objects with this prefix
        objects = list_objects_with_prefix(client, prefix)
        total_found += len(objects)
        
        for obj_key in objects:
            standard_path = get_standard_path(obj_key)
            logger.info(f"WOULD MIGRATE: {obj_key} -> {standard_path}")
    
    logger.info(f"\nDRY RUN COMPLETE: Found {total_found} objects that would be migrated")

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate Spaces objects to standardized paths")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Show what would be migrated without making changes")
    parser.add_argument("--debug", action="store_true",
                       help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug or DEBUG_MODE:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    if args.dry_run:
        logger.info("🧪 RUNNING IN DRY-RUN MODE - No changes will be made")
        dry_run_migration()
    else:
        logger.info("🚀 RUNNING LIVE MIGRATION - Changes will be made")
        
        # Confirm with user unless in automated mode
        if os.getenv("AUTOMATED_MIGRATION") != "true":
            response = input("Are you sure you want to proceed with live migration? (yes/no): ")
            if response.lower() != "yes":
                logger.info("Migration cancelled by user")
                return
        
        total_found, successful, failed = migrate_objects()
        
        logger.info(f"\n📊 MIGRATION SUMMARY:")
        logger.info(f"   Total objects found: {total_found}")
        logger.info(f"   Successfully migrated: {successful}")
        logger.info(f"   Failed migrations: {failed}")
        
        if failed > 0:
            logger.warning(f"⚠️ {failed} objects failed to migrate - check logs above")
        else:
            logger.info("✅ All objects migrated successfully!")

if __name__ == "__main__":
    main()