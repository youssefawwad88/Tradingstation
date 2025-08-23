"""Environment validation module for startup/runtime validation.

This module validates critical environment variables to ensure they match
canonical values and formats required by the system.
"""

import os
import re
from typing import Optional
from pathlib import Path

# Import here to avoid circular imports when needed
try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


def validate_spaces_endpoint(endpoint: str) -> None:
    """Validate that SPACES_ENDPOINT matches expected format.
    
    Args:
        endpoint: The SPACES_ENDPOINT value
        
    Raises:
        RuntimeError: If endpoint doesn't match expected format
    """
    if not endpoint:
        raise RuntimeError("SPACES_ENDPOINT is required and cannot be empty")
    
    # Allow endpoints with or without scheme for input validation
    # The normalized version will always have https://
    endpoint_pattern = r'^https?://[a-z0-9.-]*digitaloceanspaces\.com/?$'
    
    if not re.match(endpoint_pattern, endpoint):
        raise RuntimeError(
            f"Invalid SPACES_ENDPOINT format. Must match pattern "
            f"'https://[region].digitaloceanspaces.com'. Got: {endpoint}"
        )


def validate_spaces_bucket_name(bucket_name: Optional[str]) -> None:
    """Validate that SPACES_BUCKET_NAME is provided and non-empty.
    
    Args:
        bucket_name: The SPACES_BUCKET_NAME value
        
    Raises:
        RuntimeError: If bucket_name is empty or None
    """
    if not bucket_name:
        raise RuntimeError("SPACES_BUCKET_NAME is required and cannot be empty")


def validate_spaces_base_prefix(base_prefix: str) -> None:
    """Validate that SPACES_BASE_PREFIX ends with a trailing slash.
    
    Args:
        base_prefix: The SPACES_BASE_PREFIX value
        
    Raises:
        RuntimeError: If base_prefix doesn't end with /
    """
    if not base_prefix:
        raise RuntimeError("SPACES_BASE_PREFIX is required and cannot be empty")
    
    if not base_prefix.endswith('/'):
        raise RuntimeError(
            f"SPACES_BASE_PREFIX must end with a trailing slash '/'. Got: {base_prefix}"
        )


def perform_soft_live_check(
    endpoint: str, 
    bucket_name: str, 
    base_prefix: str,
    access_key_id: Optional[str], 
    secret_access_key: Optional[str]
) -> tuple[bool, str]:
    """Perform a soft live check of Spaces connectivity if credentials are available.
    
    Args:
        endpoint: Normalized endpoint URL
        bucket_name: Bucket name
        base_prefix: Base prefix to list under
        access_key_id: Access key ID
        secret_access_key: Secret access key
        
    Returns:
        Tuple of (success, message)
    """
    if not all([access_key_id, secret_access_key, HAS_BOTO3]):
        return True, "Skipped (credentials not available or boto3 not installed)"
    
    try:
        # Extract region from endpoint
        region = endpoint.replace("https://", "").replace(".digitaloceanspaces.com", "")
        
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
        
        # Test with MaxKeys=1 under base prefix
        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=base_prefix,
            MaxKeys=1
        )
        
        return True, f"Connected successfully (found {len(response.get('Contents', []))} objects under {base_prefix})"
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        return False, f"ClientError: {error_code} - {str(e)}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"


def print_spaces_summary_table(
    endpoint_normalized: str,
    bucket: str,
    base_prefix: str,
    origin_url: str,
    live_check_result: tuple[bool, str]
) -> None:
    """Print a summary table of Spaces configuration.
    
    Args:
        endpoint_normalized: Normalized endpoint URL
        bucket: Bucket name  
        base_prefix: Base prefix
        origin_url: Origin URL
        live_check_result: Result of live connectivity check
    """
    success, message = live_check_result
    status_icon = "✅" if success else "❌"
    
    print("\n" + "="*60)
    print("SPACES CONFIGURATION SUMMARY")
    print("="*60)
    print(f"Endpoint (normalized): {endpoint_normalized}")
    print(f"Bucket Name:          {bucket}")
    print(f"Base Prefix:          {base_prefix}")
    print(f"Origin URL:           {origin_url}")
    print(f"Live Check:           {status_icon} {message}")
    print("="*60)


def validate_paths(data_root: str, universe_key: str) -> None:
    """Validate DATA_ROOT and UNIVERSE_KEY match canonical layout.
    
    Args:
        data_root: The DATA_ROOT value
        universe_key: The UNIVERSE_KEY value
        
    Raises:
        RuntimeError: If paths don't match canonical values
    """
    # Validate DATA_ROOT
    expected_data_root = "data"
    if data_root != expected_data_root:
        raise RuntimeError(
            f'Invalid DATA_ROOT. Expected "{expected_data_root}", got: {data_root}'
        )
    
    # Validate UNIVERSE_KEY (case-sensitive)
    expected_universe_key = "data/universe/master_tickerlist.csv"
    if universe_key != expected_universe_key:
        raise RuntimeError(
            f'Invalid UNIVERSE_KEY. Expected "{expected_universe_key}", got: {universe_key}'
        )


def validate_do_ids(app_id: Optional[str]) -> None:
    """Validate DO_APP_ID matches UUID format.
    
    Args:
        app_id: The DO_APP_ID value
        
    Raises:
        RuntimeError: If app_id doesn't match UUID format
    """
    if not app_id:
        raise RuntimeError("Invalid DO_APP_ID. Must be provided and cannot be empty")
    
    # UUID regex: 8-4-4-4-12 hex digits
    uuid_pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    
    if not re.match(uuid_pattern, app_id):
        raise RuntimeError(
            f"Invalid DO_APP_ID. Must be a UUID (36 chars). Got: {app_id}"
        )


def validate_all_environment_variables() -> None:
    """Validate all critical environment variables at startup.
    
    This is a convenience function that validates all environment variables
    in one call. Should be called early in application startup.
    
    Raises:
        RuntimeError: If any validation fails
    """
    # Load environment variables from .env file if it exists
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        # python-dotenv not installed, continue with system environment variables
        pass
    
    # Import config here to get normalized values
    from utils.config import config
    
    # Get all required environment variables (both raw and normalized)
    raw_endpoint = os.getenv("SPACES_ENDPOINT", "")
    spaces_endpoint = config.SPACES_ENDPOINT  # This is the normalized version
    spaces_bucket_name = config.SPACES_BUCKET_NAME
    spaces_base_prefix = config.SPACES_BASE_PREFIX
    data_root = config.DATA_ROOT
    universe_key = config.UNIVERSE_KEY
    do_app_id = os.getenv("DO_APP_ID", "")
    
    # Run all validations
    validate_spaces_endpoint(spaces_endpoint)
    validate_spaces_bucket_name(spaces_bucket_name)
    validate_spaces_base_prefix(spaces_base_prefix)
    validate_paths(data_root, universe_key)
    validate_do_ids(do_app_id)
    
    # Get origin URL for summary
    origin_url = config.get_spaces_origin_url()
    
    # Perform soft live check
    live_check_result = perform_soft_live_check(
        spaces_endpoint,
        spaces_bucket_name or "",
        spaces_base_prefix,
        config.SPACES_ACCESS_KEY_ID,
        config.SPACES_SECRET_ACCESS_KEY
    )
    
    # Print summary table
    print_spaces_summary_table(
        spaces_endpoint,
        spaces_bucket_name or "",
        spaces_base_prefix,
        origin_url,
        live_check_result
    )


if __name__ == "__main__":
    """Run validation when executed directly."""
    try:
        validate_all_environment_variables()
        print("✅ All environment variables validated successfully")
    except RuntimeError as e:
        print(f"❌ Environment validation failed: {e}")
        exit(1)