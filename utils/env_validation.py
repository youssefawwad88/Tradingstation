"""Environment validation module for startup/runtime validation.

This module validates critical environment variables to ensure they match
canonical values and formats required by the system.
"""

import re
from typing import Optional


def validate_spaces_endpoint(endpoint: str, region: str) -> None:
    """Validate that SPACES_ENDPOINT matches expected format for the region.
    
    Args:
        endpoint: The SPACES_ENDPOINT value
        region: The SPACES_REGION value
        
    Raises:
        RuntimeError: If endpoint doesn't match expected format
    """
    if region == "nyc3":
        expected = "https://nyc3.digitaloceanspaces.com"
        if endpoint != expected:
            raise RuntimeError(
                f"Invalid SPACES_ENDPOINT. Expected {expected}, got: {endpoint}"
            )
    else:
        # For other regions, check the general format
        expected_pattern = f"https://{region}.digitaloceanspaces.com"
        if endpoint != expected_pattern:
            raise RuntimeError(
                f"Invalid SPACES_ENDPOINT for region {region}. "
                f"Expected {expected_pattern}, got: {endpoint}"
            )


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
    import os
    from pathlib import Path
    
    # Load environment variables from .env file if it exists
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        # python-dotenv not installed, continue with system environment variables
        pass
    
    # Get all required environment variables
    spaces_endpoint = os.getenv("SPACES_ENDPOINT", "")
    spaces_region = os.getenv("SPACES_REGION", "")
    data_root = os.getenv("DATA_ROOT", "")
    universe_key = os.getenv("UNIVERSE_KEY", "")
    do_app_id = os.getenv("DO_APP_ID", "")
    
    # Run all validations
    validate_spaces_endpoint(spaces_endpoint, spaces_region)
    validate_paths(data_root, universe_key)
    validate_do_ids(do_app_id)


if __name__ == "__main__":
    """Run validation when executed directly."""
    try:
        validate_all_environment_variables()
        print("✅ All environment variables validated successfully")
    except RuntimeError as e:
        print(f"❌ Environment validation failed: {e}")
        exit(1)