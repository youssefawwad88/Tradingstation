"""
Configuration file for DigitalOcean Spaces integration.
"""
import os

# DigitalOcean Spaces Configuration
# These can be overridden by environment variables if needed
DO_SPACES_CONFIG = {
    'access_key_id': os.getenv('SPACES_ACCESS_KEY_ID', ''),
    'secret_access_key': os.getenv('SPACES_SECRET_ACCESS_KEY', ''),
    'bucket_name': os.getenv('SPACES_BUCKET_NAME', 'trading-station-data-youssef'),
    'region': os.getenv('SPACES_REGION', 'nyc3'),
    'endpoint_url': os.getenv('SPACES_ENDPOINT_URL', 'https://nyc3.digitaloceanspaces.com')
}