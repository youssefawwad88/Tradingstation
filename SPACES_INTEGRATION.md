# DigitalOcean Spaces Integration

This document explains how to use the new DigitalOcean Spaces integration in the Trading Station system.

## Configuration

### Option 1: Environment Variables (Recommended for Production)
Set these environment variables:
```bash
export SPACES_ACCESS_KEY_ID="your_access_key_here"
export SPACES_SECRET_ACCESS_KEY="your_secret_key_here"
export SPACES_BUCKET_NAME="trading-station-data-youssef"
export SPACES_REGION="nyc3"
```

### Option 2: Update Config File (For Development)
Edit `utils/config.py` and update the `DO_SPACES_CONFIG` dictionary:
```python
DO_SPACES_CONFIG = {
    'access_key_id': 'your_access_key_here',
    'secret_access_key': 'your_secret_key_here',
    'bucket_name': 'trading-station-data-youssef',
    'region': 'nyc3',
    'endpoint_url': 'https://nyc3.digitaloceanspaces.com'
}
```

## Usage

### Using the SpacesManager Directly

```python
from utils.spaces_manager import spaces_manager
import pandas as pd

# Upload a DataFrame
df = pd.DataFrame({'symbol': ['AAPL'], 'price': [150.0]})
spaces_manager.upload_dataframe(df, 'signals/my_signals.csv')

# Download a DataFrame
df = spaces_manager.download_dataframe('signals/my_signals.csv')

# Upload a list of tickers
tickers = ['AAPL', 'MSFT', 'GOOGL']
spaces_manager.upload_list(tickers, 'data/watchlist.txt')

# Download a list
tickers = spaces_manager.download_list('data/watchlist.txt')

# List objects in a folder
objects = spaces_manager.list_objects('signals/')

# Check if object exists
exists = spaces_manager.object_exists('data/my_file.csv')
```

### Using Existing Helper Functions (Backward Compatible)

All existing code continues to work without changes:

```python
from utils.helpers import save_df_to_s3, read_df_from_s3

# These functions now use the new SpacesManager under the hood
df = pd.DataFrame({'data': [1, 2, 3]})
save_df_to_s3(df, 'path/to/file.csv')
loaded_df = read_df_from_s3('path/to/file.csv')
```

## Testing

Run the integration test to verify everything is working:

```bash
python test_spaces_integration.py
```

This will test all upload/download functionality and provide clear feedback on the status of your Spaces integration.

## File Structure

- `utils/config.py` - Configuration settings for DigitalOcean Spaces
- `utils/spaces_manager.py` - Main SpacesManager class with all upload/download functions
- `utils/helpers.py` - Updated to use SpacesManager (maintains backward compatibility)
- `test_spaces_integration.py` - Test script to verify Spaces functionality

## Signal File Storage

All screener signal files are automatically uploaded to DigitalOcean Spaces:

- Breakout signals: `data/signals/breakout_signals.csv`
- AVWAP signals: `data/signals/avwap_signals.csv`
- EMA Pullback signals: `data/signals/ema_pullback_signals.csv`
- etc.

The system automatically handles the upload process when screeners complete their analysis.