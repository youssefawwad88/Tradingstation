# Environment Variables Configuration

This document outlines all required environment variables for the TradingStation system.

## Required Environment Variables

### MarketData.io API
```bash
MARKETDATA_TOKEN=your_marketdata_token
```
- **Purpose**: Access to MarketData.io market data API
- **Required**: Yes
- **Usage**: All data fetching operations

### DigitalOcean Spaces Configuration
```bash
SPACES_ACCESS_KEY_ID=DO00LRPNKCCXNDX8NZRH
SPACES_SECRET_ACCESS_KEY=w+5GuIgaXuGBIoEDXRO7r84YZC9tL0mnbEwltzQEd9o
SPACES_REGION=nyc3
SPACES_BUCKET_NAME=trading-station-data-youssef
SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
```
- **Purpose**: Cloud storage for all trading data
- **Required**: Yes
- **Usage**: Data persistence, signal storage, dashboard data

### Application Configuration
```bash
APP_ENV=production
DEPLOYMENT_TAG=auto-generated
```
- **Purpose**: Environment identification and deployment tracking
- **Required**: Yes for production
- **Usage**: Logging, monitoring, configuration selection

## Setting Environment Variables

### DigitalOcean App Platform
1. Navigate to your app in the DigitalOcean control panel
2. Go to Settings → Environment Variables
3. Add each variable with its value
4. Deploy the app to apply changes

### Local Development
Create a `.env` file in the project root:
```bash
# .env (DO NOT COMMIT)
MARKETDATA_TOKEN=your_marketdata_token
SPACES_ACCESS_KEY_ID=DO00LRPNKCCXNDX8NZRH
SPACES_SECRET_ACCESS_KEY=w+5GuIgaXuGBIoEDXRO7r84YZC9tL0mnbEwltzQEd9o
SPACES_REGION=nyc3
SPACES_BUCKET_NAME=trading-station-data-youssef
SPACES_ENDPOINT=https://nyc3.digitaloceanspaces.com
APP_ENV=development
```

Load with:
```bash
source .env  # or use python-dotenv
```

### Docker
```bash
docker run -e MARKETDATA_TOKEN=value -e SPACES_ACCESS_KEY_ID=value ...
```

## Security Notes

1. **Never commit secrets to version control**
2. **Use different buckets/keys for dev/staging/prod**
3. **Rotate API keys regularly**
4. **Monitor API usage and costs**
5. **Ensure Spaces bucket has proper access controls**

## Test Mode

The system gracefully degrades when environment variables are missing:
- Uses test/mock data instead of live API calls
- Logs warnings about missing configuration
- Continues operation with reduced functionality

## Validation

Run the verification tool to check configuration:
```bash
python3 tools/verify_deployment.py
```

This will validate all environment variables and test connectivity.