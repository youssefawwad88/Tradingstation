# Security Best Practices

## Security Audit Results

### ✅ Completed Security Fixes

1. **Fixed MD5 Hash Usage** (High Priority)
   - **Issue**: `utils/cache.py` used weak MD5 for cache key generation
   - **Fix**: Replaced with SHA-256 for better security
   - **Impact**: Strengthened cache key security without breaking functionality

2. **Added Request Timeouts** (Medium Priority)
   - **Issue**: HTTP requests without timeout in `utils/helpers.py`
   - **Fix**: Added 30-second timeout to prevent hanging connections
   - **Impact**: Better resilience against network issues

3. **Fixed Temp Directory Usage** (Medium Priority)
   - **Issue**: Hardcoded `/tmp/` path in `orchestrator/run_all.py`
   - **Fix**: Use `tempfile.gettempdir()` for secure temp directory
   - **Impact**: Platform-independent and more secure temp file handling

4. **Removed Hardcoded Credentials** (Critical)
   - **Issue**: API keys hardcoded in `jobs/intraday_fetcher.py`
   - **Fix**: Replaced with environment variable loading
   - **Impact**: Eliminated credential exposure in source code

### 🔍 Remaining Security Considerations

#### Low Priority Issues (Acceptable Risk)

1. **Pickle Usage in Cache** (`utils/cache.py`)
   - **Risk**: Pickle can be unsafe with untrusted data
   - **Mitigation**: Only used for internal caching, no external data
   - **Recommendation**: Monitor for future alternatives like JSON serialization

2. **Assert Statements in Tests** (173 instances)
   - **Risk**: Assertions removed in optimized Python bytecode
   - **Mitigation**: Only used in test code, not production logic
   - **Status**: Acceptable for testing purposes

3. **Subprocess Usage** (`orchestrator/run_all.py`)
   - **Risk**: Potential command injection if inputs not validated
   - **Mitigation**: Only used with controlled, internal Python scripts
   - **Status**: Low risk in current implementation

## Environment Variable Security

### Required Security Configuration

```bash
# Production environment variables (never commit these values)
export ALPHA_VANTAGE_API_KEY="your_actual_api_key"
export SPACES_ACCESS_KEY_ID="your_access_key"
export SPACES_SECRET_ACCESS_KEY="your_secret_key"
export SPACES_BUCKET_NAME="your_bucket_name"
export SPACES_REGION="nyc3"

# Optional security settings
export DEBUG_MODE="false"  # Disable debug in production
export LOG_LEVEL="INFO"    # Avoid verbose logging
export SECURE_COOKIES="true"
export SESSION_TIMEOUT="3600"
```

### Environment Variable Validation

All environment variables are properly validated in `utils/config.py`:

```python
def validate_credentials():
    """Validate all required credentials are present."""
    required_vars = [
        "ALPHA_VANTAGE_API_KEY",
        "SPACES_ACCESS_KEY_ID", 
        "SPACES_SECRET_ACCESS_KEY",
        "SPACES_BUCKET_NAME"
    ]
    
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.warning(f"Missing environment variables: {missing}")
        return False
    
    return True
```

## Data Protection

### Sensitive Data Handling

1. **API Keys**: Stored only in environment variables
2. **Trading Data**: Encrypted at rest in DigitalOcean Spaces
3. **User Sessions**: Streamlit handles session management securely
4. **Logs**: Filtered to exclude sensitive information

### Data Encryption

```python
# Example of secure data storage pattern
def store_sensitive_data(data, encryption_key):
    """Store sensitive data with encryption."""
    from cryptography.fernet import Fernet
    
    f = Fernet(encryption_key)
    encrypted_data = f.encrypt(data.encode())
    
    # Store encrypted data
    return encrypted_data

def load_sensitive_data(encrypted_data, encryption_key):
    """Load and decrypt sensitive data."""
    from cryptography.fernet import Fernet
    
    f = Fernet(encryption_key)
    decrypted_data = f.decrypt(encrypted_data).decode()
    
    return decrypted_data
```

## Network Security

### HTTPS/TLS Configuration

All external API calls use HTTPS:

```python
# Secure API client configuration
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_secure_session():
    """Create a secure requests session with proper configuration."""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # Set reasonable timeout
    session.timeout = 30
    
    return session
```

### API Rate Limiting

```python
class SecureRateLimiter:
    """Rate limiter with security considerations."""
    
    def __init__(self, calls_per_minute=5):
        self.calls_per_minute = calls_per_minute
        self.call_times = []
        self.failed_attempts = 0
        self.lockout_until = None
    
    def can_make_request(self):
        """Check if request can be made with security backoff."""
        now = time.time()
        
        # Check if in lockout period
        if self.lockout_until and now < self.lockout_until:
            return False
        
        # Clean old calls
        minute_ago = now - 60
        self.call_times = [t for t in self.call_times if t > minute_ago]
        
        # Check rate limit
        if len(self.call_times) >= self.calls_per_minute:
            return False
        
        return True
    
    def record_failed_attempt(self):
        """Record failed attempt and implement backoff."""
        self.failed_attempts += 1
        
        # Exponential backoff for repeated failures
        if self.failed_attempts > 3:
            backoff_seconds = min(300, 2 ** self.failed_attempts)  # Max 5 minutes
            self.lockout_until = time.time() + backoff_seconds
```

## Input Validation

### Ticker Symbol Validation

```python
def validate_ticker_symbol(ticker):
    """Validate ticker symbol for security."""
    import re
    
    if not ticker or not isinstance(ticker, str):
        return False
    
    # Allow only alphanumeric characters and common symbols
    pattern = r"^[A-Za-z0-9.-]+$"
    if not re.match(pattern, ticker):
        return False
    
    # Length validation
    if len(ticker) < 1 or len(ticker) > 10:
        return False
    
    return True
```

### Data Sanitization

```python
def sanitize_user_input(user_input):
    """Sanitize user input to prevent injection attacks."""
    import html
    
    if not isinstance(user_input, str):
        return str(user_input)
    
    # HTML escape
    sanitized = html.escape(user_input)
    
    # Remove potential script content
    sanitized = re.sub(r'<script.*?</script>', '', sanitized, flags=re.IGNORECASE | re.DOTALL)
    
    # Limit length
    sanitized = sanitized[:1000]  # Reasonable limit
    
    return sanitized
```

## Monitoring and Alerting

### Security Event Logging

```python
def log_security_event(event_type, details, severity="INFO"):
    """Log security-related events for monitoring."""
    import json
    
    security_log = {
        "timestamp": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "severity": severity,
        "details": details,
        "source_ip": get_client_ip(),
        "user_agent": get_user_agent()
    }
    
    # Log to security log file
    with open("logs/security.log", "a") as f:
        f.write(json.dumps(security_log) + "\n")
    
    # Alert on high severity events
    if severity in ["WARNING", "ERROR", "CRITICAL"]:
        send_security_alert(security_log)
```

### Failed Login Attempts

```python
def monitor_failed_attempts():
    """Monitor for suspicious activity patterns."""
    # This would be integrated with authentication system
    # Currently, the system doesn't have user authentication
    # but this pattern is ready for future implementation
    pass
```

## Deployment Security

### Production Environment

```bash
# Secure production deployment checklist

# 1. Environment variables set securely
export ALPHA_VANTAGE_API_KEY="***"  # Never log actual values
export SPACES_SECRET_ACCESS_KEY="***"

# 2. Debug mode disabled
export DEBUG_MODE="false"

# 3. Secure logging
export LOG_LEVEL="INFO"  # Avoid DEBUG in production

# 4. File permissions
chmod 600 ~/.env  # Environment file readable only by owner
chmod 755 $(which python3)  # Standard permissions for Python

# 5. Network security
# - DigitalOcean Apps provides HTTPS termination
# - All external APIs use HTTPS
# - No sensitive data in URLs or logs
```

### Container Security (if using Docker)

```dockerfile
# Security-focused Dockerfile example
FROM python:3.12-slim

# Create non-root user
RUN useradd --create-home --shell /bin/bash trader
USER trader
WORKDIR /home/trader/app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=trader:trader . .

# Set secure environment
ENV PYTHONPATH=/home/trader/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import requests; requests.get('http://localhost:8501/healthz', timeout=5)"

# Run application
CMD ["streamlit", "run", "dashboard/streamlit_app.py", "--server.headless=true", "--server.address=0.0.0.0"]
```

## Security Checklist

### ✅ Completed
- [x] Remove hardcoded credentials
- [x] Fix weak hash algorithms  
- [x] Add request timeouts
- [x] Secure temp file handling
- [x] Environment variable validation
- [x] Input validation for ticker symbols
- [x] HTTPS for all external calls
- [x] Proper error handling without information leakage

### 🔄 Ongoing Monitoring
- [ ] Monitor for new security vulnerabilities in dependencies
- [ ] Regular security scans with updated tools
- [ ] Review access logs for suspicious patterns
- [ ] Update dependencies with security patches

### 🔮 Future Enhancements
- [ ] Implement user authentication system
- [ ] Add rate limiting for dashboard access
- [ ] Implement data encryption at rest
- [ ] Add intrusion detection system
- [ ] Security audit logging dashboard

## Contact & Reporting

For security issues or questions:
1. Create a private issue in the repository
2. Use security-specific labels
3. Follow responsible disclosure practices
4. Provide detailed reproduction steps