# Trading Station - Contributing Guide

## Development Workflow

### Branch Strategy
- `main`: Production-ready code only (protected)
- `dev`: Long-lived integration branch for active development
- `feature/*`: Feature branches (PR to dev)

### Pull Request Checklist

Before submitting a PR, ensure you have completed:

#### Code Quality
- [ ] Updated/added unit tests for each new public function
- [ ] Integration test covers a full ticker path (fetch→store→screen)
- [ ] No hardcoded paths; config-driven only
- [ ] Logging at INFO/DEBUG appropriate; no prints
- [ ] Code follows black formatting (line length 120)
- [ ] Imports sorted with isort

#### Data Contracts
- [ ] Data schemas unchanged or migration included
- [ ] No breaking changes to signal output format
- [ ] File naming conventions maintained
- [ ] Storage paths follow established patterns

#### Testing
- [ ] Manual ticker load verified (include path found + sample count in PR logs)
- [ ] TEST_MODE works with fixtures; CI green on dev
- [ ] All jobs can be executed through orchestrator
- [ ] Health checks pass

#### Documentation
- [ ] Updated docstrings for new functions
- [ ] Updated README if adding new features
- [ ] Added configuration examples if needed

### Testing Your Changes

1. **Run in TEST_MODE:**
   ```bash
   TEST_MODE=true python orchestrator/run_all.py --validate-only
   TEST_MODE=true python tests/test_mode_validation.py
   ```

2. **Test Individual Components:**
   ```bash
   # Test job registry
   TEST_MODE=true python orchestrator/run_all.py --list-jobs
   
   # Test specific job
   TEST_MODE=true python orchestrator/run_all.py --run-job opportunity_ticker_finder
   
   # Test screener
   TEST_MODE=true python screeners/gapgo.py
   ```

3. **Validate Configuration:**
   ```bash
   python orchestrator/run_all.py --health
   ```

### Code Style

We use:
- **Black** for code formatting (120 character line length)
- **isort** for import sorting
- **flake8** for linting

Format your code before submitting:
```bash
black --line-length 120 .
isort --profile black .
flake8 .
```

### Data Contract Changes

If your PR involves changes to data schemas or contracts:

1. **Signal Schema Changes:**
   - Update `utils/validators.py` with new schema validation
   - Update all screeners to use new schema
   - Provide migration script if needed

2. **Storage Path Changes:**
   - Update `utils/config.py` with new path constants
   - Update all jobs to use new paths
   - Ensure backward compatibility or migration path

3. **API Changes:**
   - Update `utils/alpha_vantage_api.py`
   - Add corresponding test fixtures
   - Update TEST_MODE fixture loading

### Adding New Features

#### New Screener Strategy
1. Create new file in `screeners/` following existing patterns
2. Implement standardized signal schema output
3. Add to job registry in `orchestrator/job_registry.py`
4. Add to schedule configuration in `config/schedules.yml`
5. Add test fixtures for TEST_MODE
6. Add unit and integration tests

#### New Data Source
1. Update `utils/alpha_vantage_api.py` or create new API module
2. Add corresponding jobs in `jobs/` directory
3. Update data validation in `utils/validators.py`
4. Add test fixtures and TEST_MODE support

#### New Job Type
1. Create job module in `jobs/` directory
2. Register in `orchestrator/job_registry.py`
3. Add to appropriate schedule in `config/schedules.yml`
4. Add health monitoring support
5. Add error handling and retry logic

### Security Guidelines

- Never commit API keys or secrets
- Use environment variables for configuration
- Validate all external inputs
- Follow principle of least privilege
- Run security scans before submitting PRs

### Performance Guidelines

- Respect API rate limits (5 calls/minute default)
- Use efficient pandas operations
- Implement proper caching where appropriate
- Monitor memory usage for large datasets
- Use connection pooling for storage operations

### Error Handling

- Use structured logging with context
- Implement proper retry logic with exponential backoff
- Handle API failures gracefully
- Provide meaningful error messages
- Use appropriate exception types

### Documentation

- Write clear docstrings for all public functions
- Include type hints for function parameters and returns
- Update README for new features
- Add configuration examples
- Document any breaking changes

## Getting Help

- Check existing issues and PRs first
- Join discussions in GitHub issues
- Review code comments and documentation
- Look at existing implementations for patterns

## Release Process

1. **Feature Development:**
   - Work in feature branches
   - PR to `dev` branch
   - Code review and testing

2. **Release Preparation:**
   - Merge `dev` to release candidate
   - Full integration testing
   - Performance validation
   - Security review

3. **Production Release:**
   - PR from release candidate to `main`
   - Required approvals
   - Automated deployment
   - Post-release monitoring

Thank you for contributing to Trading Station!