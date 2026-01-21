# Test Quick Reference

See `../TESTING.md` for complete testing strategy and rationale.

## Unit Tests (Fast)

```bash
# All unit tests
pytest tests/ -v -m "not integration"

# Specific module tests
pytest tests/test_sftp_uploader.py -v
pytest tests/test_generator.py -v

# With coverage
pytest tests/ --cov=. --cov-report=html -m "not integration"
```

## Integration Tests (Requires Docker)

See `integration/README.md` for details.

```bash
# From mno_data_source_simulator directory
cd tests/integration && docker-compose -f docker-compose-test.yml up -d && sleep 5
cd ../.. && pytest tests/integration/ -v -m integration
cd tests/integration && docker-compose -f docker-compose-test.yml down -v
```

## View Coverage

```bash
open htmlcov/index.html  # Target: >90% for core modules
```
