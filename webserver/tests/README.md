# Webserver Tests

## Running Tests

### Install Test Dependencies

```bash
pip install -r requirements-test.txt
```

### Run All Tests

```bash
pytest tests/ -v
```

### Run with Coverage

```bash
pytest tests/ --cov=. --cov-report=html --cov-report=term
```

View coverage report:
```bash
open htmlcov/index.html
```

### Run Specific Test Files

```bash
# Storage backend tests only
pytest tests/test_storage.py -v

# Specific test class
pytest tests/test_storage.py::TestLocalStorageBackend -v

# Specific test
pytest tests/test_storage.py::TestLocalStorageBackend::test_write_and_read_file -v
```

## Test Structure

- `test_storage.py` - Storage abstraction layer tests
  - `TestLocalStorageBackend` - Local filesystem backend tests
  - `TestS3StorageBackend` - S3/MinIO backend tests (mocked)
  - `TestGetStorageBackend` - Factory function tests

## Coverage Goals

- Storage backends: >90%
- File processing: >85%
- Overall: >80%
