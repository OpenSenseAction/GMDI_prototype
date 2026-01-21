# Testing Strategy

## Approach

**Priority 1: Unit Tests** - Test components independently with mocks (fast, no infrastructure)
**Priority 2: Integration Tests** - Test with real SFTP server (validates actual behavior)
**Priority 3: E2E Tests** - Full pipeline testing (planned)

## Current Status

### ✅ Unit Tests (`tests/test_sftp_uploader.py`, `tests/test_generator.py`)
- 12 SFTP uploader tests (92% coverage) + data generator tests
- Mock external dependencies (paramiko)
- Test initialization, uploads, error handling, file archiving
- Run: `pytest tests/ -v -m "not integration"`

### ✅ Integration Tests (`tests/integration/test_sftp_integration.py`)
- 4 tests with real SFTP server (Docker)
- Verify actual network operations and file transfers
- SFTP uploader automatically creates remote directories
- Run: See `tests/integration/README.md`

---

### 🔄 Priority 3: End-to-End Tests (Planned)
**Goal:** Full pipeline from data generation → SFTP upload → reception → processing → database

**Approach:**
- Add SFTP receiver (separate Docker service recommended)
- Test complete workflow with all services running
- Verify data appears correctly in database and dashboards

---

## Quick Commands

See `tests/README.md` for command reference.

## Coverage Targets

- `sftp_uploader.py`: >90% ✅ (currently 92%)
- `data_generator.py`: >90%
- `main.py`: >70%

## CI/CD

GitHub Actions workflow (`.github/workflows/test_mno_data_source_simulator.yml`):
- **Unit tests** run first for fast feedback
- **Integration tests** run only if unit tests pass
- Both run on PRs and pushes to main branch

## Next Steps

1. ✅ Unit tests for SFTP uploader
2. ✅ Integration test infrastructure
3. ✅ CI/CD pipeline with Docker integration tests
4. 🔄 Add SFTP receiver to webserver
5. 🔄 End-to-end integration tests

## Architecture Decision

**SFTP Receiver:** Use separate Docker service (not embedded in Flask)
- Simpler to test independently
- Better separation of concerns
- Can use standard SFTP server image
