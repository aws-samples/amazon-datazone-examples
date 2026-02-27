# Unit Tests for SMUS-Collibra Integration

This directory contains unit tests for the SMUS-Collibra Integration Lambda functions.

## Setup

Install test dependencies:

```bash
pip install -r requirements-test.txt
```

## Running Tests

Run all tests:
```bash
pytest
```

Run tests with coverage (default configuration includes coverage):
```bash
pytest
```

Run specific test file:
```bash
pytest tests/adapter/test_collibra_adapter.py
```

Run tests with specific marker:
```bash
pytest -m unit
```

Run tests without coverage:
```bash
pytest --no-cov
```

## Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures and configuration
├── adapter/                 # Tests for adapter layer
├── business/                # Tests for business logic
├── handler/                 # Tests for Lambda handlers
├── model/                   # Tests for data models
└── utils/                   # Tests for utility functions
```

## Writing Tests

- Use descriptive test names: `test_<function_name>_<scenario>_<expected_result>`
- Use fixtures from `conftest.py` for common setup (mock_logger, mock_env_vars)
- Use `unittest.mock` for mocking external dependencies (AWS services, HTTP requests)
- Mark unit tests with `@pytest.mark.unit`

Example:
```python
import pytest
from unittest.mock import MagicMock, patch

@pytest.mark.unit
def test_example_function(mock_logger, mock_env_vars):
    # Your test code here
    pass
```

## Coverage Reports

Coverage reports are generated automatically. View the HTML report:
```bash
open htmlcov/index.html
```
