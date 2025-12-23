"""
Pytest configuration and fixtures for memory price monitor tests.
"""

import pytest
from hypothesis import settings, Verbosity
import tempfile
import shutil
from pathlib import Path
import os

# Configure Hypothesis for faster test runs
settings.register_profile("fast", max_examples=5, deadline=5000, verbosity=Verbosity.quiet)
settings.register_profile("thorough", max_examples=100, deadline=30000, verbosity=Verbosity.normal)

# Use fast profile by default
settings.load_profile("fast")


@pytest.fixture(scope="session")
def temp_db_dir():
    """Create a temporary directory for test databases."""
    temp_dir = tempfile.mkdtemp(prefix="memory_price_monitor_test_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def temp_db_path(temp_db_dir):
    """Create a temporary database path for each test."""
    db_path = Path(temp_db_dir) / f"test_{os.getpid()}_{id(temp_db_dir)}.db"
    yield str(db_path)
    # Cleanup is handled by temp_db_dir fixture


@pytest.fixture(scope="session")
def test_config():
    """Provide test configuration."""
    return {
        "database": {
            "type": "sqlite",
            "path": ":memory:"  # Use in-memory database for tests
        },
        "crawler": {
            "request_timeout": 5,
            "retry_attempts": 2,
            "retry_delay": 0.1,
            "rate_limit_delay": 0.1
        },
        "notification": {
            "retry_attempts": 2,
            "retry_delay": 0.1
        }
    }


def pytest_configure(config):
    """Configure pytest with custom settings."""
    # Set environment variable to use fast Hypothesis profile
    os.environ["HYPOTHESIS_PROFILE"] = "fast"
    
    # Configure logging for tests
    import logging
    logging.getLogger("memory_price_monitor").setLevel(logging.WARNING)
    logging.getLogger("hypothesis").setLevel(logging.WARNING)


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers."""
    for item in items:
        # Mark property-based tests
        if "property" in item.name.lower() or "test_" in item.name and any(
            marker.name == "given" for marker in item.iter_markers()
        ):
            item.add_marker(pytest.mark.property)
        
        # Mark slow tests
        if "integration" in item.name.lower() or item.fspath.basename.startswith("test_") and "integration" in item.fspath.basename:
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)


# Pytest command line options
def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests"
    )


def pytest_sessionstart(session):
    """Configure session based on command line options."""
    # Always use fast profile for checkpoint tests
    settings.load_profile("fast")
    print("Using Hypothesis profile: fast")