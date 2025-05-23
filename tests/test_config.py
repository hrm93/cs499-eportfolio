# tests/test_config.py

"""
Unit tests for the gis_tool.config module.

These tests verify that configuration constants:
- Provide correct default values when environment variables are not set.
- Correctly override defaults when environment variables are set.
- Gracefully handle invalid environment variable values (e.g., invalid float conversion).

Tests use pytest fixtures and monkeypatching to isolate environment variable effects,
and dynamically reload the config module to ensure environment changes are applied.
"""

import pytest
import importlib
import gis_tool.config as config

@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    """
    Fixture to clear relevant environment variables before each test,
    ensuring no cross-test pollution from prior environment settings.
    """
    keys = [
        "MONGODB_URI",
        "DB_NAME",
        "DEFAULT_CRS",
        "DEFAULT_BUFFER_DISTANCE_FT",
        "LOG_FILENAME"
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield

def reload_config():
    """
    Reload the gis_tool.config module to re-import environment variables.

    Returns:
        module: The reloaded config module.
    """
    importlib.reload(config)
    return config

@pytest.mark.parametrize("env_var, env_value, expected", [
    ("MONGODB_URI", None, "mongodb://localhost:27017/"),
    ("MONGODB_URI", "mongodb://testhost:1234/", "mongodb://testhost:1234/"),
    ("DB_NAME", None, "gis_database"),
    ("DB_NAME", "test_db", "test_db"),
    ("DEFAULT_CRS", None, "EPSG:32633"),
    ("DEFAULT_CRS", "EPSG:4326", "EPSG:4326"),
    ("DEFAULT_BUFFER_DISTANCE_FT", None, 25.0),
    ("DEFAULT_BUFFER_DISTANCE_FT", "50.5", 50.5),
    ("LOG_FILENAME", None, "pipeline_processing.log"),
    ("LOG_FILENAME", "custom.log", "custom.log"),
])
def test_env_var_override(monkeypatch, env_var, env_value, expected):
    """
       Test that configuration constants return the expected value based on
       the presence or absence of their corresponding environment variables.

       Args:
           monkeypatch: pytest monkeypatch fixture for modifying environment variables.
           env_var (str): Environment variable name to set or clear.
           env_value (str|None): Value to set the environment variable to, or None to clear it.
           expected (any): The expected resulting value from the config constant.
       """
    if env_value is not None:
        monkeypatch.setenv(env_var, env_value)
    else:
        monkeypatch.delenv(env_var, raising=False)
    new_config = reload_config()
    value = getattr(new_config, env_var)
    # Convert numeric string to float for buffer distance test
    if env_var == "DEFAULT_BUFFER_DISTANCE_FT" and isinstance(value, str):
        value = float(value)
    assert value == expected

def test_default_buffer_distance_invalid(monkeypatch):
    """
    Test that an invalid value for DEFAULT_BUFFER_DISTANCE_FT environment variable
    falls back to the default numeric value of 25.0.
    """
    monkeypatch.setenv("DEFAULT_BUFFER_DISTANCE_FT", "not_a_number")
    new_config = reload_config()
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 25.0
