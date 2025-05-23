# tests/test_config.py

"""
Unit tests for the gis_tool.config module.

These tests verify:
- Default values when environment variables are not set.
- Correct overrides when environment variables are explicitly set.
- Graceful fallback for invalid inputs (e.g., invalid float, int, or boolean).
- Boolean environment variables accept common truthy/falsey formats.

All tests isolate changes using monkeypatching and module reloading.
"""

import pytest
import importlib
import gis_tool.config as config


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    """
    Clears environment variables before each test to prevent side effects.
    """
    keys = [
        "MONGODB_URI",
        "DB_NAME",
        "DEFAULT_CRS",
        "DEFAULT_BUFFER_DISTANCE_FT",
        "LOG_FILENAME",
        "LOG_LEVEL",
        "MAX_WORKERS",
        "OUTPUT_FORMAT",
        "ALLOW_OVERWRITE_OUTPUT",
        "DRY_RUN_MODE",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def reload_config():
    """
    Reloads the gis_tool.config module after environment changes.

    Returns:
        module: The reloaded module.
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
    ("LOG_LEVEL", None, "INFO"),
    ("LOG_LEVEL", "debug", "DEBUG"),
    ("MAX_WORKERS", None, 2),
    ("MAX_WORKERS", "4", 4),
    ("OUTPUT_FORMAT", None, "shp"),
    ("OUTPUT_FORMAT", "GeoJSON", "geojson"),
    ("ALLOW_OVERWRITE_OUTPUT", None, False),
    ("ALLOW_OVERWRITE_OUTPUT", "true", True),
    ("DRY_RUN_MODE", None, False),
    ("DRY_RUN_MODE", "yes", True),
])


def test_env_var_override(monkeypatch, env_var, env_value, expected):
    """
    Parametric test to verify environment variables override config values.

    Args:
        monkeypatch: pytest monkeypatch fixture.
        env_var (str): Name of the environment variable to test.
        env_value (str|None): Value to set the variable to (None means unset).
        expected (Any): Expected config value after applying the env var.
    """
    if env_value is not None:
        monkeypatch.setenv(env_var, env_value)
    else:
        monkeypatch.delenv(env_var, raising=False)
    new_config = reload_config()
    value = getattr(new_config, env_var)
    assert value == expected


@pytest.mark.parametrize("bad_value", ["abc", "", "12.3.4"])
def test_default_buffer_distance_invalid(monkeypatch, bad_value):
    """
    Ensure DEFAULT_BUFFER_DISTANCE_FT falls back to 25.0 if value is not a valid float.
    """
    monkeypatch.setenv("DEFAULT_BUFFER_DISTANCE_FT", bad_value)
    new_config = reload_config()
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 25.0


@pytest.mark.parametrize("bad_value", ["abc", "", "10.5.3"])
def test_max_workers_invalid(monkeypatch, bad_value):
    """
    Ensure MAX_WORKERS falls back to 2 if value is not a valid integer.
    """
    monkeypatch.setenv("MAX_WORKERS", bad_value)
    new_config = reload_config()
    assert new_config.MAX_WORKERS == 2


@pytest.mark.parametrize("env_value,expected", [
    ("true", True),
    ("1", True),
    ("yes", True),
    ("on", True),
    ("false", False),
    ("0", False),
    ("no", False),
    ("off", False),
    ("", False),
    ("nonsense", False),
])


def test_boolean_parsing(monkeypatch, env_value, expected):
    """
    Test boolean environment parsing for DRY_RUN_MODE.
    """
    monkeypatch.setenv("DRY_RUN_MODE", env_value)
    new_config = reload_config()
    assert new_config.DRY_RUN_MODE == expected
