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
import importlib
import pytest

import gis_tool.config as config
from gis_tool import logger


@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    """
    Pytest fixture to set up logging for the test session.

    This fixture automatically initializes the logging configuration
    defined in the gis_tool.logger module before any tests run.
    """
    logger.setup_logging()


@pytest.fixture(autouse=True)
def clear_env(monkeypatch, tmp_path):
    """
    Clears environment variables and provides a temporary config.ini for each test.
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

    # Write a temporary config.ini file
    config_file = tmp_path / "config.ini"
    config_file.write_text("""
[DEFAULT]
output_format = shp
allow_overwrite_output = false
dry_run_mode = false
max_workers = 2

[DATABASE]
mongodb_uri = mongodb://localhost:27017/
db_name = test_db_ini

[SPATIAL]
default_crs = EPSG:32633
geographic_crs = EPSG:4326
buffer_layer_crs = EPSG:32633
default_buffer_distance_ft = 50.0

[LOGGING]
log_filename = test.log
log_level = debug

[OUTPUT]
output_format = geojson
""")

    monkeypatch.chdir(tmp_path)  # Switch to the tmp_path with config.ini
    yield


def reload_config():
    """
    Reloads the gis_tool.config module after environment or config.ini changes.

    Returns:
        module: The reloaded module.
    """
    importlib.reload(config)
    return config


def test_config_ini_defaults(monkeypatch):
    """
    Test that config.ini values are used when no environment variables are set.
    """
    new_config = reload_config()

    assert new_config.DB_NAME == "test_db_ini"
    assert new_config.MONGODB_URI == "mongodb://localhost:27017/"
    assert new_config.DEFAULT_CRS == "EPSG:32633"
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 50.0
    assert new_config.LOG_FILENAME == "test.log"
    assert new_config.LOG_LEVEL == "DEBUG"
    assert new_config.MAX_WORKERS == 2
    assert new_config.OUTPUT_FORMAT == "geojson"
    assert new_config.ALLOW_OVERWRITE_OUTPUT is False
    assert new_config.DRY_RUN_MODE is False


@pytest.mark.parametrize("env_var, env_value, expected", [
    ("DB_NAME", "test_db", "test_db"),
    ("MONGODB_URI", "mongodb://testhost:1234/", "mongodb://testhost:1234/"),
    ("DEFAULT_BUFFER_DISTANCE_FT", "60.5", 60.5),
    ("DEFAULT_CRS", "EPSG:4326", "EPSG:4326"),
    ("LOG_FILENAME", "custom.log", "custom.log"),
    ("LOG_LEVEL", "info", "INFO"),
    ("MAX_WORKERS", "4", 4),
    ("OUTPUT_FORMAT", "Shp", "shp"),
    ("ALLOW_OVERWRITE_OUTPUT", "true", True),
    ("DRY_RUN_MODE", "yes", True),
])
def test_env_var_override(monkeypatch, env_var, env_value, expected):
    """
    Parametric test to verify environment variables override config.ini values.
    """
    monkeypatch.setenv(env_var, env_value)
    new_config = reload_config()
    value = getattr(new_config, env_var)
    assert value == expected


@pytest.mark.parametrize("env_value, expected", [
    ("true", True), ("1", True), ("yes", True), ("on", True),
    ("false", False), ("0", False), ("no", False), ("off", False), ("", False),
    ("nonsense", False),
])
def test_boolean_parsing(monkeypatch, env_value, expected):
    """
    Test boolean environment parsing for DRY_RUN_MODE.
    """
    monkeypatch.setenv("DRY_RUN_MODE", env_value)
    new_config = reload_config()
    assert new_config.DRY_RUN_MODE == expected


@pytest.mark.parametrize("bad_value", ["abc", "", "12.3.4"])
def test_default_buffer_distance_invalid(monkeypatch, bad_value):
    """
    Ensure DEFAULT_BUFFER_DISTANCE_FT falls back to 25.0 if value is invalid.
    """
    monkeypatch.setenv("DEFAULT_BUFFER_DISTANCE_FT", bad_value)
    new_config = reload_config()
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 25.0


@pytest.mark.parametrize("bad_value", ["abc", "", "10.5.3"])
def test_max_workers_invalid(monkeypatch, bad_value):
    """
    Ensure MAX_WORKERS falls back to 2 if value is invalid.
    """
    monkeypatch.setenv("MAX_WORKERS", bad_value)
    new_config = reload_config()
    assert new_config.MAX_WORKERS == 2
