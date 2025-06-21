"""
Unit tests for the gis_tool.config module.

These tests verify:
- Default values when environment variables are not set.
- Correct overrides when environment variables are explicitly set.
- Graceful fallback for invalid inputs (e.g., invalid float, int, or boolean).
- Boolean environment variables accept common truthy/falsey formats.

All tests isolate changes using monkeypatching and module reloading.
"""
import logging
import pytest
import builtins
import io
import sys

import gis_tool.config
from gis_tool.config import reload_config

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs for test diagnostics


def test_config_ini_defaults(monkeypatch):
    """
    Test that config.ini values are loaded when no environment variables are present.
    """
    logger.debug("Running test_config_ini_defaults")
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
    Test that environment variables override config file values.
    """
    logger.debug(f"Running test_env_var_override with {env_var}={env_value}")
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
    Test DRY_RUN_MODE boolean parsing from various environment string formats.
    """
    logger.debug(f"Running test_boolean_parsing with value '{env_value}' expecting {expected}")
    monkeypatch.setenv("DRY_RUN_MODE", env_value)
    new_config = reload_config()
    assert new_config.DRY_RUN_MODE == expected


@pytest.mark.parametrize("bad_value", ["abc", "", "12.3.4"])
def test_default_buffer_distance_invalid(monkeypatch, bad_value):
    """
    Test fallback to default buffer distance when environment value is invalid.
    """
    logger.debug(f"Running test_default_buffer_distance_invalid with bad value '{bad_value}'")
    monkeypatch.setenv("DEFAULT_BUFFER_DISTANCE_FT", bad_value)
    new_config = reload_config()
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 25.0


@pytest.mark.parametrize("bad_value", ["abc", "", "10.5.3"])
def test_max_workers_invalid(monkeypatch, bad_value):
    """
    Test fallback to default max_workers when value is invalid.
    """
    logger.debug(f"Running test_max_workers_invalid with bad value '{bad_value}'")
    monkeypatch.setenv("MAX_WORKERS", bad_value)
    new_config = reload_config()
    assert new_config.MAX_WORKERS == 5


def test_config_yaml_override(monkeypatch):
    """
    Simulate loading settings from a YAML config file instead of environment or .ini.
    """
    logger.debug("Running test_config_yaml_override")
    valid_yaml_data = """\
SPATIAL:
  default_crs: EPSG:4326
  default_buffer_distance_ft: 25.0
LOGGING:
  log_filename: yaml.log
  log_level: WARNING
"""

    monkeypatch.setattr(gis_tool.config.os.path, "exists", lambda f: f.endswith("config.yaml"))

    monkeypatch.setattr(gis_tool.config.os, "getenv", lambda *args, **kwargs: None)

    monkeypatch.setattr(gis_tool.config.config, "read", lambda filenames: None)

    gis_tool.config.config.clear()

    mod = sys.modules[gis_tool.config.__name__]
    mod.config_yaml = {}

    real_open = builtins.open

    def fake_open(filepath, *args, **kwargs):
        if filepath.endswith("config.yaml"):
            return io.StringIO(valid_yaml_data)
        return real_open(filepath, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    new_config = reload_config()

    assert new_config.DEFAULT_CRS == "EPSG:4326"
    assert new_config.DEFAULT_BUFFER_DISTANCE_FT == 25.0
    assert new_config.LOG_FILENAME == "yaml.log"
    assert new_config.LOG_LEVEL == "WARNING"


def test_invalid_yaml(monkeypatch):
    """
    Test resilience to malformed config.yaml; config should fall back to defaults.
    """
    logger.debug("Running test_invalid_yaml")
    monkeypatch.setattr(gis_tool.config.os.path, "exists", lambda f: True)
    monkeypatch.setattr(gis_tool.config.os, "getenv", lambda *args, **kwargs: None)

    real_open = builtins.open

    def fake_open(filepath, *args, **kwargs):
        if filepath.endswith("config.yaml"):
            return io.StringIO(":::invalid_yaml:::")
        return real_open(filepath, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    gis_tool.config.config_yaml = None

    new_config = reload_config()

    assert isinstance(new_config.DEFAULT_CRS, str)


def test_output_formats_comma_string(monkeypatch):
    """
    Test parsing multiple output formats from comma-separated environment string.
    """
    logger.debug("Running test_output_formats_comma_string")
    monkeypatch.setenv("OUTPUT_FORMAT", "shp, geojson, csv")
    gis_tool.config.config_yaml = None
    new_config = reload_config()
    assert new_config.OUTPUT_FORMATS == ["shp", "geojson", "csv"]


def test_output_formats_list(monkeypatch):
    """
    Test parsing list of formats from YAML.
    """
    logger.debug("Running test_output_formats_list")
    gis_tool.config.config_yaml = None

    monkeypatch.setattr(gis_tool.config.os.path, "exists", lambda f: f.endswith("config.yaml"))
    monkeypatch.setattr(gis_tool.config.os, "getenv", lambda *args, **kwargs: None)

    yaml_data = """
OUTPUT:
  output_format:
    - shp
    - geojson
"""  # no leading indentation

    real_open = builtins.open

    def fake_open(filepath, *args, **kwargs):
        if filepath.endswith("config.yaml"):
            return io.StringIO(yaml_data)
        return real_open(filepath, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    new_config = reload_config()
    assert set(new_config.OUTPUT_FORMATS) == {"shp", "geojson"}


def test_unknown_yaml_keys(monkeypatch):
    """
    Ensure unknown keys in YAML do not raise exceptions or affect core config.
    """
    logger.debug("Running test_unknown_yaml_keys")
    monkeypatch.setattr(gis_tool.config.os.path, "exists", lambda f: f.endswith("config.yaml"))
    monkeypatch.setattr(gis_tool.config.os, "getenv", lambda *args, **kwargs: None)

    yaml_with_unknown = """
UNKNOWN:
  random_value: 42
"""  # no leading indentation

    real_open = builtins.open

    def fake_open(filepath, *args, **kwargs):
        if filepath.endswith("config.yaml"):
            return io.StringIO(yaml_with_unknown)
        return real_open(filepath, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    gis_tool.config.config_yaml = None

    new_config = reload_config()

    assert isinstance(new_config.DEFAULT_CRS, str)
