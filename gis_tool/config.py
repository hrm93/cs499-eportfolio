"""
config.py

Configuration constants and utilities for GIS pipeline processing.

Supports overriding settings via environment variables,
`config.ini`, and `config.yaml` files with priority:
Environment > config.ini > config.yaml > defaults.

Includes utilities to parse environment variables as
typed values and helpers for spatial file formats.

Author: Hannah Rose Morgenstein
Date: 2025-06-18
"""

import os
import configparser
from typing import cast

import yaml

# Initialize configparser and load config.ini if present
config = configparser.ConfigParser()
config.read('config.ini')

# Load config.yaml if present
config_yaml = {}
if os.path.exists('config.yaml'):
    with open('config.yaml', 'r') as f:
        config_yaml = yaml.safe_load(f) or {}

def getenv_or_config(section: str, key: str, default):
    """
    Retrieve configuration value from environment variable,
    then config.ini, then config.yaml, else default.

    Args:
        section (str): Config file section name.
        key (str): Configuration key name.
        default: Default value if not found.

    Returns:
        The resolved configuration value.
    """
    env_val = os.getenv(key.upper())
    if env_val is not None:
        return env_val
    if config.has_option(section, key):
        return config.get(section, key)
    return config_yaml.get(section, {}).get(key, default)

def getenv_float(var_name: str, default: float, section='DEFAULT') -> float:
    """
    Retrieve a float configuration value from environment,
    config.ini, or config.yaml.

    Args:
        var_name (str): Variable name.
        default (float): Default value if unset or invalid.
        section (str): Config file section to check.

    Returns:
        float: Parsed float or default.
    """
    val = os.getenv(var_name)
    if val is not None:
        try:
            return float(val)
        except ValueError:
            return default

    if config.has_option(section, var_name.lower()):
        try:
            return config.getfloat(section, var_name.lower())
        except ValueError:
            return default

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            try:
                return float(val_yaml)
            except (ValueError, TypeError):
                return default

    val_yaml = config_yaml.get(var_name)
    if val_yaml is not None:
        try:
            return float(cast(str, val_yaml))
        except (ValueError, TypeError):
            return default

    return default

def getenv_int(var_name: str, default: int, section='DEFAULT') -> int:
    """
    Retrieve an integer configuration value from environment,
    config.ini, or config.yaml.

    Args:
        var_name (str): Variable name.
        default (int): Default if unset or invalid.
        section (str): Config file section.

    Returns:
        int: Parsed integer or default.
    """
    val = os.getenv(var_name)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            return default

    if config.has_option(section, var_name.lower()):
        try:
            return config.getint(section, var_name.lower())
        except ValueError:
            return default

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            try:
                return int(val_yaml)
            except (ValueError, TypeError):
                return default

    val_yaml = config_yaml.get(var_name)
    if val_yaml is not None:
        try:
            return int(cast(str, val_yaml))
        except (ValueError, TypeError):
            return default

    return default

def getenv_bool(var_name: str, default: bool, section='DEFAULT') -> bool:
    """
    Retrieve a boolean configuration value from environment,
    config.ini, or config.yaml.

    Recognizes '1', 'true', 'yes', 'on' (case-insensitive) as True.

    Args:
        var_name (str): Variable name.
        default (bool): Default if unset or invalid.
        section (str): Config file section.

    Returns:
        bool: Parsed boolean or default.
    """
    val = os.getenv(var_name)
    if val is not None:
        return val.strip().lower() in ['1', 'true', 'yes', 'on']

    if config.has_option(section, var_name.lower()):
        val = config.get(section, var_name.lower())
        return val.strip().lower() in ['1', 'true', 'yes', 'on']

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            if isinstance(val_yaml, bool):
                return val_yaml
            if isinstance(val_yaml, str):
                return val_yaml.strip().lower() in ['1', 'true', 'yes', 'on']

    val_yaml = config_yaml.get(var_name)
    if val_yaml is not None:
        if isinstance(val_yaml, bool):
            return val_yaml
        if isinstance(val_yaml, str):
            return val_yaml.strip().lower() in ['1', 'true', 'yes', 'on']

    return default

def get_driver_from_extension(path: str) -> str:
    """
    Determine GDAL driver string based on file extension.

    Args:
        path (str): Output file path.

    Returns:
        str: Driver string ('GeoJSON' or 'ESRI Shapefile').
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".geojson":
        return "GeoJSON"
    return "ESRI Shapefile"

def _get_output_formats() -> list[str]:
    """
    Parse output formats from config sources.

    Supports comma-separated strings or lists, normalized to lowercase.

    Returns:
        list[str]: List of output formats.
    """
    raw_val = getenv_or_config('OUTPUT', 'output_format', 'shp')

    if isinstance(raw_val, str):
        # Split comma-separated string into list
        return [fmt.strip().lower() for fmt in raw_val.split(',') if fmt.strip()]
    elif isinstance(raw_val, list):
        # List of formats
        return [str(fmt).strip().lower() for fmt in raw_val if str(fmt).strip()]
    else:
        return ['shp']

# MongoDB connection settings
MONGODB_URI = getenv_or_config('DATABASE', 'mongodb_uri', 'mongodb://localhost:27017/')
"""str: MongoDB connection URI."""

DB_NAME = getenv_or_config('DATABASE', 'db_name', 'gis_database')
"""str: MongoDB database name."""

# Spatial settings
DEFAULT_CRS = getenv_or_config('SPATIAL', 'default_crs', 'EPSG:4326')
"""str: Default coordinate reference system (CRS)."""

GEOGRAPHIC_CRS = getenv_or_config('SPATIAL', 'geographic_crs', 'EPSG:4326')
"""str: Geographic CRS for latitude/longitude data."""

BUFFER_LAYER_CRS = getenv_or_config('SPATIAL', 'buffer_layer_crs', DEFAULT_CRS)
"""str: CRS to assume for buffers if missing."""

DEFAULT_BUFFER_DISTANCE_FT = getenv_float('DEFAULT_BUFFER_DISTANCE_FT', 25.0, section='SPATIAL')
"""float: Default buffer distance in feet."""

# Logging settings
LOG_FILENAME = getenv_or_config('LOGGING', 'log_filename', 'pipeline_processing.log')
"""str: Log file name."""

LOG_LEVEL = getenv_or_config('LOGGING', 'log_level', 'INFO').upper()
"""str: Logging level (e.g., 'DEBUG', 'INFO')."""

# Parallel processing
MAX_WORKERS = getenv_int('MAX_WORKERS', 5)
"""int: Number of parallel worker processes."""

PARALLEL = getenv_bool('PARALLEL', False)
"""bool: Enable multiprocessing for processing."""

# Output format settings
OUTPUT_FORMATS = _get_output_formats()
"""list[str]: List of output formats, e.g. ['shp', 'geojson']."""

# Legacy single output format for backward compatibility
OUTPUT_FORMAT = OUTPUT_FORMATS[0]
"""str: Primary output format."""

ALLOW_OVERWRITE_OUTPUT = getenv_bool('ALLOW_OVERWRITE_OUTPUT', False)
"""bool: Allow overwriting existing output files."""

DRY_RUN_MODE = getenv_bool('DRY_RUN_MODE', False)
"""bool: Enable dry-run mode (no file or DB writes)."""
