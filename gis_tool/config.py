# config.py
"""
Configuration constants for GIS pipeline processing.
Supports overriding via environment variables and a config.ini file.

Environment Variables (override config.ini):
- MONGODB_URI: MongoDB connection URI (default: 'mongodb://localhost:27017/')
- DB_NAME: MongoDB database name (default: 'gis_database')
- DEFAULT_CRS: Default Coordinate Reference System (default: 'EPSG:32633')
- DEFAULT_BUFFER_DISTANCE_FT: Default buffer distance in feet (default: 25.0)
- LOG_FILENAME: Log file name (default: 'pipeline_processing.log')
- LOG_LEVEL: Logging level (default: 'INFO')
- MAX_WORKERS: Default number of parallel workers (default: 2)
- OUTPUT_FORMAT: Default output format (default: 'shp')
- ALLOW_OVERWRITE_OUTPUT: Allow overwriting existing output (default: False)
- DRY_RUN_MODE: Enable dry run mode (default: False)
"""
import os
import configparser
from typing import cast

import yaml

# Load config.ini if it exists
config = configparser.ConfigParser()
config.read('config.ini')

# Load config.yaml if it exists
config_yaml = {}
if os.path.exists('config.yaml'):
    with open('config.yaml', 'r') as f:
        config_yaml = yaml.safe_load(f) or {}


def getenv_or_config(section, key, default):
    """
    Get value from environment variable or config file.

    Priority: ENV > config.ini > default
    """
    env_val = os.getenv(key.upper())
    if env_val is not None:
        return env_val
    if config.has_option(section, key):
        return config.get(section, key)
    # fallback to YAML
    return config_yaml.get(section, {}).get(key, default)


def getenv_float(var_name: str, default: float, section='DEFAULT') -> float:
    """
    Get an environment variable as a float.

    Args:
        var_name (str): The name of the environment variable.
        default (float): The default value to use if the variable is not set or invalid.
        section (str, optional): The config file section to read the variable from if not found in environment. Defaults to 'DEFAULT'.

    Returns:
        float: The float value of the environment variable or the default.
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
        val_yaml = config_yaml[section].get(var_name.lower(), None)
        if val_yaml is not None:
            try:
                return float(val_yaml)
            except (ValueError, TypeError):
                return default

    val_yaml = config_yaml.get(var_name, None)
    if val_yaml is not None:
        try:
            return float(cast(str, val_yaml))
        except (ValueError, TypeError):
            return default

    return default


def getenv_int(var_name: str, default: int, section='DEFAULT') -> int:
    """
    Get an environment variable as an integer.

    Args:
        var_name (str): The name of the environment variable.
        default (int): The default value to use if the variable is not set or invalid.
        section (str, optional): The config file section to read the variable from if not found in environment. Defaults to 'DEFAULT'.

    Returns:
        int: The integer value of the environment variable or the default.
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
        val_yaml = config_yaml[section].get(var_name.lower(), None)
        if val_yaml is not None:
            try:
                return int(val_yaml)
            except (ValueError, TypeError):
                return default

    val_yaml = config_yaml.get(var_name, None)
    if val_yaml is not None:
        try:
            return int(cast(str, val_yaml))
        except (ValueError, TypeError):
            return default

    return default


def getenv_bool(var_name: str, default: bool, section='DEFAULT') -> bool:
    """
    Get an environment variable as a boolean.

    Accepts common truthy values like '1', 'true', 'yes', 'on' (case-insensitive).

    Args:
        var_name (str): The name of the environment variable.
        default (bool): The default value to use if the variable is not set or invalid.
        section (str, optional): The config file section to read the variable from if not found in environment. Defaults to 'DEFAULT'.

    Returns:
        bool: The boolean value of the environment variable or the default.
    """
    val = os.getenv(var_name)
    if val is not None:
        return val.strip().lower() in ['1', 'true', 'yes', 'on']

    if config.has_option(section, var_name.lower()):
        val = config.get(section, var_name.lower())
        return val.strip().lower() in ['1', 'true', 'yes', 'on']

    # Try to get from config_yaml under the section if it exists
    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower(), None)
        if val_yaml is not None:
            if isinstance(val_yaml, bool):
                return val_yaml
            if isinstance(val_yaml, str):
                return val_yaml.strip().lower() in ['1', 'true', 'yes', 'on']

    # Fall back to top-level YAML key (for root-level keys like PARALLEL)
    val_yaml = config_yaml.get(var_name, None)
    if val_yaml is not None:
        if isinstance(val_yaml, bool):
            return val_yaml
        if isinstance(val_yaml, str):
            return val_yaml.strip().lower() in ['1', 'true', 'yes', 'on']

    return default


def get_driver_from_extension(path: str) -> str:
    """
    Get the appropriate file driver for saving GeoDataFrames based on file extension.

    Args:
        path (str): File path.

    Returns:
        str: GDAL driver string (e.g., 'GeoJSON' or 'ESRI Shapefile').
    """
    ext = os.path.splitext(path)[1].lower()
    if ext == ".geojson":
        return "GeoJSON"
    return "ESRI Shapefile"


# MongoDB connection settings
MONGODB_URI = getenv_or_config('DATABASE', 'mongodb_uri', 'mongodb://localhost:27017/')
"""str: URI to connect to MongoDB."""

DB_NAME = getenv_or_config('DATABASE', 'db_name', 'gis_database')
"""str: Name of the MongoDB database."""

# Spatial settings
DEFAULT_CRS = getenv_or_config('SPATIAL', 'default_crs', 'EPSG:4326')
"""str: Default Coordinate Reference System for spatial data."""

GEOGRAPHIC_CRS = getenv_or_config('SPATIAL', 'geographic_crs', 'EPSG:4326')
"""str: Geographic CRS for lat/lon data (default: WGS84)."""

BUFFER_LAYER_CRS = getenv_or_config('SPATIAL', 'buffer_layer_crs', DEFAULT_CRS)
"""str: CRS to assume for buffer input if missing (defaults to DEFAULT_CRS)."""

DEFAULT_BUFFER_DISTANCE_FT = getenv_float('DEFAULT_BUFFER_DISTANCE_FT', 25.0, section='SPATIAL')
"""float: Default buffer distance around gas lines in feet."""

# Logging settings
LOG_FILENAME = getenv_or_config('LOGGING', 'log_filename', 'pipeline_processing.log')
"""str: Path to the log file."""

LOG_LEVEL = getenv_or_config('LOGGING', 'log_level', 'INFO').upper()
"""str: Logging level (e.g., 'DEBUG', 'INFO')."""

# Parallel processing
MAX_WORKERS = getenv_int('MAX_WORKERS', 2)
"""int: Default number of parallel worker processes."""

PARALLEL = getenv_bool('PARALLEL', False)
"""bool: Whether to enable multiprocessing for report processing."""

OUTPUT_FORMAT = getenv_or_config('OUTPUT', 'output_format', 'shp').lower()
"""str: Default output format for buffer file: 'shp' or 'geojson'."""

ALLOW_OVERWRITE_OUTPUT = getenv_bool('ALLOW_OVERWRITE_OUTPUT', False)
"""bool: Whether existing output files can be overwritten."""

DRY_RUN_MODE = getenv_bool('DRY_RUN_MODE', False)
"""bool: Whether to enable dry-run mode (no file writes or DB operations)."""
