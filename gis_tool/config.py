# config.py
"""
Configuration constants for GIS pipeline processing.
Supports overriding via environment variables.

Environment Variables:
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


def getenv_float(var_name: str, default: float) -> float:
    """
       Get an environment variable as a float.

       Args:
           var_name (str): The name of the environment variable.
           default (float): The default value to use if the variable is not set or invalid.

       Returns:
           float: The float value of the environment variable or the default.
       """
    try:
        return float(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default


def getenv_int(var_name: str, default: int) -> int:
    """
       Get an environment variable as an integer.

       Args:
           var_name (str): The name of the environment variable.
           default (int): The default value to use if the variable is not set or invalid.

       Returns:
           int: The integer value of the environment variable or the default.
       """
    try:
        return int(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default


def getenv_bool(var_name: str, default: bool) -> bool:
    """
       Get an environment variable as a boolean.

       Accepts common truthy values like '1', 'true', 'yes', 'on' (case-insensitive).

       Args:
           var_name (str): The name of the environment variable.
           default (bool): The default value to use if the variable is not set or invalid.

       Returns:
           bool: The boolean value of the environment variable or the default.
       """
    val = os.getenv(var_name)
    if val is None:
        return default
    return val.strip().lower() in ['1', 'true', 'yes', 'on']


# MongoDB connection settings
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
"""str: URI to connect to MongoDB."""

DB_NAME = os.getenv('DB_NAME', 'gis_database')
"""str: Name of the MongoDB database."""

# Spatial settings
DEFAULT_CRS = os.getenv('DEFAULT_CRS', 'EPSG:32633')
"""str: Default Coordinate Reference System for spatial data."""

DEFAULT_BUFFER_DISTANCE_FT = getenv_float('DEFAULT_BUFFER_DISTANCE_FT', 25.0)
"""float: Default buffer distance around gas lines in feet."""

# Logging settings
LOG_FILENAME = os.getenv('LOG_FILENAME', 'pipeline_processing.log')
"""str: Path to the log file."""

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
"""str: Logging level (e.g., 'DEBUG', 'INFO')."""

# Parallel processing
MAX_WORKERS = getenv_int('MAX_WORKERS', 2)
"""int: Default number of parallel worker processes."""

OUTPUT_FORMAT = os.getenv('OUTPUT_FORMAT', 'shp').lower()
"""str: Default output format for buffer file: 'shp' or 'geojson'."""

ALLOW_OVERWRITE_OUTPUT = getenv_bool('ALLOW_OVERWRITE_OUTPUT', False)
"""bool: Whether existing output files can be overwritten."""

DRY_RUN_MODE = getenv_bool('DRY_RUN_MODE', False)
"""bool: Whether to enable dry-run mode (no file writes or DB operations)."""
