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
"""

import os

def getenv_float(var_name: str, default: float) -> float:
    """Helper to get environment variable as float with fallback."""
    try:
        return float(os.getenv(var_name, str(default)))
    except (TypeError, ValueError):
        return default

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'gis_database')
DEFAULT_CRS = os.getenv('DEFAULT_CRS', 'EPSG:32633')
DEFAULT_BUFFER_DISTANCE_FT = getenv_float('DEFAULT_BUFFER_DISTANCE_FT', 25.0)
LOG_FILENAME = os.getenv('LOG_FILENAME', 'pipeline_processing.log')
