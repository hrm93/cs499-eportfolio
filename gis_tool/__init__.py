# gis_tool/__init__.py
"""
Minimal package initialization for gis_tool.

Only exposes core configuration constants to avoid import-related issues
and circular dependencies.
"""

from .config import (
    MONGODB_URI,
    DB_NAME,
    DEFAULT_CRS,
    DEFAULT_BUFFER_DISTANCE_FT,
    LOG_FILENAME,
)