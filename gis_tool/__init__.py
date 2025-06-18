"""
gis_tool/__init__.py

Minimal package initialization for gis_tool.

Only exposes core configuration constants to avoid import-related issues
and circular dependencies.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

from . import config

__all__ = ["config"]
