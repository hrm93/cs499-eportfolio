# gis_tool/__init__.py
"""
Minimal package initialization for gis_tool.

Only exposes core configuration constants to avoid import-related issues
and circular dependencies.
"""
from . import config

__all__ = ["config"]