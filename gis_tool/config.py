"""
config.py

Configuration constants and utilities for GIS pipeline processing.

Supports overriding settings via environment variables,
`config.ini`, and `config.yaml` files with priority:
Environment > config.ini > config.yaml > defaults.

Includes utilities to parse environment variables as
typed values and helpers for spatial file formats.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import os
import sys
import configparser
from typing import Union

import yaml

# Global state for configs
config = configparser.ConfigParser()
config_yaml = {}


def getenv_or_config(section: str, key: str, default: Union[str, int, float, bool]) -> Union[str, int, float, bool]:
    """
    Retrieve configuration value in this priority:
    1) Environment variable (uppercase key)
    2) YAML config (section/key)
    3) INI config (section/key)
    4) default
    """
    env_val = os.getenv(key.upper())
    if env_val is not None:
        return env_val

    if section in config_yaml and key in config_yaml[section]:
        return config_yaml[section][key]

    if config.has_option(section, key):
        return config.get(section, key)

    return default



def getenv_float(var_name: str, default: float, section: str = 'DEFAULT') -> float:
    val = os.getenv(var_name.upper())
    if val is not None:
        try:
            return float(val)
        except ValueError:
            return default

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            try:
                return float(val_yaml)
            except (ValueError, TypeError):
                return default

    if config.has_option(section, var_name.lower()):
        try:
            return config.getfloat(section, var_name.lower())
        except ValueError:
            return default

    # fallback: look in root YAML keys
    val_yaml = config_yaml.get(var_name.lower())
    if val_yaml is not None:
        try:
            return float(val_yaml)
        except (ValueError, TypeError):
            return default

    return default


def getenv_int(var_name: str, default: int, section: str = 'DEFAULT') -> int:
    val = os.getenv(var_name.upper())
    if val is not None:
        try:
            return int(val)
        except ValueError:
            return default

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            try:
                return int(val_yaml)
            except (ValueError, TypeError):
                return default

    if config.has_option(section, var_name.lower()):
        try:
            return config.getint(section, var_name.lower())
        except ValueError:
            return default

    val_yaml = config_yaml.get(var_name.lower())
    if val_yaml is not None:
        try:
            return int(val_yaml)
        except (ValueError, TypeError):
            return default

    return default


def getenv_bool(var_name: str, default: bool, section: str = 'DEFAULT') -> bool:
    val = os.getenv(var_name.upper())
    if val is not None:
        return val.strip().lower() in ['1', 'true', 'yes', 'on']

    if section in config_yaml and isinstance(config_yaml[section], dict):
        val_yaml = config_yaml[section].get(var_name.lower())
        if val_yaml is not None:
            if isinstance(val_yaml, bool):
                return val_yaml
            if isinstance(val_yaml, str):
                return val_yaml.strip().lower() in ['1', 'true', 'yes', 'on']

    if config.has_option(section, var_name.lower()):
        val_ini = config.get(section, var_name.lower())
        return val_ini.strip().lower() in ['1', 'true', 'yes', 'on']

    val_yaml_root = config_yaml.get(var_name.lower())
    if val_yaml_root is not None:
        if isinstance(val_yaml_root, bool):
            return val_yaml_root
        if isinstance(val_yaml_root, str):
            return val_yaml_root.strip().lower() in ['1', 'true', 'yes', 'on']

    return default

def get_driver_from_extension(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".geojson":
        return "GeoJSON"
    return "ESRI Shapefile"

def _get_output_formats() -> list[str]:
    raw_val = getenv_or_config('OUTPUT', 'output_format', 'shp')

    if isinstance(raw_val, str):
        return [fmt.strip().lower() for fmt in raw_val.split(',') if fmt.strip()]
    elif isinstance(raw_val, list):
        return [str(fmt).strip().lower() for fmt in raw_val if str(fmt).strip()]
    else:
        return ['shp', 'geojson']

# Default placeholders
MONGODB_URI: str = 'mongodb://localhost:27017/'
DB_NAME: str = 'gis_database'
DEFAULT_CRS: str = 'EPSG:4326'
GEOGRAPHIC_CRS: str = 'EPSG:4326'
BUFFER_LAYER_CRS: str = DEFAULT_CRS
DEFAULT_BUFFER_DISTANCE_FT: float = 25.0
LOG_FILENAME: str = 'pipeline_processing.log'
LOG_LEVEL: str = 'INFO'
MAX_WORKERS: int = 5
PARALLEL: bool = False
OUTPUT_FORMATS: list[str] = ['shp']
OUTPUT_FORMAT: str = OUTPUT_FORMATS[0]
ALLOW_OVERWRITE_OUTPUT: bool = False
DRY_RUN_MODE: bool = False

def reload_config():
    global config, config_yaml
    global MONGODB_URI, DB_NAME
    global DEFAULT_CRS, GEOGRAPHIC_CRS, BUFFER_LAYER_CRS, DEFAULT_BUFFER_DISTANCE_FT
    global LOG_FILENAME, LOG_LEVEL
    global MAX_WORKERS, PARALLEL
    global OUTPUT_FORMATS, OUTPUT_FORMAT
    global ALLOW_OVERWRITE_OUTPUT, DRY_RUN_MODE

    # Load INI config
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Load YAML config
    try:
        with open('config.yaml', 'r') as f:
            config_yaml = yaml.safe_load(f) or {}
    except (FileNotFoundError, yaml.YAMLError):
        config_yaml = {}

    # Load all config variables using priority Env > YAML > INI > default
    MONGODB_URI = getenv_or_config('DATABASE', 'mongodb_uri', 'mongodb://localhost:27017/')
    DB_NAME = getenv_or_config('DATABASE', 'db_name', 'gis_database')

    DEFAULT_CRS = getenv_or_config('SPATIAL', 'default_crs', 'EPSG:4326')
    GEOGRAPHIC_CRS = getenv_or_config('SPATIAL', 'geographic_crs', 'EPSG:4326')
    BUFFER_LAYER_CRS = getenv_or_config('SPATIAL', 'buffer_layer_crs', DEFAULT_CRS)
    DEFAULT_BUFFER_DISTANCE_FT = getenv_float('default_buffer_distance_ft', 25.0, section='SPATIAL')

    LOG_FILENAME = getenv_or_config('LOGGING', 'log_filename', 'pipeline_processing.log')
    LOG_LEVEL = getenv_or_config('LOGGING', 'log_level', 'INFO').upper()

    MAX_WORKERS = getenv_int('max_workers', 5)
    PARALLEL = getenv_bool('parallel', False)
    ALLOW_OVERWRITE_OUTPUT = getenv_bool('allow_overwrite_output', False)
    DRY_RUN_MODE = getenv_bool('dry_run_mode', False)

    OUTPUT_FORMATS = _get_output_formats()
    OUTPUT_FORMAT = OUTPUT_FORMATS[0] if OUTPUT_FORMATS else 'shp'

    return sys.modules[__name__]

# Initialize config on import
reload_config()
