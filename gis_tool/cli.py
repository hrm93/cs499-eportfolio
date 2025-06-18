"""
cli.py

Command-line interface for the GIS Pipeline Tool.

This module defines and parses command-line arguments for the GIS pipeline tool
that processes pipeline reports, generates buffer zones around gas lines,
merges buffers into future development layers, and optionally integrates with MongoDB.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import argparse
import os
import json
import yaml

import gis_tool.config as config


def load_config_file(path):
    """
    Load configuration settings from a YAML or JSON file.

    Args:
        path (str): Path to the configuration file.

    Returns:
        dict: Loaded configuration settings.
    """
    with open(path, 'r') as f:
        if path.endswith('.json'):
            return json.load(f)
        else:
            return yaml.safe_load(f)


def parse_args():
    """
    Parse command-line arguments for the GIS pipeline tool.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Process GIS pipeline reports (.txt or .geojson), "
            "generate buffer zones, merge with future development layers, "
            "and optionally integrate with MongoDB."
        )
    )

    # Required arguments
    parser.add_argument(
        '--input-folder', type=str, required=True,
        help="Path to the folder containing pipeline report files."
    )
    parser.add_argument(
        '--output-path', type=str, required=True,
        help="Path to save the output buffer shapefile."
    )
    parser.add_argument(
        '--future-dev-path', type=str, required=True,
        help="Path to the Future Development shapefile for planning layer."
    )
    parser.add_argument(
        '--gas-lines-path', type=str, required=True,
        help="Path to the Gas Lines shapefile."
    )
    parser.add_argument(
        '--report-files', nargs='+', required=True,
        help="List of report files (.txt or .geojson) to process."
    )

    # Optional arguments
    parser.add_argument(
        '--buffer-distance', type=float, default=config.DEFAULT_BUFFER_DISTANCE_FT,
        help=f"Buffer distance around gas lines in feet (default: {config.DEFAULT_BUFFER_DISTANCE_FT})."
    )
    parser.add_argument(
        "--parks-path", default=None,
        help="Optional path to park polygons to subtract."
    )
    parser.add_argument(
        '--crs', type=str, default=config.DEFAULT_CRS,
        help=f"CRS for spatial data (default: {config.DEFAULT_CRS})."
    )
    parser.add_argument(
        '--parallel', action='store_true', default=config.PARALLEL,
        help="Enable multiprocessing for report processing."
    )
    parser.add_argument(
        '--max-workers', type=int, default=config.MAX_WORKERS,
        help=f"Maximum number of worker processes for parallel processing (default: {config.MAX_WORKERS})."
    )
    parser.add_argument(
        '--log-level', type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=config.LOG_LEVEL,
        help=f"Set the logging level (default: {config.LOG_LEVEL})."
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help='Set logging level to DEBUG (overrides --log-level).'
    )
    parser.add_argument(
        '--log-file', type=str, default=config.LOG_FILENAME,
        help=f"Path to the log file (default: {config.LOG_FILENAME})."
    )
    parser.add_argument(
        '--output-format', type=str, choices=['shp', 'geojson'],
        default=config.OUTPUT_FORMAT,
        help=f"Output format for buffer file: 'shp' or 'geojson' (default: {config.OUTPUT_FORMAT})."
    )
    parser.add_argument(
        '--dry-run', action='store_true', default=config.DRY_RUN_MODE,
        help='Run the pipeline without writing outputs or modifying databases (for testing).'
    )
    parser.add_argument(
        '--config-file', type=str, default=None,
        help='Path to a configuration file with pipeline settings.'
    )
    parser.add_argument(
        '--no-config', action='store_true',
        help='Ignore config file and only use CLI arguments.'
    )
    parser.add_argument(
        '--overwrite-output', action='store_true', default=config.ALLOW_OVERWRITE_OUTPUT,
        help='Allow overwriting existing output files.'
    )

    # Mutually exclusive group for MongoDB integration
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--use-mongodb', dest='use_mongodb', action='store_true',
        help='Enable MongoDB integration.'
    )
    group.add_argument(
        '--no-mongodb', dest='use_mongodb', action='store_false',
        help='Disable MongoDB integration.'
    )
    parser.set_defaults(use_mongodb=False)  # default if neither specified

    args = parser.parse_args()

    # === Early validation ===
    if args.config_file and not os.path.isfile(args.config_file):
        parser.error(f"❌ Config file does not exist: {args.config_file}")

    # === Merge config file values if applicable ===
    if args.config_file and not args.no_config:
        config_values = load_config_file(args.config_file)
        print(f"🔧 Loaded settings from {args.config_file}")

        for key, value in config_values.items():
            if hasattr(args, key):
                default_val = parser.get_default(key)
                current_val = getattr(args, key)
                # Override only if arg equals parser default
                if current_val == default_val:
                    setattr(args, key, value)
                    print(f"🔁 Config override: {key} = {value}")
                else:
                    print(f"🛑 CLI overrides config: {key} = {current_val} (ignored config value = {value})")
    elif args.no_config:
        print("🚫 Skipping config file loading due to --no-config flag.")

    # === Additional validations ===

    # Validate input folder existence
    if not os.path.isdir(args.input_folder):
        parser.error(f"❌ Input folder does not exist or is not a directory: {args.input_folder}")

    # Validate report files: extension and existence
    def is_valid_report_file(file: str) -> bool:
        return os.path.splitext(file)[1].lower() in ['.txt', '.geojson']

    invalid_files = []
    missing_files = []

    for f in args.report_files:
        if not is_valid_report_file(f):
            invalid_files.append(f)
        full_path = os.path.join(args.input_folder, f)
        if not os.path.isfile(full_path):
            missing_files.append(f)

    if invalid_files:
        parser.error(f"❌ Unsupported report file extensions: {', '.join(invalid_files)}")
    if missing_files:
        parser.error(f"❌ Report files not found in input folder: {', '.join(missing_files)}")

    # Validate output path directory existence
    output_dir = os.path.dirname(args.output_path)
    if output_dir and not os.path.isdir(output_dir):
        parser.error(f"❌ Output path directory does not exist: {output_dir}")

    # Basic CRS format check
    if not args.crs.upper().startswith('EPSG:'):
        parser.error(f"❌ Invalid CRS format. Expected format like 'EPSG:32633', got: {args.crs}")

    # Buffer distance positive check
    if args.buffer_distance <= 0:
        parser.error("❌ Buffer distance must be a positive number.")

    # Max workers validation
    if args.max_workers is not None and args.max_workers < 1:
        parser.error("❌ Max workers must be at least 1.")

    # Verbose flag overrides log_level
    if args.verbose:
        args.log_level = 'DEBUG'
        print("⚠️ Verbose mode activated: Logging level set to DEBUG.")

    # === Post-validation notices ===
    if args.use_mongodb and not args.config_file:
        print("⚠️ Warning: MongoDB integration is enabled, but no --config-file provided. Using default MongoDB settings.")

    if args.dry_run:
        print("⚠️ Warning: Dry-run mode enabled. No outputs will be written or databases modified.")

    if args.overwrite_output:
        print("⚠️ Warning: Existing output files will be overwritten if they exist.")

    return args
