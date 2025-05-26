### cli.py

import argparse
import os

def parse_args():
    """
    Parse command-line arguments for the GIS pipeline tool.

    This function defines and parses all necessary command-line arguments
    required to run the GIS pipeline tool that processes pipeline reports,
    creates buffer zones around gas lines, merges buffers into future
    development layers, and optionally integrates with MongoDB.

    Returns:
        argparse.Namespace: An object containing parsed command-line arguments with the following attributes:
            - input_folder (str): Path to the folder containing pipeline report files.
            - buffer_distance (float): Buffer distance around gas lines in feet (default: 25.0).
            - output_path (str): Path to save the output buffer shapefile.
            - future_dev_path (str): Path to the Future Development shapefile for planning layer.
            - gas_lines_path (str): Path to the Gas Lines shapefile.
            - crs (str): Coordinate Reference System for spatial data (default: 'EPSG:32633').
            - parallel (bool): Flag to enable multiprocessing for report processing.
            - report_files (list of str): List of report files (.txt or .geojson) to process.
            - use_mongodb (bool): Flag to enable or disable MongoDB integration (default: False).

    Raises:
        SystemExit: If invalid report file extensions are provided or required arguments are missing.
    """
    parser = argparse.ArgumentParser(
        description="Process GIS pipeline reports (.txt or .geojson), "
                    "generate buffer zones, merge with future development layers, "
                    "and optionally integrate with MongoDB."
    )

    # Required arguments
    parser.add_argument('--input-folder', type=str, required=True,
                        help="Path to the folder containing pipeline report files.")
    parser.add_argument('--output-path', type=str, required=True,
                        help="Path to save the output buffer shapefile.")
    parser.add_argument('--future-dev-path', type=str, required=True,
                        help="Path to the Future Development shapefile for planning layer.")
    parser.add_argument('--gas-lines-path', type=str, required=True,
                        help="Path to the Gas Lines shapefile.")
    parser.add_argument('--report-files', nargs='+', required=True,
                        help="List of report files (.txt or .geojson) to process.")

    # Optional arguments
    parser.add_argument('--buffer-distance', type=float, default=25.0,
                        help="Buffer distance around gas lines in feet (default: 25 feet).")
    parser.add_argument("--parks-path", default=None,
                        help="Optional path to park polygons to subtract.")
    parser.add_argument('--crs', type=str, default='EPSG:32633',
                        help="CRS for spatial data (default: EPSG:32633).")
    parser.add_argument('--parallel', action='store_true',
                        help="Enable multiprocessing for report processing.")
    parser.add_argument('--max-workers', type=int, default=None,
                        help='Maximum number of worker processes for parallel processing (default: number of CPUs).')
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO',
                        help="Set the logging level (default: INFO).")
    parser.add_argument('--verbose', action='store_true',
                        help='Set logging level to DEBUG (overrides --log-level).')
    parser.add_argument('--log-file', type=str, default=None,
                        help='Path to the log file. Defaults to config value.')
    parser.add_argument('--output-format', type=str, choices=['shp', 'geojson'], default='shp',
                        help="Output format for buffer file: 'shp' or 'geojson' (default: shp).")
    parser.add_argument('--dry-run', action='store_true',
                        help='Run the pipeline without writing outputs or modifying databases (for testing).')
    parser.add_argument('--config-file', type=str, default=None,
                        help='Path to a configuration file with pipeline settings.')
    parser.add_argument('--overwrite-output', action='store_true',
                        help='Overwrite existing output files if they exist.')

    # Mutually exclusive group for MongoDB integration
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--use-mongodb', dest='use_mongodb', action='store_true',
                       help='Enable MongoDB integration.')
    group.add_argument('--no-mongodb', dest='use_mongodb', action='store_false',
                       help='Disable MongoDB integration.')
    parser.set_defaults(use_mongodb=False)  # default value if neither specified

    args = parser.parse_args()

    # === VALIDATION START ===

    # Input folder must exist
    if not os.path.isdir(args.input_folder):
        parser.error(f"Input folder does not exist or is not a directory: {args.input_folder}")

    # Validate report file extensions and existence
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
        parser.error(f"Unsupported report file extensions: {', '.join(invalid_files)}")
    if missing_files:
        parser.error(f"Report files not found in input folder: {', '.join(missing_files)}")

    # Output path’s parent directory must exist
    output_dir = os.path.dirname(args.output_path)
    if output_dir and not os.path.isdir(output_dir):
        parser.error(f"Output path directory does not exist: {output_dir}")

    # CRS basic format check
    if not args.crs.upper().startswith('EPSG:'):
        parser.error(f"Invalid CRS format. Expected format like 'EPSG:32633', got: {args.crs}")

    # Buffer distance must be positive
    if args.buffer_distance <= 0:
        parser.error("Buffer distance must be a positive number.")

    # Validate max workers if specified
    if args.max_workers is not None and args.max_workers < 1:
        parser.error("Max workers must be at least 1.")

    # Warn if using MongoDB but no config file specified
    if args.use_mongodb and not args.config_file:
        print("Warning: MongoDB enabled but no --config-file provided. Using defaults.")

    # Handle verbose flag override
    if args.verbose:
        args.log_level = 'DEBUG'

    # === VALIDATION END ===

    return args
