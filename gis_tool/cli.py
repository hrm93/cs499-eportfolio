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

    # Validate report file extensions
    for f in args.report_files:
        ext = os.path.splitext(f)[1].lower()
        if ext not in ['.txt', '.geojson']:
            parser.error(f"Unsupported report file extension: {f}")

    # Adjust log level if verbose flag is set
    if args.verbose:
        args.log_level = 'DEBUG'

    return args