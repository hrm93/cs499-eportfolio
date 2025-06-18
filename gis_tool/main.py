"""
Main module for the GIS pipeline tool.

This script orchestrates the entire pipeline workflow including:
- Parsing command-line arguments.
- Connecting to MongoDB (optional).
- Finding and processing new report files.
- Creating buffer polygons around gas lines.
- Writing buffer outputs in user-specified formats.
- Merging buffer results into future development planning files.

Designed for robust, scalable, and parallel execution with clear logging.
"""
import logging
import yaml

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import geopandas as gpd
from pymongo.errors import PyMongoError

from gis_tool.buffer_creation import  create_buffer_with_geopandas
from gis_tool.buffer_processor import (
    merge_buffers_into_planning_file,
    fix_geometry,
)
from gis_tool.cli import parse_args
from gis_tool.config import (
    DEFAULT_BUFFER_DISTANCE_FT,
    DEFAULT_CRS,
    MAX_WORKERS,
    OUTPUT_FORMAT,
    ALLOW_OVERWRITE_OUTPUT,
    DRY_RUN_MODE,
    PARALLEL,
)
from gis_tool.data_loader import create_pipeline_features
from gis_tool.report_processor import process_report_chunk
from gis_tool.db_utils import (
    connect_to_mongodb,
    ensure_spatial_index,
    ensure_collection_schema,
    spatial_feature_schema,
)
from gis_tool.logger import setup_logging
from gis_tool.output_writer import write_gis_output, generate_html_report
from gis_tool.report_reader import read_reports, find_new_reports

logger = logging.getLogger("gis_tool")  # Get the configured logger


def main() -> None:
    """
    Main entry point for the GIS pipeline tool.

    Workflow:
    - Setup logging and parse CLI arguments.
    - Optionally connect to MongoDB.
    - Detect new report files and process them (optionally in parallel).
    - Generate buffer polygons around gas lines.
    - Write buffer outputs in specified formats.
    - Merge buffer results into a future development planning file.
    """
    setup_logging()
    args = parse_args()
    config = {}

    # If CLI provides a config file path, load it here
    if args.config_file:
        config_path = Path(args.config_file)
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f) or {}
            logger.info(f"Loaded configuration file: {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load config file {config_path}: {e}")
            print(f"⚠️  Warning: Failed to load config file {config_path}: {e}")

    # Helper function to get config value with CLI override priority
    def get_config_value(key: str, default=None):
        cli_value = getattr(args, key, None)
        if cli_value is not None:
            return cli_value
        keys = key.split(".")
        value = config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    # Then use get_config_value() to set your vars
    use_mongodb = get_config_value("use_mongodb", False)
    buffer_distance = get_config_value("buffer_distance", DEFAULT_BUFFER_DISTANCE_FT)
    spatial_reference = config.get("SPATIAL", {}).get("default_crs", DEFAULT_CRS)
    geographic_crs = config.get("SPATIAL", {}).get("geographic_crs", "EPSG:4326")
    buffer_layer_crs = config.get("SPATIAL", {}).get("buffer_layer_crs", "EPSG:32633")
    use_parallel = get_config_value("parallel", PARALLEL)
    output_format = get_config_value("output_format", OUTPUT_FORMAT)
    overwrite_output = get_config_value("overwrite_output", ALLOW_OVERWRITE_OUTPUT)
    dry_run = get_config_value("dry_run", DRY_RUN_MODE)
    max_workers = MAX_WORKERS

    # Also handle file paths similarly
    input_folder = get_config_value("input_folder", None)
    output_path = get_config_value("output_path", None)
    future_development_shp = get_config_value("future_dev_path", None)
    gas_lines_shp = get_config_value("gas_lines_path", None)

    logger.info("Pipeline processing started.")

    gas_lines_collection = None

    if use_mongodb:
        try:
            db = connect_to_mongodb()
            ensure_collection_schema(db, "features", spatial_feature_schema)
            gas_lines_collection = db["features"]
            ensure_spatial_index(gas_lines_collection)
            logger.info("Connected to MongoDB.")
        except PyMongoError as e:
            print(f"⚠️  Warning: Could not connect to MongoDB - {e}")
            logger.warning(f"MongoDB connection failed: {e}")
            use_mongodb = False  # Disable MongoDB if connection fails

    # Ensure output path is inside 'output' directory if no parent specified
    output_path_obj = Path(output_path)
    if output_path_obj.parent == Path('.'):
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path_obj = output_dir / output_path_obj.name
    output_path = str(output_path_obj)
    logger.info(f"Output path set to: {output_path}")

    # Find new report files to process
    report_files = find_new_reports(input_folder)
    if not report_files:
        print("ℹ️  No new reports to process. Exiting.")
        logger.info("No new reports to process. Exiting.")
        return

    reports_folder_path = Path(input_folder)

    if use_parallel:
        logger.info("Buffering will run in parallel mode.")
        print(f"ℹ️  Starting parallel processing of {len(report_files)} reports using up to {max_workers} workers...")
        logger.info(f"Starting parallel processing of {len(report_files)} report files with max_workers={max_workers}.")
        chunk_size = max(1, len(report_files) // max_workers)
        chunks = [report_files[i: i + chunk_size] for i in range(0, len(report_files), chunk_size)]
        logger.debug(f"Parallel processing with chunk size {chunk_size}.")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_report_chunk,
                    chunk,
                    gas_lines_shp,
                    reports_folder_path,
                    spatial_reference,
                    None,  # MongoDB disabled inside workers
                    False,  # Explicitly disable MongoDB usage in parallel workers
                )
                for chunk in chunks
            ]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error in parallel processing: {e}")
                    logger.error(f"Error in parallel processing: {e}")
    else:
        logger.info("Buffering will run in sequential mode.")
        print("ℹ️  Starting sequential processing of reports...")
        logger.info("Sequential processing of report files.")
        geojson_reports, txt_reports = read_reports(report_files, reports_folder_path)
        logger.debug(f"Read {len(geojson_reports)} GeoJSON reports and {len(txt_reports)} TXT reports.")

        logger.info(f"Loading gas lines shapefile: {gas_lines_shp}")
        gas_lines_gdf = gpd.read_file(gas_lines_shp)

        fixed_geometries = []
        for geom in gas_lines_gdf.geometry:
            if geom is not None:
                fixed = fix_geometry(geom)
                fixed_geometries.append(fixed)
            else:
                fixed_geometries.append(None)
        gas_lines_gdf.geometry = fixed_geometries
        logger.debug("Geometries fixed.")

        create_pipeline_features(
            geojson_reports=geojson_reports,
            txt_reports=txt_reports,
            gas_lines_gdf=gas_lines_gdf,
            spatial_reference=spatial_reference,
            gas_lines_collection=gas_lines_collection,
            use_mongodb=use_mongodb,
        )

        logger.info("Generating buffer polygons around gas lines.")
        gdf_buffer = create_buffer_with_geopandas(
            gas_lines_shp,
            buffer_distance_ft=buffer_distance,
            parks_path=args.parks_path,
            use_multiprocessing=use_parallel,
        )

        if gdf_buffer.empty:
            print("⚠️  Warning: Buffer output is empty. No output files will be written.")
            logger.warning("Generated buffer GeoDataFrame is empty. Skipping output write and merge.")
        else:
            if not dry_run:
                print("ℹ️  Writing GIS output and merging into future development file.")
                logger.info("Writing GIS output files.")
                write_gis_output(
                    gdf_buffer,
                    output_path,
                    output_format=output_format,
                    overwrite=overwrite_output,
                )
                logger.info("Merging buffer polygons into future development planning file.")
                merge_buffers_into_planning_file(output_path, future_development_shp)

                # ====== HTML REPORT GENERATION ======
                # Generate HTML report path based on the output filename (including prefixes)
                report_html_path = output_path_obj.with_suffix('')
                report_html_path = report_html_path.parent / f"{report_html_path.name}_buffer_report.html"

                generate_html_report(
                    gdf_buffer=gdf_buffer,
                    buffer_distance_m=buffer_distance * 0.3048,
                    output_path=str(report_html_path),
                )

                print(f"✅ HTML buffer report generated: {report_html_path}")
                logger.info(f"HTML buffer report generated: {report_html_path}")

            else:
                print("ℹ️  Dry run enabled - skipping writing output files and merging.")
                logger.info("Dry run enabled - skipping writing output files and merging.")

    logger.info("Pipeline processing completed.")
    print("✅ Pipeline processing completed successfully.")


if __name__ == "__main__":
    main()
