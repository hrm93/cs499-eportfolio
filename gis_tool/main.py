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

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import fiona.errors
import geopandas as gpd
from pymongo.errors import PyMongoError

from gis_tool.buffer_processor import (
    create_buffer_with_geopandas,
    merge_buffers_into_planning_file,
    fix_geometry,
)
from gis_tool.cli import parse_args
from gis_tool.config import (
    DEFAULT_CRS,
    DEFAULT_BUFFER_DISTANCE_FT,
    PARALLEL,
    OUTPUT_FORMAT,
    ALLOW_OVERWRITE_OUTPUT,
    DRY_RUN_MODE,
    MAX_WORKERS,
)
from gis_tool.data_loader import create_pipeline_features
from gis_tool.db_utils import connect_to_mongodb
from gis_tool.logger import setup_logging
from gis_tool.output_writer import write_gis_output
from gis_tool.report_reader import read_reports, find_new_reports

logger = logging.getLogger("gis_tool")  # Get the configured logger


def process_report_chunk(
    report_chunk: list[str],
    gas_lines_shp: str,
    reports_folder: Path,
    spatial_reference: str,
    gas_lines_collection,
    use_mongodb: bool,
) -> None:
    """
    Process a chunk of report files in parallel. Designed for use with multiprocessing.

    Note:
        - MongoDB insert/update operations are disabled in parallel workers because
          MongoDB connections usually cannot be shared across processes.
        - gas_lines_collection will be None in parallel workers.
        - use_mongodb flag may still be True, but no DB writes happen in workers.

    Args:
        report_chunk (list[str]): List of report filenames to process.
        gas_lines_shp (str): Path to gas lines shapefile.
        reports_folder (Path): Folder containing report files.
        spatial_reference (str): Coordinate reference system identifier.
        gas_lines_collection: MongoDB collection object or None.
        use_mongodb (bool): Flag to enable MongoDB insert/update operations (ignored in workers).
    """
    try:
        if gas_lines_collection is None:
            print("⚠️  MongoDB insert disabled for parallel workers.")
            logger.info("MongoDB insert disabled for this process (parallel worker).")

        logger.debug(f"Starting to process report chunk: {report_chunk}")
        reports_folder_path = Path(reports_folder)
        logger.debug(f"Reports folder path resolved: {reports_folder_path}")

        geojson_reports, txt_reports = read_reports(report_chunk, reports_folder_path)
        logger.info(f"Read {len(geojson_reports)} GeoJSON and {len(txt_reports)} TXT reports.")

        gas_lines_gdf = gpd.read_file(gas_lines_shp)
        logger.info(f"Loaded gas lines shapefile with {len(gas_lines_gdf)} features.")

        create_pipeline_features(
            geojson_reports=geojson_reports,
            txt_reports=txt_reports,
            gas_lines_gdf=gas_lines_gdf,
            spatial_reference=spatial_reference,
            gas_lines_collection=gas_lines_collection,
            use_mongodb=use_mongodb,
        )
        logger.info(f"Finished processing chunk: {report_chunk}")

    except (FileNotFoundError, OSError, fiona.errors.DriverError) as e:
        print(f"❌ I/O error while processing chunk {report_chunk}: {e}")
        logger.error(f"I/O error in chunk {report_chunk}: {e}")

    except Exception as e:
        print(f"❌ Unexpected error in chunk {report_chunk}: {e}")
        logger.exception(f"Unexpected error in chunk {report_chunk}: {e}")


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
    logger.info("Pipeline processing started.")

    # Use config values as fallback defaults for CLI args
    use_mongodb = args.use_mongodb if args.use_mongodb is not None else False
    buffer_distance = args.buffer_distance or DEFAULT_BUFFER_DISTANCE_FT
    spatial_reference = args.crs or DEFAULT_CRS
    use_parallel = args.parallel if args.parallel is not None else PARALLEL
    output_format = args.output_format or OUTPUT_FORMAT
    overwrite_output = args.overwrite_output if args.overwrite_output is not None else ALLOW_OVERWRITE_OUTPUT
    dry_run = args.dry_run if args.dry_run is not None else DRY_RUN_MODE

    max_workers = MAX_WORKERS

    gas_lines_collection = None

    if use_mongodb:
        try:
            db = connect_to_mongodb()
            gas_lines_collection = db["gas_lines"]
            logger.info("Connected to MongoDB.")
        except PyMongoError as e:
            print(f"⚠️  Warning: Could not connect to MongoDB - {e}")
            logger.warning(f"MongoDB connection failed: {e}")
            use_mongodb = False  # Disable MongoDB if connection fails

    input_folder = args.input_folder
    output_path = args.output_path
    future_development_shp = args.future_dev_path
    gas_lines_shp = args.gas_lines_path

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
            else:
                print("ℹ️  Dry run enabled - skipping writing output files and merging.")
                logger.info("Dry run enabled - skipping writing output files and merging.")

    logger.info("Pipeline processing completed.")
    print("✅ Pipeline processing completed successfully.")


if __name__ == "__main__":
    main()
