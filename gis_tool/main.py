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

from concurrent.futures import ProcessPoolExecutor
import os
import fiona.errors
import logging
from pathlib import Path
import geopandas as gpd
from pymongo.errors import PyMongoError
from gis_tool.db_utils import connect_to_mongodb
from gis_tool.cli import parse_args
from gis_tool.logger import setup_logging
from gis_tool.config import DEFAULT_CRS
from gis_tool.data_loader import (
    find_new_reports,
    create_pipeline_features,
)
from gis_tool.buffer_processor import (
    create_buffer_with_geopandas,
    merge_buffers_into_planning_file,
)
from gis_tool.output_writer import write_gis_output

logger = logging.getLogger("gis_tool")  # Get the configured logger

def read_reports(report_names: list[str], reports_folder_path: Path):
    """
    Reads reports from given filenames.

    Returns:
        geojson_reports (list of tuples): (filename, GeoDataFrame)
        txt_reports (list of tuples): (filename, list of lines)
    """
    geojson_reports = []
    txt_reports = []

    for report_name in report_names:
        report_path = reports_folder_path / report_name
        if report_name.lower().endswith(".geojson"):
            try:
                gdf = gpd.read_file(report_path)
                geojson_reports.append((report_name, gdf))
            except (FileNotFoundError, OSError, fiona.errors.DriverError) as e:
                logger.error(f"Failed to read GeoJSON report {report_name}: {e}")
        elif report_name.lower().endswith(".txt"):
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                txt_reports.append((report_name, lines))
            except (FileNotFoundError, OSError) as e:
                logger.error(f"Failed to read TXT report {report_name}: {e}")
        else:
            logger.warning(f"Unsupported report type: {report_name}")

    return geojson_reports, txt_reports


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
        reports_folder (str): Folder containing report files.
        spatial_reference (str): Coordinate reference system identifier.
        gas_lines_collection: MongoDB collection object or None.
        use_mongodb (bool): Flag to enable MongoDB insert/update operations.
    """
    try:
        if gas_lines_collection is None:
            logger.info("MongoDB insert disabled for this process (parallel worker).")

        reports_folder_path = Path(reports_folder)
        geojson_reports, txt_reports = read_reports(report_chunk, reports_folder_path)

        gas_lines_gdf = gpd.read_file(gas_lines_shp)

        create_pipeline_features(
            geojson_reports=geojson_reports,
            txt_reports=txt_reports,
            gas_lines_gdf=gas_lines_gdf,
            spatial_reference=spatial_reference,
            gas_lines_collection=gas_lines_collection,
            use_mongodb=use_mongodb,
        )

    except (FileNotFoundError, OSError, fiona.errors.DriverError) as e:
        logger.error(f"I/O error in multiprocessing report chunk: {e}")

    except Exception as e:
        logger.error(f"Unexpected error in multiprocessing report chunk: {e}")


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

    use_mongodb = args.use_mongodb
    gas_lines_collection = None

    if use_mongodb:
        try:
            db = connect_to_mongodb()
            gas_lines_collection = db["gas_lines"]
            logger.info("Connected to MongoDB.")
        except PyMongoError as e:
            logger.warning(f"MongoDB connection failed: {e}")
            use_mongodb = False  # Disable MongoDB if connection fails

    input_folder = args.input_folder
    buffer_distance = args.buffer_distance
    output_path = args.output_path
    future_development_shp = args.future_dev_path
    gas_lines_shp = args.gas_lines_path
    spatial_reference = args.crs or DEFAULT_CRS
    use_parallel = args.parallel

    # Ensure output path is inside 'output' folder by default
    output_path_obj = Path(output_path)
    if output_path_obj.parent == Path('.'):
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path_obj = output_dir / output_path_obj.name
    output_path = str(output_path_obj)
    logger.info(f"Output path set to: {output_path}")

    report_files = find_new_reports(input_folder)
    if not report_files:
        logger.info("No new reports to process. Exiting.")
        return

    reports_folder_path = Path(args.input_folder)

    if use_parallel:
        # Parallel processing -- no need to create geojson_reports or txt_reports here
        logger.info(f"Starting parallel processing of {len(report_files)} report files.")
        cpu_count = os.cpu_count() or 1
        chunk_size = max(1, len(report_files) // cpu_count)
        chunks = [report_files[i: i + chunk_size] for i in range(0, len(report_files), chunk_size)]

        with ProcessPoolExecutor() as executor:
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
                    logger.error(f"Error in parallel processing: {e}")
    else:
        # Sequential processing - reuse the same function
        geojson_reports, txt_reports = read_reports(report_files, reports_folder_path)

        gas_lines_gdf = gpd.read_file(gas_lines_shp)

        create_pipeline_features(
            geojson_reports=geojson_reports,
            txt_reports=txt_reports,
            gas_lines_gdf=gas_lines_gdf,
            spatial_reference=spatial_reference,
            gas_lines_collection=gas_lines_collection,
            use_mongodb=use_mongodb,
        )

    # Create buffer polygons around gas lines using GeoPandas-based logic
    gdf_buffer = create_buffer_with_geopandas(
        gas_lines_shp,
        buffer_distance_ft=buffer_distance,
        parks_path=args.parks_path,
    )

    if gdf_buffer.empty:
        logger.warning("Generated buffer GeoDataFrame is empty. Skipping output write and merge.")
    else:
        # Write GIS output in specified format
        write_gis_output(gdf_buffer, output_path, output_format=args.output_format)

        # Merge buffer results into future development planning shapefile
        merge_buffers_into_planning_file(output_path, future_development_shp)

    logger.info("Pipeline processing completed.")

if __name__ == "__main__":
    main()
