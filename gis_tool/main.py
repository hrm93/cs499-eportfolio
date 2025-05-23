### main.py
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
import logging

from gis_tool.cli import parse_args
from gis_tool.logger import setup_logging
from gis_tool.config import DEFAULT_CRS
from gis_tool.data_loader import find_new_reports, create_pipeline_features, connect_to_mongodb
from gis_tool.buffer_processor import create_buffer_with_geopandas, merge_buffers_into_planning_file
from gis_tool.output_writer import write_gis_output
from pathlib import Path


def process_report_chunk(
    report_chunk: list[str],
    gas_lines_shp: str,
    reports_folder: str,
    spatial_reference: str,
    gas_lines_collection,
    use_mongodb: bool,
) -> None:
    """
    Process a chunk of report files in parallel. Designed for use with multiprocessing.

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
            logging.info("MongoDB insert disabled for this process.")
        create_pipeline_features(
            report_chunk,
            gas_lines_shp,
            reports_folder,
            spatial_reference,
            gas_lines_collection,
            use_mongodb=use_mongodb,
        )
    except Exception as e:
        logging.error(f"Error in multiprocessing report chunk: {e}")


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
    logging.info("Pipeline processing started.")

    use_mongodb = args.use_mongodb
    gas_lines_collection = None
    if use_mongodb:
        try:
            db = connect_to_mongodb()
            gas_lines_collection = db["gas_lines"]
        except Exception as e:
            logging.warning(f"MongoDB connection failed: {e}")
            use_mongodb = False  # Disable MongoDB if connection fails

    input_folder = args.input_folder
    buffer_distance = args.buffer_distance
    output_path = args.output_path
    future_development_shp = args.future_dev_path
    gas_lines_shp = args.gas_lines_path
    spatial_reference = args.crs or DEFAULT_CRS
    use_parallel = args.parallel

    # Ensure output path is inside 'output' folder by default if not specified
    output_path_obj = Path(output_path)
    if output_path_obj.parent == Path('.') or not output_path_obj.parent.parts:
        # No folder given, prepend 'output'
        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)  # create folder if needed
        output_path_obj = output_dir / output_path_obj.name
    output_path = str(output_path_obj)
    logging.info(f"Output path set to: {output_path}")

    report_files = find_new_reports(input_folder)
    if not report_files:
        logging.info("No new reports to process. Exiting.")
        return

    if use_parallel:
        logging.info("Starting parallel processing of reports.")
        cpu_count = os.cpu_count() or 1
        chunk_size = max(1, len(report_files) // cpu_count)
        chunks = [report_files[i:i + chunk_size] for i in range(0, len(report_files), chunk_size)]

        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(
                    process_report_chunk,
                    chunk,
                    gas_lines_shp,
                    input_folder,
                    spatial_reference,
                    None,  # Disable MongoDB in parallel workers for safety
                    use_mongodb,
                )
                for chunk in chunks
            ]
            for future in futures:
                future.result()
    else:
        create_pipeline_features(
            report_files,
            gas_lines_shp,
            input_folder,
            spatial_reference,
            gas_lines_collection,
            use_mongodb=use_mongodb,
        )

    # Create buffer polygons
    gdf_buffer = create_buffer_with_geopandas(
        gas_lines_shp,
        buffer_distance_ft=buffer_distance,
    )
    # Write GIS output using updated path
    write_gis_output(gdf_buffer, output_path, output_format=args.output_format)

    # Merge buffers into future planning shapefile
    merge_buffers_into_planning_file(output_path, future_development_shp)

    logging.info("Pipeline processing completed.")


if __name__ == "__main__":
    main()
