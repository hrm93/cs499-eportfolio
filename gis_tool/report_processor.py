"""
report_processor.py

Handles the parallel processing of report files by reading GeoJSON/TXT reports,
loading gas line features, and generating spatial features.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
from pathlib import Path

import fiona.errors
import geopandas as gpd

from gis_tool.data_loader import create_pipeline_features
from gis_tool.report_reader import read_reports

# Set up logger for this module
logger = logging.getLogger("gis_tool.report_processor")


def process_chunk_wrapper(
    chunk,
    gas_lines_path,
    reports_folder_path,
    spatial_reference,
):
    return process_report_chunk(
        chunk,
        gas_lines_path,
        reports_folder_path,
        spatial_reference,
        None,
        False,
    )

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

        # Read GeoJSON and TXT reports from disk
        geojson_reports, txt_reports = read_reports(report_chunk, reports_folder_path)
        logger.info(f"Read {len(geojson_reports)} GeoJSON and {len(txt_reports)} TXT reports.")

        # Load the gas lines shapefile
        gas_lines_gdf = gpd.read_file(gas_lines_shp)
        logger.info(f"Loaded gas lines shapefile with {len(gas_lines_gdf)} features.")

        # Create spatial features using the loaded reports and gas lines
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
