import os
from pathlib import Path
from typing import List, Union
import geopandas as gpd
import fiona
import logging

logger = logging.getLogger("gis_tool")


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


def find_new_reports(input_folder: str) -> List[str]:
    """
    Scan the specified folder and return a list of new report files
    with supported extensions (.txt, .geojson).

    Args:
        input_folder (str): Directory path containing report files.

    Returns:
        List[str]: Filenames of new report files found.
    """
    try:
        all_files = [entry.name for entry in os.scandir(input_folder) if entry.is_file()]
    except FileNotFoundError:
        logger.error(f"Input folder does not exist: {input_folder}")
        return []

    new_reports = [f for f in all_files if f.lower().endswith(('.txt', '.geojson'))]

    if not new_reports:
        logger.info("No new reports found in the input folder.")
    else:
        logger.info(f"Found {len(new_reports)} new report(s): {new_reports}")

    return new_reports


def load_geojson_report(file_path: Union[Path, str], crs: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON report from file and convert CRS to the target spatial_reference.
    """
    file_path = str(file_path)  # Convert to string for compatibility
    gdf = gpd.read_file(file_path)
    if gdf.crs is None or gdf.crs.to_string() != crs:
        gdf = gdf.to_crs(crs)
    return gdf


def load_txt_report_lines(filepath: str) -> List[str]:
    """
    Load a TXT report from file and return its non-empty lines without trailing newline characters.

    Args:
        filepath (str): Path to the TXT report.

    Returns:
        List[str]: List of non-empty report lines.
    """
    try:
        with open(filepath, 'r') as f:
            return [line for line in f.read().splitlines() if line.strip()]
    except FileNotFoundError:
        logger.error(f"TXT report file not found: {filepath}")
        return []
    except IOError as e:
        logger.error(f"Error reading TXT report file {filepath}: {e}")
        return []