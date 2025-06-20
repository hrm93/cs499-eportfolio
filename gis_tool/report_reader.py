"""
report_reader.py

Handles loading, parsing, and reading of GeoJSON and TXT report files.
Includes utilities to find new reports and parse their contents into structured data.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import csv
import logging
import os
import warnings
from pathlib import Path
from typing import List, Tuple, Union

import fiona
import geopandas as gpd
from shapely.geometry import Point

from gis_tool.utils import robust_date_parse

# Logger for this module
logger = logging.getLogger("gis_tool.report_reader")


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
        warnings.warn(f"Input folder does not exist: {input_folder}", UserWarning)
        logger.error(f"Input folder does not exist: {input_folder}")
        return []

    new_reports = [f for f in all_files if f.lower().endswith(('.txt', '.geojson'))]

    if not new_reports:
        warnings.warn("No new report files found in input folder.", UserWarning)
        logger.info("No new reports found in the input folder.")
    else:
        logger.info(f"Found {len(new_reports)} new report(s): {new_reports}")

    return new_reports


def load_geojson_report(file_path: Union[Path, str], crs: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON report from file and convert CRS to the target spatial_reference.

    Args:
        file_path (Union[Path, str]): Path to GeoJSON file.
        crs (str): Target coordinate reference system string.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with geometries in the target CRS.
    """
    file_path = str(file_path)  # Convert to string for compatibility
    try:
        gdf = gpd.read_file(file_path)
    except Exception as e:
        warnings.warn(f"Failed to load GeoJSON report: {file_path}. Error: {e}", UserWarning)
        logger.error(f"Failed to load GeoJSON report {file_path}: {e}")
        raise
    if gdf.crs is None or gdf.crs.to_string() != crs:
        warnings.warn(f"CRS mismatch or missing in {file_path}. Reprojecting to {crs}.", UserWarning)
        logger.warning(f"CRS mismatch or missing in {file_path}. Reprojecting to {crs}.")
        gdf = gdf.to_crs(crs)
    return gdf


def parse_geojson_report(gdf: gpd.GeoDataFrame) -> List[dict]:
    """
    Parse GeoDataFrame features into a list of dictionaries with cleaned attributes.

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame to parse.

    Returns:
        List[dict]: Parsed feature dictionaries with geometry and attributes.
    """
    features = []
    for idx, row in gdf.iterrows():
        props = row.drop('geometry').to_dict()
        props["Date"] = robust_date_parse(props.get("Date"))
        geometry = row.geometry
        if geometry is None or geometry.is_empty:
            logger.warning(f"Skipping feature at index {idx} with empty geometry")
            continue
        props["geometry"] = geometry
        props["Material"] = props.get("Material", "").lower() if props.get("Material") else ""
        features.append(props)
    return features


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
            lines = [line for line in f.read().splitlines() if line.strip()]
        if not lines:
            warnings.warn(f"TXT report {filepath} is empty.", UserWarning)
            logger.warning(f"TXT report {filepath} is empty.")
        return lines
    except FileNotFoundError:
        warnings.warn(f"TXT report file not found: {filepath}", UserWarning)
        logger.error(f"TXT report file not found: {filepath}")
        return []
    except IOError as e:
        warnings.warn(f"Error reading TXT report file {filepath}: {e}", UserWarning)
        logger.error(f"Error reading TXT report file {filepath}: {e}")
        return []


def parse_txt_report(filepath: str) -> List[dict]:
    """
    Parse TXT report files that may be CSV-formatted or key-value pairs.
    Tries CSV parsing first, then falls back to key-value line parsing.

    Args:
        filepath (str): Path to the TXT report file.

    Returns:
        List[dict]: Parsed records with attributes and Point geometries.
    """
    records = []

    # Attempt CSV parsing first
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            first_row = next(reader, None)
            if first_row is None:
                logger.warning(f"CSV TXT report {filepath} is empty.")
                return []
            records.append(_normalize_txt_record(first_row))
            for row in reader:
                records.append(_normalize_txt_record(row))
        logger.info(f"Successfully parsed CSV TXT report: {filepath} with {len(records)} records")
        return records
    except Exception as e_csv:
        logger.warning(f"CSV parsing failed for {filepath} with error: {e_csv}. Falling back to key-value parsing.")

    # Fallback to key-value pair parsing by line
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            current_record = {}
            for line in f:
                line = line.strip()
                if not line:
                    # Blank line indicates end of a record
                    if current_record:
                        records.append(_normalize_txt_record(current_record))
                        current_record = {}
                    continue
                if ':' in line:
                    try:
                        key, value = line.split(':', 1)
                        current_record[key.strip()] = value.strip()
                    except Exception as e_line:
                        logger.warning(f"Skipping line due to parsing error: {line} | Error: {e_line}")
                else:
                    logger.warning(f"Skipping malformed line: {line}")
            # Add last record if any
            if current_record:
                records.append(_normalize_txt_record(current_record))
        logger.info(f"Successfully parsed key-value TXT report: {filepath} with {len(records)} records")
        return records
    except Exception as e_kv:
        logger.error(f"Failed to parse TXT report {filepath} by any method. Error: {e_kv}")
        return []


def _normalize_txt_record(record: dict) -> dict:
    """
    Normalize a single TXT report record dictionary: parse date, lower Material,
    convert numeric fields, and create Point geometry.

    Args:
        record (dict): Raw record dictionary.

    Returns:
        dict: Normalized record dictionary with geometry.
    """
    normalized = {
        "Name": record.get("Name") or record.get("ID") or "",
        "Date": robust_date_parse(record.get("Date")),
        "Material": (record.get("Material") or "").lower(),
        "PSI": None,
        "geometry": None,
    }

    # Convert PSI to float safely
    try:
        normalized["PSI"] = float(record.get("PSI"))
    except (TypeError, ValueError):
        normalized["PSI"] = None
        logger.warning(f"Failed to convert PSI value '{record.get('PSI')}' to float.")

    # Convert Latitude and Longitude to float and create Point geometry
    try:
        lat = float(record.get("Latitude"))
        lon = float(record.get("Longitude"))
        normalized["geometry"] = Point(lon, lat)
    except (TypeError, ValueError):
        # Try parsing Location if present (format "lat, lon" or "lon, lat")
        loc = record.get("Location")
        if loc:
            try:
                parts = [p.strip() for p in loc.split(",")]
                if len(parts) == 2:
                    # Heuristic: assume Location is "lat, lon" (common)
                    lat, lon = float(parts[0]), float(parts[1])
                    normalized["geometry"] = Point(lon, lat)
                else:
                    logger.warning(f"Malformed Location field '{loc}'")
            except (ValueError, TypeError):
                logger.warning(f"Failed to parse Location field '{loc}'")
        else:
            normalized["geometry"] = None
            logger.warning("No coordinate information found to create geometry.")

    return normalized


def read_reports(
    report_names: List[str],
    reports_folder_path: Path
) -> Tuple[
    List[Tuple[str, gpd.GeoDataFrame]],  # geojson_reports: list of (filename, GeoDataFrame)
    List[Tuple[str, List[dict]]]           # txt_reports: list of (filename, list of lines)
]:
    """
    Read multiple reports from given filenames, distinguishing by file type.

    Args:
        report_names (List[str]): List of report filenames.
        reports_folder_path (Path): Path object to the folder containing the reports.

    Returns:
        Tuple[
            List[Tuple[str, gpd.GeoDataFrame]],  # geojson_reports
            List[Tuple[str, List[dict]]]          # txt_reports parsed to dicts
        ]
    """
    geojson_reports = []
    txt_reports = []

    logger.debug(f"Reading reports from folder {reports_folder_path} for files: {report_names}")

    for report_name in report_names:
        report_path = reports_folder_path / report_name
        logger.debug(f"Processing report: {report_name}")

        if report_name.lower().endswith(".geojson"):
            try:
                gdf = gpd.read_file(report_path)
                assert isinstance(gdf, gpd.GeoDataFrame), f"Loaded data is not a GeoDataFrame for {report_name}"

                if gdf.empty:
                    warnings.warn(f"GeoJSON report {report_name} is empty. Skipping.", UserWarning)
                    logger.warning(f"GeoJSON report {report_name} is empty.")
                    continue

                geojson_reports.append((report_name, gdf))
                logger.info(f"Successfully read GeoJSON report: {report_name}")

            except AssertionError as e:
                warnings.warn(f"Assertion error in GeoJSON report {report_name}: {e}", UserWarning)
                logger.error(f"Assertion error in GeoJSON report {report_name}: {e}")

            except (FileNotFoundError, OSError, fiona.errors.DriverError) as e:
                warnings.warn(f"Failed to read GeoJSON report {report_name}: {e}", UserWarning)
                logger.error(f"Failed to read GeoJSON report {report_name}: {e}")

        elif report_name.lower().endswith(".txt"):
            try:
                # Read raw lines first:
                parsed_records = parse_txt_report(str(report_path))  # pass filename/path, not lines list

                if not parsed_records:
                    warnings.warn(f"TXT report {report_name} parsed to empty records.", UserWarning)
                    logger.warning(f"TXT report {report_name} parsed to empty records.")
                    continue

                txt_reports.append((report_name, parsed_records))
                logger.info(f"Successfully parsed TXT report: {report_name} with {len(parsed_records)} records")

            except Exception as e:
                warnings.warn(f"Failed to parse TXT report {report_name}: {e}", UserWarning)
                logger.error(f"Failed to parse TXT report {report_name}: {e}")
        else:
            warnings.warn(f"Unsupported report type skipped: {report_name}", UserWarning)
            logger.warning(f"Unsupported report type: {report_name}")

    logger.info(f"Finished reading reports. GeoJSON: {len(geojson_reports)}, TXT: {len(txt_reports)}")

    return geojson_reports, txt_reports
