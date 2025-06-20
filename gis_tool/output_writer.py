"""
output_writer.py

Module to handle writing GIS output files in various formats for the GIS pipeline tool.

Features:
- Writing GeoJSON or Shapefile from GeoDataFrames.
- Writing CSV files from tabular data.
- Writing plain text reports.
- Generating HTML summary reports from buffer results.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import warnings
from pathlib import Path
from colorama import Fore, Style

import geopandas as gpd
import pandas as pd

import gis_tool.config as config

logger = logging.getLogger("gis_tool.output_writer")


def generate_html_report(
    gdf_buffer: gpd.GeoDataFrame,
    buffer_distance_m: float,
    output_path: str,
) -> None:
    """
    Generate a simple HTML summary report of the buffer operation.

    Args:
        gdf_buffer (gpd.GeoDataFrame): GeoDataFrame with buffered geometries.
        buffer_distance_m (float): The buffer distance used, in meters.
        output_path (str): Path where to save the HTML report (without extension).

    Raises:
        FileNotFoundError: If the output directory does not exist.
        Exception: If HTML generation fails.
    """
    path = Path(output_path).with_suffix(".html")
    if not path.parent.exists():
        warning_msg = f"Output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)

    try:
        num_features = len(gdf_buffer)
        geom_types = gdf_buffer.geometry.geom_type.value_counts().to_dict()
        total_area = gdf_buffer.geometry.area.sum()

        # Display first 10 rows of attributes with WKT geometry
        attr_table = gdf_buffer.head(10).copy()
        attr_table['geometry'] = attr_table.geometry.apply(lambda g: g.wkt if g else "None")

        html = f"""
        <html>
        <head>
            <title>Buffer Operation Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; }}
                th {{ background-color: #4CAF50; color: white; }}
            </style>
        </head>
        <body>
            <h1>Buffer Operation Report</h1>
            <p><strong>Buffer Distance:</strong> {buffer_distance_m:.2f} meters</p>
            <p><strong>Number of Features Buffered:</strong> {num_features}</p>
            <p><strong>Geometry Types in Result:</strong> {geom_types}</p>
            <p><strong>Total Area of Buffers:</strong> {total_area:.2f} square meters</p>
            <h2>Sample of Buffered Features</h2>
            {attr_table.to_html(index=False, escape=False)}
            <p>Output files and maps can be found at: {path.parent}</p>
        </body>
        </html>
        """
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info(f"HTML report generated at {path}")
    except Exception as e:
        logger.exception(f"Failed to generate HTML report: {e}")
        warnings.warn(f"Failed to generate HTML report: {e}", UserWarning)
        raise


def write_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Write a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame containing tabular data.
        output_path (str): Path to the output CSV file.

    Raises:
        FileNotFoundError: If output directory does not exist.
        Exception: If file write fails.
    """
    path = Path(output_path)
    if not path.parent.exists():
        warning_msg = f"Output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)

    try:
        df.to_csv(path, index=False)
        logger.info(f"CSV file written to {path}")
    except Exception as e:
        warning_msg = f"Failed to write CSV file to {path}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise


def write_geojson(gdf: gpd.GeoDataFrame, output_path: str) -> None:
    """
    Write a GeoDataFrame to a GeoJSON file.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame containing spatial features.
        output_path (str): Path to the output GeoJSON file.

    Raises:
        FileNotFoundError: If output directory does not exist.
        Exception: If file write fails.
    """
    path = Path(output_path)
    if not path.parent.exists():
        warning_msg = f"Output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)

    try:
        gdf.to_file(str(path), driver="GeoJSON")
        logger.info(f"GeoJSON file written to {path}")
    except Exception as e:
        warning_msg = f"Failed to write GeoJSON file to {path}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise


def write_gis_output(
    gdf: gpd.GeoDataFrame,
    output_path: str,
    output_format: str = config.OUTPUT_FORMAT,
    overwrite: bool = False,
    interactive: bool = False
) -> None:
    """
    Write a GeoDataFrame to a GIS file in the specified format.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame with spatial features.
        output_path (str): Path to the output file (.shp or .geojson).
        output_format (str): Desired format ('shp' or 'geojson').
        overwrite (bool): Whether to overwrite existing files.
        interactive (bool): Whether to prompt the user before overwriting.

    Raises:
        FileExistsError: If file exists and overwrite is False (unless user confirms).
        FileNotFoundError: If output directory doesn't exist.
        ValueError: If unsupported format is specified.
        Exception: If file write fails.
    """
    if gdf.empty:
        warning_msg = f"GeoDataFrame is empty; no file written to {output_path}"
        warnings.warn(warning_msg, UserWarning)
        logger.warning(warning_msg)
        return

    path = Path(output_path)
    if path.exists() and not overwrite:
        if interactive:
            confirm = input(f"⚠️  {output_path} exists. Overwrite? [y/N]: ").strip().lower()
            if confirm != "y":
                warning_msg = f"User declined to overwrite {output_path}"
                warnings.warn(warning_msg, UserWarning)
                logger.error(warning_msg)
                raise FileExistsError(warning_msg)
        else:
            warning_msg = f"{output_path} exists and overwriting is disabled; file not written."
            warnings.warn(warning_msg, UserWarning)
            logger.error(warning_msg)
            raise FileExistsError(warning_msg)

    if not path.parent.exists():
        warning_msg = f"Output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)

    if config.DRY_RUN_MODE:
        logger.info(f"[DRY-RUN] Would write to {output_path} ({output_format})")
        print(Fore.YELLOW + f"[DRY-RUN] Would write: {output_path} ({output_format})" + Style.RESET_ALL)
        return

    try:
        if output_format == "geojson":
            gdf.to_file(output_path, driver="GeoJSON")
        elif output_format == "shp":
            gdf.to_file(output_path, driver="ESRI Shapefile")
        else:
            warning_msg = f"Unsupported output format: {output_format}"
            warnings.warn(warning_msg, UserWarning)
            logger.error(warning_msg)
            raise ValueError(warning_msg)

        logger.info(f"{output_format.upper()} file written to {output_path}")
    except Exception as e:
        warning_msg = f"Failed to write {output_format} file to {output_path}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise


def write_report(text: str, output_path: str) -> None:
    """
    Write a plain text report to a file.

    Args:
        text (str): Report content as a string.
        output_path (str): Path to the output text file.

    Raises:
        FileNotFoundError: If output directory does not exist.
        Exception: If file write fails.
    """
    path = Path(output_path)
    if not path.parent.exists():
        warning_msg = f"Report output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)

    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        logger.info(f"Report file written to {path}")
    except Exception as e:
        warning_msg = f"Failed to write report file to {path}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise
