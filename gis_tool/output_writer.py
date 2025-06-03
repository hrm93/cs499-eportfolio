### output_writer.py

"""
Module to handle writing GIS output files in various formats.

Currently, supports:
- Writing GeoJSON or Shapefile from GeoDataFrames
- Writing CSV files from attribute data (DataFrames)
- Writing plain text reports
"""
import logging
import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd

import gis_tool.config as config

logger = logging.getLogger("gis_tool")


def write_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Write a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): DataFrame containing tabular data.
        output_path (str): Path to the output CSV file.
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
    """
    path = Path(output_path)
    if not path.parent.exists():
        warning_msg = f"Output directory does not exist: {path.parent}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise FileNotFoundError(warning_msg)
    try:
        gdf.to_file(str(path), driver="GeoJSON")
        logger.info(f"GEOJSON file written to {path}")
    except Exception as e:
        warning_msg = f"Failed to write GeoJSON file to {path}: {e}"
        warnings.warn(warning_msg, UserWarning)
        logger.error(warning_msg)
        raise


def write_gis_output(gdf: gpd.GeoDataFrame, output_path: str, output_format: str = config.OUTPUT_FORMAT, overwrite: bool = False) -> None:
    """
    Write a GeoDataFrame to a file in the specified format.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame containing spatial features.
        output_path (str): Path to the output file (.shp or .geojson).
        output_format (str): Output format: 'shp' or 'geojson'.
        overwrite (bool): Whether to overwrite existing files.
    """
    if gdf.empty:
        warning_msg = f"GeoDataFrame is empty; no file written to {output_path}"
        warnings.warn(warning_msg, UserWarning)
        logger.warning(warning_msg)
        return
    path = Path(output_path)
    if path.exists() and not overwrite:
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
        warning_msg = f"Dry run enabled: skipping write of {output_path}"
        warnings.warn(warning_msg, UserWarning)
        logger.info(warning_msg)
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
        text (str): Text content of the report.
        output_path (str): Path to the output text file.
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
