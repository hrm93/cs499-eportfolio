### output_writer.py

"""
Module to handle writing GIS output files in various formats.

Currently, supports:
- Writing GeoJSON or Shapefile from GeoDataFrames
- Writing CSV files from attribute data (DataFrames)
- Writing plain text reports
"""
import logging
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
        raise FileNotFoundError(f"Directory does not exist: {path.parent}")
    df.to_csv(path, index=False)
    logger.info(f"CSV file written to {path}")


def write_geojson(gdf: gpd.GeoDataFrame, output_path: str) -> None:
    """
    Write a GeoDataFrame to a GeoJSON file.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame containing spatial features.
        output_path (str): Path to the output GeoJSON file.
    """
    path = Path(output_path)
    if not path.parent.exists():
        raise FileNotFoundError(f"Directory does not exist: {path.parent}")
    gdf.to_file(str(path), driver="GeoJSON")
    logger.info(f"GEOJSON file written to {path}")


def write_gis_output(gdf: gpd.GeoDataFrame, output_path: str, output_format: str = config.OUTPUT_FORMAT) -> None:
    """
    Write a GeoDataFrame to a file in the specified format.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame containing spatial features.
        output_path (str): Path to the output file (.shp or .geojson).
        output_format (str): Output format: 'shp' or 'geojson'.
    """
    if gdf.empty:
        logger.warning(f"GeoDataFrame is empty. No file written to {output_path}.")
        return
    try:
        path = Path(output_path)
        if path.exists() and not config.ALLOW_OVERWRITE_OUTPUT:
            raise FileExistsError(f"{output_path} exists and overwriting is disabled.")
        if config.DRY_RUN_MODE:
            logger.info(f"Dry run enabled: Skipping write of {output_path}")
            return
        if output_format == "geojson":
            gdf.to_file(output_path, driver="GeoJSON")
        elif output_format == "shp":
            gdf.to_file(output_path, driver="ESRI Shapefile")
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        logger.info(f"{output_format.upper()} file written to {output_path}")
    except Exception as e:
        logger.error(f"Failed to write {output_format} file: {e}")
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
        raise FileNotFoundError(f"Directory does not exist: {path.parent}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    logger.info(f"Report file written to {path}")
