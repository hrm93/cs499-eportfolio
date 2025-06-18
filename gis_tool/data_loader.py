"""
data_loader.py

Module responsible for loading and processing pipeline report data from text and GeoJSON files,
creating new geospatial features for gas line infrastructure, and optionally storing/updating
these features in a MongoDB database.

Features:
- Connects to MongoDB with connection validation.
- Identifies new pipeline report files (.txt and .geojson) from a specified directory.
- Parses and validates report data, converting it into GeoDataFrames.
- Handles spatial reference systems and geometry types consistently.
- Inserts or updates pipeline features into MongoDB, avoiding duplicates.
- Persists new or updated features to an ESRI shapefile.

This module is designed for integration with a GIS pipeline processing tool,
facilitating data ingestion and feature management within a geospatial data workflow.

Dependencies:
- geopandas
- shapely
- pymongo
- pandas
- standard Python libraries: os, logging

Typical usage:
    from gis_tool.data_loader import connect_to_mongodb, find_new_reports, create_pipeline_features

    db = connect_to_mongodb()
    new_reports = find_new_reports("/path/to/reports")
    create_pipeline_features(new_reports, "gas_lines.shp", "/path/to/reports", "EPSG:4326",
                             gas_lines_collection=db['gas_lines'])

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
from typing import List, Tuple, Optional, Set

import geopandas as gpd
from pymongo.collection import Collection

from gis_tool import report_reader
from gis_tool.data_utils import create_and_upsert_feature
from gis_tool.spatial_utils import (
    validate_and_reproject_crs,
    validate_geometry_column,
    validate_geometry_crs,
    reproject_geometry_to_crs
)
from gis_tool.db_utils import upsert_mongodb_feature

# Configure module-level logger
logger = logging.getLogger("gis_tool.data_loader")
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def create_pipeline_features(
    geojson_reports: List[Tuple[str, gpd.GeoDataFrame]],
    txt_reports: List[Tuple[str, List[str]]],
    gas_lines_gdf: gpd.GeoDataFrame,
    spatial_reference: str,
    gas_lines_collection: Optional[Collection] = None,
    processed_reports: Optional[Set[str]] = None,
    use_mongodb: bool = True
) -> Tuple[Set[str], gpd.GeoDataFrame, bool]:
    """
    Process GeoJSON and TXT pipeline reports to create or update gas line features.

    Uses the report_reader module to parse features from input reports and updates
    the gas_lines_gdf and optionally a MongoDB collection.

    Args:
        geojson_reports (List[Tuple[str, gpd.GeoDataFrame]]): List of tuples with report filenames and GeoDataFrames.
        txt_reports (List[Tuple[str, List[str]]]): List of tuples with report filenames and lines of TXT report.
        gas_lines_gdf (gpd.GeoDataFrame): Existing GeoDataFrame of gas line features.
        spatial_reference (str): Target spatial reference system (e.g., "EPSG:4326").
        gas_lines_collection (Optional[Collection]): Optional MongoDB collection for storing gas line features.
        processed_reports (Optional[Set[str]]): Set of filenames already processed.
        use_mongodb (bool): Flag to enable MongoDB upserts.

    Returns:
        Tuple containing:
            - Updated set of processed report filenames,
            - Updated gas_lines_gdf,
            - Boolean flag indicating if new features were added.
    """
    logger.info("Starting pipeline feature creation...")

    if not geojson_reports:
        logger.warning("No GeoJSON reports were found. Please check your input directory and file patterns.")
    if not txt_reports:
        logger.warning("No TXT reports were found. Please check your input directory and file patterns.")
    if gas_lines_gdf.empty:
        logger.warning("The existing gas_lines_gdf is empty. New features will create a new dataset.")

    processed_reports = processed_reports or set()
    processed_pipelines = set()
    features_added = False

    # Validate and reproject gas_lines_gdf CRS if necessary
    if gas_lines_gdf.crs is None:
        logger.error("gas_lines_gdf is missing CRS. Please define CRS before processing.")
        raise ValueError("gas_lines_gdf must have a CRS defined.")
    if gas_lines_gdf.crs.to_string() != spatial_reference:
        gas_lines_gdf = validate_and_reproject_crs(gas_lines_gdf, spatial_reference, "gas_lines_gdf")

    # Validate geometry column and types
    gas_lines_gdf = validate_geometry_column(
        gas_lines_gdf,
        "gas_lines_gdf",
        allowed_geom_types=["Point", "LineString", "Polygon"]
    )

    required_fields = ["Name", "Date", "PSI", "Material", "geometry"]

    def process_features(feature_list: List[dict]) -> None:
        """
        Internal helper to process individual features parsed from reports.

        Args:
            feature_list (List[dict]): List of feature dictionaries parsed from a report.
        """
        nonlocal gas_lines_gdf, features_added

        for feat in feature_list:
            missing_keys = [key for key in required_fields if key not in feat]
            if missing_keys:
                logger.error(f"Feature skipped due to missing required fields: {missing_keys} in feature {feat}")
                continue

            if feat["Name"] in processed_pipelines:
                continue  # Skip duplicates in current batch

            geom = feat["geometry"]

            # Assign CRS if missing
            if not hasattr(geom, "crs") or geom.crs is None:
                geom = gpd.GeoSeries([geom], crs=spatial_reference).iloc[0]
                feat["geometry"] = geom
                logger.debug(f"Assigned CRS {spatial_reference} to feature '{feat['Name']}' geometry.")

            # Reproject geometry if CRS differs
            elif geom.crs.to_string() != spatial_reference:
                source_crs = geom.crs.to_string() if geom.crs else None
                if source_crs is None:
                    logger.error(f"Geometry CRS undefined for feature '{feat['Name']}'. Cannot reproject. Skipping.")
                    continue
                geom = reproject_geometry_to_crs(geom, source_crs, spatial_reference)
                feat["geometry"] = geom
                logger.debug(f"Reprojected feature '{feat['Name']}' geometry to CRS {spatial_reference}.")

            geom_type = feat["geometry"].geom_type if feat.get("geometry") else None
            if geom_type not in ["Point", "LineString", "Polygon"]:
                logger.error(f"Unsupported geometry type '{geom_type}' in feature '{feat['Name']}'. Skipping.")
                continue

            if not validate_geometry_crs(feat["geometry"], spatial_reference):
                logger.error(f"Geometry CRS mismatch or undefined in feature '{feat['Name']}'. Skipping.")
                continue

            before_len = len(gas_lines_gdf)
            gas_lines_gdf = create_and_upsert_feature(
                name=feat["Name"],
                date=feat["Date"],
                psi=feat["PSI"],
                material=feat["Material"],
                geometry=feat["geometry"],
                spatial_reference=spatial_reference,
                gas_lines_gdf=gas_lines_gdf,
                gas_lines_collection=gas_lines_collection,
                use_mongodb=use_mongodb
            )
            if len(gas_lines_gdf) > before_len:
                processed_pipelines.add(feat["Name"])
                features_added = True

            if use_mongodb and gas_lines_collection is not None:
                try:
                    upsert_mongodb_feature(
                        collection=gas_lines_collection,
                        name=feat["Name"],
                        date=feat["Date"],
                        psi=feat["PSI"],
                        material=feat["Material"],
                        geometry=geom
                    )
                    logger.debug(f"Upserted feature '{feat['Name']}' into MongoDB.")
                except Exception as e:
                    logger.error(f"Failed to upsert feature '{feat['Name']}' into MongoDB: {e}")

    # Process GeoJSON reports
    for report_name, gdf in geojson_reports:
        if report_name in processed_reports:
            logger.info(f"Skipping already processed report: {report_name}")
            continue

        logger.info(f"Processing GeoJSON report: {report_name}")

        missing_cols = [col for col in required_fields if col not in gdf.columns]
        if missing_cols:
            logger.error(f"GeoJSON report '{report_name}' missing required fields: {missing_cols}. Skipping report.")
            processed_reports.add(report_name)
            continue

        if gdf.crs is None or gdf.crs.to_string() != spatial_reference:
            gdf = validate_and_reproject_crs(gdf, spatial_reference, report_name)

        gdf = validate_geometry_column(gdf, report_name, allowed_geom_types=["LineString", "Point"])

        features = report_reader.parse_geojson_report(gdf)
        process_features(features)
        processed_reports.add(report_name)

    # Process TXT reports
    for report_name, lines in txt_reports:
        if report_name in processed_reports:
            logger.info(f"Skipping already processed TXT report: {report_name}")
            continue

        logger.info(f"Processing TXT report: {report_name}")
        features = report_reader.parse_txt_report(lines)
        process_features(features)
        processed_reports.add(report_name)

    logger.info("Finished processing reports.")
    return processed_reports, gas_lines_gdf, features_added
