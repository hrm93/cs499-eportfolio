### data_loader.py
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
"""

import os
import logging
from typing import Any, List, Optional, Set, Tuple, Union
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, mapping
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from pymongo.database import Database
from dateutil.parser import parse
from gis_tool.config import MONGODB_URI, DB_NAME

# Note: 'material' field is normalized to lowercase for consistency.
# Other string fields like 'name' retain original casing.
SCHEMA_FIELDS = ["Name", "Date", "PSI", "Material", "geometry"]


def robust_date_parse(date_val: Any) -> Union[pd.Timestamp, pd.NaT]:
    """
    Robustly parse various date formats or objects into a pandas Timestamp.

    Args:
        date_val (Any): Input date value (can be string, Timestamp, or NaN).

    Returns:
        Union[pd.Timestamp, pd.NaT]: A valid pandas Timestamp or pd.NaT if parsing fails.
    """
    if pd.isna(date_val):
        return pd.NaT
    if isinstance(date_val, pd.Timestamp):
        return date_val
    if isinstance(date_val, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return pd.to_datetime(date_val, format=fmt)
            except (ValueError, TypeError):
                continue
        try:
            return pd.to_datetime(parse(date_val, fuzzy=False))
        except (ValueError, TypeError):
            return pd.NaT
    return pd.NaT


def connect_to_mongodb(uri: str = MONGODB_URI, db_name: str = DB_NAME) -> Database:
    """
    Establish a connection to a MongoDB database.

    Args:
        uri (str): MongoDB connection URI.
        db_name (str): Name of the database to connect.

    Returns:
        pymongo.database.Database: Connected MongoDB database instance.

    Raises:
        ConnectionFailure: If the connection to MongoDB fails.
    """
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')  # Test the connection
        logging.info("Connected to MongoDB successfully.")
        return client[db_name]
    except ConnectionFailure as e:
        logging.error(f"Could not connect to MongoDB: {e}")
        raise


def simplify_geometry(geom: Point, tolerance=0.00001) -> dict:
    # Simplify geometry to avoid floating point precision issues
    simplified = geom.simplify(tolerance, preserve_topology=True)
    return mapping(simplified)


def upsert_mongodb_feature(collection: Collection, name: str, date, psi: float, material: str, geometry: Point) -> None:
    """
    Insert or update a gas line feature in MongoDB.

    Args:
        collection (Collection): MongoDB collection to update/insert into.
        name (str): Name of the gas line feature.
        date: Date associated with the feature (can be string or datetime).
        psi (float): Pressure specification.
        material (str): Material type.
        geometry (Point): Shapely Point geometry object.
    """
    feature_doc = {
        'name': name,
        'date': date,
        'psi': psi,
        'material': material,
        'geometry': simplify_geometry(geometry)  # Use simplified geom
    }
    existing = collection.find_one({'name': name, 'geometry': feature_doc['geometry']})
    if existing:
        # Check if any fields changed before updating
        changes = {}
        if existing.get('date') != date:
            changes['date'] = date
        if existing.get('psi') != psi:
            changes['psi'] = psi
        if existing.get('material') != material:
            changes['material'] = material

        if changes:
            collection.update_one({'_id': existing['_id']}, {'$set': changes})
            logging.info(f"Updated MongoDB record for {name} with changes: {changes}")
        else:
            logging.info(f"No changes detected for MongoDB record {name}, skipping update.")
    else:
        collection.insert_one(feature_doc)
        logging.info(f"Inserted new MongoDB record for {name}.")


def make_feature(
    name: str, date: Union[str, pd.Timestamp], psi: float, material: str, geometry: Point, crs: str
) -> gpd.GeoDataFrame:
    data = {
        SCHEMA_FIELDS[0]: [name],
        SCHEMA_FIELDS[1]: [date],
        SCHEMA_FIELDS[2]: [psi],
        SCHEMA_FIELDS[3]: [material.lower()],
        SCHEMA_FIELDS[4]: [geometry]
    }
    return gpd.GeoDataFrame(data, crs=crs)


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
        logging.error(f"Input folder does not exist: {input_folder}")
        return []

    new_reports = [f for f in all_files if f.lower().endswith(('.txt', '.geojson'))]

    if not new_reports:
        logging.info("No new reports found in the input folder.")
    else:
        logging.info(f"Found {len(new_reports)} new report(s): {new_reports}")

    return new_reports


def load_geojson_report(filepath: str, crs: str) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON report from file and convert CRS to the target spatial_reference.
    """
    gdf = gpd.read_file(filepath)
    if gdf.crs is None or gdf.crs != crs:
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
        logging.error(f"TXT report file not found: {filepath}")
        return []
    except IOError as e:
        logging.error(f"Error reading TXT report file {filepath}: {e}")
        return []


def create_pipeline_features(
    geojson_reports: List[Tuple[str, gpd.GeoDataFrame]],
    txt_reports: List[Tuple[str, List[str]]],
    gas_lines_gdf: gpd.GeoDataFrame,
    spatial_reference: str,
    gas_lines_collection: Optional[Collection] = None,
    processed_reports: Optional[Set[str]] = None,
    use_mongodb: bool = True
) -> Tuple[Set[str], gpd.GeoDataFrame, bool]:
    if processed_reports is None:
        processed_reports = set()

    processed_pipelines = set()
    features_added = False

    # ✅ Normalize gas_lines_gdf CRS to spatial_reference
    if gas_lines_gdf.crs and gas_lines_gdf.crs.to_string() != spatial_reference:
        gas_lines_gdf = gas_lines_gdf.to_crs(spatial_reference)

    # ✅ Normalize GeoJSON report CRS to match spatial_reference
    for i, (report_name, gdf) in enumerate(geojson_reports):
        if gdf.crs and gdf.crs.to_string() != spatial_reference:
            gdf = gdf.to_crs(spatial_reference)
            geojson_reports[i] = (report_name, gdf)

    # Helper to align columns and dtypes, fix geometry
    def align_feature_dtypes(new_feat: gpd.GeoDataFrame, base_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        new_feat = new_feat.reindex(columns=base_gdf.columns)
        for col in base_gdf.columns:
            if col in new_feat.columns:
                try:
                    new_feat[col] = new_feat[col].astype(base_gdf[col].dtype)
                except Exception as e:
                    logging.debug(f"Could not convert column '{col}' dtype: {e}")
        if 'geometry' in new_feat.columns:
            new_feat.set_geometry('geometry', inplace=True)
        return new_feat

    # Process GeoJSON reports
    for report_name, gdf in geojson_reports:
        if report_name in processed_reports:
            logging.info(f"Skipping already processed report: {report_name}")
            continue

        # gdf already normalized to spatial_reference above

        required_fields = set(SCHEMA_FIELDS) - {"geometry"}
        missing_fields = required_fields - set(gdf.columns)
        if missing_fields:
            logging.error(f"GeoJSON report '{report_name}' missing required fields: {missing_fields}")
            processed_reports.add(report_name)
            continue

        for _, row in gdf.iterrows():
            point = row.geometry

            parsed_date = robust_date_parse(row["Date"])
            new_feature = make_feature(row["Name"], parsed_date, row["PSI"], row["Material"], point, spatial_reference)

            if use_mongodb and gas_lines_collection:
                upsert_mongodb_feature(
                    gas_lines_collection,
                    row["Name"],
                    row["Date"],
                    row["PSI"],
                    row["Material"],
                    point,
                )

            new_feature = align_feature_dtypes(new_feature, gas_lines_gdf)
            valid_rows = new_feature.dropna(how="all")

            if not valid_rows.empty:
                gas_lines_gdf = pd.concat([gas_lines_gdf, valid_rows], ignore_index=True)
                processed_pipelines.add(row["Name"])
                features_added = True

        processed_reports.add(report_name)

    # Process TXT reports
    for report_name, lines in txt_reports:
        if report_name in processed_reports:
            logging.info(f"Skipping already processed report: {report_name}")
            continue

        for line_number, line in enumerate(lines, start=1):
            if "Id Name" in line:
                continue

            data = line.strip().split()
            if len(data) < 7:
                logging.warning(
                    f"Skipping malformed line {line_number} in {report_name} "
                    f"(expected at least 7 fields): {line.strip()}"
                )
                continue

            try:
                line_name = data[1]
                x_coord = float(data[2])
                y_coord = float(data[3])
                date_completed = data[4]
                psi = float(data[5])
                material = data[6].lower()
            except (ValueError, IndexError) as e:
                logging.warning(
                    f"Skipping line {line_number} in {report_name} due to parse error: "
                    f"{line.strip()} | Error: {e}"
                )
                continue

            if line_name not in processed_pipelines:
                point = Point(x_coord, y_coord)
                parsed_date = robust_date_parse(date_completed)
                new_feature = make_feature(line_name, parsed_date, psi, material, point, spatial_reference)

                if use_mongodb and gas_lines_collection:
                    upsert_mongodb_feature(
                        gas_lines_collection, line_name, date_completed, psi, material, point
                    )

                new_feature = align_feature_dtypes(new_feature, gas_lines_gdf)
                valid_rows = new_feature.dropna(how="all")

                if not valid_rows.empty:
                    gas_lines_gdf = pd.concat([gas_lines_gdf, valid_rows], ignore_index=True)
                    processed_pipelines.add(line_name)
                    features_added = True

        processed_reports.add(report_name)

    return processed_reports, gas_lines_gdf, features_added



