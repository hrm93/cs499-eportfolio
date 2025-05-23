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
from typing import List, Optional, Set, Union

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, mapping
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from pymongo.database import Database
from dateutil.parser import parse

from gis_tool.config import MONGODB_URI, DB_NAME

def robust_date_parse(date_val):
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
            return parse(date_val, fuzzy=False)
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


def create_pipeline_features(
    report_files: List[str],
    gas_lines_shp: str,
    reports_folder: str,
    spatial_reference: str,
    gas_lines_collection: Optional[Collection] = None,
    processed_reports: Optional[Set[str]] = None,
    use_mongodb: bool = True
) -> None:
    """
    Parse pipeline reports (.txt or .geojson), create new gas line features,
    and optionally update MongoDB records.

    Args:
        report_files (List[str]): List of report filenames to process.
        gas_lines_shp (str): Path to the existing gas lines shapefile.
        reports_folder (str): Directory containing the report files.
        spatial_reference (str): Coordinate Reference System (CRS) for output features.
        gas_lines_collection (Optional[Collection]): MongoDB collection to update/insert features.
        processed_reports (Optional[Set[str]]): Set of report filenames already processed.
        use_mongodb (bool): Flag to enable MongoDB update/insert operations.

    Raises:
        Exception: Propagates exceptions encountered during processing.
    """
    processed_pipelines = set()
    features_added = False
    if processed_reports is None:
        processed_reports = set()

    # Load existing shapefile or create new GeoDataFrame with expected schema
    if os.path.exists(gas_lines_shp):
        gas_lines_gdf = gpd.read_file(gas_lines_shp)
        if gas_lines_gdf.crs is None:
            gas_lines_gdf.set_crs(spatial_reference, inplace=True)
        geom_types = gas_lines_gdf.geom_type.unique()
        if len(geom_types) != 1 or geom_types[0] != "Point":
            logging.warning(
                f"Existing shapefile {gas_lines_shp} geometry type is {geom_types}. "
                "Expected 'Point'. Creating empty GeoDataFrame with correct schema."
            )
            gas_lines_gdf = gpd.GeoDataFrame(
                columns=["Name", "Date", "PSI", "Material", "geometry"],
                crs=spatial_reference,
                geometry='geometry'
            )
    else:
        gas_lines_gdf = gpd.GeoDataFrame(
            columns=["Name", "Date", "PSI", "Material", "geometry"],
            crs=spatial_reference,
            geometry='geometry'
        )

    processed_reports_basenames = {os.path.basename(r) for r in processed_reports}

    for report_file in report_files:
        report_name = os.path.basename(report_file)

        if report_name in processed_reports_basenames:
            logging.info(f"Skipping already processed report: {report_name}")
            continue

        full_report_path = os.path.join(reports_folder, report_file)
        if not os.path.exists(full_report_path):
            logging.warning(f"Report file does not exist: {full_report_path}")
            continue

        try:
            if report_name.lower().endswith(".geojson"):
                # Process GeoJSON reports
                gdf = gpd.read_file(full_report_path).to_crs(spatial_reference)

                required_fields = {"Name", "Date", "PSI", "Material"}
                missing_fields = required_fields - set(gdf.columns)
                if missing_fields:
                    logging.error(f"GeoJSON report '{report_name}' missing required fields: {missing_fields}")
                    continue

                for _, row in gdf.iterrows():
                    point = row.geometry
                    new_feature = gpd.GeoDataFrame(
                        {
                            "Name": [row["Name"]],
                            "Date": [row["Date"]],
                            "PSI": [row["PSI"]],
                            "Material": [row["Material"].lower()],
                            "geometry": [point]
                        },
                        crs=spatial_reference
                    )

                    if use_mongodb and gas_lines_collection is not None:
                        feature_doc = {
                            'name': row["Name"],
                            'date': row["Date"],
                            'psi': row["PSI"],
                            'material': row["Material"],
                            'geometry': mapping(point)
                        }
                        existing = gas_lines_collection.find_one({
                            'name': row["Name"],
                            'geometry': feature_doc['geometry']
                        })
                        if existing:
                            gas_lines_collection.update_one(
                                {'_id': existing['_id']},
                                {'$set': {'date': row["Date"], 'psi': row["PSI"], 'material': row["Material"]}}
                            )
                            logging.info(f"Updated existing MongoDB record for {row['Name']}.")
                        else:
                            gas_lines_collection.insert_one(feature_doc)
                            logging.info(f"Inserted new MongoDB record for {row['Name']}.")

                    if not new_feature.empty and not new_feature.isna().all(axis=1).all():
                        frames_to_concat = [df for df in [gas_lines_gdf, new_feature] if
                                            not df.empty and not df.isna().all(axis=1).all()]
                        if frames_to_concat:
                            gas_lines_gdf = pd.concat(frames_to_concat, ignore_index=True)

                    processed_pipelines.add(row["Name"])
                    features_added = True

            elif report_name.lower().endswith(".txt"):
                # Process TXT reports line by line
                with open(full_report_path, 'r') as file:
                    for line_number, line in enumerate(file, start=1):
                        if "Id Name" in line:
                            continue  # Skip header or irrelevant lines

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
                            new_feature = gpd.GeoDataFrame(
                                {
                                    "Name": [line_name],
                                    "Date": [date_completed],
                                    "PSI": [psi],
                                    "Material": [material],
                                    "geometry": [point]
                                },
                                crs=spatial_reference
                            )

                            if use_mongodb and gas_lines_collection is not None:
                                feature_doc = {
                                    'name': line_name,
                                    'date': date_completed,
                                    'psi': psi,
                                    'material': material,
                                    'geometry': mapping(point)
                                }
                                existing = gas_lines_collection.find_one({
                                    'name': line_name,
                                    'geometry': feature_doc['geometry']
                                })
                                if existing:
                                    gas_lines_collection.update_one(
                                        {'_id': existing['_id']},
                                        {'$set': {'date': date_completed, 'psi': psi, 'material': material}}
                                    )
                                    logging.info(f"Updated existing MongoDB record for {line_name}.")
                                else:
                                    gas_lines_collection.insert_one(feature_doc)
                                    logging.info(f"Inserted new MongoDB record for {line_name}.")

                            if not new_feature.empty and not new_feature.isna().all(axis=1).all():
                                frames_to_concat = [df for df in [gas_lines_gdf, new_feature] if
                                                    not df.empty and not df.isna().all(axis=1).all()]
                                if frames_to_concat:
                                    gas_lines_gdf = pd.concat(frames_to_concat, ignore_index=True)
                                    processed_pipelines.add(line_name)
                                    features_added = True
            else:
                logging.warning(f"Unsupported report file type: {report_name}")

            processed_reports.add(report_name)

        except Exception as e:
            logging.error(f"Error processing report {report_name}: {e}")
            continue

    # Save updated shapefile if new features were added or file doesn't exist
    if features_added or not os.path.exists(gas_lines_shp):
        # Use robust parsing on the 'Date' column
        gas_lines_gdf['Date'] = gas_lines_gdf['Date'].apply(robust_date_parse)

        # Convert to shapefile-safe string format
        gas_lines_gdf['Date'] = gas_lines_gdf['Date'].dt.strftime('%Y-%m-%d')

        gas_lines_gdf.to_file(gas_lines_shp, driver="ESRI Shapefile")
        logging.info(f"Saved {len(gas_lines_gdf)} features to {gas_lines_shp}.")
    else:
        logging.info("No new pipeline features added; shapefile remains unchanged.")
