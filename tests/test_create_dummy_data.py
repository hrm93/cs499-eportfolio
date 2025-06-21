"""
Script to generate dummy geospatial and text report files for testing the GIS pipeline.

This script creates:
- Point, line, and polygon shapefiles and GeoJSON files with dummy attributes
- Two .txt report files with CSV-style dummy data
- All files are saved in the 'data/input' directory

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import logging
import os
from datetime import date

import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon

# Configure logging for this script
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)

# Ensure the output directory exists
output_dir = "data/input"
os.makedirs(output_dir, exist_ok=True)
logger.info(f"Output directory ensured at: {output_dir}")

# ---- Common attribute values ----
dummy_date = date.today().isoformat()  # Example: '2025-06-21'
dummy_material = "PVC"
dummy_psi = 100

# ---- Create dummy points layer ----
points_data = {
    "id": [1, 2, 3],
    "Name": ["P1", "P2", "P3"],
    "Date": [dummy_date] * 3,
    "Material": [dummy_material] * 3,
    "PSI": [dummy_psi] * 3,
    "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]
}
gdf_points = gpd.GeoDataFrame(points_data, crs="EPSG:4326")
gdf_points.to_file(f"{output_dir}/dummy_points.shp")
gdf_points.to_file(f"{output_dir}/dummy_points.geojson", driver="GeoJSON")
logger.info("Dummy points layer created.")

# ---- Create dummy lines layer ----
lines_data = {
    "id": [1, 2],
    "Name": ["L1", "L2"],
    "Date": [dummy_date] * 2,
    "Material": [dummy_material] * 2,
    "PSI": [dummy_psi] * 2,
    "geometry": [
        LineString([(0, 0), (1, 1)]),
        LineString([(1, 0), (2, 2)])
    ]
}
gdf_lines = gpd.GeoDataFrame(lines_data, crs="EPSG:4326")
gdf_lines.to_file(f"{output_dir}/dummy_lines.shp")
gdf_lines.to_file(f"{output_dir}/dummy_lines.geojson", driver="GeoJSON")
logger.info("Dummy lines layer created.")

# ---- Create dummy polygons layer ----
polygons_data = {
    "id": [1],
    "Name": ["Poly1"],
    "Date": [dummy_date],
    "Material": [dummy_material],
    "PSI": [dummy_psi],
    "geometry": [Polygon([(0, 0), (1, 1), (1, 0), (0, 0)])]
}
gdf_polygons = gpd.GeoDataFrame(polygons_data, crs="EPSG:4326")
gdf_polygons.to_file(f"{output_dir}/dummy_polygons.shp")
gdf_polygons.to_file(f"{output_dir}/dummy_polygons.geojson", driver="GeoJSON")
logger.info("Dummy polygons layer created.")

# ---- Create dummy text report files ----
report1_lines = [
    "Name1,2025-06-01,PVC,100,Extra1,Extra2,Extra3\n",
    "Name2,2025-06-02,PVC,100,Extra4,Extra5,Extra6\n",
    "Name3,2025-06-03,PVC,100,Extra7,Extra8,Extra9\n"
]
report2_lines = [
    "PipeA,2025-06-01,PVC,150,X1,X2,X3\n",
    "PipeB,2025-06-02,PVC,150,Y1,Y2,Y3\n",
    "PipeC,2025-06-03,PVC,150,Z1,Z2,Z3\n"
]

with open(f"{output_dir}/report1.txt", "w") as f:
    f.writelines(report1_lines)
logger.info("Dummy report1.txt file created.")

with open(f"{output_dir}/report2.txt", "w") as f:
    f.writelines(report2_lines)
logger.info("Dummy report2.txt file created.")

logger.info(f"✅ All dummy geospatial and report files successfully created in: {output_dir}")
