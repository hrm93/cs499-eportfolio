import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
import os
from datetime import date

# Create output folder
output_dir = "data/input"
os.makedirs(output_dir, exist_ok=True)

# Common dummy values
dummy_date = date.today().isoformat()  # e.g., '2025-06-01'
dummy_material = "PVC"
dummy_psi = 100

# Dummy points layer with required fields
points_data = {
    "id": [1, 2, 3],
    "Name": ["P1", "P2", "P3"],
    "Date": [dummy_date]*3,
    "Material": [dummy_material]*3,
    "PSI": [dummy_psi]*3,
    "geometry": [Point(0, 0), Point(1, 1), Point(2, 2)]
}
gdf_points = gpd.GeoDataFrame(points_data, crs="EPSG:4326")
gdf_points.to_file(f"{output_dir}/dummy_points.shp")
gdf_points.to_file(f"{output_dir}/dummy_points.geojson", driver="GeoJSON")

# Dummy lines layer
lines_data = {
    "id": [1, 2],
    "Name": ["L1", "L2"],
    "Date": [dummy_date]*2,
    "Material": [dummy_material]*2,
    "PSI": [dummy_psi]*2,
    "geometry": [LineString([(0, 0), (1, 1)]), LineString([(1, 0), (2, 2)])]
}
gdf_lines = gpd.GeoDataFrame(lines_data, crs="EPSG:4326")
gdf_lines.to_file(f"{output_dir}/dummy_lines.shp")
gdf_lines.to_file(f"{output_dir}/dummy_lines.geojson", driver="GeoJSON")

# Dummy polygons layer
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

# Create dummy text report files with at least 7 comma-separated fields per line
with open(f"{output_dir}/report1.txt", "w") as f:
    f.write(
        "Name1,2025-06-01,PVC,100,Extra1,Extra2,Extra3\n"
        "Name2,2025-06-02,PVC,100,Extra4,Extra5,Extra6\n"
        "Name3,2025-06-03,PVC,100,Extra7,Extra8,Extra9\n"
    )

with open(f"{output_dir}/report2.txt", "w") as f:
    f.write(
        "PipeA,2025-06-01,PVC,150,X1,X2,X3\n"
        "PipeB,2025-06-02,PVC,150,Y1,Y2,Y3\n"
        "PipeC,2025-06-03,PVC,150,Z1,Z2,Z3\n"
    )

print("✅ Dummy geospatial and report files created in:", output_dir)
