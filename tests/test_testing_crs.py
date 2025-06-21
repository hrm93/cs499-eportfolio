import geopandas as gpd

parks_path = "/data/shapefiles/parks.shp"
parks_gdf = gpd.read_file(parks_path)
print("CRS detected:", parks_gdf.crs)
