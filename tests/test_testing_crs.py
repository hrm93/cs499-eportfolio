import geopandas as gpd

parks_path = "C:/Users/xrose/PycharmProjects/PythonProject/data/parks.shp"
parks_gdf = gpd.read_file(parks_path)
print("CRS detected:", parks_gdf.crs)
