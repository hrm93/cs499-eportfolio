import os
import fiona
import pyproj
import geopandas as gpd
from shapely.geometry import Point

def test_fiona():
    assert "GeoJSON" in fiona.supported_drivers

def test_shapely():
    pt = Point(1, 2).buffer(1.0)
    assert pt.area > 0

def test_pyproj():
    crs = pyproj.CRS.from_epsg(4326)
    assert crs.to_epsg() == 4326

def test_geopandas_local():
    path = os.path.join("data", "ne_110m_populated_places.shp")
    gdf = gpd.read_file(path)
    assert not gdf.empty
