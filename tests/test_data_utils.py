### Tests for data_utils

import logging
import pytest
import geopandas as gpd
import pandas as pd

from unittest.mock import MagicMock
from shapely.geometry import Point, LineString, Polygon

from gis_tool import data_utils
from gis_tool.data_utils import make_feature
from gis_tool.config import DEFAULT_CRS

# Logger setup
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


# ---- UNIT TESTS ----

def test_make_feature_basic():
    name = "Pipe1"
    date = "2023-06-01"
    psi = 120.5
    material = "Steel"
    point = Point(-90.1, 29.9)
    crs = "EPSG:4326"

    gdf = data_utils.make_feature(name, date, psi, material, point, crs)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.crs.to_string() == crs
    assert len(gdf) == 1
    assert gdf.iloc[0]["Name"] == name
    assert gdf.iloc[0]["Date"] == date
    assert gdf.iloc[0]["PSI"] == psi
    assert gdf.iloc[0]["Material"] == material.lower()
    assert gdf.iloc[0].geometry.equals(point)


def test_make_feature_date_as_timestamp():
    # date as pd.Timestamp should work too
    name = "Pipe2"
    date = pd.Timestamp("2023-06-02")
    psi = 100.0
    material = "Copper"
    point = Point(-80, 35)
    crs = "EPSG:4326"

    gdf = data_utils.make_feature(name, date, psi, material, point, crs)
    assert gdf.iloc[0]["Date"] == date


def test_create_and_upsert_feature_adds_row(monkeypatch):
    # Setup initial GeoDataFrame
    crs = "EPSG:4326"
    initial_gdf = gpd.GeoDataFrame(
        columns=["Name", "Date", "PSI", "Material", "geometry"], crs=crs
    )

    # Patch upsert_mongodb_feature to a mock so it doesn't actually run
    monkeypatch.setattr(data_utils, "upsert_mongodb_feature", lambda *args, **kwargs: None)

    new_name = "Pipe3"
    new_date = "2023-06-03"
    new_psi = 90.0
    new_material = "PVC"
    new_point = Point(-75, 40)
    use_mongodb = True
    gas_lines_collection = MagicMock()

    updated_gdf = data_utils.create_and_upsert_feature(
        name=new_name,
        date=new_date,
        psi=new_psi,
        material=new_material,
        geometry=new_point,
        spatial_reference=crs,
        gas_lines_gdf=initial_gdf,
        gas_lines_collection=gas_lines_collection,
        use_mongodb=use_mongodb,
    )

    # Original should be empty
    assert len(initial_gdf) == 0
    # Updated should have one row
    assert len(updated_gdf) == 1
    # Columns should match
    assert set(updated_gdf.columns) == set(initial_gdf.columns)
    # Geometry should be preserved
    assert updated_gdf.iloc[0].geometry.equals(new_point)
    # Material should be lowercased
    assert updated_gdf.iloc[0]["Material"] == new_material.lower()


def test_create_and_upsert_feature_skips_mongodb(monkeypatch):
    # MongoDB insertion skipped if use_mongodb is False
    crs = "EPSG:4326"
    initial_gdf = gpd.GeoDataFrame(
        columns=["Name", "Date", "PSI", "Material", "geometry"], crs=crs
    )
    monkeypatch.setattr(data_utils, "upsert_mongodb_feature", lambda *args, **kwargs: pytest.fail("Should not be called"))

    updated_gdf = data_utils.create_and_upsert_feature(
        name="Pipe4",
        date="2023-06-04",
        psi=85.0,
        material="Steel",
        geometry=Point(-70, 45),
        spatial_reference=crs,
        gas_lines_gdf=initial_gdf,
        gas_lines_collection=None,
        use_mongodb=False,
    )
    assert len(updated_gdf) == 1


def test_create_and_upsert_feature_reindex_and_geometry_set():
    # Test reindexing and geometry column set_geometry call
    crs = "EPSG:4326"
    initial_gdf = gpd.GeoDataFrame(
        columns=["Name", "Date", "PSI", "Material", "geometry"], crs=crs
    )

    # Create a feature with columns out of order
    new_feature = gpd.GeoDataFrame(
        {
            "PSI": [50],
            "Date": ["2023-01-01"],
            "Name": ["Pipe5"],
            "Material": ["steel"],
            "geometry": [Point(0, 0)],
        },
        crs=crs,
    )

    # Manually call reindex to simulate inside function behavior
    reindexed = new_feature.reindex(columns=initial_gdf.columns)
    assert list(reindexed.columns) == list(initial_gdf.columns)

    # Check if geometry column is set correctly (should not raise)
    reindexed.set_geometry("geometry", inplace=True)

    # Use create_and_upsert_feature to confirm no errors and proper geometry handling
    updated_gdf = data_utils.create_and_upsert_feature(
        name="Pipe5",
        date="2023-01-01",
        psi=50,
        material="steel",
        geometry=Point(0, 0),
        spatial_reference=crs,
        gas_lines_gdf=initial_gdf,
        gas_lines_collection=None,
        use_mongodb=False,
    )
    assert isinstance(updated_gdf, gpd.GeoDataFrame)
    assert "geometry" in updated_gdf.columns
    assert updated_gdf.crs.to_string() == crs


def test_make_feature_creates_valid_gdf():
    """
    Tests make_feature creates a GeoDataFrame with the expected schema,
    applies case normalization to material, and sets the CRS.
    """
    logger.info("Testing make_feature function.")
    feature = make_feature("LineX", "2022-05-01", 300, "PVC", Point(1, 2), DEFAULT_CRS)
    assert isinstance(feature, gpd.GeoDataFrame)
    assert feature.iloc[0]["Material"] == "pvc"
    assert feature.crs.to_string() == DEFAULT_CRS
    logger.debug("make_feature created a valid GeoDataFrame with correct CRS and normalized material.")
    logger.info("make_feature test passed.")


@pytest.mark.parametrize("geom", [
    Point(-75, 40),
    LineString([(-75, 40), (-76, 41), (-77, 39)]),
    Polygon([(-75, 40), (-76, 41), (-77, 39), (-75, 40)])
])
def test_create_and_upsert_feature_various_geometries(monkeypatch, geom):
    crs = "EPSG:4326"
    initial_gdf = gpd.GeoDataFrame(
        columns=["Name", "Date", "PSI", "Material", "geometry"], crs=crs
    )

    monkeypatch.setattr(data_utils, "upsert_mongodb_feature", lambda *args, **kwargs: None)

    updated_gdf = data_utils.create_and_upsert_feature(
        name="TestPipe",
        date="2023-06-10",
        psi=100.0,
        material="TestMaterial",
        geometry=geom,
        spatial_reference=crs,
        gas_lines_gdf=initial_gdf,
        gas_lines_collection=None,
        use_mongodb=False,
    )

    assert len(updated_gdf) == 1
    assert updated_gdf.iloc[0].geometry.equals(geom)
    assert updated_gdf.iloc[0]["Material"] == "testmaterial"
