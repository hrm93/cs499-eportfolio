### Tests for utils.py
import logging
import pytest
import pandas as pd
import geopandas as gpd

from pyproj import CRS
from shapely.geometry.base import BaseGeometry

from gis_tool.utils import (
    robust_date_parse,
    convert_ft_to_m,
)

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs


@pytest.mark.parametrize("input_str, expected", [
    ("2023-05-01", pd.Timestamp("2023-05-01")),
    ("01/05/2023", pd.Timestamp("2023-05-01")),
    ("05/01/2023", pd.Timestamp("2023-01-05")),
    ("not a date", pd.NaT),
    (None, pd.NaT),
])
def test_robust_date_parse(input_str, expected):
    """
     Tests the robust_date_parse function with various date string formats and invalid inputs.
     Verifies that the function correctly parses valid dates or returns pd.NaT for invalid inputs.
     """
    logger.info(f"Testing robust_date_parse with input: {input_str}")
    result = robust_date_parse(input_str)
    if pd.isna(expected):
        assert pd.isna(result)
        logger.debug(f"Input '{input_str}' correctly parsed as NaT.")
    else:
        assert result == expected
        logger.debug(f"Input '{input_str}' correctly parsed as {result}.")


def test_convert_ft_to_m():
    assert convert_ft_to_m(1) == 0.3048
    assert convert_ft_to_m(0) == 0.0
    assert abs(convert_ft_to_m(100) - 30.48) < 1e-6


def assert_geodataframes_equal(gdf1, gdf2, tol=1e-6):
    """
      Assert that two GeoDataFrames are equal in terms of CRS, length, and geometry,
      with geometries compared using an exact match within a given tolerance.

      Parameters
      ----------
      gdf1 : geopandas.GeoDataFrame
          The first GeoDataFrame to compare.
      gdf2 : geopandas.GeoDataFrame
          The second GeoDataFrame to compare.
      tol : float, optional
          Tolerance for geometry equality comparison (default is 1e-6).

      Raises
      ------
      AssertionError
          If any of the checks (type, CRS, length, geometry equality) fail.
      """
    logger.info("Comparing two GeoDataFrames for equality.")
    assert isinstance(gdf1, gpd.GeoDataFrame), "First input is not a GeoDataFrame"
    assert isinstance(gdf2, gpd.GeoDataFrame), "Second input is not a GeoDataFrame"

    # Normalize CRS for comparison
    crs1 = gdf1.crs
    crs2 = gdf2.crs
    assert crs1 is not None and crs2 is not None, "One or both GeoDataFrames lack CRS"
    assert CRS(crs1).equals(CRS(crs2)), f"CRS mismatch: {crs1} != {crs2}"

    assert len(gdf1) == len(gdf2), "GeoDataFrames have different lengths"

    for i, (geom1, geom2) in enumerate(zip(gdf1.geometry, gdf2.geometry)):
        # Check for None or invalid geometry
        if not isinstance(geom1, BaseGeometry) or geom1.is_empty or geom1 is None:
            raise AssertionError(f"Invalid geometry at index {i} in first GeoDataFrame")
        if not isinstance(geom2, BaseGeometry) or geom2.is_empty or geom2 is None:
            raise AssertionError(f"Invalid geometry at index {i} in second GeoDataFrame")

        equal = geom1.equals_exact(geom2, tolerance=tol)
        logger.debug(f"Geometry check at index {i}: {'equal' if equal else 'not equal'}")
        assert equal, f"Geometries at index {i} differ"

    logger.info("GeoDataFrames are equal.")
