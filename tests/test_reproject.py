import logging
import pytest
from shapely.geometry import Point
from pyproj import Transformer

# Logger setup
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)


def test_round_trip_projection_wgs84_webmercator():
    """
    Tests round-trip reprojection from WGS84 (EPSG:4326) to Web Mercator (EPSG:3857) and back.

    Validates that projecting a coordinate to Web Mercator and then back to WGS84 returns
    coordinates very close to the original ones, within an acceptable floating point tolerance.
    """
    lon_original = -29.7
    lat_original = 75.0

    wgs84 = "EPSG:4326"
    mercator = "EPSG:3857"

    logger.info("Creating transformers for WGS84 <-> Web Mercator.")
    to_mercator = Transformer.from_crs(wgs84, mercator, always_xy=True)
    to_wgs84 = Transformer.from_crs(mercator, wgs84, always_xy=True)

    logger.info(f"Transforming original WGS84 point ({lon_original}, {lat_original}) to Web Mercator.")
    x, y = to_mercator.transform(lon_original, lat_original)
    logger.debug(f"Projected Web Mercator coordinates: ({x}, {y})")

    logger.info("Transforming back to WGS84.")
    lon_result, lat_result = to_wgs84.transform(x, y)
    logger.debug(f"Reprojected WGS84 coordinates: ({lon_result}, {lat_result})")

    assert lon_result == pytest.approx(lon_original, rel=1e-6)
    assert lat_result == pytest.approx(lat_original, rel=1e-6)
    logger.info("Round-trip reprojection passed.")


def test_point_reprojection_webmercator_to_wgs84_sanity():
    """
    Sanity check: Reprojects a known point from Web Mercator (EPSG:3857) to WGS84 (EPSG:4326),
    and verifies that the result falls within valid longitude and latitude bounds.
    """
    src_crs = "EPSG:3857"
    target_crs = "EPSG:4326"
    point = Point(-3310651.91, 14845919.69)

    logger.info(f"Reprojecting point {point} from EPSG:3857 to EPSG:4326.")
    transformer = Transformer.from_crs(src_crs, target_crs, always_xy=True)
    lon, lat = transformer.transform(point.x, point.y)
    logger.debug(f"Reprojected WGS84 coordinates: ({lon}, {lat})")

    assert -180 <= lon <= 180
    assert -90 <= lat <= 90
    logger.info("Sanity reprojection passed: coordinates within expected WGS84 bounds.")
