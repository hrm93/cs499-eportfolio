import pytest
import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import logging
from pathlib import Path

from gis_tool.spatial_utils import ensure_projected_crs

# Configure logger for tests
logger = logging.getLogger("gis_tool")
logging.basicConfig(level=logging.DEBUG)


def get_gas_lines_shapefile_path() -> Path:
    """
    Dynamically resolve path to data/shapefiles/gas_lines.shp relative to this test file.
    """
    return Path(__file__).parent.parent / "data" / "shapefiles" / "gas_lines.shp"


def test_gas_lines_with_ensure_projected_crs(monkeypatch):
    """
    Test that gas_lines.shp loads, gets projected CRS via ensure_projected_crs,
    and plots without error.
    """
    shp_path = get_gas_lines_shapefile_path()
    if not shp_path.exists():
        pytest.skip(f"Real shapefile not found at {shp_path}")

    gas_lines = gpd.read_file(shp_path)

    try:
        gas_lines_projected = ensure_projected_crs(gas_lines)
        logger.info(f"Gas lines CRS after ensure_projected_crs: {gas_lines_projected.crs}")
    except ValueError as e:
        pytest.fail(f"ensure_projected_crs raised ValueError unexpectedly: {e}")

    # Patch plt.show() to avoid GUI interruption during test
    monkeypatch.setattr(plt, "show", lambda: None)
    gas_lines_projected.plot()
    plt.title("Gas Lines after ensure_projected_crs")
    plt.show()

    logger.info("test_gas_lines_with_ensure_projected_crs passed.")
