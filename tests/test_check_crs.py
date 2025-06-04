import pytest
import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import logging
from pathlib import Path

# Import the functions under test
from gis_tool.spatial_utils import ensure_projected_crs
shapefile_path = Path(__file__).parent.parent / "data" / "gas_lines.shp"
gas_lines_gdf = gpd.read_file(shapefile_path)

# Configure logger for tests
logger = logging.getLogger("gis_tool")
logging.basicConfig(level=logging.DEBUG)


def get_gas_lines_shapefile_path() -> Path:
    """
    Dynamically resolve path to data/gas_lines.shp relative to this test file.
    """
    return Path(__file__).parent.parent / "data" / "gas_lines.shp"


def test_gas_lines_with_ensure_projected_crs(monkeypatch):
    """
    Test that gas_lines.shp loads, gets projected CRS via ensure_projected_crs,
    and plots without error.
    """
    shp_path = get_gas_lines_shapefile_path()
    if not shp_path.exists():
        pytest.skip(f"Real shapefile not found at {shp_path}")

    gas_lines = gpd.read_file(shp_path)
    gas_lines_projected = None  # initialize before try

    # This should raise ValueError if no CRS; or reproject if geographic CRS
    try:
        gas_lines_projected = ensure_projected_crs(gas_lines)
        logger.info(f"Gas lines CRS after ensure_projected_crs: {gas_lines_projected.crs}")
    except ValueError as e:
        pytest.fail(f"ensure_projected_crs raised ValueError unexpectedly: {e}")

    # Patch plt.show() to avoid opening a window during tests
    monkeypatch.setattr(plt, "show", lambda: None)
    gas_lines_projected.plot()
    plt.title("Gas Lines after ensure_projected_crs")
    plt.show()

    logger.info("test_gas_lines_with_ensure_projected_crs passed.")
