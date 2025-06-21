import pytest
import geopandas as gpd
import matplotlib

# Use non-interactive Agg backend for matplotlib to avoid GUI issues during testing
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import logging
from pathlib import Path

from gis_tool.spatial_utils import ensure_projected_crs

# Configure logger for detailed debug output during tests
logger = logging.getLogger("gis_tool")
logging.basicConfig(level=logging.DEBUG)


def get_gas_lines_shapefile_path() -> Path:
    """
    Resolve the absolute path to the gas_lines.shp shapefile used for testing.

    The path is calculated relative to this test file's location:
    - Parent directory of the test file
    - /data/shapefiles/gas_lines.shp

    Returns
    -------
    Path
        A pathlib.Path object pointing to the gas_lines shapefile location.
    """
    return Path(__file__).parent.parent / "data" / "shapefiles" / "gas_lines.shp"


def test_gas_lines_with_ensure_projected_crs(monkeypatch):
    """
    Test loading of the gas_lines shapefile, applying CRS projection enforcement,
    and plotting without raising errors.

    Steps:
    1. Dynamically locate the shapefile.
    2. Skip test if the shapefile is missing (to allow CI or partial environment runs).
    3. Load shapefile into GeoDataFrame.
    4. Call ensure_projected_crs to ensure geometries are in a projected CRS.
    5. Log the resulting CRS for verification.
    6. Patch matplotlib.pyplot.show to a no-op to prevent GUI blocking during tests.
    7. Plot the projected GeoDataFrame and call plt.show (patched).
    8. Confirm no exceptions raised during projection or plotting.

    Parameters
    ----------
    monkeypatch : _pytest.monkeypatch.MonkeyPatch
        Pytest fixture used here to patch plt.show to a no-op.

    Raises
    ------
    pytest.fail
        If ensure_projected_crs unexpectedly raises a ValueError.
    """
    logger.debug("Starting test_gas_lines_with_ensure_projected_crs")
    shp_path = get_gas_lines_shapefile_path()

    # Skip test if shapefile is not available on the test system
    if not shp_path.exists():
        logger.warning(f"Real shapefile not found at {shp_path}, skipping test")
        pytest.skip(f"Real shapefile not found at {shp_path}")

    # Load gas lines shapefile into GeoDataFrame
    gas_lines = gpd.read_file(shp_path)

    try:
        # Ensure gas lines GeoDataFrame uses a projected CRS
        gas_lines_projected = ensure_projected_crs(gas_lines)
        logger.info(f"Gas lines CRS after ensure_projected_crs: {gas_lines_projected.crs}")
    except ValueError as e:
        # Fail the test if a ValueError is raised unexpectedly
        pytest.fail(f"ensure_projected_crs raised ValueError unexpectedly: {e}")

    # Patch plt.show() to avoid GUI pop-up blocking the test run
    monkeypatch.setattr(plt, "show", lambda: None)

    # Plot the projected gas lines GeoDataFrame to check for plotting errors
    gas_lines_projected.plot()
    plt.title("Gas Lines after ensure_projected_crs")
    plt.show()  # This is patched to do nothing

    logger.info("test_gas_lines_with_ensure_projected_crs passed.")
