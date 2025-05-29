# Test for checking crs
import logging

import geopandas as gpd
import matplotlib.pyplot as plt

logger = logging.getLogger("gis_tool")


def load_and_plot_gas_lines(shapefile_path: str) -> None:
    """
    Load gas lines shapefile, check and set CRS if missing, then plot.

    Args:
        shapefile_path (str): Path to the gas lines shapefile.

    Behavior:
        - Loads the shapefile using GeoPandas.
        - Checks if the CRS is defined; if not, sets it to UTM zone 10N (EPSG:32610).
        - Logs CRS status and a preview of geometries.
        - Displays a plot of the gas lines.
    """
    logger.info(f"Loading gas lines shapefile from {shapefile_path}")
    gas_lines = gpd.read_file(shapefile_path)

    if gas_lines.crs is None:
        logger.warning("Gas lines CRS is undefined. Setting to EPSG:32610 (UTM zone 10N).")
        gas_lines = gas_lines.set_crs("EPSG:32610", allow_override=True)

    logger.info(f"Gas lines CRS is: {gas_lines.crs}")
    logger.debug(f"Sample geometries:\n{gas_lines.geometry.head()}")

    gas_lines.plot()
    plt.title("Gas Lines with assumed CRS")
    plt.show()
    logger.info("Gas lines plotted successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    shapefile = "data/gas_lines.shp"
    load_and_plot_gas_lines(shapefile)
