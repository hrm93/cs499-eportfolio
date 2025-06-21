"""
fix_missing_crs.py

Utility script to check and fix missing Coordinate Reference System (CRS)
information in shapefiles. If a shapefile lacks CRS metadata, this script
assigns a default CRS and optionally overwrites the original file or saves a
new copy with the assigned CRS.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""
import sys
import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_missing_crs")

def fix_shapefile_crs(shp_path, default_crs="EPSG:32633", overwrite=True):
    """
      Check a shapefile for missing CRS and assign a default CRS if absent.

      Parameters:
          shp_path (str): Path to the shapefile (.shp) to check and fix.
          default_crs (str): The CRS to assign if missing. Default is "EPSG:32633".
          overwrite (bool): Whether to overwrite the original shapefile or save a new
                            file with "_with_crs" suffix. Default is True (overwrite).

      Behavior:
          - Reads the shapefile using GeoPandas.
          - Checks if CRS is defined.
          - If missing, sets the default CRS.
          - Saves either overwriting the original or as a new file.
          - Logs informative messages about actions taken.

      Exceptions:
          - Logs an error if any failure occurs during reading or writing.
      """
    try:
        gdf = gpd.read_file(shp_path)
        if gdf.crs is None:
            logger.warning(f"Shapefile '{shp_path}' missing CRS. Assigning default CRS '{default_crs}'.")
            gdf = gdf.set_crs(default_crs)
            if overwrite:
                gdf.to_file(shp_path)
                logger.info(f"Saved shapefile with CRS to '{shp_path}'.")
            else:
                out_path = shp_path.replace(".shp", "_with_crs.shp")
                gdf.to_file(out_path)
                logger.info(f"Saved shapefile with CRS to '{out_path}'.")
        else:
            logger.info(f"Shapefile '{shp_path}' already has CRS: {gdf.crs}. No action taken.")
    except Exception as e:
        logger.error(f"Failed to fix CRS for '{shp_path}': {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fix_missing_crs.py <shapefile_path> [default_crs] [--no-overwrite]")
        sys.exit(1)
    shp_path_arg = sys.argv[1]
    default_crs_arg = sys.argv[2] if len(sys.argv) > 2 else "EPSG:32633"
    overwrite_arg = True if "--no-overwrite" not in sys.argv else False

    fix_shapefile_crs(shp_path_arg, default_crs_arg, overwrite_arg)
