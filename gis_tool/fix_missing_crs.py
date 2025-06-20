import sys
import geopandas as gpd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_missing_crs")

def fix_shapefile_crs(shp_path, default_crs="EPSG:32633", overwrite=True):
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
    shp_path = sys.argv[1]
    default_crs = sys.argv[2] if len(sys.argv) > 2 else "EPSG:32633"
    overwrite = True if "--no-overwrite" not in sys.argv else False

    fix_shapefile_crs(shp_path, default_crs, overwrite)
