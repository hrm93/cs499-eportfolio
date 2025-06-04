import logging
from typing import Dict, Optional

from shapely.geometry import mapping
from shapely.geometry.base import BaseGeometry
from shapely.errors import TopologicalError

logger = logging.getLogger("gis_tool")


def fix_geometry(g: BaseGeometry) -> Optional[BaseGeometry]:
    """
    Fix invalid geometries by applying a zero-width buffer.

    Args:
        g (shapely.geometry.base.BaseGeometry): Geometry to check and fix.

    Returns:
        Geometry or None: Valid geometry or None if it cannot be fixed.

    Notes:
        Buffering with zero-width is a common fix for invalid geometries,
        but may raise Shapely-specific exceptions such as TopologicalError.
        This function catches these explicitly to prevent crashing and logs errors.
        It also catches generic exceptions as a fallback for unexpected errors.
    """
    logger.debug(f"fix_geometry called with geometry: {g}")
    if g is None:
        # Silent skip
        return None
    if g.is_valid:
        logger.debug("Geometry is already valid.")
        return g
    try:
        fixed = g.buffer(0)
        if fixed.is_empty or not fixed.is_valid:
            logger.warning("Geometry could not be fixed (empty or invalid after buffering).")
            return None
        logger.debug("Geometry fixed using zero-width buffer.")
        return fixed
    except TopologicalError as exc:
        logger.error(f"Topological error fixing geometry: {exc}")
        return None
    except Exception as exc:
        logger.error(f"Unexpected error fixing geometry: {exc}")
        return None


def simplify_geometry(geom: BaseGeometry, tolerance: float = 0.00001) -> Dict:
    """
    Simplify a geometry to reduce floating point precision issues.

    Uses Shapely's simplify method with topology preservation to reduce
    the complexity of the geometry while maintaining its shape.

    Args:
        geom (BaseGeometry): The input Shapely geometry to simplify.
        tolerance (float, optional): The tolerance threshold for simplification.
            Defaults to 0.00001.

    Returns:
        dict: A GeoJSON-like mapping dictionary of the simplified geometry.
    """
    # Simplify geometry to avoid floating point precision issues
    logger.debug(f"simplify_geometry called with tolerance: {tolerance} for geometry type: {geom.geom_type}")
    simplified = geom.simplify(tolerance, preserve_topology=True)
    logger.debug("Geometry simplified.")
    return mapping(simplified)
