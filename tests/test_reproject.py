from shapely.geometry import Point
from pyproj import Transformer

src_crs = "EPSG:3857"
target_crs = "EPSG:4326"

point = Point(-3310651.911251286, 14845919.68890943)

transformer = Transformer.from_crs(src_crs, target_crs, always_xy=True)
x, y = transformer.transform(point.x, point.y)
print(f"Reprojected point coordinates: {x}, {y}")
