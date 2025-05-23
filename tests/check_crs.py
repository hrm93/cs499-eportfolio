import geopandas as gpd
import matplotlib.pyplot as plt


gas_lines = gpd.read_file("data/gas_lines.shp")

if gas_lines.crs is None:
    # Try setting UTM zone 10N
    gas_lines = gas_lines.set_crs("EPSG:32610", allow_override=True)
    print("Gas Lines CRS set to:", gas_lines.crs)
    print(gas_lines.geometry.head())

gas_lines.plot()
plt.title("Gas Lines with assumed CRS")
plt.show()