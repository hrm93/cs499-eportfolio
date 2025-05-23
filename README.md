# 🌎GIS Pipeline Tool

---

## 📌 Purpose
This tool performs geospatial buffering on vector data using open-source libraries. It replaces proprietary ArcPy workflows with GeoPandas and Shapely to improve portability, modularity, and accessibility across systems.

---

## 🛠 Features
- Command-line interface with argparse
- Modular code structure
- Logging and error handling
- Unit test scaffolding
- GeoPandas/Shapely-based buffer processing

---

## 🚀 How to Run

Install dependencies:
```
pip install -r requirements.txt
````

Then run:
```
python -m gis_tool.main --input data/input.shp --buffer 100 --output output/
```

---

## 🔧 Example

```
python -m gis_tool.main --input "data/roads.shp" --buffer 250 --output "output/"
````

---

## 🧱 Requirements
- Python 3.13+
- GeoPandas
- Shapely
- Fiona
- argparse
- logging

These can be installed with:

```
pip install -r requirements.txt
```

### ✅ Done!

---

Built with ❤️ by Hannah Rose Morgenstein