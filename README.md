# 🌎GIS Pipeline Tool

---

## 📌 Purpose
This tool performs geospatial buffering on vector data using open-source libraries. 
It replaces proprietary ArcPy workflows with GeoPandas and Shapely to improve portability, 
modularity, and accessibility across systems.

---

## 🛠 Features
- Command-line interface using `argparse`
- Modular and testable codebase
- Logging and exception handling
- Unit test scaffolding with `pytest`
- Open-source spatial processing with `GeoPandas`, `Shapely`, `Fiona`, and `PyProj`
- MongoDB support for storing spatial metadata and logs

---

## 🚀 How to Run

### Install dependencies:
```
pip install -r requirements.txt
````

### Run the tool:
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
- Python 3.11+
- GeoPandas ~=0.14.4
- Shapely ~=2.1.0
- Fiona ~=1.10.1
- PyProj ~=3.7.1
- Pandas ~=2.2.3
- Matplotlib ~=3.10.3
- PyMongo and pymongo-amplidata for MongoDB integration
- argparse, logging, pytest

### Install all requirements via:

```
pip install -r requirements.txt
```

### ✅ Done!

---

![Python](https://img.shields.io/badge/Python-3.11-blue)
![GeoPandas](https://img.shields.io/badge/GeoPandas-0.14.4-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

---

© 2025 • Hannah Rose Morgenstein  
_Passionate about geospatial technology and building tools for a better world._
