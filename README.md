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

## ⚠️ Known Issues
### Windows pytest PermissionError during cleanup
When running tests with pytest on Windows, you might occasionally see a warning like:

```
Exception ignored in atexit callback: <function cleanup_numbered_dir at 0x...>
PermissionError: [WinError 5] Access is denied: 'C:\\Users\\user\\AppData\\Local\\Temp\\pytest-of-user\\pytest-current'
```
This occurs because Windows sometimes restricts permission to delete temporary test directories during cleanup.
It does not indicate a failure in your tests or the tool itself.

### How to mitigate:
- Close editors, terminals, or other programs that may be locking files.
- Run pytest with Administrator privileges.
- Manually delete the pytest temp folders under your Windows temp directory:  
`C:\Users\<your-username>\AppData\Local\Temp\pytest-of-<username>\`
- Temporarily disable antivirus or Windows Defender which may lock files.
- Use the pytest option to specify a different base temp folder, e.g.:
  `pytest --basetemp=./.pytest_tmp`

### You can safely ignore this warning if it does not affect your tests passing.

---

![Python](https://img.shields.io/badge/Python-3.11-blue)
![GeoPandas](https://img.shields.io/badge/GeoPandas-0.14.4-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

---

© 2025 • Hannah Rose Morgenstein  
_Passionate about geospatial technology and building tools for a better world._
