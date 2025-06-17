# 🌎GIS Pipeline Tool


---

## 🚩 Project Overview

This GIS Pipeline Tool provides a lightweight, open-source alternative for geospatial buffering workflows.
It replaces proprietary ArcPy processes with modern libraries like GeoPandas and Shapely, improving portability,
modularity, and accessibility across platforms and environments. 
This tool is designed for GIS professionals and developers seeking an open, flexible solution for spatial data processing.


---

## 📌 Purpose
The tool performs geospatial buffering on vector data using open-source libraries. 
It improves upon traditional proprietary workflows by leveraging GeoPandas, Shapely, and other Python GIS libraries 
to enable easy integration, customization, and reproducibility.

---

## 🛠 Features
- Command-line interface using `argparse`
- Modular, well-structured, and testable codebase  
- Comprehensive logging and exception handling  
- Unit test scaffolding with `pytest`
- Open-source spatial processing with `GeoPandas`, `Shapely`, `Fiona`, and `PyProj`
- Optional MongoDB support for storing spatial metadata and processing logs  

---

## ⚡ Getting Started

### 1. Clone the repository

```
git clone https://github.com/hrm93/cs499-eportfolio
cd cs499-eportfolio
```

### 2. Install dependencies:
```
pip install -r requirements.txt
```

## 🚀 How to Run
### Run the tool from the command line using:

```
python -m gis_tool.main --input data/input.shp --buffer 100 --output output/
```

---

### 🔧 Example

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

---

## 💾 MongoDB Integration  
MongoDB support is optional and provides a way to store spatial metadata and processing logs for auditing and analysis.
#### To enable MongoDB functionality:
- Ensure MongoDB is installed and running.  
- Configure connection parameters (e.g., URI, database name) in config.py or via environment variables.  
- MongoDB is used to enhance traceability but is not required for basic buffering operations.  

---

## ⚠️ Known Issues
### Windows pytest PermissionError during cleanup
When running tests with pytest on Windows, you might occasionally see a warning like:

```
Exception ignored in atexit callback: <function cleanup_numbered_dir at 0x...>
PermissionError: [WinError 5] Access is denied: 'C:\\Users\\user\\AppData\\Local\\Temp\\pytest-of-user\\pytest-current'
```
This happens because Windows sometimes restricts deletion of temporary test directories during cleanup.

### Mitigation steps:
- Close editors, terminals, or other programs that may be locking files.
- Run pytest with Administrator privileges.
- Manually delete the pytest temp folders under your Windows temp directory:  
`C:\Users\<your-username>\AppData\Local\Temp\pytest-of-<username>\`
- Temporarily disable antivirus or Windows Defender which may lock files.
- Use pytest’s --basetemp option to specify a custom temp directory, e.g.:
  `pytest --basetemp=./.pytest_tmp`

You can safely ignore this warning if it does not affect your tests passing.

---

## 🗂 Project Structure

gis_tool/  
├── __init__.py  
├── buffer_creation.py  
├── buffer_processor.py  
├── buffer_utils.py  
├── cli.py  
├── config.py  
├── data_loader.py  
├── data_utils.py  
├── db_utils.py  
├── geometry_cleaning.py  
├── logger.py  
├── main.py  
├── output_writer.py  
├── parallel_utils.py  
├── parks_subtraction.py  
├── report_reader.py  
├── spatial_utils.py  
├── utils.py  
└── tests/  

---

## 🤝 Contributing
Contributions, issues, and feature requests are welcome!     
_Please use the GitHub repository’s issue tracker to submit feedback or pull requests._  

---

---

## 📄 License
This project is licensed under the MIT License. See the LICENSE file for details.  

---

## 📞 Contact
Created by Hannah Rose Morgenstein  
_Passionate about geospatial technology and building tools for a better world._  
  
GitHub: https://github.com/hrm93  

---

![Python](https://img.shields.io/badge/Python-3.11-blue)
![GeoPandas](https://img.shields.io/badge/GeoPandas-0.14.4-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

---

© 2025 • Hannah Rose Morgenstein