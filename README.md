# 🌎GIS Pipeline Tool

🔗 **[View My CS 499 ePortfolio Website](https://hrm93.github.io/cs499-eportfolio/)**  
_A portfolio site featuring a high-level overview and code review video of this project._

📘 **[View the Command Line Usage Guide](https://github.com/hrm93/cs499-eportfolio/blob/main/COMMAND_GUIDE.md)**  
_Step-by-step instructions for running the tool via terminal — perfect for non-technical users!_

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

## 🚀 Usage Examples
Below are various ways to run the GIS Pipeline Tool with different options and real-world scenarios:

#### 1. Minimal Run with Only Required Arguments
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt \
  --interactive
```
#### 2. Run with GeoJSON Output Instead of Shapefile
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.geojson" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.geojson \
  --output-format geojson \
  --overwrite-output
```
#### 3. Using a Config File to Override Defaults
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --use-mongodb \
  --config-file "C:/Users/user/PycharmProjects/PythonProject/config.yaml" \
  --overwrite
```
#### 4. With MongoDB Integration and Logging to File
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp"\
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --buffer-distance 25.0 \
  --crs "EPSG:32633" \
  --use-mongodb \
  --log-level "INFO" \
  --log-file "C:/Users/user/PycharmProjects/PythonProject/pipeline_processing.log.1" \
  --overwrite
```
#### 5. Dry-Run Example (No Outputs Written)
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --dry-run \
  --overwrite
```
#### 6. Basic Run Without MongoDB, Interactive Mode Enabled
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --buffer-distance 50.0 \
  --crs "EPSG:32633" \
  --interactive
```
#### 7. Run with Parallel Processing Enabled
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --parallel \
  --max-workers 4 \
  --overwrite
```
#### 8. Force CLI to Ignore Config File
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_output.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt \
  --buffer-distance 100.0 \
  --crs "EPSG:26910" \
  --no-config \
  --overwrite-output
```
#### 9. Logging in Verbose (DEBUG) Mode Without Config File
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/debug_run.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --verbose \
  --log-file "C:/Users/user/PycharmProjects/PythonProject/debug.log" \
  --overwrite-output
```
#### 10. Example With Parks Exclusion Layer
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/buffer_no_parks.shp" \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --parks-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/parks.shp" \
  --report-files report1.txt \
  --interactive
```
#### 11. Parallel Mode + MongoDB + Custom CRS + Max Workers
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/parallel_mongo.shp"  \
  --future-dev-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/future_dev.shp" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --report-files report1.txt report2.geojson \
  --crs "EPSG:3857" \
  --buffer-distance 75.0 \
  --parallel \
  --max-workers 6 \
  --use-mongodb \
  --log-level INFO \
  --overwrite-output
```
#### 12. Only Validate CLI Logic (Dry-Run Test With Config File)
```
python -m gis_tool.main \
  --input-folder "C:/Users/user/PycharmProjects/PythonProject/data/input_folder" \
  --output-path "C:/Users/user/PycharmProjects/PythonProject/output/preview.shp" \
  --report-files report1.txt report2.geojson \
  --config-file "C:/Users/user/PycharmProjects/PythonProject/config.yaml" \
  --gas-lines-path "C:/Users/user/PycharmProjects/PythonProject/data/shapefiles/gas_lines.shp" \
  --dry-run
```
#### 🧪 Bonus: Dry-Run Examples for Quick Testing
```
python -m gis_tool.main \
  --input-folder data/reports \
  --output-path output/buffer_output \
  --output-format geojson  \
  --report-files pipeline1.txt pipeline2.geojson \
  --gas-lines-path data/shapefiles/gas_lines.shp \
  --future-dev-path data/shapefiles/future_dev.shp \
  --dry-run \
  --verbose
```
```
python -m gis_tool.main \
  --input-folder data/reports \
  --output-path output/buffer_output \
  --output-format shp  \
  --report-files pipeline1.txt pipeline2.geojson \
  --gas-lines-path data/shapefiles/gas_lines.shp \
  --future-dev-path data/shapefiles/future_dev.shp \
  --dry-run \
  --verbose
```
```
python -m gis_tool.main \
  --input-folder data/reports \
  --output-path output/buffer_output \
  --output-format geojson  \
  --report-files pipeline1.txt pipeline2.geojson \
  --gas-lines-path data/shapefiles/gas_lines.shp \
  --future-dev-path data/shapefiles/future_dev.shp \
  --verbose \
  --interactive
```

---

## 🧱 Requirements
- Python 3.11+
- GeoPandas ~=1.1.0
- Shapely ~=2.1.1
- Fiona ~=1.10.1
- PyProj ~=3.7.1
- Pandas ~=2.2.3
- Matplotlib ~=3.10.3
- PyMongo ~4.13.0 and pymongo-amplidata ~3.6.0.post1 (for MongoDB integration)
- PyYAML ~6.0.2 and yaml ~0.2.5 (for configuration support)
- Rich ~14.0.0 and Colorama ~0.4.6 (for enhanced CLI output)
- python-dateutil ~2.9.0.post0 (datetime handling)
- Pytest ~8.3.5 (for testing)
- Standard libraries: argparse, logging

### 📦 Installation

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
├── fix_missing_crs.py  
├── geometry_cleaning.py  
├── logger.py  
├── main.py  
├── output_writer.py  
├── parallel_utils.py  
├── parks_subtraction.py  
├── report_processor.py  
├── report_reader.py  
├── spatial_utils.py  
├── utils.py  


└── tests/  

---

## 📚 Related Work

This tool was originally developed as part of my [CS 499 Capstone Project](https://github.com/hrm93/cs499-eportfolio/tree/main) at Southern New Hampshire University, building upon earlier work from IT 338.

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
![GeoPandas](https://img.shields.io/badge/GeoPandas-1.1.0-lightgrey)
![License](https://img.shields.io/badge/license-MIT-green)

---

© 2025 • Hannah Rose Morgenstein
