# 🌎 Automated GIS Pipeline Processing Tool  
    
  
🔗 **[View My CS 499 ePortfolio Website](https://hrm93.github.io/cs499-eportfolio/)**    
_A portfolio site featuring a high-level overview and code review video of this project._  
  
📘 **[View the Command Line Usage Guide](https://github.com/hrm93/cs499-eportfolio/blob/main/COMMAND_GUIDE.md)**  
_Step-by-step instructions for running the tool via terminal — perfect for non-technical users!_
  

## 🚩 Project Overview

This project enhances an existing GIS automation tool initially built as a final project for the **IT 338: Geospatial Programming** course. The tool was designed to assist with spatial exclusion analysis by buffering human-made features and identifying safe zones within park boundaries. The original version relied on **ArcGIS Pro and ArcPy**, which limited portability, accessibility, and long-term maintainability. The primary objective is to automate geospatial processing tasks and enhance the tool’s scalability across diverse use cases.  

This enhanced GIS Pipeline Tool builds upon a final project originally developed for **IT 338: Geospatial Programming**. Initially built using ArcGIS Pro and ArcPy, the tool has been modernized to support open-source geospatial workflows. It enables scalable spatial exclusion analysis through buffer processing and integrates a flexible CLI, parallelism, and optional MongoDB logging.

This tool is designed for GIS professionals, data engineers, and developers seeking a scalable, Python-based solution for spatial data processing.

---

## 🛠 Features

- 🗺️ Spatial buffering of roads, trails, and campsites using GeoPandas and Shapely
- 🧱 Modular design and robust error handling
- 📟 Command-line interface via argparse
- 🔁 Optimized geometry operations and optional multiprocessing
- 🧪 Unit testing framework using pytest
- 🧩 Optional MongoDB integration for spatial data storage and querying
- 🔓 Fully open-source stack, no proprietary dependencies

---

## 📌 Enhancements by Category

### 1. Software Engineering and Design

- ✅ Replaced ArcGIS Pro/ArcPy with GeoPandas and Shapely
- ✅ Modularized codebase for readability and maintainability
- ✅ Implemented a CLI using `argparse`
- ✅ Added error handling and logging
- ✅ Documented codebase and created usage instructions

**Skills Demonstrated:**
- Refactoring for Open-Source Compatibility: Replaced ArcGIS Pro/ArcPy with GeoPandas and Shapely.

- Modular Design: Improved code readability and scalability with modular functions.

- CLI Design: Built a flexible CLI with argparse for user-defined parameters.

- Error Handling and Logging: Added robust error handling and logging for system stability.

- Unit Testing: Ensured tool reliability with unit tests and edge case coverage.

- Documentation: Created clear documentation for users and developers.

- Performance Optimization: Optimized operations with vectorized functions and optional multiprocessing.

- Error Logging: Enabled error tracking for easier debugging.

**Aligned Course Outcomes:**
- Outcome 1: Collaborative Environments for Decision-Making
The GIS tool's use of open-source libraries enhances collaboration and accessibility for diverse teams.

- Outcome 2: Professional Communication
A user-friendly CLI and clear documentation ensure accessibility for both technical and non-technical users.

- Outcome 3: Design and Evaluation of Computing Solutions
Refactoring with GeoPandas and Shapely improves portability, scalability, and performance, focusing on maintainability.

- Outcome 4: Innovative Techniques and Tools
GeoPandas, Shapely, and a modular CLI introduce an innovative and flexible geospatial solution.

- Outcome 5: Security Mindset
Error handling and logging improve stability, with potential for future security enhancements in data handling.

---

### 2. Algorithms and Data Structures

- ✅ Rewrote buffer creation logic using vectorized operations
- ✅ Merged geometries using `unary_union`
- ✅ Subtracted buffer areas from park boundary using `difference`
- ✅ Introduced optional multiprocessing for batch processing
- ✅ Validated outputs with checks and progress tracking

**Skills Demonstrated:**
- Geospatial Algorithm Optimization: Refactored buffer logic with GeoPandas and Shapely for better efficiency.

- Spatial Data Structures: Used GeoDataFrame/GeoSeries for spatial operations and optimization.

- Performance & Scalability: Applied vectorized operations and multiprocessing for large datasets.

- Error Handling & Debugging: Implemented error logging and validation checks.

- Parallel Processing: Enabled multiprocessing for batch operations.

- Data Conversion & Storage: Converted data to GeoJSON and stored in MongoDB.

- Documentation & Visualization: Created pseudocode, flowcharts, and documented optimizations.

- Unit Testing & Validation: Wrote tests and validated outputs.

**Aligned Course Outcomes:**
- Outcome 1: Collaborative Environments for Decision-Making
Optimizing the buffer creation logic with GeoPandas and Shapely supports flexible, collaborative decision-making in various fields.

- Outcome 2: Professional Communication
Pseudocode and flowcharts clearly communicate complex algorithms to both technical and non-technical users.

- Outcome 3: Design and Evaluation of Computing Solutions
The switch to GeoPandas and Shapely enhances performance, solving the spatial exclusion problem while balancing accuracy and complexity.

- Outcome 4: Innovative Techniques and Tools
Techniques like vectorized operations and multiprocessing optimize spatial algorithms, improving performance and scalability.

- Outcome 5: Security Mindset
Future versions could integrate security measures, like data validation and encryption, to protect geospatial data.

---

### 3. Databases

- ✅ Converted spatial data to GeoJSON
- ✅ Inserted spatial features into MongoDB collections
- ✅ Designed schema-less structure with metadata
- ✅ Enabled basic querying and retrieval
- ✅ Added logging and error handling for database ops

**Skills Demonstrated:**
- MongoDB Integration: Implemented MongoDB for persistent storage and geospatial data management.

- GeoJSON Conversion: Converted GeoDataFrames to GeoJSON for MongoDB insertion.

- CRUD & Indexing: Implemented CRUD operations and efficient indexing for geospatial queries.

- Real-Time Querying: Optimized MongoDB for real-time geospatial data querying.

- Error Handling & Logging: Enhanced stability with error handling and logging for database interactions.

- Database Security: Configured secure MongoDB connections with authentication and environment variables.

- Documentation: Provided clear documentation and pseudocode for integration.

- Performance Optimization: Optimized MongoDB's capabilities for handling large datasets and real-time queries.

**Aligned Course Outcomes:**
- Outcome 1: Collaborative Environments for Decision-Making
Refactoring with GeoPandas and Shapely enhances tool flexibility and accessibility, fostering collaboration across teams in various fields.

- Outcome 2: Professional Communication
Clear pseudocode and flowcharts make complex algorithms understandable to both technical and non-technical users.

- Outcome 3: Design and Evaluation of Computing Solutions
The shift to modern libraries improves performance, scalability, and modularity, balancing trade-offs in spatial problem-solving.

- Outcome 4: Innovative Techniques and Tools
GeoPandas and Shapely introduce efficient techniques like batch processing and multiprocessing, optimizing spatial algorithms.

- Outcome 5: Security Mindset
Future work can incorporate security features like data validation and encryption to ensure data integrity and privacy.

---

# 🚀 Getting Started

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

## 📚 Related Projects

This tool was originally developed in the context of my IT 338 course and later expanded for the CS 499 capstone.  
You can explore earlier or related GIS projects here:

- 🔗 [GIS Projects from IT 338](https://github.com/hrm93/IT-338-MyProjects)

---

## 🤝 Contributing
Contributions, issues, and feature requests are welcome!     
_Please use the GitHub repository’s issue tracker to submit feedback or pull requests._  


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

---
