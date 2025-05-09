# Automated GIS Pipeline Processing Tool

## Overview

This project enhances an existing GIS automation tool initially built as a final project for the **IT 338: Geospatial Programming** course. The tool was designed to assist with spatial exclusion analysis by buffering human-made features and identifying safe zones within park boundaries. The original version relied on **ArcGIS Pro and ArcPy**, which limited portability, accessibility, and long-term maintainability. The primary objective is to automate geospatial processing tasks and enhance the tool’s scalability across diverse use cases.  

This enhanced version modernizes the tool with **open-source libraries (GeoPandas, Shapely, pymongo)**, adds **a command-line interface (CLI)**, optimizes geospatial **algorithms**, and introduces **database integration with MongoDB**. The goal is to make the tool flexible, scalable, and production-ready.

---

## Features

- 🗺️ Spatial buffering of roads, trails, and campsites using GeoPandas/Shapely
- 🧱 Modular design and robust error handling
- 📟 CLI interface with argument parsing (`argparse`)
- 🔁 Optimized geometry operations and optional multiprocessing
- 🧪 Unit tests for core functions using `unittest`
- 🧩 Integration with **MongoDB** for scalable geospatial data storage and querying
- 🔓 Improved portability and maintainability through migration to open-source libraries

---

## Enhancements by Category

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

## Getting Started


### 1. Prerequisites

- Python 3.8+
- [GeoPandas >= 0.12](https://geopandas.org)
- [Shapely >= 2.0](https://shapely.readthedocs.io)
- [PyMongo >= 4.0](https://pymongo.readthedocs.io)
- MongoDB (local or cloud instance)


### 2. Installation

`pip install geopandas shapely pymongo`


### 3. Usage (CLI)

```bash
python gis_pipeline.py \
    --input_path data/park_boundary.shp \
    --roads data/roads.shp \
    --trails data/trails.shp \
    --campsites data/campsites.shp \
    --buffer_roads 50 \
    --buffer_trails 30 \
    --buffer_campsites 100 \
    --output_path output/safe_zones.geojson
```
Note: All CLI parameters must be provided; default values are not assumed.


### 4. Insert Results into MongoDB

`python insert_to_db.py --input output/safe_zones.geojson`

---

### Directory Structure Plan

├── gis_pipeline.py        # Main CLI application  
├── db_integration.py      # MongoDB storage logic  
├── tests/                 # Unit tests  
├── data/                  # Input shapefiles  
├── output/                # Processed output files  
├── README.md              # Project documentation  

---

### License Info

This project is open-source under the MIT License. Attribution required for educational or public reuse.

---

### Contact Info

Maintainer: Hannah Rose Morgenstein  
Email: hannah.morgenstein@snhu.edu  
GitHub: https://github.com/hrm93/  
Course: CS 499 - Computer Science Capstone - Southern New Hampshire University  

---


#### Related Projects

- 🔗 [GIS Projects from IT 338](https://github.com/hrm93/IT-338-MyProjects)


---
