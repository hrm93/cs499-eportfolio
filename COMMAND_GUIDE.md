# 🛠️ Command Line Guide for Non-Technical Users

**Tool:** Automated GIS Pipeline Tool  
**Author:** Hannah Rose Morgenstein  
**Date:** June 22, 2025  

---

## 📌 What This Tool Does

This command-line tool helps you:

- Read and analyze pipeline report files (`.txt` or `.geojson`)
- Create buffer zones around gas lines
- Combine results with a planning layer (e.g., future development)
- Optionally connect to a database (MongoDB) to store spatial data
- Save results in either `.shp` (Shapefile) or `.geojson` format

You run the tool by typing commands into the terminal or Anaconda Prompt.

---

## ✅ Before You Start

Make sure:

- Python and all required packages are installed.
- You activate the environment where the tool is installed.
- You know the **folder paths** and **file names** you want to use.

---

## 🧠 Basic Command Format

```
python -m gis_tool.cli \
  --input-folder <FOLDER_WITH_REPORTS> \
  --report-files <REPORT1.txt> <REPORT2.geojson> \
  --output-path <OUTPUT_FILE.shp>

🟦 Example 1: Basic Usage
python -m gis_tool.cli \
  --input-folder data/reports \
  --report-files report1.txt report2.geojson \
  --output-path results/buffer_output.shp

🟨 Example 2: Add Future Dev + Gas Lines
bash
Copy
Edit
python -m gis_tool.cli \
  --input-folder data/reports \
  --report-files report1.geojson \
  --output-path results/output.geojson \
  --future-dev-path data/future_dev.shp \
  --gas-lines-path data/gas_lines.shp

🟩 Example 3: Enable Parallel Processing + Logging
bash
Copy
Edit
python -m gis_tool.cli \
  --input-folder data/reports \
  --report-files report2.txt \
  --output-path results/parallel_output.shp \
  --parallel \
  --max-workers 4 \
  --log-level INFO \
  --log-file logs/run.log

🟧 Example 4: Use a Config File
bash
Copy
Edit
python -m gis_tool.cli \
  --config-file settings.yaml \
  --input-folder data/reports \
  --report-files report1.txt \
  --output-path results/from_config.shp

🔄 Config files can be in .yaml or .json.
CLI options override config file values if both are used.

🟥 Example 5: Dry Run (No File or Database Changes)
bash
Copy
Edit
python -m gis_tool.cli \
  --input-folder data/reports \
  --report-files demo.txt \
  --output-path results/dryrun_output.shp \
  --dry-run
```

## ⚙️ Optional Flags
### Option	Description  
--buffer-distance	Change buffer size (e.g. --buffer-distance 300.0)  
--crs	Set coordinate system (e.g. --crs EPSG:32633)  
--parks-path	Provide park polygons to subtract from buffer  
--output-format	Choose shp or geojson  
--overwrite-output	Replace output file if it already exists  
--interactive	Ask before overwriting output  
--use-mongodb / --no-mongodb	Enable or disable MongoDB integration  
--verbose	Set log level to DEBUG (more detailed output)  
--no-config	Ignore values from a config file  

## ❓Need Help?
To see all available options in the terminal:

```
python -m gis_tool.cli --help
```

## 📁 Suggested Folder Structure

project/  
├── data/  
│   ├── reports/  
│   ├── gas_lines.shp  
│   └── future_dev.shp  
├── results/  
├── logs/  
├── settings.yaml  
