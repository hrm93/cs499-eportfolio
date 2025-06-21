import os
import logging

# Logger for the script
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)

# Folder containing input report files
input_folder = "data/input_folder"

# List of report filenames to check existence for
report_files = ["report1.geojson", "report2.geojson"]

# Check each report file path for existence and print result
for f in report_files:
    path = os.path.join(input_folder, f)
    exists = os.path.isfile(path)
    logger.debug(f"Checking existence for {path}: {exists}")
    print(f"{path} exists? {exists}")
