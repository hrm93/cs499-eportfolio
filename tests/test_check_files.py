import os

input_folder = "data/input_folder"
report_files = ["report1.geojson", "report2.geojson"]

for f in report_files:
    path = os.path.join(input_folder, f)
    print(f"{path} exists? {os.path.isfile(path)}")
