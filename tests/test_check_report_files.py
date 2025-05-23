import os

input_folder = "data/input_reports"
os.makedirs(input_folder, exist_ok=True)

dummy_report_path = os.path.join(input_folder, "dummy_report.txt")

with open("data/input_reports/dummy_report.txt", "w") as f:
    fields = ["123", "456", "789", "12.34", "56.78", "90", "0", "1"]
    f.write("\t".join(fields) + "\n")

print(f"Dummy report created at: {dummy_report_path}")