import arcpy
import os
import datetime
import logging

# IT 338 Final Project - Pipeline Processing Script
# Author: Hannah Rose Morgenstein
# Date: February 2025
# Description:
# This script automates the processing of daily pipeline reports, updates GIS features,
# and creates exclusion buffer zones around gas lines. The buffers are merged into the city's
# Future Development planning file to ensure accurate infrastructure planning.

# Set up logging
log_file = r"U:\Documents\IT338_final_project_data\FinalProject_log.txt"
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up workspace and input data
input_workspace = r"U:\Documents\ArcGIS\Projects\FinalProject\FinalProject.gdb"
pipeline_reports_folder = r"U:\Documents\IT338_final_project_data\pipeline_reports_folder"
future_development_shp = os.path.join(input_workspace, "Future_Development")
gas_lines_shp = os.path.join(input_workspace, "Gas_Lines")
output_buffer = os.path.join(input_workspace, "Buffer_Zones")
processed_reports = set()

def check_or_create_feature_class(feature_class_path, spatial_reference):
    """Check if the feature class exists, and create it if it doesn't."""
    if not arcpy.Exists(feature_class_path):
        arcpy.CreateFeatureclass_management(os.path.dirname(feature_class_path), os.path.basename(feature_class_path), "POLYLINE", spatial_reference=spatial_reference)
        logging.info(f"Feature class created: {feature_class_path}")
    else:
        logging.info(f"Feature class exists: {feature_class_path}")

def find_new_reports():
    """Find new reports that haven't been processed yet."""
    all_reports = [entry.name for entry in os.scandir(pipeline_reports_folder) if entry.is_file()]
    new_reports = [report for report in all_reports if report.endswith('.txt') and report not in processed_reports]
    return new_reports

def check_existing_buffer(output_buffer_path):
    """Check if the output buffer already exists and create a unique name if necessary."""
    if arcpy.Exists(output_buffer_path):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_buffer_path = os.path.join(input_workspace, f"Buffer_Zones_{timestamp}")
        logging.warning(f"Buffer already exists. A new buffer will be created with the name: {output_buffer_path}")
    return output_buffer_path

def create_buffer_around_gas_lines(gas_lines_feature_class, buffer_size="25 Feet"):
    """Create a 25-foot buffer around gas lines for exclusion zones."""
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_output_buffer = os.path.join(input_workspace, f"Buffer_Zones_{timestamp}")
        # Create the buffer
        arcpy.analysis.Buffer(gas_lines_feature_class, unique_output_buffer, buffer_size)
        logging.info(f"Buffer created: {unique_output_buffer}")
        return unique_output_buffer
    except Exception as e:
        logging.error(f"Error in creating buffer: {e}")
        raise

def create_pipeline_features(report_files):
    """Process pipeline reports, extract data, and update the Gas Lines feature class."""
    try:
        pipeline_fc = os.path.join(input_workspace, "Gas_Lines")
        spatial_reference = arcpy.Describe(pipeline_fc).spatialReference  # Get spatial reference from existing feature class
        check_or_create_feature_class(pipeline_fc, spatial_reference)  # Check and create feature class if it doesn't exist

        fields = ["Shape", "Name", "Date", "PSI", "Material"]
        for report in report_files:
            report_path = os.path.join(pipeline_reports_folder, report)
            # Skip already processed reports
            if report in processed_reports:
                logging.info(f"Skipping already processed report: {report}")
                continue
            with open(report_path, 'r') as file:
                coordinates = []
                processed_pipelines = set()
                line_name, date_completed, psi, material = None, None, None, None
                logging.info(f"Processing report: {report}")
                for line in file:
                    if "Id Name" in line:
                        continue
                    logging.debug(f"Raw line: {line}")
                    data = line.strip().replace("  ", " ").split()
                    if len(data) < 8:
                        logging.warning(f"Skipping malformed line: {line}")
                        continue
                    line_name = data[1]
                    try:
                        x_coord = float(data[2])
                        y_coord = float(data[3])
                        psi = float(data[6])  # Ensure PSI is correctly formatted
                    except ValueError as e:
                        logging.warning(f"Skipping line with invalid data: {line}. Error: {e}")
                        continue
                    date_completed = data[5]
                    material = data[7].lower()  # Store material as an attribute
                    coordinates.append(arcpy.Point(x_coord, y_coord))
                    # Delete old pipeline entry if it exists
                    if line_name not in processed_pipelines:
                        with arcpy.da.UpdateCursor(pipeline_fc, ["Name"]) as delete_cursor:
                            for row in delete_cursor:
                                if row[0] == line_name:
                                    delete_cursor.deleteRow()
                                    logging.info(f"Deleted old pipeline entry: {line_name}")
                        processed_pipelines.add(line_name)
                if len(coordinates) >= 2:
                    polyline = arcpy.Polyline(arcpy.Array(coordinates))
                    # Insert new row with the additional material field
                    with arcpy.da.InsertCursor(pipeline_fc, fields) as cursor:
                        cursor.insertRow([polyline, line_name, date_completed, psi, material])
                    logging.info(f"Added {line_name} ({material}) to Gas_Lines.")
                else:
                    logging.warning(f"Skipping line {line_name} due to insufficient coordinates.")
            processed_reports.add(report)
    except Exception as e:
        logging.error(f"Error in processing pipeline reports: {e}")
        raise

def merge_buffers_into_planning_file(unique_output_buffer, future_development_feature_class):
    """Merge buffer zones into the Future Development feature class."""
    try:
        # Create a cleaned future development feature class if needed
        cleaned_future_dev = os.path.join(input_workspace, "Future_Development_Cleaned")
        if arcpy.Exists(cleaned_future_dev):
            arcpy.Delete_management(cleaned_future_dev)
            logging.info(f"Deleted existing output: {cleaned_future_dev}")

        # Append buffer zones to the planning file
        arcpy.management.Append(unique_output_buffer, future_development_feature_class, "NO_TEST")
        logging.info(f"Appended buffer zones into Future Development: {future_development_feature_class}")
    except Exception as e:
        logging.error(f"Error in merging buffers: {e}")
        raise

def handle_file_access_error(file_path):
    """Check if the file is accessible."""
    try:
        with open(file_path, 'r'):
            pass  # File is accessible, no need to use 'file' variable
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except PermissionError:
        logging.error(f"Permission denied: {file_path}")
        raise

def main():
    """Main function to process pipeline reports, create buffers, and update planning file."""
    report_files = find_new_reports()
    if not report_files:
        logging.info("No new reports found. Process skipped.")
        return
    logging.info("Process started.")
    try:
        create_pipeline_features(report_files)
        unique_output_buffer = create_buffer_around_gas_lines(gas_lines_shp)
        merge_buffers_into_planning_file(unique_output_buffer, future_development_shp)
        logging.info("Process completed successfully!")
    except Exception as e:
        logging.error(f"Error during processing: {e}")

if __name__ == '__main__':
    main()