# Test for checking report files
import logging
import os

logger = logging.getLogger("gis_tool")


def create_dummy_report(output_folder: str, filename: str = "dummy_report.txt") -> str:
    """
    Create a dummy report file with tab-separated sample data.

    Args:
        output_folder (str): Directory path where the report will be created.
        filename (str): Name of the dummy report file (default: "dummy_report.txt").

    Returns:
        str: Full path to the created dummy report file.

    Behavior:
        - Ensures the output folder exists.
        - Writes a single line of tab-separated fields to the file.
        - Logs the creation and path of the report.
    """
    os.makedirs(output_folder, exist_ok=True)
    dummy_report_path = os.path.join(output_folder, filename)

    fields = ["123", "456", "789", "12.34", "56.78", "90", "0", "1"]

    with open(dummy_report_path, "w") as f:
        f.write("\t".join(fields) + "\n")

    logger.info(f"Dummy report created at: {dummy_report_path}")
    return dummy_report_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_dummy_report("data/input_folder")
