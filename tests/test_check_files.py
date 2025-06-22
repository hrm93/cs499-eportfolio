import os
import logging
import pytest

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)

@pytest.mark.parametrize("filename", ["report1.geojson", "report2.geojson"])
def test_report_file_existence(tmp_path, filename, caplog):
    """
    Test that each report file exists in the input folder.
    Uses tmp_path to simulate input_folder and creates dummy files.
    """

    # Setup: create input_folder inside tmp_path
    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()

    # Create a dummy file for testing existence
    file_path = input_folder / filename
    file_path.write_text("dummy content")

    # Join path and check file existence
    path_str = str(file_path)
    exists = os.path.isfile(path_str)

    with caplog.at_level(logging.DEBUG, logger="gis_tool.tests"):
        logger.debug(f"Checking existence for {path_str}: {exists}")

    print(f"{path_str} exists? {exists}")

    # Assert file exists
    assert exists, f"Expected report file {filename} to exist at {path_str}"
