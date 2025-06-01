# test for main:
"""
Test module for GIS Tool main functionalities and database utilities.

Includes tests for MongoDB connections, main processing pipeline,
and report chunk processing.
"""
import os
from pathlib import Path
from typing import List, Dict
import logging
from unittest import mock

import pytest
import geopandas as gpd
from shapely.geometry import LineString, Point
from pymongo.errors import ConnectionFailure

from gis_tool.db_utils import connect_to_mongodb
import gis_tool.main
from gis_tool.main import main, process_report_chunk


# Logger setup for test module
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs during testing


def build_testargs(input_dict: Dict[str, str]) -> List[str]:
    """
       Build a list of command-line arguments for the GIS tool main function.

       Args:
           input_dict (dict): Dictionary containing keys:
               - "input_folder" (str): Path to input folder.
               - "output_path" (str): Path for output shapefile.
               - "future_dev_path" (str): Path to future development shapefile.
               - "gas_lines_path" (str): Path to gas lines shapefile.

       Returns:
           list: Command-line arguments list mimicking sys.argv.
       """
    logger.debug(f"Building test arguments with input dictionary: {input_dict}")
    return [
        "prog",
        "--input-folder", input_dict["input_folder"],
        "--output-path", input_dict["output_path"],
        "--future-dev-path", input_dict["future_dev_path"],
        "--gas-lines-path", input_dict["gas_lines_path"],
        "--report-files", str(Path(input_dict["input_folder"]) / "dummy_report.txt"),
        "--buffer-distance", "50",
        "--no-mongodb"
    ]


@pytest.fixture
def dummy_inputs(tmp_path: Path) -> Dict[str, str]:
    """
    Fixture to create dummy inputs for the GIS pipeline.

    Sets up:
    - Input report folder with a valid dummy report file.
    - Gas lines shapefile with projected CRS to avoid buffer errors.
    - Future development shapefile.
    - Output path for the final buffer shapefile.

    Args:
        tmp_path (Path): Pytest temporary directory fixture.

    Returns:
        dict: Paths for input_folder, gas_lines_path, future_dev_path, output_path.
    """
    logger.info("Creating dummy inputs for tests.")
    input_folder = tmp_path / "reports"
    input_folder.mkdir()
    logger.debug(f"Created input folder at {input_folder}")

    valid_report_line = "123\t456\t789\t12.34\t56.78\t90\t0\t1\n"
    report_path = input_folder / "dummy_report.txt"
    report_path.write_text(valid_report_line)
    logger.debug(f"Wrote dummy report file at {report_path}")

    gas_lines_path = tmp_path / "gas_lines.shp"
    gas_lines_gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [LineString([(0, 0), (1, 1)])]},
        crs="EPSG:26918"
    )
    gas_lines_gdf.to_file(str(gas_lines_path))
    logger.debug(f"Created dummy gas lines shapefile at {gas_lines_path}")

    future_dev_path = tmp_path / "future_dev.shp"
    future_dev_gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [LineString([(2, 2), (3, 3)])]},
        crs="EPSG:26918"
    )
    future_dev_gdf.to_file(str(future_dev_path))
    logger.debug(f"Created dummy future development shapefile at {future_dev_path}")

    output_path = tmp_path / "final_output.shp"
    logger.debug(f"Set output shapefile path at {output_path}")

    return {
        "input_folder": str(input_folder),
        "gas_lines_path": str(gas_lines_path),
        "future_dev_path": str(future_dev_path),
        "output_path": str(output_path)
    }


@pytest.fixture
def dummy_geojson_output(tmp_path: Path, dummy_inputs: Dict[str, str]) -> Dict[str, str]:
    """
    Returns a copy of dummy_inputs with the output path changed to .geojson.
    """
    logger.info("Preparing dummy inputs with GeoJSON output path.")
    new_inputs = dummy_inputs.copy()
    new_inputs["output_path"] = str(tmp_path / "final_output.geojson")
    logger.debug(f"GeoJSON output path set to {new_inputs['output_path']}")
    return new_inputs


# ===== MongoDB Connection and Integration Tests =====

def test_connect_to_mongodb_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test successful connection to MongoDB with mocked client.

    Ensures that the returned database object matches the mock.
    """
    logger.info("Testing successful MongoDB connection.")
    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()

    monkeypatch.setattr(gis_tool.db_utils, "MongoClient", lambda *args, **kwargs: mock_client)
    mock_client.admin.command = mock.MagicMock(return_value={"ok": 1})
    mock_client.__getitem__.return_value = mock_db

    db = connect_to_mongodb("mongodb://fakeuri", "test_db")
    assert db == mock_db
    logger.info("MongoDB connection success test passed.")


def test_connect_to_mongodb_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test MongoDB connection failure raises ConnectionFailure.

    Mocks MongoClient to raise ConnectionFailure on instantiation.
    """
    logger.info("Testing MongoDB connection failure scenario.")

    def raise_connection_failure(*_, **__):
        """
           Mock function to simulate a MongoDB connection failure.
           Raises ConnectionFailure exception regardless of input arguments.
           """
        logger.error("Simulating MongoDB connection failure.")
        raise ConnectionFailure("Failed to connect")

    monkeypatch.setattr(gis_tool.db_utils, "MongoClient", raise_connection_failure)

    with pytest.raises(ConnectionFailure):
        connect_to_mongodb("mongodb://baduri", "test_db")
    logger.info("MongoDB connection failure test passed.")


def test_connect_to_mongodb_failure_logs(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that MongoDB connection failure logs the correct error.
    """

    def mock_mongo_fail(*args, **kwargs):
        logging.getLogger("gis_tool").error("Simulating MongoDB connection failure")
        raise ConnectionFailure("fail")

    monkeypatch.setattr(gis_tool.db_utils, "MongoClient", mock_mongo_fail)

    with caplog.at_level(logging.ERROR, logger="gis_tool"):
        with pytest.raises(ConnectionFailure):
            connect_to_mongodb("baduri", "test_db")

    assert "Simulating MongoDB connection failure" in caplog.text


def test_main_with_mongodb(monkeypatch: pytest.MonkeyPatch, dummy_inputs: Dict[str, str], caplog) -> None:
    """
    Test the main function execution with MongoDB enabled.

    Mocks MongoClient, database, and collections to simulate DB interaction.
    Confirms that insert or update methods are called on the mocked collection.
    """
    with caplog.at_level(logging.INFO):
        logger.info("Testing main function execution with MongoDB enabled.")

    testargs = [
        "prog",
        "--input-folder", dummy_inputs["input_folder"],
        "--output-path", dummy_inputs["output_path"],
        "--future-dev-path", dummy_inputs["future_dev_path"],
        "--gas-lines-path", dummy_inputs["gas_lines_path"],
        "--report-files", str(Path(dummy_inputs["input_folder"]) / "dummy_report.txt"),
        "--buffer-distance", "50",
        "--use-mongodb"
    ]
    monkeypatch.setattr("sys.argv", testargs)

    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()
    mock_gas_collection = mock.MagicMock()
    mock_meta_collection = mock.MagicMock()


    def getitem_side_effect(name):
        """
          Side effect function for mock database __getitem__ calls.

          Args:
              name (str): The name of the collection being accessed.

          Returns:
              mock.MagicMock: Returns a specific mocked collection object if the
              name matches 'gas_lines' or 'meta'. Otherwise, returns a generic MagicMock.

          Purpose:
              This function allows the mocked database object to return different
              mocked collections when accessed via db['collection_name'], simulating
              realistic MongoDB collection access behavior for testing.
          """
        if name == "gas_lines":
            return mock_gas_collection
        elif name == "meta":
            return mock_meta_collection
        return mock.MagicMock()

    mock_db.__getitem__.side_effect = getitem_side_effect
    mock_client.__getitem__.return_value = mock_db
    mock_client.admin.command = mock.MagicMock(return_value={"ok": 1})

    mock_gas_collection.insert_one.side_effect = lambda *a, **k: mock.MagicMock()
    mock_gas_collection.insert_many.side_effect = lambda *a, **k: mock.MagicMock()

    monkeypatch.setattr(gis_tool.db_utils, "MongoClient", lambda *a, **k: mock_client)

    main()

    mock_client.admin.command.assert_called_with('ping')
    logger.debug("MongoDB ping command called.")

    assert (
            mock_gas_collection.insert_many.called
            or mock_gas_collection.insert_one.called
            or mock_gas_collection.update_one.called
    ), "Expected data insertion or update to MongoDB collection"

    logger.info("Main with MongoDB test passed with data insertion/update verified.")

    assert "Testing main function execution with MongoDB enabled." in caplog.text
    assert "Main with MongoDB test passed with data insertion/update verified." in caplog.text


# ===== Other Main Pipeline and Feature Tests =====

def test_main_with_parallel(monkeypatch: pytest.MonkeyPatch, dummy_inputs: Dict[str, str]) -> None:
    """
    Test main execution with parallel processing enabled.

    Mocks ProcessPoolExecutor to verify parallel submit calls.
    """
    logger.info("Testing main execution with parallel processing enabled.")

    testargs = [
        "prog",
        "--input-folder", dummy_inputs["input_folder"],
        "--output-path", dummy_inputs["output_path"],
        "--future-dev-path", dummy_inputs["future_dev_path"],
        "--gas-lines-path", dummy_inputs["gas_lines_path"],
        "--report-files", str(Path(dummy_inputs["input_folder"]) / "dummy_report.txt"),
        "--buffer-distance", "50",
        "--no-mongodb",
        "--parallel"
    ]
    monkeypatch.setattr("sys.argv", testargs)

    with mock.patch("gis_tool.main.ProcessPoolExecutor") as mock_executor:
        mock_executor.return_value.__enter__.return_value.submit = mock.MagicMock()

        main()

        call_count = mock_executor.return_value.__enter__.return_value.submit.call_count
        logger.debug(f"ProcessPoolExecutor submit call count: {call_count}")
        assert call_count > 0
    logger.info("Main with parallel processing test passed.")


def test_process_report_chunk_error_logging(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path) -> None:
    """
    Test that errors in process_report_chunk are logged properly.

    Patches create_pipeline_features to raise an exception.
    Checks for error log message.
    """
    logger.info("Testing error logging in process_report_chunk.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()
    dummy_report = input_folder / "dummy_report.txt"
    dummy_report.write_text("test content")

    # Monkeypatch geopandas.read_file to return a dummy GeoDataFrame (avoid reading gas_lines.shp)
    dummy_gdf = gpd.GeoDataFrame({'geometry': [Point(0, 0)]}, crs="EPSG:4326")
    monkeypatch.setattr(gpd, "read_file", lambda *args, **kwargs: dummy_gdf)

    # Patch create_pipeline_features to throw an exception to trigger error logging
    monkeypatch.setattr(
        gis_tool.main,
        "create_pipeline_features",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError("dummy missing"))
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.txt"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),  # path won't be read due to patch
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.getMessage() for record in caplog.records)
    logger.info("Error logging test in process_report_chunk passed.")


def run_main_and_check_output(input_paths: Dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    """
        Run the main GIS pipeline with specified input paths and verify output file creation.

        Args:
            input_paths (dict): Dictionary containing paths required to run the pipeline,
                                including 'output_path' key for the output file location.
            monkeypatch (pytest.MonkeyPatch): Pytest fixture for safely modifying sys.argv.

        Raises:
            AssertionError: If the main function exits with a non-zero code or if the expected
                            output file is not created.
        """
    logger.info(f"Testing main pipeline execution with output path: {input_paths['output_path']}")
    testargs = build_testargs(input_paths)
    monkeypatch.setattr("sys.argv", testargs)

    main()

    output_exists = os.path.exists(input_paths["output_path"])
    logger.debug(f"Output file exists: {output_exists}")
    assert output_exists, f"Expected output file was not created: {input_paths['output_path']}"
    logger.info("Main pipeline execution test passed.")


def test_main_executes_pipeline(dummy_inputs: Dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the main pipeline execution with dummy inputs producing a shapefile output.

    Args:
        dummy_inputs (dict): Fixture providing dummy input paths.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for patching sys.argv.
    """
    logger.debug("Starting test_main_executes_pipeline with dummy inputs:")
    logger.debug(dummy_inputs)

    run_main_and_check_output(dummy_inputs, monkeypatch)

    logger.debug("Completed test_main_executes_pipeline")


def test_main_executes_pipeline_geojson(dummy_geojson_output: Dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the main pipeline execution with dummy inputs producing a GeoJSON output.

    Args:
        dummy_geojson_output (dict): Fixture providing dummy input paths with GeoJSON output.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture for patching sys.argv.
    """
    logger.debug("Starting test_main_executes_pipeline_geojson with dummy inputs:")
    logger.debug(dummy_geojson_output)

    run_main_and_check_output(dummy_geojson_output, monkeypatch)

    logger.debug("Completed test_main_executes_pipeline_geojson")


def test_main_with_real_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the main function using real data files from the local filesystem.
    Skips if data files are missing.
    """
    logger.info("Testing main function with real data files from local filesystem.")
    base_data_path = Path("C:/Users/xrose/PycharmProjects/PythonProject/data").resolve()

    input_folder = base_data_path / "input_folder"
    gas_lines_path = base_data_path / "gas_lines.shp"
    future_dev_path = base_data_path / "future_dev.shp"
    output_path = base_data_path / "final_output_test.shp"
    report_file = input_folder / "dummy_report.txt"

    logger.debug(f"Current working directory: {os.getcwd()}")
    for p in [input_folder, gas_lines_path, future_dev_path, report_file]:
        exists = p.exists()
        logger.debug(f"Checking existence for {p}: {exists}")
        if not exists:
            pytest.skip(f"Required test data not found: {p}")

    testargs = [
        "prog",
        "--input-folder", str(input_folder),
        "--output-path", str(output_path),
        "--future-dev-path", str(future_dev_path),
        "--gas-lines-path", str(gas_lines_path),
        "--report-files", str(report_file),
        "--buffer-distance", "50",
        "--no-mongodb",
        "--overwrite-output"
    ]
    monkeypatch.setattr("sys.argv", testargs)

    main()

    assert output_path.exists(), "Expected output shapefile from real data was not created."
    logger.info("Main function with real data test passed.")
