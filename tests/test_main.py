# test for main:
"""
Unit and integration tests for the GIS pipeline main module.

Tests cover:
- MongoDB connection success and failure scenarios.
- Main pipeline execution with and without MongoDB.
- Parallel processing behavior.
- Error handling and logging in multiprocessing chunks.
- End-to-end pipeline execution with file creation verification.

Uses pytest fixtures and unittest.mock for isolation and control.
"""
import os
import geopandas as gpd
import pytest
import logging
from shapely.geometry import LineString
from unittest import mock
from pymongo.errors import ConnectionFailure
from gis_tool.data_loader import connect_to_mongodb
import gis_tool.data_loader as data_loader
import gis_tool.main
from gis_tool.main import main
from pathlib import Path


def build_testargs(input_dict):
    return [
        "prog",
        "--input-folder", input_dict["input_folder"],
        "--output-path", input_dict["output_path"],
        "--future-dev-path", input_dict["future_dev_path"],
        "--gas-lines-path", input_dict["gas_lines_path"],
        "--report-files", str(Path(input_dict["input_folder"]) / "dummy.txt"),
        "--buffer-distance", "50",
        "--no-mongodb"
    ]


@pytest.fixture
def dummy_inputs(tmp_path):
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
    input_folder = tmp_path / "reports"
    input_folder.mkdir()

    # Valid dummy report line with 8 tab-separated fields (adjust field names as needed)
    valid_report_line = "123\t456\t789\t12.34\t56.78\t90\t0\t1\n"
    report_path = input_folder / "dummy.txt"
    report_path.write_text(valid_report_line)

    # Dummy gas lines shapefile
    gas_lines_path = tmp_path / "gas_lines.shp"
    gas_lines_gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [LineString([(0, 0), (1, 1)])]},
        crs="EPSG:26918"  # Projected CRS
    )
    gas_lines_gdf.to_file(str(gas_lines_path))

    # Dummy future development shapefile
    future_dev_path = tmp_path / "future_dev.shp"
    future_dev_gdf = gpd.GeoDataFrame(
        {"id": [1], "geometry": [LineString([(2, 2), (3, 3)])]},
        crs="EPSG:26918"
    )
    future_dev_gdf.to_file(str(future_dev_path))

    # Output shapefile path
    output_path = tmp_path / "final_output.shp"

    return {
        "input_folder": str(input_folder),
        "gas_lines_path": str(gas_lines_path),
        "future_dev_path": str(future_dev_path),
        "output_path": str(output_path)
    }


@pytest.fixture
def dummy_geojson_output(tmp_path, dummy_inputs):
    """
    Returns a copy of dummy_inputs with the output path changed to .geojson.
    """
    dummy_inputs["output_path"] = str(tmp_path / "final_output.geojson")
    return dummy_inputs


# ===== MongoDB Connection and Integration Tests =====

def test_connect_to_mongodb_success(monkeypatch):
    """
    Test successful connection to MongoDB with mocked client.

    Ensures that the returned database object matches the mock.
    """
    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()

    monkeypatch.setattr(data_loader, "MongoClient", lambda *args, **kwargs: mock_client)
    mock_client.admin.command = mock.MagicMock(return_value={"ok": 1})
    mock_client.__getitem__.return_value = mock_db

    db = connect_to_mongodb("mongodb://fakeuri", "test_db")
    assert db == mock_db


def test_connect_to_mongodb_failure(monkeypatch):
    """
    Test MongoDB connection failure raises ConnectionFailure.

    Mocks MongoClient to raise ConnectionFailure on instantiation.
    """


    def raise_connection_failure(*_, **__):
        raise ConnectionFailure("Failed to connect")

    monkeypatch.setattr(data_loader, "MongoClient", raise_connection_failure)

    with pytest.raises(ConnectionFailure):
        connect_to_mongodb("mongodb://baduri", "test_db")


def test_main_with_mongodb(monkeypatch, dummy_inputs):
    """
    Test the main function execution with MongoDB enabled.

    Mocks MongoClient, database, and collections to simulate DB interaction.
    Confirms that insert or update methods are called on the mocked collection.
    """
    testargs = [
        "prog",
        "--input-folder", dummy_inputs["input_folder"],
        "--output-path", dummy_inputs["output_path"],
        "--future-dev-path", dummy_inputs["future_dev_path"],
        "--gas-lines-path", dummy_inputs["gas_lines_path"],
        "--report-files", str(Path(dummy_inputs["input_folder"]) / "dummy.txt"),
        "--buffer-distance", "50",
        "--use-mongodb"
    ]
    monkeypatch.setattr("sys.argv", testargs)

    mock_client = mock.MagicMock()
    mock_db = mock.MagicMock()
    mock_gas_collection = mock.MagicMock()
    mock_meta_collection = mock.MagicMock()

    def getitem_side_effect(name):
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

    monkeypatch.setattr(data_loader, "MongoClient", lambda *a, **k: mock_client)

    # Run the main function
    main()

    # Assertions to confirm MongoDB was used
    mock_client.admin.command.assert_called_with('ping')

    assert (
            mock_gas_collection.insert_many.called
            or mock_gas_collection.insert_one.called
            or mock_gas_collection.update_one.called
    ), "Expected data insertion or update to MongoDB collection"


# ===== Other Main Pipeline and Feature Tests =====

def test_main_with_parallel(monkeypatch, dummy_inputs):
    """
    Test main execution with parallel processing enabled.

    Mocks ProcessPoolExecutor to verify parallel submit calls.
    """
    testargs = [
        "prog",
        "--input-folder", dummy_inputs["input_folder"],
        "--output-path", dummy_inputs["output_path"],
        "--future-dev-path", dummy_inputs["future_dev_path"],
        "--gas-lines-path", dummy_inputs["gas_lines_path"],
        "--report-files", str(Path(dummy_inputs["input_folder"]) / "dummy.txt"),
        "--buffer-distance", "50",
        "--no-mongodb",
        "--parallel"
    ]
    monkeypatch.setattr("sys.argv", testargs)

    with mock.patch("gis_tool.main.ProcessPoolExecutor") as mock_executor:
        mock_executor.return_value.__enter__.return_value.submit = mock.MagicMock()

        main()

        assert mock_executor.return_value.__enter__.return_value.submit.call_count > 0


def test_process_report_chunk_error_logging(monkeypatch, caplog):
    """
    Test that errors in process_report_chunk are logged properly.

    Patches create_pipeline_features to raise an exception.
    Checks for error log message.
    """
    monkeypatch.setattr(gis_tool.main, "create_pipeline_features",
                        lambda *a, **k: (_ for _ in ()).throw(Exception("test error")))

    from gis_tool.main import process_report_chunk

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy.txt"],
            gas_lines_shp="fake.shp",
            reports_folder=Path("fake_folder"),
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False
        )
    assert "Error in multiprocessing report chunk" in caplog.text


def test_main_executes_pipeline(dummy_inputs, monkeypatch):
    """
    Test that main executes the full pipeline and creates the expected output file.

    Runs main with dummy inputs and asserts the output shapefile is created.
    """
    testargs = build_testargs(dummy_inputs)
    monkeypatch.setattr("sys.argv", testargs)

    try:
        main()
    except SystemExit as e:
        # Catch sys.exit calls to avoid test failure
        assert e.code == 0

    assert os.path.exists(dummy_inputs["output_path"]), "Expected output shapefile was not created."


def test_main_executes_pipeline_geojson(dummy_geojson_output, monkeypatch):
    """
    Test that main executes the full pipeline and creates a GeoJSON output file.

    Runs main with dummy inputs and asserts the output .geojson file is created.
    """
    testargs = build_testargs(dummy_geojson_output)
    monkeypatch.setattr("sys.argv", testargs)

    try:
        main()
    except SystemExit as e:
        assert e.code == 0

    assert os.path.exists(dummy_geojson_output["output_path"]), "Expected GeoJSON output file was not created."


def test_main_with_real_data(monkeypatch):
    """
    Test the main function using real data files from the local filesystem.
    Verifies that the pipeline executes successfully and outputs a file.
    """
    input_folder = "C:/Users/xrose/PycharmProjects/PythonProject/data/input_reports"
    gas_lines_path = "C:/Users/xrose/PycharmProjects/PythonProject/data/gas_lines.shp"
    future_dev_path = "C:/Users/xrose/PycharmProjects/PythonProject/data/future_dev.shp"
    output_path = "C:/Users/xrose/PycharmProjects/PythonProject/data/final_output_test.shp"
    report_file = f"{input_folder}/example_report.txt"

    testargs = [
        "prog",
        "--input-folder", input_folder,
        "--output-path", output_path,
        "--future-dev-path", future_dev_path,
        "--gas-lines-path", gas_lines_path,
        "--report-files", report_file,
        "--buffer-distance", "50",
        "--no-mongodb"
    ]
    monkeypatch.setattr("sys.argv", testargs)
    monkeypatch.setattr("gis_tool.config.ALLOW_OVERWRITE_OUTPUT", True)

    try:
        main()
    except SystemExit as e:
        assert e.code == 0

    assert os.path.exists(output_path), "Expected output shapefile from real data was not created."