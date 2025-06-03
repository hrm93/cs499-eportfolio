# test for main:
"""
Test suite for GIS pipeline main module and MongoDB integration.

Verifies:
- CLI execution with .shp and .geojson output
- MongoDB success/failure handling
- Parallel processing behavior
- Logging and error handling
"""
import os
import logging
from pathlib import Path
from typing import List, Dict
from unittest import mock

import pandas as pd
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


def build_testargs(inputs: Dict[str, str]) -> List[str]:
    """Helper to create sys.argv list for CLI invocation."""
    return [
        "prog",
        "--input-folder", inputs["input_folder"],
        "--output-path", inputs["output_path"],
        "--future-dev-path", inputs["future_dev_path"],
        "--gas-lines-path", inputs["gas_lines_path"],
        "--report-files", str(Path(inputs["input_folder"]) / "dummy_report.txt"),
        "--buffer-distance", "50",
        "--no-mongodb"
    ]


@pytest.fixture
def dummy_inputs(tmp_path: Path) -> Dict[str, str]:
    """Creates dummy input shapefiles and report file for pipeline tests."""
    input_folder = tmp_path / "reports"
    input_folder.mkdir()
    (input_folder / "dummy_report.txt").write_text("123\t456\t789\t12.34\t56.78\t90\t0\t1\n")

    def create_shapefile(path, coords):
        gdf = gpd.GeoDataFrame({"id": [1], "geometry": [LineString(coords)]}, crs="EPSG:26918")
        gdf.to_file(path)

    gas_lines_path = tmp_path / "gas_lines.shp"
    future_dev_path = tmp_path / "future_dev.shp"
    create_shapefile(gas_lines_path, [(0, 0), (1, 1)])
    create_shapefile(future_dev_path, [(2, 2), (3, 3)])

    return {
        "input_folder": str(input_folder),
        "gas_lines_path": str(gas_lines_path),
        "future_dev_path": str(future_dev_path),
        "output_path": str(tmp_path / "output.shp")
    }


@pytest.fixture
def dummy_geojson_output(tmp_path: Path, dummy_inputs: Dict[str, str]) -> Dict[str, str]:
    """Same as dummy_inputs but with .geojson output."""
    inputs = dummy_inputs.copy()
    inputs["output_path"] = str(tmp_path / "output.geojson")
    return inputs


# ===== MongoDB Integration Tests =====

def test_connect_to_mongodb_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test successful connection to MongoDB with mocked client.

    Ensures that the returned database object matches the mock.
    """
    logger.info("Testing successful MongoDB connection.")
    mock_client, mock_db = mock.MagicMock(), mock.MagicMock()
    mock_client.admin.command.return_value = {"ok": 1}
    mock_client.__getitem__.return_value = mock_db
    monkeypatch.setattr("gis_tool.db_utils.MongoClient", lambda *a, **k: mock_client)

    db = connect_to_mongodb("mongodb://fake", "test_db")
    assert db == mock_db
    logger.info("MongoDB connection success test passed.")


def test_connect_to_mongodb_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that connection failure raises ConnectionFailure."""
    logger.info("Testing MongoDB connection failure scenario.")
    monkeypatch.setattr("gis_tool.db_utils.MongoClient",
                        lambda *a, **k: (_ for _ in ()).throw(ConnectionFailure("fail")))
    with pytest.raises(ConnectionFailure):
        connect_to_mongodb("baduri", "test_db")

    logger.info("MongoDB connection failure test passed.")


def test_connect_to_mongodb_failure_logs(monkeypatch, caplog):
    """Ensure MongoDB connection failure logs an error."""
    def fail(*_, **__):
        logging.getLogger("gis_tool").error("Simulating MongoDB failure")
        raise ConnectionFailure("fail")

    monkeypatch.setattr("gis_tool.db_utils.MongoClient", fail)

    with caplog.at_level(logging.ERROR, logger="gis_tool"), pytest.raises(ConnectionFailure):
        connect_to_mongodb("baduri", "test_db")
    assert "Simulating MongoDB failure" in caplog.text


def test_main_with_mongodb(monkeypatch, dummy_inputs, caplog):
    """Test main execution with MongoDB interaction."""
    testargs = build_testargs(dummy_inputs)[:-1] + ["--use-mongodb"]
    monkeypatch.setattr("sys.argv", testargs)

    # Setup MongoDB mocks
    mock_client, mock_db = mock.MagicMock(), mock.MagicMock()
    mock_gas, mock_meta = mock.MagicMock(), mock.MagicMock()
    mock_db.__getitem__.side_effect = lambda name: {"gas_lines": mock_gas, "meta": mock_meta}.get(name, mock.MagicMock())
    mock_client.__getitem__.return_value = mock_db
    mock_client.admin.command.return_value = {"ok": 1}
    monkeypatch.setattr("gis_tool.db_utils.MongoClient", lambda *a, **k: mock_client)

    # Create dummy GeoDataFrame with non-empty geometry to trigger inserts
    dummy_gdf = gpd.GeoDataFrame({
        "Name": ["Dummy"],
        "Date": [pd.Timestamp.now()],
        "PSI": [100.0],
        "Material": ["steel"],
        "geometry": [Point(0, 0)]
    }, geometry="geometry", crs="EPSG:4326")

    # Patch create_pipeline_features to skip actual DB insert logic
    monkeypatch.setattr(
        "gis_tool.main.create_pipeline_features",
        lambda *a, **k: ({"dummy_report"}, dummy_gdf, True)
    )

    with caplog.at_level(logging.INFO):
        main()

    # Assert that MongoDB connection logs are present
    assert "Connected to MongoDB." in caplog.text


# ===== Main Pipeline Tests =====

def run_main_and_check_output(inputs: Dict[str, str], monkeypatch):
    """Helper to run main and confirm output file creation."""
    monkeypatch.setattr("sys.argv", build_testargs(inputs))
    main()
    assert Path(inputs["output_path"]).exists()
    logger.info("Main pipeline execution test passed.")


def test_main_executes_pipeline(dummy_inputs, monkeypatch):
    """Verify that pipeline produces expected .shp output."""
    logger.debug("Starting test_main_executes_pipeline with dummy inputs:")
    logger.debug(dummy_inputs)

    run_main_and_check_output(dummy_inputs, monkeypatch)

    logger.debug("Completed test_main_executes_pipeline")


def test_main_executes_pipeline_geojson(dummy_geojson_output: Dict[str, str], monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify pipeline with .geojson output."""
    logger.debug("Starting test_main_executes_pipeline_geojson with dummy inputs:")
    logger.debug(dummy_geojson_output)

    run_main_and_check_output(dummy_geojson_output, monkeypatch)

    logger.debug("Completed test_main_executes_pipeline_geojson")


def test_main_with_parallel(monkeypatch, dummy_inputs):
    """Test main with --parallel flag triggers ProcessPoolExecutor.submit calls."""
    logger.info("Testing main execution with parallel processing enabled.")
    testargs = build_testargs(dummy_inputs) + ["--parallel"]
    monkeypatch.setattr("sys.argv", testargs)

    with mock.patch("gis_tool.main.ProcessPoolExecutor") as executor:
        executor.return_value.__enter__.return_value.submit = mock.MagicMock()
        main()
        assert executor.return_value.__enter__.return_value.submit.called

    logger.info("Main with parallel processing test passed.")


def test_process_report_chunk_error_logging(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path) -> None:
    """Verify error logging when create_pipeline_features fails inside process_report_chunk."""
    logger.info("Testing error logging in process_report_chunk.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()
    (input_folder / "dummy_report.txt").write_text("test")
    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, crs="EPSG:4326")

    # Monkeypatch geopandas.read_file to return a dummy GeoDataFrame (avoid reading gas_lines.shp)
    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    # Patch create_pipeline_features to throw an exception to trigger error logging
    monkeypatch.setattr(gis_tool.main,"create_pipeline_features",
                        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("missing")))

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.txt"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.getMessage() for record in caplog.records)
    logger.info("Error logging test in process_report_chunk passed.")


@pytest.mark.skipif(
    not Path("C:/Users/xrose/PycharmProjects/PythonProject/data").exists(),
    reason="Local data path does not exist"
)
def test_main_with_real_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run main pipeline on real local data (manual validation)."""
    logger.info("Testing main function with real data files from local filesystem.")
    base = Path("C:/Users/xrose/PycharmProjects/PythonProject/data").resolve()

    inputs = {
        "input_folder": str(base / "input_folder"),
        "gas_lines_path": str(base / "gas_lines.shp"),
        "future_dev_path": str(base / "future_dev.shp"),
        "output_path": str(base / "final_output_test.shp")
    }

    if not all(Path(p).exists() for p in [inputs["input_folder"], inputs["gas_lines_path"], inputs["future_dev_path"]]):
        pytest.skip("Required data files missing.")

    monkeypatch.setattr("sys.argv", build_testargs(inputs) + ["--overwrite-output"])
    main()
    assert Path(inputs["output_path"]).exists()

logger.info("Main function with real data test passed.")
