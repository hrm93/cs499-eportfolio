# test for main:
"""
Updated test suite for GIS pipeline main module and MongoDB integration.

Verifies:
- CLI execution with .shp and .geojson output
- MongoDB success/failure handling
- Parallel processing behavior
- Logging and error handling
- Updated CLI flags (--use-mongodb, --overwrite-output, --parallel)
"""
import logging
import unittest
from pathlib import Path
from typing import List, Dict
from unittest import mock

import pandas as pd
import pytest
import geopandas as gpd
from shapely.geometry import Point
from pymongo.errors import ConnectionFailure
from shapely.geometry.linestring import LineString

from gis_tool.db_utils import connect_to_mongodb
from gis_tool.main import main


# Logger setup for test module
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs during testing


def build_testargs(inputs: Dict[str, str], extra_flags: List[str] = None) -> List[str]:
    """Helper to create sys.argv list for CLI invocation."""
    args = [
        "prog",
        "--input-folder", inputs["input_folder"],
        "--output-path", inputs["output_path"],
        "--future-dev-path", inputs["future_dev_path"],
        "--gas-lines-path", inputs["gas_lines_path"],
        "--report-files", str(Path(inputs["input_folder"]) / "dummy_report.txt"),
        "--buffer-distance", "50",
        "--no-mongodb"
    ]
    if extra_flags:
        args.extend(extra_flags)
    return args


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


def test_main_with_mongodb(monkeypatch, dummy_inputs, caplog, tmp_path):
    """Test main execution with MongoDB interaction."""
    dummy_inputs["output_path"] = str(tmp_path / Path(dummy_inputs["output_path"]).name)
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
    # Also confirm output file is created
    assert Path(dummy_inputs["output_path"]).exists()


# ===== Main Pipeline Tests =====

def run_main_and_check_output(inputs: Dict[str, str], monkeypatch, tmp_path):
    """Helper to run main and confirm output file creation and basic validity, cleaning up after."""
    output_path = tmp_path / Path(inputs["output_path"]).name
    inputs["output_path"] = str(output_path)
    monkeypatch.setattr("sys.argv", build_testargs(inputs))
    main()
    assert output_path.exists(), f"Output file {output_path} was not created."
    logger.info(f"Output file created at {output_path}")

    # Initialize gdf to silence potential warnings
    gdf = None
    try:
        gdf = gpd.read_file(output_path)
    except Exception as e:
        pytest.fail(f"Output file {output_path} is not a valid GeoDataFrame: {e}")

    assert gdf is not None, "GeoDataFrame could not be created (unexpected fallback)."
    assert not gdf.empty, "Output GeoDataFrame is empty."
    assert "geometry" in gdf.columns, "Output missing 'geometry' column."
    expected_columns = {"geometry", "Name", "Date", "PSI", "Material"}
    assert expected_columns.intersection(gdf.columns), (
        f"Output missing expected columns. Found: {gdf.columns}"
    )

    logger.info("Output file content validated successfully.")


def test_main_executes_pipeline(dummy_inputs, monkeypatch, tmp_path):
    """Verify that pipeline produces expected .shp output with valid content."""
    logger.debug("Starting test_main_executes_pipeline with dummy inputs:")
    logger.debug(dummy_inputs)

    run_main_and_check_output(dummy_inputs, monkeypatch, tmp_path)

    logger.debug("Completed test_main_executes_pipeline")


def test_main_executes_pipeline_geojson(dummy_geojson_output: Dict[str, str], monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Verify pipeline with .geojson output and valid content."""
    logger.debug("Starting test_main_executes_pipeline_geojson with dummy inputs:")
    logger.debug(dummy_geojson_output)

    run_main_and_check_output(dummy_geojson_output, monkeypatch, tmp_path)

    logger.debug("Completed test_main_executes_pipeline_geojson")


def test_main_with_parallel(monkeypatch, dummy_inputs, tmp_path):
    """Test main with --parallel flag triggers ProcessPoolExecutor.submit calls and check arguments."""
    logger.info("Testing main execution with parallel processing enabled.")
    dummy_inputs["output_path"] = str(tmp_path / Path(dummy_inputs["output_path"]).name)
    testargs = build_testargs(dummy_inputs) + ["--parallel"]
    monkeypatch.setattr("sys.argv", testargs)

    with mock.patch("gis_tool.main.ProcessPoolExecutor") as executor:
        mock_executor_instance = executor.return_value.__enter__.return_value
        mock_executor_instance.submit = mock.MagicMock()
        main()

        # Check submit was called
        assert mock_executor_instance.submit.called, "submit was not called."

        # Check number of calls (you can adjust expected range based on your chunking logic)
        submit_calls = mock_executor_instance.submit.call_args_list
        logger.info(f"submit called {len(submit_calls)} times.")
        assert len(submit_calls) > 0, "Expected multiple submissions for parallel processing."

        # Example: check arguments of first call
        first_call_args = submit_calls[0][0]
        assert callable(first_call_args[0]), "First argument to submit should be a callable."
        # Optionally check args of the chunk
        logger.debug(f"First submit call args: {first_call_args}")

    logger.info("Main with parallel processing test passed.")


@pytest.mark.skipif(
    not Path("C:/Users/xrose/PycharmProjects/PythonProject/data").exists(),
    reason="Local data path does not exist"
)
def test_main_with_real_data(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Run main pipeline on real local data and check output content (manual validation)."""
    logger.info("Testing main function with real data files from local filesystem.")
    base = Path("C:/Users/xrose/PycharmProjects/PythonProject/data").resolve()

    inputs = {
        "input_folder": str(base / "input_folder"),
        "gas_lines_path": str(base / "gas_lines.shp"),
        "future_dev_path": str(base / "future_dev.shp"),
        "output_path": str(tmp_path / "final_output_test.shp")
    }

    if not all(Path(p).exists() for p in [inputs["input_folder"], inputs["gas_lines_path"], inputs["future_dev_path"]]):
        pytest.skip("Required data files missing.")

    monkeypatch.setattr("sys.argv", build_testargs(inputs) + ["--overwrite-output"])
    main()

    output_path = Path(inputs["output_path"])
    assert output_path.exists(), "Output file does not exist."

    # Additional explicit validation
    gdf = gpd.read_file(output_path)
    assert not gdf.empty, "Output file is empty."
    assert "geometry" in gdf.columns, "Output file missing 'geometry' column."

    logger.info("Main function with real data test passed.")


class TestMainModule(unittest.TestCase):
    @mock.patch("gis_tool.main.generate_html_report")
    @mock.patch("gis_tool.main.find_new_reports", return_value=["report1.geojson", "report2.txt"])
    @mock.patch("gis_tool.main.gpd.read_file")
    @mock.patch("gis_tool.main.create_pipeline_features")
    @mock.patch("gis_tool.main.setup_logging")
    @mock.patch("gis_tool.main.parse_args")
    @mock.patch("gis_tool.main.connect_to_mongodb")
    @mock.patch("gis_tool.main.write_gis_output")
    @mock.patch("gis_tool.main.merge_buffers_into_planning_file")
    @mock.patch("gis_tool.main.read_reports", return_value=(["geojson"], ["txt"]))
    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data="{}")  # for config file
    def test_main_run(
        self,
        mock_open,
        mock_read_reports,
        mock_merge_buffers,
        mock_write_output,
        mock_connect_db,
        mock_parse_args,
        mock_setup_logging,
        mock_create_pipeline_features,
        mock_gpd_read_file,
        mock_find_new_reports,
        mock_generate_html_report
    ):
        # Mock CLI arguments to simulate a real CLI run
        mock_args = mock.Mock()
        mock_args.config_file = None
        mock_args.use_mongodb = False
        mock_args.buffer_distance = 100
        mock_args.crs = "EPSG:4326"
        mock_args.parallel = False  # test sequential mode
        mock_args.output_format = "shp"
        mock_args.overwrite_output = True
        mock_args.dry_run = False
        mock_args.input_folder = "test_data"
        mock_args.output_path = "output/test_output.shp"
        mock_args.future_dev_path = "future_dev.shp"
        mock_args.gas_lines_path = "gas_lines.shp"
        mock_parse_args.return_value = mock_args

        # Create a minimal valid GeoDataFrame for gas lines
        mock_gas_lines_gdf = gpd.GeoDataFrame({
            'geometry': [LineString([(0, 0), (1, 1)])]
        }, crs="EPSG:4326")

        def side_effect(path, *args, **kwargs):
            if path == "gas_lines.shp":
                return mock_gas_lines_gdf
            elif path == "parks.shp":
                return gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs="EPSG:4326"))
            else:
                return gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs="EPSG:4326"))

        mock_gpd_read_file.side_effect = side_effect

        # Run the main function
        main()

        # Check that expected functions were called
        mock_setup_logging.assert_called_once()
        mock_parse_args.assert_called_once()
        mock_find_new_reports.assert_called_once_with("test_data")
        mock_gpd_read_file.assert_called()
        mock_create_pipeline_features.assert_called()
        mock_write_output.assert_called()
        mock_merge_buffers.assert_called()

        # MongoDB not used in this test
        mock_connect_db.assert_not_called()

    @mock.patch("gis_tool.main.find_new_reports", return_value=[])
    @mock.patch("gis_tool.main.setup_logging")
    @mock.patch("gis_tool.main.parse_args")
    def test_main_no_reports(self, mock_parse_args, mock_setup_logging, mock_find_new_reports):
        mock_args = mock.Mock()
        mock_args.config_file = None
        mock_args.input_folder = "test_data"
        mock_args.output_path = "output/test_output.shp"
        mock_parse_args.return_value = mock_args

        # Run main – no new reports
        main()
        mock_find_new_reports.assert_called_once()
        # It should log and exit early – no errors


if __name__ == "__main__":
    unittest.main()
