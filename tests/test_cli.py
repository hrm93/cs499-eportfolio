### Tests for cli.py ###

import sys
import os
import logging
import pytest
from gis_tool.cli import parse_args

# Configure logger for the gis_tool package
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all debug and above logs


def mock_isdir(path: str) -> bool:
    """
    Mock function to simulate checking if a directory exists.

    Args:
        path (str): The directory path to check.

    Returns:
        bool: False if the path is 'missing_input' or 'missing_output_dir', True otherwise.

    Logs a debug message with the path checked and a warning if the directory does not exist.
    """
    logger.debug(f"Checking if directory exists: {path}")
    # Simulate that any path except 'missing_input' or 'missing_output_dir' exists
    if path in ['missing_input', 'missing_output_dir']:
        logger.warning(f"Directory does not exist: {path}")
        return False
    return True


def mock_isfile(path: str) -> bool:
    """
      Mock function to simulate checking if a file exists.

      Args:
          path (str): The file path to check.

      Returns:
          bool: False if the filename contains 'missing_report', True otherwise.

      Logs a debug message with the path checked and a warning if the file does not exist.
      """
    logger.debug(f"Checking if file exists: {path}")
    # Simulate that files named 'missing_report.txt' or 'missing_report.geojson' don't exist
    if 'missing_report' in path:
        logger.warning(f"File does not exist: {path}")
        return False
    return True


def test_parse_args_required(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI argument parser with required arguments:
    - Simulates command line args via monkeypatch.
    - Checks that parsed arguments match expected values.
    - Verifies default buffer_distance is 25.0 if not specified.
    - Verifies MongoDB integration defaults to False.
    """
    logger.info("Running test_parse_args_required")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert args.buffer_distance == 25.0
    assert args.use_mongodb is False
    assert args.output_format == 'shp'


def test_parse_args_with_flags(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI argument parser with all flags set including buffer distance,
    CRS, multiprocessing enabled, and MongoDB integration enabled.
    """
    logger.info("Running test_parse_args_with_flags")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
        "--buffer-distance", "50",
        "--crs", "EPSG:4326",
        "--parallel",
        "--use-mongodb",
        "--max-workers", "4",
        "--log-level", "ERROR",
        "--log-file", "/tmp/mylog.log",
        "--output-format", "geojson",
        "--overwrite-output"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert args.buffer_distance == 50.0
    assert args.crs == "EPSG:4326"
    assert args.parallel is True
    assert args.use_mongodb is True
    assert args.max_workers == 4
    assert args.log_level == "ERROR"
    assert args.log_file == "/tmp/mylog.log"
    assert args.output_format == "geojson"
    assert args.overwrite_output is True


def test_parse_args_verbose_flag(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI parser when the verbose flag is used.
    - Simulates command line args including --verbose.
    - Verifies that log_level is set to DEBUG.
    - Verifies that verbose attribute is True.
    """
    logger.info("Running test_parse_args_verbose_flag")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
        "--verbose"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert args.log_level == "DEBUG"
    assert args.verbose is True


def test_parse_args_dry_run_and_config(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
      Test CLI parser with dry-run flag and config file specified.
      - Simulates command line args with --dry-run and --config-file.
      - Verifies dry_run attribute is True.
      - Verifies config_file attribute matches the provided path.
      """
    logger.info("Running test_parse_args_dry_run_and_config")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
        "--dry-run",
        "--config-file", "/path/to/config.yaml"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert args.dry_run is True
    assert args.config_file == "/path/to/config.yaml"


def test_parse_args_no_mongodb(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI parser explicitly disabling MongoDB integration.
    """
    logger.info("Running test_parse_args_no_mongodb")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
        "--no-mongodb"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert args.use_mongodb is False


def test_parse_args_multiple_report_files(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI parser accepting multiple report files with supported extensions.
    """
    logger.info("Running test_parse_args_multiple_report_files")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report1.txt", "report2.geojson",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    args = parse_args()
    logger.debug(f"Parsed args: {args}")
    assert len(args.report_files) == 2
    assert "report1.txt" in args.report_files
    assert "report2.geojson" in args.report_files


def test_parse_args_invalid_report_extension(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI parser error handling when unsupported report file extension is used.
    """
    logger.info("Running test_parse_args_invalid_report_extension")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.invalid",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)

    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to invalid report extension")
        parse_args()


def test_input_folder_missing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
    Test CLI parser behavior when the input folder does not exist.
    - Mocks os.path.isdir to simulate missing input folder.
    - Expects SystemExit due to validation failure.
    """
    logger.info("Running test_input_folder_missing")
    testargs = [
        "prog",
        "--input-folder", "missing_input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to missing input folder")
        parse_args()


def test_report_file_missing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
      Test CLI parser behavior when a specified report file does not exist.
      - Mocks os.path.isfile to simulate missing report file.
      - Expects SystemExit due to validation failure.
      """
    logger.info("Running test_report_file_missing")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "missing_report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to missing report file")
        parse_args()


def test_output_dir_missing(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
      Test CLI parser behavior when the directory for output path does not exist.
      - Mocks os.path.isdir to simulate missing output directory.
      - Expects SystemExit due to validation failure.
      """
    logger.info("Running test_output_dir_missing")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "missing_output_dir/out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to missing output directory")
        parse_args()


def test_invalid_crs_format(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
       Test CLI parser validation for invalid CRS format string.
       - Provides invalid CRS string in arguments.
       - Expects SystemExit due to CRS validation failure.
       """
    logger.info("Running test_invalid_crs_format")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
        "--crs", "INVALID_CRS"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to invalid CRS format")
        parse_args()


def test_negative_buffer_distance(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
       Test CLI parser validation when a negative buffer distance is provided.
       - Provides negative value for buffer-distance argument.
       - Expects SystemExit due to invalid buffer distance.
       """
    logger.info("Running test_negative_buffer_distance")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
        "--buffer-distance", "-5"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to negative buffer distance")
        parse_args()


def test_invalid_max_workers(monkeypatch: "pytest.MonkeyPatch") -> None:
    """
      Test CLI parser validation for invalid max_workers value (<= 0).
      - Provides zero for max-workers argument.
      - Expects SystemExit due to invalid max workers.
      """
    logger.info("Running test_invalid_max_workers")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
        "--max-workers", "0"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)
    with pytest.raises(SystemExit):
        logger.debug("Expecting SystemExit due to invalid max workers")
        parse_args()
