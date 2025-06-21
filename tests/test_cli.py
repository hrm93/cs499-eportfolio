### Tests for cli.py ###

import sys
import logging
import pytest

from gis_tool.cli import parse_args

from io import StringIO
from contextlib import redirect_stdout, redirect_stderr

# Configure logger to capture debug-level logs during tests
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)


def test_default_log_level(monkeypatch):
    """
    Test that the default log level is WARNING when no explicit log-level is specified.
    """
    logger.debug("Running test_default_log_level")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    # Patch sys.argv to simulate CLI input arguments
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.log_level == "WARNING"


def test_help_output(monkeypatch):
    """
    Test that the --help flag prints usage and exits the program.
    """
    logger.debug("Running test_help_output")
    testargs = ["prog", "--help"]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_parse_args_required(monkeypatch):
    """
    Test parsing of required arguments with default optional values.
    """
    logger.debug("Running test_parse_args_required")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.geojson",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.buffer_distance == 25.0  # default buffer distance
    assert args.use_mongodb is False     # default flag state
    assert args.output_format == "geojson"  # default output format


def test_parse_args_with_flags(monkeypatch):
    """
    Test parsing all flags and options explicitly set by user.
    """
    logger.debug("Running test_parse_args_with_flags")
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
    args = parse_args()
    assert args.buffer_distance == 50.0
    assert args.crs == "EPSG:4326"
    assert args.parallel is True
    assert args.use_mongodb is True
    assert args.max_workers == 4
    assert args.log_level == "ERROR"
    assert args.log_file == "/tmp/mylog.log"
    assert args.output_format == "geojson"
    assert args.overwrite_output is True


def test_parse_args_verbose_flag(monkeypatch):
    """
    Test that --verbose sets the log level to DEBUG and verbose flag to True.
    """
    logger.debug("Running test_parse_args_verbose_flag")
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
    args = parse_args()
    assert args.log_level == "DEBUG"
    assert args.verbose is True


def test_parse_args_dry_run_and_config(monkeypatch):
    """
    Test that --dry-run flag is set and config file overrides buffer_distance.
    """
    logger.debug("Running test_parse_args_dry_run_and_config")
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

    # Mock load_config_file to simulate config file loading and override
    monkeypatch.setattr("gis_tool.cli.load_config_file", lambda path: {
        "buffer_distance": 50,
        "crs": "EPSG:4326",
    })

    args = parse_args()
    assert args.dry_run is True
    assert args.config_file == "/path/to/config.yaml"
    assert args.buffer_distance == 50  # Confirm override from config


def test_parse_args_no_mongodb(monkeypatch):
    """
    Test that --no-mongodb disables MongoDB usage.
    """
    logger.debug("Running test_parse_args_no_mongodb")
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
    args = parse_args()
    assert args.use_mongodb is False


def test_parse_args_multiple_report_files(monkeypatch):
    """
    Test that multiple report files are accepted and parsed correctly.
    """
    logger.debug("Running test_parse_args_multiple_report_files")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report1.txt", "report2.geojson",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert len(args.report_files) == 2
    assert "report1.txt" in args.report_files
    assert "report2.geojson" in args.report_files


def test_parse_args_invalid_report_extension(monkeypatch):
    """
    Test that invalid report file extensions cause the program to exit.
    """
    logger.debug("Running test_parse_args_invalid_report_extension")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.invalid",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_input_folder_missing(monkeypatch):
    """
    Test that specifying a missing input folder causes program exit.
    """
    logger.debug("Running test_input_folder_missing")
    testargs = [
        "prog",
        "--input-folder", "missing_input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_report_file_missing(monkeypatch):
    """
    Test that specifying a missing report file causes program exit.
    """
    logger.debug("Running test_report_file_missing")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "missing_report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_output_dir_missing(monkeypatch):
    """
    Test that specifying an output path with a non-existent directory causes exit.
    """
    logger.debug("Running test_output_dir_missing")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "missing_output_dir/out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_invalid_crs_format(monkeypatch):
    """
    Test that providing an invalid CRS string causes program exit.
    """
    logger.debug("Running test_invalid_crs_format")
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
    with pytest.raises(SystemExit):
        parse_args()


def test_negative_buffer_distance(monkeypatch):
    """
    Test that providing a negative buffer distance causes program exit.
    """
    logger.debug("Running test_negative_buffer_distance")
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
    with pytest.raises(SystemExit):
        parse_args()


def test_invalid_max_workers(monkeypatch):
    """
    Test that setting max workers to zero or less causes program exit.
    """
    logger.debug("Running test_invalid_max_workers")
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
    with pytest.raises(SystemExit):
        parse_args()


def test_default_output_format(monkeypatch):
    """
    Test that default output format is geojson if not specified.
    """
    logger.debug("Running test_default_output_format")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.output_format == "geojson"


def test_overwrite_output_flag(monkeypatch):
    """
    Test that the --overwrite-output flag is parsed correctly as True.
    """
    logger.debug("Running test_overwrite_output_flag")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.shp",
        "--gas-lines-path", "gas.shp",
        "--report-files", "report.txt",
        "--overwrite-output"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.overwrite_output is True


# ---- NEW TESTS ----


def test_mongodb_conflicting_flags(monkeypatch):
    """
    Test that mutually exclusive --use-mongodb and --no-mongodb flags cause an error.
    """
    logger.debug("Running test_mongodb_conflicting_flags")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--use-mongodb",
        "--no-mongodb"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_config_file_with_no_config_flag(monkeypatch):
    """
    Test that specifying --no-config skips config file loading even if --config-file is provided.
    """
    logger.debug("Running test_config_file_with_no_config_flag")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--config-file", "/path/to/config.yaml",
        "--no-config"
    ]
    monkeypatch.setattr(sys, "argv", testargs)

    # Mock load_config_file to fail test if called, confirming it's not invoked
    monkeypatch.setattr("gis_tool.cli.load_config_file", lambda path: pytest.fail("load_config_file should not be called"))

    args = parse_args()
    assert args.config_file == "/path/to/config.yaml"


def test_interactive_flag(monkeypatch):
    """
    Test that --interactive flag sets interactive to True and defaults to False if absent.
    """
    logger.debug("Running test_interactive_flag")
    # Test with --interactive present
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--interactive"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.interactive is True

    # Test with --interactive absent (default False)
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.interactive is False


def test_invalid_future_dev_path_extension(monkeypatch):
    """
    Test behavior when future-dev-path has an unexpected file extension.
    (Adjust this test based on your actual validation implementation.)
    """
    logger.debug("Running test_invalid_future_dev_path_extension")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.geojson",  # non-.shp extension
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.future_dev_path == "future.geojson"


def test_invalid_gas_lines_path_extension(monkeypatch):
    """
    Test behavior when gas-lines-path has an unexpected file extension.
    """
    logger.debug("Running test_invalid_gas_lines_path_extension")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--gas-lines-path", "gas.json",  # non-.shp extension
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.gas_lines_path == "gas.json"


def test_print_config_override_and_dry_run(monkeypatch):
    """
    Test that config overrides and dry-run warning messages are printed to stdout.
    """
    logger.debug("Running test_print_config_override_and_dry_run")
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--config-file", "/path/to/config.yaml",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", testargs)

    # Mock load_config_file to return example overrides
    monkeypatch.setattr("gis_tool.cli.load_config_file", lambda path: {
        "buffer_distance": 50,
        "crs": "EPSG:4326"
    })

    f = StringIO()
    with redirect_stdout(f), redirect_stderr(f):
        args = parse_args()

    output = f.getvalue()
    assert "Loaded settings from /path/to/config.yaml" in output
    assert "Config override: buffer_distance = 50" in output
    assert "Warning: Dry-run mode enabled" in output or "⚠️ Warning: Dry-run mode enabled" in output
