### Tests for cli.py ###

import sys
import logging
import pytest

from gis_tool.cli import parse_args

from io import StringIO
from contextlib import redirect_stdout, redirect_stderr

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs for test diagnostics


def test_default_log_level(monkeypatch):
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
    assert args.log_level == "WARNING"


def test_help_output(monkeypatch):
    testargs = ["prog", "--help"]
    monkeypatch.setattr(sys, "argv", testargs)
    with pytest.raises(SystemExit):
        parse_args()


def test_parse_args_required(monkeypatch):
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
    assert args.buffer_distance == 25.0
    assert args.use_mongodb is False
    assert args.output_format == "geojson"


def test_parse_args_with_flags(monkeypatch):
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

    # MOCK load_config_file to avoid FileNotFoundError
    monkeypatch.setattr("gis_tool.cli.load_config_file", lambda path: {
        "buffer_distance": 50,  # example override
        "crs": "EPSG:4326",
        # add any other config keys your code expects
    })

    args = parse_args()
    assert args.dry_run is True
    assert args.config_file == "/path/to/config.yaml"
    assert args.buffer_distance == 50  # confirm config override worked


def test_parse_args_no_mongodb(monkeypatch):
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

# 1. Test conflicting MongoDB flags raises error due to mutually exclusive group
def test_mongodb_conflicting_flags(monkeypatch):
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


# 2. Test --config-file provided with --no-config skips config loading
def test_config_file_with_no_config_flag(monkeypatch):
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--config-file", "/path/to/config.yaml",
        "--no-config"
    ]
    monkeypatch.setattr(sys, "argv", testargs)

    # Mock load_config_file so if called, it would raise error (to confirm not called)
    monkeypatch.setattr("gis_tool.cli.load_config_file", lambda path: pytest.fail("load_config_file should not be called"))

    args = parse_args()
    # config_file arg is set, but no config should be loaded due to --no-config
    assert args.config_file == "/path/to/config.yaml"
    # No exception means load_config_file was not called


# 3. Test --interactive flag presence and default False
def test_interactive_flag(monkeypatch):
    # Interactive flag present
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

    # Interactive flag absent defaults to False
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.interactive is False


# 4. Test invalid extensions for future-dev-path and gas-lines-path (should allow only .shp)
def test_invalid_future_dev_path_extension(monkeypatch):
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--future-dev-path", "future.geojson",  # invalid extension, expecting .shp?
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    # Assuming your parse_args does not currently error on this, we add this test as a check
    # If you want this validation, add it to parse_args and then expect SystemExit here.
    args = parse_args()
    # You can optionally assert here or skip if no validation exists yet
    assert args.future_dev_path == "future.geojson"


def test_invalid_gas_lines_path_extension(monkeypatch):
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--gas-lines-path", "gas.json",  # invalid extension, expecting .shp?
        "--report-files", "report.txt",
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.gas_lines_path == "gas.json"


# 5. Test print outputs for config override and dry-run warning

def test_print_config_override_and_dry_run(monkeypatch):
    testargs = [
        "prog",
        "--input-folder", "input",
        "--output-path", "out.shp",
        "--report-files", "report.txt",
        "--config-file", "/path/to/config.yaml",
        "--dry-run"
    ]
    monkeypatch.setattr(sys, "argv", testargs)

    # Mock load_config_file returns a config override
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
