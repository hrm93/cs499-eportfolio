### Tests for cli.py ###

import sys
import os
import logging
import pytest
from gis_tool.cli import parse_args

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs for test diagnostics


@pytest.fixture(autouse=True)
def patch_os_path(monkeypatch):
    """Patch os.path.isdir and os.path.isfile globally for all tests."""
    def mock_isdir(path: str) -> bool:
        logger.debug(f"Mock isdir check: {path}")
        if path in ['missing_input', 'missing_output_dir']:
            logger.warning(f"Directory missing: {path}")
            return False
        return True

    def mock_isfile(path: str) -> bool:
        logger.debug(f"Mock isfile check: {path}")
        if 'missing_report' in path:
            logger.warning(f"File missing: {path}")
            return False
        return True

    monkeypatch.setattr(os.path, "isdir", mock_isdir)
    monkeypatch.setattr(os.path, "isfile", mock_isfile)


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
    assert args.log_level == "INFO"


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
    assert args.output_format == 'shp'


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
    assert args.output_format == "shp"


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
