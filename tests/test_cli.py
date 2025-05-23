### test for cli.py

import pytest
from gis_tool.cli import parse_args
import sys


def test_parse_args_required(monkeypatch):
    """
    Test CLI argument parser with required arguments:
    - Simulates command line args via monkeypatch.
    - Checks that parsed arguments match expected values.
    - Verifies default buffer_distance is 25.0 if not specified.
    - Verifies MongoDB integration defaults to False.
    """
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


def test_parse_args_with_flags(monkeypatch):
    """
    Test CLI argument parser with all flags set including buffer distance,
    CRS, multiprocessing enabled, and MongoDB integration enabled.
    """
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
        "--use-mongodb"
    ]
    monkeypatch.setattr(sys, "argv", testargs)
    args = parse_args()
    assert args.buffer_distance == 50.0
    assert args.crs == "EPSG:4326"
    assert args.parallel is True
    assert args.use_mongodb is True


def test_parse_args_no_mongodb(monkeypatch):
    """
    Test CLI parser explicitly disabling MongoDB integration.
    """
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


def test_parse_args_invalid_report_extension(monkeypatch):
    """
    Test CLI parser error handling when unsupported report file extension is used.
    """
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


def test_parse_args_multiple_report_files(monkeypatch):
    """
    Test CLI parser accepting multiple report files with supported extensions.
    """
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