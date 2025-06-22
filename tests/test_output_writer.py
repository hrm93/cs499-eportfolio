# tests/test_output_writer.py
import logging
import warnings

from unittest import mock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from gis_tool import output_writer
import gis_tool.config as config


logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Capture all logs for tests


def test_write_geojson_creates_file(sample_gdf, tmp_path):
    """Test write_geojson creates file with correct content."""
    logger.info("Testing write_geojson for file creation and content.")
    output_file = tmp_path / "test.geojson"

    output_writer.write_geojson(sample_gdf, str(output_file))
    assert output_file.exists()
    logger.debug(f"GeoJSON file created at {output_file}")

    gdf_loaded = gpd.read_file(str(output_file))
    assert len(gdf_loaded) == 2
    assert list(gdf_loaded['name']) == ['A', 'B']
    logger.info("write_geojson test passed.")


def test_write_csv_creates_file(sample_df, tmp_path):
    """Test write_csv creates file with correct content."""
    logger.info("Testing write_csv for file creation and content.")
    output_file = tmp_path / "test.csv"

    output_writer.write_csv(sample_df, str(output_file))
    assert output_file.exists()
    logger.debug(f"CSV file created at {output_file}")

    df_loaded = pd.read_csv(str(output_file))
    assert list(df_loaded['col1']) == [1, 2, 3]
    assert list(df_loaded['col2']) == ['a', 'b', 'c']
    logger.info("write_csv test passed.")


def test_write_report_creates_file(tmp_path):
    """Test write_report creates plain text report file."""
    logger.info("Testing write_report for file creation and content.")
    text = "This is a test report.\nLine 2."
    output_file = tmp_path / "report.txt"

    output_writer.write_report(text, str(output_file))
    assert output_file.exists()
    logger.debug(f"Report file created at {output_file}")

    with open(output_file, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == text
    logger.info("write_report test passed.")


@pytest.mark.parametrize("func, args", [
    (output_writer.write_geojson, gpd.GeoDataFrame({'name': ['A']}, geometry=[Point(0, 0)], crs="EPSG:4326")),
    (output_writer.write_csv, pd.DataFrame({'a': [1]})),
    (output_writer.write_report, "test content")
])
def test_invalid_path_raises_file_not_found_error(func, args):
    """
    Test that write functions raise FileNotFoundError when
    given an invalid output directory.
    """
    logger.info(f"Testing invalid path for {func.__name__}")
    invalid_path = "C:\\invalid:folder\\file"

    with pytest.raises(FileNotFoundError):
        func(args, invalid_path)
    logger.info(f"{func.__name__} invalid path test passed.")


def test_generate_html_report_creates_file(sample_gdf, tmp_path):
    """Test generate_html_report writes a valid HTML file."""
    logger.info("Testing generate_html_report file creation and content.")
    output_path = tmp_path / "buffer_report"

    output_writer.generate_html_report(sample_gdf, 100.0, str(output_path))
    html_file = output_path.with_suffix(".html")
    assert html_file.exists()
    logger.debug(f"HTML report created at {html_file}")

    content = html_file.read_text(encoding="utf-8")
    assert "Buffer Operation Report" in content
    assert "Buffer Distance" in content
    assert "Sample of Buffered Features" in content
    logger.info("generate_html_report test passed.")


def test_generate_html_report_invalid_directory(sample_gdf, tmp_path):
    """Test generate_html_report raises FileNotFoundError for missing dir."""
    logger.info("Testing generate_html_report with invalid directory.")
    invalid_path = tmp_path / "nonexistent_dir" / "buffer_report"

    with pytest.raises(FileNotFoundError):
        output_writer.generate_html_report(sample_gdf, 50, str(invalid_path))
    logger.info("generate_html_report invalid directory test passed.")


def test_write_gis_output_shp_and_geojson(tmp_path, sample_gdf):
    """Test write_gis_output writes .shp and .geojson files correctly."""
    logger.info("Testing write_gis_output with both SHP and GeoJSON formats.")

    shp_path = tmp_path / "output.shp"
    geojson_path = tmp_path / "output.geojson"

    # Write Shapefile
    output_writer.write_gis_output(sample_gdf, str(shp_path), output_format="shp", overwrite=True)
    assert shp_path.exists()
    logger.debug(f"Shapefile written to {shp_path}")

    # Write GeoJSON
    output_writer.write_gis_output(sample_gdf, str(geojson_path), output_format="geojson", overwrite=True)
    assert geojson_path.exists()
    logger.debug(f"GeoJSON file written to {geojson_path}")

    logger.info("write_gis_output shp and geojson test passed.")


def test_write_gis_output_empty_gdf(tmp_path):
    """Test write_gis_output does nothing and logs warning for empty GeoDataFrame."""
    logger.info("Testing write_gis_output with empty GeoDataFrame.")
    empty_gdf = gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")
    output_path = tmp_path / "empty_output.shp"

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")  # Catch all warnings
        output_writer.write_gis_output(empty_gdf, str(output_path))

        # Verify at least one warning contains the message
        assert any("GeoDataFrame is empty" in str(warn.message) for warn in w), \
            "Expected warning about empty GeoDataFrame was not raised"

    assert not output_path.exists()
    logger.info("write_gis_output empty GeoDataFrame test passed.")


def test_write_gis_output_unsupported_format(tmp_path, sample_gdf):
    """Test write_gis_output raises ValueError for unsupported format."""
    logger.info("Testing write_gis_output with unsupported output format.")

    with pytest.raises(ValueError):
        output_writer.write_gis_output(sample_gdf, str(tmp_path / "file.xyz"), output_format="xyz")
    logger.info("write_gis_output unsupported format test passed.")


def test_write_gis_output_overwrite_behavior(tmp_path, sample_gdf):
    """Test that write_gis_output respects overwrite flag."""
    logger.info("Testing write_gis_output overwrite behavior.")

    output_file = tmp_path / "output.shp"
    # First write to create the file
    output_writer.write_gis_output(sample_gdf, str(output_file), overwrite=True)
    assert output_file.exists()

    # Try writing again without overwrite should raise FileExistsError
    with pytest.raises(FileExistsError):
        output_writer.write_gis_output(sample_gdf, str(output_file), overwrite=False)
    logger.info("write_gis_output overwrite behavior test passed.")


def test_write_gis_output_interactive_overwrite(tmp_path, sample_gdf):
    """Test interactive overwrite prompt allows or disallows file overwrite."""
    logger.info("Testing interactive overwrite prompt behavior.")

    output_file = tmp_path / "output.shp"
    # Create initial file (overwrite=True so it writes)
    output_writer.write_gis_output(sample_gdf, str(output_file), overwrite=True)
    assert output_file.exists()

    # Patch input to decline overwrite - should NOT raise anymore, just skip writing
    with mock.patch("builtins.input", return_value="n"):
        # No exception expected now
        output_writer.write_gis_output(sample_gdf, str(output_file), overwrite=False, interactive=True)
    logger.info("Interactive prompt correctly skips overwrite on 'n' without error.")

    # Patch input to accept overwrite - should overwrite without error
    with mock.patch("builtins.input", return_value="y"):
        output_writer.write_gis_output(sample_gdf, str(output_file), overwrite=False, interactive=True)
    logger.info("Interactive prompt correctly allows overwrite on 'y'.")


def test_write_gis_output_dry_run(tmp_path, sample_gdf, caplog):
    """Test write_gis_output dry-run mode logs the intended write without creating files."""
    logger.info("Testing write_gis_output in dry-run mode.")

    output_path = tmp_path / "output.geojson"
    # Enable dry run
    config.DRY_RUN_MODE = True

    with caplog.at_level(logging.INFO):
        output_writer.write_gis_output(sample_gdf, str(output_path), output_format="geojson", overwrite=True)
        assert f"[DRY-RUN] Would write" in caplog.text

    assert not output_path.exists()

    # Disable dry run for other tests
    config.DRY_RUN_MODE = False
    logger.info("write_gis_output dry-run test passed.")
