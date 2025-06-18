import logging
import pytest
import geopandas as gpd

from shapely.geometry import Point

from gis_tool.main import process_report_chunk

# Logger setup for test module
logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Capture all logs during testing


def test_process_report_chunk_error_logging(monkeypatch: pytest.MonkeyPatch, caplog, tmp_path) -> None:
    """Verify error logging when create_pipeline_features fails inside process_report_chunk."""
    logger.info("Testing error logging in process_report_chunk.")

    input_folder = tmp_path / "input_folder"
    input_folder.mkdir()

    # Add a dummy .geojson file to trigger create_pipeline_features
    dummy_report = input_folder / "dummy_report.geojson"
    dummy_report.write_text('{"type": "FeatureCollection", "features": []}')

    dummy_gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, crs="EPSG:4326")

    # Monkeypatch read_file to return dummy data
    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    # Monkeypatch create_pipeline_features to raise FileNotFoundError
    monkeypatch.setattr(
        "gis_tool.report_processor.create_pipeline_features",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("Simulated missing file"))
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder / "gas_lines.shp"),
            reports_folder=input_folder,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    # Confirm the error log was emitted
    assert any("I/O error in chunk" in record.getMessage() for record in caplog.records), (
        "Expected error log not found.\nCaptured logs:\n" +
        "\n".join(record.getMessage() for record in caplog.records)
    )

    logger.info("Error logging test in process_report_chunk passed.")
