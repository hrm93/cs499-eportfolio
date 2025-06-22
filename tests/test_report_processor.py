import logging
import fiona.errors
import geopandas as gpd

from gis_tool.report_processor import process_report_chunk

logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)


def test_process_report_chunk_error_logging(monkeypatch, caplog, input_folder_with_report, dummy_gdf):
    """Verify error logging when create_pipeline_features raises FileNotFoundError."""
    logger.info("Starting test_process_report_chunk_error_logging")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)
    monkeypatch.setattr(
        "gis_tool.report_processor.create_pipeline_features",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError("Simulated missing file")),
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder_with_report / "gas_lines.shp"),
            reports_folder=input_folder_with_report,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.message for record in caplog.records)


def test_process_report_chunk_success(monkeypatch, caplog, input_folder_with_report, dummy_gdf):
    """Test normal successful execution."""
    logger.info("Starting test_process_report_chunk_success")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)

    called = {"ran": False}

    def dummy_create_pipeline_features(**_kwargs):
        called["ran"] = True
        logger.info("Dummy create_pipeline_features called.")

    monkeypatch.setattr("gis_tool.report_processor.create_pipeline_features", dummy_create_pipeline_features)

    with caplog.at_level(logging.INFO):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder_with_report / "gas_lines.shp"),
            reports_folder=input_folder_with_report,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert called["ran"]
    assert any("Finished processing chunk" in record.message for record in caplog.records)


def test_process_report_chunk_shapefile_failure(monkeypatch, caplog, input_folder_with_report):
    """Test that reading shapefile failure logs error."""
    logger.info("Starting test_process_report_chunk_shapefile_failure")

    monkeypatch.setattr(
        gpd,
        "read_file",
        lambda *a, **k: (_ for _ in ()).throw(fiona.errors.DriverError("broken shapefile")),
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder_with_report / "broken.shp"),
            reports_folder=input_folder_with_report,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("I/O error in chunk" in record.message for record in caplog.records)


def test_process_report_chunk_unexpected_exception(monkeypatch, caplog, input_folder_with_report, dummy_gdf):
    """Test unexpected exception is caught and logged."""
    logger.info("Starting test_process_report_chunk_unexpected_exception")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)
    monkeypatch.setattr(
        "gis_tool.report_processor.create_pipeline_features",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("unexpected failure")),
    )

    with caplog.at_level(logging.ERROR):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder_with_report / "gas_lines.shp"),
            reports_folder=input_folder_with_report,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=False,
        )

    assert any("Unexpected error in chunk" in record.message for record in caplog.records)


def test_process_report_chunk_mongodb_warning(monkeypatch, caplog, input_folder_with_report, dummy_gdf):
    """Check MongoDB insert warning is logged when gas_lines_collection is None but use_mongodb True."""
    logger.info("Starting test_process_report_chunk_mongodb_warning")

    monkeypatch.setattr(gpd, "read_file", lambda *a, **k: dummy_gdf)
    monkeypatch.setattr("gis_tool.report_processor.create_pipeline_features", lambda **kwargs: None)

    with caplog.at_level(logging.INFO):
        process_report_chunk(
            report_chunk=["dummy_report.geojson"],
            gas_lines_shp=str(input_folder_with_report / "gas_lines.shp"),
            reports_folder=input_folder_with_report,
            spatial_reference="EPSG:4326",
            gas_lines_collection=None,
            use_mongodb=True,
        )

    assert any("MongoDB insert disabled for this process" in record.message for record in caplog.records)
