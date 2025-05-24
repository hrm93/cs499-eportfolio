### test for __init__.py
"""
Tests for gis_tool package __init__.py to verify minimal exports.

Ensures that only the intended constants are exposed at the package level,
and that functions and other modules are not imported into the package namespace
when using a minimal __init__.py.
"""
import gis_tool


def test_init_exports_minimal():
    # Only check for the constants that your minimal __init__.py exposes
    assert hasattr(gis_tool, "MONGODB_URI")
    assert hasattr(gis_tool, "DB_NAME")
    assert hasattr(gis_tool, "DEFAULT_CRS")
    assert hasattr(gis_tool, "DEFAULT_BUFFER_DISTANCE_FT")
    assert hasattr(gis_tool, "LOG_FILENAME")

def test_no_functions_or_main_in_package_namespace():
    # Functions and main should NOT be in package namespace with minimal __init__.py
    assert not hasattr(gis_tool, "merge_buffers_into_planning_file")
    assert not hasattr(gis_tool, "create_buffer_with_geopandas")
    assert not hasattr(gis_tool, "find_new_reports")
    assert not hasattr(gis_tool, "create_pipeline_features")
    assert not hasattr(gis_tool, "connect_to_mongodb")
    assert not hasattr(gis_tool, "setup_logging")
    assert not hasattr(gis_tool, "main")
    assert not hasattr(gis_tool, "process_report_chunk")
    assert not hasattr(gis_tool, "parse_args")
