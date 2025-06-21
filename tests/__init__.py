"""
Tests for gis_tool package __init__.py to verify minimal exports.

This test module ensures that the gis_tool package's minimal __init__.py
only exposes the intended constants and does not import functions or other
modules into the package namespace.

Specifically:
- `test_init_exports_minimal` confirms that only the expected constants are
  available at the package level.
- `test_no_functions_or_main_in_package_namespace` confirms that functions,
  classes, or the main entry point are not inadvertently exposed.

Author: Hannah Rose Morgenstein
Date: 2025-06-22
"""

import gis_tool


def test_init_exports_minimal():
    """
    Verify that only expected constants are exported in gis_tool package namespace.

    This test checks the presence of key configuration constants which should be
    exposed by the minimal __init__.py of the gis_tool package.

    The constants tested include:
    - MONGODB_URI: MongoDB connection URI string constant
    - DB_NAME: Default database name
    - DEFAULT_CRS: Default coordinate reference system identifier
    - DEFAULT_BUFFER_DISTANCE_FT: Default buffer distance in feet
    - LOG_FILENAME: Default log filename

    If any of these constants are missing, it suggests that the minimal __init__.py
    is incomplete or has been modified incorrectly.
    """
    assert hasattr(gis_tool, "MONGODB_URI"), "MONGODB_URI should be exposed in package namespace"
    assert hasattr(gis_tool, "DB_NAME"), "DB_NAME should be exposed in package namespace"
    assert hasattr(gis_tool, "DEFAULT_CRS"), "DEFAULT_CRS should be exposed in package namespace"
    assert hasattr(gis_tool, "DEFAULT_BUFFER_DISTANCE_FT"), "DEFAULT_BUFFER_DISTANCE_FT should be exposed in package namespace"
    assert hasattr(gis_tool, "LOG_FILENAME"), "LOG_FILENAME should be exposed in package namespace"


def test_no_functions_or_main_in_package_namespace():
    """
    Confirm that functions and the main entry point are NOT exposed in the package namespace.

    This test asserts that the minimal __init__.py does NOT import or expose
    any functions, methods, or the main script. This helps to maintain a clean
    and minimal package API surface.

    The following functions and entry points are verified to be absent:
    - merge_buffers_into_planning_file
    - create_buffer_with_geopandas
    - find_new_reports
    - create_pipeline_features
    - connect_to_mongodb
    - setup_logging
    - main
    - process_report_chunk
    - parse_args

    Presence of these attributes would indicate a violation of minimal export policy.
    """
    assert not hasattr(gis_tool, "merge_buffers_into_planning_file"), \
        "Function 'merge_buffers_into_planning_file' should NOT be exposed"
    assert not hasattr(gis_tool, "create_buffer_with_geopandas"), \
        "Function 'create_buffer_with_geopandas' should NOT be exposed"
    assert not hasattr(gis_tool, "find_new_reports"), \
        "Function 'find_new_reports' should NOT be exposed"
    assert not hasattr(gis_tool, "create_pipeline_features"), \
        "Function 'create_pipeline_features' should NOT be exposed"
    assert not hasattr(gis_tool, "connect_to_mongodb"), \
        "Function 'connect_to_mongodb' should NOT be exposed"
    assert not hasattr(gis_tool, "setup_logging"), \
        "Function 'setup_logging' should NOT be exposed"
    assert not hasattr(gis_tool, "main"), \
        "Function 'main' should NOT be exposed"
    assert not hasattr(gis_tool, "process_report_chunk"), \
        "Function 'process_report_chunk' should NOT be exposed"
    assert not hasattr(gis_tool, "parse_args"), \
        "Function 'parse_args' should NOT be exposed"
