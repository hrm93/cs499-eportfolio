# Tests and validation utility for checking completeness of GeoJSON feature properties.

import logging

# Configure logger
logger = logging.getLogger("gis_tool.tests")
logger.setLevel(logging.DEBUG)  # Capture all log levels for debugging and info
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')


def validate_geojson_features(geojson_data, required_fields=None):
    """
    Validate that each GeoJSON feature includes a set of required fields in its properties.

    Parameters:
        geojson_data (dict): The GeoJSON object containing features.
        required_fields (list[str], optional): A list of property field names to check for.
                                               Defaults to ["PSI", "Date", "Name", "Material"].

    Returns:
        list: A list of features that contain all required fields with non-empty values.
    """
    if required_fields is None:
        required_fields = ["PSI", "Date", "Name", "Material"]

    valid_features = []
    total_features = len(geojson_data.get("features", []))

    logger.info(f"Starting validation of {total_features} features for required fields: {required_fields}")

    # Iterate through each feature in the collection
    for feature in geojson_data.get("features", []):
        properties = feature.get("properties", {})

        # Identify any required fields that are missing or empty
        missing_fields = [
            field for field in required_fields
            if field not in properties or properties[field] in (None, "", [])
        ]

        if missing_fields:
            # Report ID used for logging context; fallback to UNKNOWN
            report_id = properties.get("report_id", "UNKNOWN")
            logger.warning(
                f"Feature with report_id '{report_id}' is missing required fields: {missing_fields}. "
                "Skipping this feature."
            )
            continue

        # Feature passes validation
        valid_features.append(feature)

    logger.info(f"Validation complete. {len(valid_features)} valid features out of {total_features}.")

    return valid_features


# ------------------------
# Sample GeoJSON Fixtures
# ------------------------

# Sample feature that includes all required fields
geojson_complete = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "report_id": "test001",
                "PSI": 42,
                "Date": "2025-05-30",
                "Name": "Sample Report",
                "Material": "Steel",
                "status": "planned"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-122.4194, 37.7749]
            }
        }
    ]
}

# Sample feature that is missing required fields
geojson_missing = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "report_id": "test002",  # Missing PSI, Name, Material
                "Date": "2025-05-30",
                "status": "planned"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [-123.4194, 36.7749]
            }
        }
    ]
}


# ------------------------
# Test Cases
# ------------------------

def test_validate_all_fields_present(caplog):
    """
    Test that features with all required fields are returned as valid.
    """
    logger.info("Testing with complete GeoJSON data...")

    with caplog.at_level(logging.INFO):
        valid_features = validate_geojson_features(geojson_complete)

    logger.info(f"Number of valid features: {len(valid_features)}")

    # Assert one valid feature and correct report ID
    assert len(valid_features) == 1
    assert valid_features[0]["properties"]["report_id"] == "test001"

    # Check that completion message is logged
    assert "Validation complete" in caplog.text


def test_validate_missing_fields_skips_feature(caplog):
    """
    Test that features missing required fields are skipped and logged as warnings.
    """
    logger.info("Testing with GeoJSON missing required fields...")

    with caplog.at_level(logging.WARNING):
        valid_features = validate_geojson_features(geojson_missing)

    logger.warning(f"Number of valid features: {len(valid_features)} (expected 0)")

    # Assert that no valid features remain and proper warnings were logged
    assert len(valid_features) == 0
    assert "missing required fields" in caplog.text
    assert "test002" in caplog.text
