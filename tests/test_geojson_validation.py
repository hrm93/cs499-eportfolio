import logging

logger = logging.getLogger("gis_tool")
logger.setLevel(logging.DEBUG)  # Set level to DEBUG to capture all logs
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(name)s:%(message)s')


def validate_geojson_features(geojson_data, required_fields=None):
    if required_fields is None:
        required_fields = ["PSI", "Date", "Name", "Material"]

    valid_features = []
    total_features = len(geojson_data.get("features", []))

    logger.info(f"Starting validation of {total_features} features for required fields: {required_fields}")

    for feature in geojson_data.get("features", []):
        properties = feature.get("properties", {})
        missing_fields = [field for field in required_fields if
                          field not in properties or properties[field] in (None, "", [])]

        if missing_fields:
            report_id = properties.get('report_id', 'UNKNOWN')
            logger.warning(
                f"Feature with report_id '{report_id}' is missing required fields: {missing_fields}. Skipping this feature."
            )
            continue

        valid_features.append(feature)

    logger.info(f"Validation complete. {len(valid_features)} valid features out of {total_features}.")

    return valid_features


# Sample GeoJSON data with all required fields
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

# Sample GeoJSON with missing fields
geojson_missing = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "report_id": "test002",
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

def test_validate_all_fields_present(caplog):
    logger.info("Testing with complete GeoJSON data...")
    with caplog.at_level(logging.INFO):
        valid_features = validate_geojson_features(geojson_complete)
    logger.info(f"Number of valid features: {len(valid_features)}")
    assert len(valid_features) == 1
    assert valid_features[0]["properties"]["report_id"] == "test001"
    assert "Validation complete" in caplog.text

def test_validate_missing_fields_skips_feature(caplog):
    logger.info("Testing with GeoJSON missing required fields...")
    with caplog.at_level(logging.WARNING):
        valid_features = validate_geojson_features(geojson_missing)
    logger.warning(f"Number of valid features: {len(valid_features)} (expected 0)")
    assert len(valid_features) == 0
    assert "missing required fields" in caplog.text
    assert "test002" in caplog.text