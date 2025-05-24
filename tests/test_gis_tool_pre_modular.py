"""
import unittest
from unittest.mock import patch
import geopandas as gpd
from shapely.geometry import Point
from gis_tool.buffer_processor import create_buffer_with_geopandas
from gis_tool.data_loader import create_pipeline_features
from pathlib import Path

class TestGISPipelineFunctions(unittest.TestCase):

    @patch("geopandas.read_file")
    def test_create_buffer_with_geopandas(self, mock_read_file, tmp_path):
        mock_gdf = gpd.GeoDataFrame({
            "Name": ["Line1", "Line2"],
            "geometry": [Point(1, 1), Point(2, 2)]
        })
        mock_gdf.set_crs('EPSG:32633', inplace=True)
        mock_read_file.return_value = mock_gdf

        output_dir = tmp_path / "mock_output_dir"
        output_dir.mkdir()

        output_file = create_buffer_with_geopandas("mock_gas_lines.shp", str(output_dir), 25)

        assert Path(output_file).exists()

    @patch("geopandas.read_file")
    def test_create_pipeline_features(self, mock_read_file, tmp_path):
        mock_gdf = gpd.GeoDataFrame({
            "Name": ["Line1", "Line2"],
            "geometry": [Point(1, 1), Point(2, 2)]
        })
        mock_gdf.set_crs('EPSG:32633', inplace=True)
        mock_read_file.return_value = mock_gdf

        reports_folder = tmp_path / "mock_reports_folder"
        reports_folder.mkdir()

        report1 = reports_folder / "report1.txt"
        report2 = reports_folder / "report2.txt"
        report1.write_text("Line1|2023-12-31|100|Steel|1,1")
        report2.write_text("Line2|2023-12-31|150|PVC|2,2")

        report_files = [report1.name, report2.name]
        processed_reports = set()

        with self.assertLogs(level='INFO') as log:
            create_pipeline_features(report_files, "mock_gas_lines.shp", str(reports_folder), 'EPSG:32633', set(),
                                     processed_reports)

if __name__ == "__main__":
    unittest.main()
"""