import json
import shutil
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd
from tests.unit_test.fixtures.utils_processor import *

from application.utils_processor import _format_string, write_doi_files

TESTED_MODULE = "application.utils_processor"


class TestProcessor(TestCase):
    @patch(f"{TESTED_MODULE}.append_to_es_index_sourcefile")
    def test_write_doi_files_and_enrich_doi(self, mock_append):
        # Given
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        fr_doi_file_name = f"{_format_string(sample_affiliations.doi.values[0])}.json"
        output_dir = fixture_path / "doi_files"
        expected_fr_doi_file = fixture_path / "expected_fr_doi.json"
        with expected_fr_doi_file.open("r", encoding="utf-8") as f:
            expected_content = json.load(f)
        is_fr = (sample_affiliations.is_publisher_fr | sample_affiliations.is_clientId_fr | sample_affiliations.is_countries_fr)
        # When
        write_doi_files(
            sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir)
        )
        # Then
        output_files = [file.name for file in output_dir.glob("*.json")]
        fr_doi_file = next(
            file for file in output_dir.glob("*.json") if file.name == fr_doi_file_name
        )
        self.assertEqual(sorted(expected_output_files), sorted(output_files))
        with fr_doi_file.open("r", encoding="utf-8") as f:
            content = json.load(f)
        mock_append.assert_called_with(content, expected_mongo_obj)
        self.assertEqual(content, expected_content)
        shutil.rmtree(output_dir)
