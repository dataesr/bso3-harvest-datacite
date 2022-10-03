import json
import shutil
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd
from tests.unit_test.fixtures.utils_processor import *

from application.utils_processor import _format_doi, write_doi_files

TESTED_MODULE = "application.utils_processor"


class TestProcessor(TestCase):
    @patch(f"{TESTED_MODULE}.push_to_mongo")
    @patch(f"{TESTED_MODULE}.get_mongo_repo")
    def test_write_doi_files_and_enrich_doi(self, mock_get_mongo_repo, mock_push_to_mongo):
        # Given
        mongo_repo_mock = Mock()
        mock_get_mongo_repo.return_value = mongo_repo_mock
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        fr_doi_file_name = f"{_format_doi(sample_affiliations.doi.values[0])}.json"
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
        self.assertEqual(expected_output_files, output_files)
        with fr_doi_file.open("r", encoding="utf-8") as f:
            content = json.load(f)
        mock_push_to_mongo.assert_called_with(content, expected_mongo_obj, mongo_repo_mock)
        self.assertEqual(content, expected_content)
        shutil.rmtree(output_dir)
