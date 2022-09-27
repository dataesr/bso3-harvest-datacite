import json
import shutil
from pathlib import Path
from unittest import TestCase

import pandas as pd
from tests.unit_test.fixtures.utils_processor import *

from application.utils_processor import _format_doi, write_doi_files

TESTED_MODULE = "application.utils_processor"


class TestProcessor(TestCase):
    def test_write_doi_files_and_enrich_doi(self):
        # Given
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        fr_doi_file_name = f"{_format_doi(sample_affiliations.doi.values[0])}.json"
        output_dir = fixture_path / "doi_files"
        expected_fr_doi_file = fixture_path / "expected_fr_doi.json"
        with expected_fr_doi_file.open("r", encoding="utf-8") as f:
            expected_content = json.load(f)
        # When
        write_doi_files(sample_affiliations, fixture_path / "sample.ndjson", output_dir=str(output_dir))
        # Then
        output_files = [file.name for file in output_dir.glob("*.json")]
        fr_doi_file = next(file for file in output_dir.glob("*.json") if file.name == fr_doi_file_name)
        self.assertEqual(expected_output_files, output_files)
        with fr_doi_file.open("r", encoding="utf-8") as f:
            content = json.load(f)
        self.assertEqual(content, expected_content)
        shutil.rmtree(output_dir)
