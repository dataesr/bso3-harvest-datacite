import json
from pathlib import Path
from domain.model.datas import Doi
from unittest import TestCase


class TestDescriptions(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_descriptions(self):
        self.assertEqual(
            self.doi.attributes.descriptions[1].description,
            "depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age",
        )
        self.assertEqual(self.doi.attributes.descriptions[1].descriptionType, "TechnicalInfo")
