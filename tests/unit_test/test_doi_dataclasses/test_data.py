import json
from pathlib import Path
from domain.model.datas import *
from unittest import TestCase


class TestData(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_data(self):
        self.assertEqual(self.doi.id, "10.1594/pangaea.52464")
        self.assertNotEqual(self.doi.id, "10.1574/pangaea.52464")
        self.assertEqual(self.doi.type, "dois")
