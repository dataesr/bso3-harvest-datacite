import json
from pathlib import Path
from unittest import TestCase
from domain.model.datas import Doi


class TestClient(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_client(self):
        self.assertDictEqual(
            self.doi.relationships.client.data.to_dict(),
            {"id": "pangaea.repository", "type": "clients"},
        )
