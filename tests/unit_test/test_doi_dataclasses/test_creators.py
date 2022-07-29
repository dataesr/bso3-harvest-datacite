import json
from pathlib import Path
from domain.model.datas import *
from unittest import TestCase


class TestCreators(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_creators(self):
        self.assertEqual(self.doi.attributes.creators[1].name, "Klas, Mieczyslawa")
        self.assertEqual(self.doi.attributes.creators[1].givenName, "Mieczyslawa")
        self.assertEqual(self.doi.attributes.creators[1].familyName, "Klas")
        self.assertListEqual(self.doi.attributes.creators[1].affiliation, [])
        self.assertListEqual(self.doi.attributes.creators[1].nameIdentifiers, [])
