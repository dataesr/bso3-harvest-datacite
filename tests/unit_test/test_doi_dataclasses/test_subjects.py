import json
from pathlib import Path
from domain.model.datas import Doi
from unittest import TestCase


class TestSubjects(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_subjects(self):
        self.assertEqual(self.doi.attributes.subjects[0].subject, "Event label")
        self.assertEqual(self.doi.attributes.subjects[0].subjectScheme, "Parameter")
