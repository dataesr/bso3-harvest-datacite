import json
from pathlib import Path
from project.server.main.dataclasses_dc.root import Root
from unittest import TestCase


class TestLinks(TestCase):
    doi = None

    @classmethod
    def setUpClass(cls):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.ndjson"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            cls.doi = Root.from_dict_custom(jsonstring)

    def test_link(cls):
        cls.assertEqual(
            cls.doi.links.self,
            "https://api.datacite.org/dois?affiliation=true&page%5Bcursor%5D=1&page%5Bsize%5D=100&query=updated%3A%5B2022-07-05T00%3A00%3A00Z+TO+2022-07-05T23%3A59%3A59Z%5D&state=findable",
        )
        cls.assertEqual(
            cls.doi.links.next,
            "https://api.datacite.org/dois?affiliation=true&page%5Bcursor%5D=MTQzMTUxMTM0OTAwMCwxMC4xNTQ2OC91aHJoc3o&page%5Bsize%5D=100&query=updated%3A%5B2022-07-05T00%3A00%3A00Z+TO+2022-07-05T23%3A59%3A59Z%5D",
        )
