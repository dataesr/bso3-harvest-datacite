import json
from pathlib import Path
from project.server.main.dataclasses_dc.root import *
from unittest import TestCase

class TestRelationships(TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'tests/unit_test/test_doi_dataclasses/dcdump-test.ndjson'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Root.from_dict_custom(jsonstring)
                
    def test_relationships(cls):
        cls.assertDictEqual(cls.doi.data[0].relationships.client.to_dict(),{"data":{"id":"pangaea.repository","type":"clients"}})