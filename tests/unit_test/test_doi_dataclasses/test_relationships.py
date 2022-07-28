import json
from pathlib import Path
from project.server.main.dataclasses_dc.datas import *
from unittest import TestCase

class TestRelationships(TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'tests/unit_test/test_doi_dataclasses/dcdump-test.json'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Doi.from_dict_custom(jsonstring)
                
    def test_relationships(cls):
        cls.assertDictEqual(cls.doi.relationships.client.to_dict(),{"data":{"id":"pangaea.repository","type":"clients"}})