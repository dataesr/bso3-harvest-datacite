import json
from pathlib import Path
from project.server.main.dataclasses_dc.datas import *
from unittest import TestCase

class TestData(TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'tests/unit_test/test_doi_dataclasses/dcdump-test.json'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Doi.from_dict_custom(jsonstring)
                
    def test_data(cls):
        cls.assertEqual(cls.doi.id,"10.1594/pangaea.52464")
        cls.assertNotEqual(cls.doi.id,"10.1574/pangaea.52464")
        cls.assertEqual(cls.doi.type,"dois")