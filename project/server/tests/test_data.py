import json
from pathlib import Path
from project.server.main.dataclasses_dc.root import *
import unittest

class TestCase(unittest.TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'project/server/tests/test_doi_dataclasses/dcdump-test.ndjson'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Root.from_dict_custom(jsonstring)
                
    def test_data(cls):
        #print(cls.doi.data[0].to_dict())
        cls.assertEqual(cls.doi.data[0].id,"10.1594/pangaea.52464")
        cls.assertNotEqual(cls.doi.data[0].id,"10.1574/pangaea.52464")
        cls.assertEqual(cls.doi.data[0].type,"dois")