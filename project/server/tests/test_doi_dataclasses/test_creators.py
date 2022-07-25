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
                
    def test_creators(cls):
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].name,"Klas, Mieczyslawa")
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].givenName,"Mieczyslawa")
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].familyName,"Klas")
        cls.assertListEqual(cls.doi.data[0].attributes.creators[1].affiliation,[])
        cls.assertListEqual(cls.doi.data[0].attributes.creators[1].nameIdentifiers,[])