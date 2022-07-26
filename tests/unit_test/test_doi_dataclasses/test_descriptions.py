import json
from pathlib import Path
from project.server.main.dataclasses_dc.root import *
from unittest import TestCase

class TestDescriptions(TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'tests/unit_test/test_doi_dataclasses/dcdump-test.ndjson'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Root.from_dict_custom(jsonstring)
                
    def test_descriptions(cls):
        cls.assertEqual(cls.doi.data[0].attributes.descriptions[1].description,"depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age")
        cls.assertEqual(cls.doi.data[0].attributes.descriptions[1].descriptionType,"TechnicalInfo")
