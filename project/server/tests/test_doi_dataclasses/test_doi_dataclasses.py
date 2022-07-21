import json
from pathlib import Path
from project.server.main.dataclasses_dc import *
import unittest
import dataclasses
from typing import Any, List

class TestCase(unittest.TestCase):
    doi=None

    @classmethod
    def setUpClass(cls):
         path_file=Path.cwd() /'project/server/tests/test_doi_dataclasses/doi.json'
         with path_file.open( 'r', encoding='utf-8') as f:
             jsonstring= json.load(f)
             cls.doi = Root.from_dict_custom(jsonstring)
                
    def test_link(cls):
        cls.assertEqual(cls.doi.links.self,"https://api.datacite.org/dois?affiliation=true&page%5Bcursor%5D=1&page%5Bsize%5D=100&query=updated%3A%5B2022-07-05T00%3A00%3A00Z+TO+2022-07-05T23%3A59%3A59Z%5D&state=findable")
        cls.assertEqual(cls.doi.links.next,"https://api.datacite.org/dois?affiliation=true&page%5Bcursor%5D=MTQzMTUxMTM0OTAwMCwxMC4xNTQ2OC91aHJoc3o&page%5Bsize%5D=100&query=updated%3A%5B2022-07-05T00%3A00%3A00Z+TO+2022-07-05T23%3A59%3A59Z%5D")

    def test_data(cls):
        #print(cls.doi.data[0].to_dict())
        cls.assertEqual(cls.doi.data[0].id,"10.1594/pangaea.52464")
        cls.assertNotEqual(cls.doi.data[0].id,"10.1574/pangaea.52464")
        cls.assertEqual(cls.doi.data[0].type,"dois")
    
    def test_attributtes(cls):

        cls.assertEqual(cls.doi.data[0].attributes.doi,"10.1594/pangaea.52464")
        cls.assertListEqual(cls.doi.data[0].attributes.identifiers,[])
        cls.assertDictEqual(cls.doi.data[0].attributes.creators[0].to_dict(),{'name': 'Broecker, Wallace S', 'givenName': 'Wallace S', 'familyName': 'Broecker', 'affiliation': [], 'nameIdentifiers': ["{'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-4816-0903', 'nameIdentifierScheme': 'ORCID'}"]})
        cls.assertDictEqual(cls.doi.data[0].attributes.titles[0].to_dict(),{'title': 'Sedimentation rates calculated on surface sediment samples from different site of the Atlantic and Pacific Oceans (Table 1), supplement to: Broecker, Wallace S; Klas, Mieczyslawa; Clark, Elizabeth; Bonani, Georges; Ivy, Susan; Wolfli, Willy (1991): The influence of CaCO3 dissolution on core top radiocarbon ages for deep-sea sediments. Paleoceanography, 6(5), 593-608'})
        cls.assertEqual(cls.doi.data[0].attributes.publisher,"PANGAEA - Data Publisher for Earth & Environmental Science")
        #######rcontainer "{}" != {}
        cls.assertEqual(cls.doi.data[0].attributes.container,"{}")
        cls.assertEqual(cls.doi.data[0].attributes.publicationYear,1991)
        cls.assertDictEqual(cls.doi.data[0].attributes.subjects[1].to_dict(),{'subject': 'Latitude of event', 'subjectScheme': 'Parameter'})
        cls.assertListEqual(cls.doi.data[0].attributes.contributors,[])
        cls.assertDictEqual(cls.doi.data[0].attributes.dates[0].to_dict(),{'date': '1991', 'dateType': 'Issued'})
        cls.assertEqual(cls.doi.data[0].attributes.language,"en")
        cls.assertDictEqual(cls.doi.data[0].attributes.types.to_dict(),{'ris': 'DATA', 'bibtex': 'misc', 'citeproc': 'dataset', 'schemaOrg': 'Dataset', 'resourceType': 'Supplementary Dataset', 'resourceTypeGeneral': 'Dataset'})
        cls.assertDictEqual(cls.doi.data[0].attributes.relatedIdentifiers[2].to_dict(),{'relationType': 'References', 'relatedIdentifier': '10.1017/s0033822200007542', 'relatedIdentifierType': 'DOI'})
        cls.assertListEqual(cls.doi.data[0].attributes.sizes,["219 data points"])
        cls.assertListEqual(cls.doi.data[0].attributes.formats,["text/tab-separated-values"])
        cls.assertEqual(cls.doi.data[0].attributes.version,None)
        cls.assertDictEqual(cls.doi.data[0].attributes.rightsList[0].to_dict(),{'rights': 'Creative Commons Attribution 3.0 Unported', 'rightsUri': 'https://creativecommons.org/licenses/by/3.0/legalcode', 'schemeUri': 'https://spdx.org/licenses/', 'rightsIdentifier': 'cc-by-3.0', 'rightsIdentifierScheme': 'SPDX'})
        cls.assertDictEqual(cls.doi.data[0].attributes.descriptions[1].to_dict(),{'description': 'depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age', 'descriptionType': 'TechnicalInfo'})
        cls.assertListEqual(cls.doi.data[0].attributes.fundingReferences,["{'awardUri': 'https://cordis.europa.eu/project/rcn/47700/factsheet/en', 'awardTitle': 'Quaternary Environment of the Eurasian North', 'funderName': 'Fourth Framework Programme', 'awardNumber': 'MAS3980185', 'funderIdentifier': 'https://doi.org/10.13039/100011105', 'funderIdentifierType': 'Crossref Funder ID'}"])
        cls.assertEqual(cls.doi.data[0].attributes.contentUrl,None)
        cls.assertEqual(cls.doi.data[0].attributes.schemaVersion,"http://datacite.org/schema/kernel-4")
        cls.assertEqual(cls.doi.data[0].attributes.source,"mds")
        cls.assertEqual(cls.doi.data[0].attributes.state,"findable")
        cls.assertEqual(cls.doi.data[0].attributes.reason,None)
        cls.assertEqual(cls.doi.data[0].attributes.viewCount,0)
        cls.assertEqual(cls.doi.data[0].attributes.referenceCount,1)
        cls.assertEqual(cls.doi.data[0].attributes.created,"2011-08-10T12:58:08Z")
        cls.assertEqual(cls.doi.data[0].attributes.registered,"2005-04-08T04:11:40Z")
        cls.assertEqual(cls.doi.data[0].attributes.published,None)
        cls.assertEqual(cls.doi.data[0].attributes.updated,"2022-07-05T01:06:26Z")



    def test_relationships(cls):
        cls.assertDictEqual(cls.doi.data[0].relationships.client.to_dict(),{"data":{"id":"pangaea.repository","type":"clients"}})
        
    def test_client(cls):
        cls.assertDictEqual(cls.doi.data[0].relationships.client.data.to_dict(),{"id":"pangaea.repository","type":"clients"})
    
    def test_subjects(cls):
        cls.assertEqual(cls.doi.data[0].attributes.subjects[0].subject,"Event label")
        cls.assertEqual(cls.doi.data[0].attributes.subjects[0].subjectScheme,"Parameter")

        
    def test_creators(cls):
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].name,"Klas, Mieczyslawa")
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].givenName,"Mieczyslawa")
        cls.assertEqual(cls.doi.data[0].attributes.creators[1].familyName,"Klas")
        cls.assertListEqual(cls.doi.data[0].attributes.creators[1].affiliation,[])
        cls.assertListEqual(cls.doi.data[0].attributes.creators[1].nameIdentifiers,[])
   
    def test_descriptions(cls):
        cls.assertEqual(cls.doi.data[0].attributes.descriptions[1].description,"depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age")
        cls.assertEqual(cls.doi.data[0].attributes.descriptions[1].descriptionType,"TechnicalInfo")

        
