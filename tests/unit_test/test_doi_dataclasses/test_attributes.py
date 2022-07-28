import json
from pathlib import Path
from project.server.main.dataclasses_dc.datas import *
from unittest import TestCase


class TestAttributes(TestCase):
    doi = None

    @classmethod
    def setUpClass(cls):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            cls.doi = Doi.from_dict_custom(jsonstring)

    def test_attributtes(cls):

        cls.assertEqual(cls.doi.attributes.doi, "10.1594/pangaea.52464")
        cls.assertListEqual(cls.doi.attributes.identifiers, [])
        cls.assertDictEqual(
            cls.doi.attributes.creators[0].to_dict(),
            {
                "name": "Broecker, Wallace S",
                "givenName": "Wallace S",
                "familyName": "Broecker",
                "affiliation": [],
                "nameIdentifiers": ["{'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-4816-0903', 'nameIdentifierScheme': 'ORCID'}"],
            },
        )
        cls.assertDictEqual(
            cls.doi.attributes.titles[0].to_dict(),
            {
                "title": "Sedimentation rates calculated on surface sediment samples from different site of the Atlantic and Pacific Oceans (Table 1), supplement to: Broecker, Wallace S; Klas, Mieczyslawa; Clark, Elizabeth; Bonani, Georges; Ivy, Susan; Wolfli, Willy (1991): The influence of CaCO3 dissolution on core top radiocarbon ages for deep-sea sediments. Paleoceanography, 6(5), 593-608"
            },
        )
        cls.assertEqual(cls.doi.attributes.publisher, "PANGAEA - Data Publisher for Earth & Environmental Science")
        #######rcontainer "{}" != {}
        cls.assertEqual(cls.doi.attributes.container, "{}")
        cls.assertEqual(cls.doi.attributes.publicationYear, 1991)
        cls.assertDictEqual(cls.doi.attributes.subjects[1].to_dict(), {"subject": "Latitude of event", "subjectScheme": "Parameter"})
        cls.assertListEqual(cls.doi.attributes.contributors, [])
        cls.assertDictEqual(cls.doi.attributes.dates[0].to_dict(), {"date": "1991", "dateType": "Issued"})
        cls.assertEqual(cls.doi.attributes.language, "en")
        cls.assertDictEqual(
            cls.doi.attributes.types.to_dict(),
            {"ris": "DATA", "bibtex": "misc", "citeproc": "dataset", "schemaOrg": "Dataset", "resourceType": "Supplementary Dataset", "resourceTypeGeneral": "Dataset"},
        )
        cls.assertDictEqual(cls.doi.attributes.relatedIdentifiers[2].to_dict(), {"relationType": "References", "relatedIdentifier": "10.1017/s0033822200007542", "relatedIdentifierType": "DOI"})
        cls.assertListEqual(cls.doi.attributes.sizes, ["219 data points"])
        cls.assertListEqual(cls.doi.attributes.formats, ["text/tab-separated-values"])
        cls.assertEqual(cls.doi.attributes.version, None)
        cls.assertDictEqual(
            cls.doi.attributes.rightsList[0].to_dict(),
            {
                "rights": "Creative Commons Attribution 3.0 Unported",
                "rightsUri": "https://creativecommons.org/licenses/by/3.0/legalcode",
                "schemeUri": "https://spdx.org/licenses/",
                "rightsIdentifier": "cc-by-3.0",
                "rightsIdentifierScheme": "SPDX",
            },
        )
        cls.assertDictEqual(
            cls.doi.attributes.descriptions[1].to_dict(),
            {"description": "depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age", "descriptionType": "TechnicalInfo"},
        )
        cls.assertListEqual(
            cls.doi.attributes.fundingReferences,
            [
                "{'awardUri': 'https://cordis.europa.eu/project/rcn/47700/factsheet/en', 'awardTitle': 'Quaternary Environment of the Eurasian North', 'funderName': 'Fourth Framework Programme', 'awardNumber': 'MAS3980185', 'funderIdentifier': 'https://doi.org/10.13039/100011105', 'funderIdentifierType': 'Crossref Funder ID'}"
            ],
        )
        cls.assertEqual(cls.doi.attributes.contentUrl, None)
        cls.assertEqual(cls.doi.attributes.schemaVersion, "http://datacite.org/schema/kernel-4")
        cls.assertEqual(cls.doi.attributes.source, "mds")
        cls.assertEqual(cls.doi.attributes.state, "findable")
        cls.assertEqual(cls.doi.attributes.reason, None)
        cls.assertEqual(cls.doi.attributes.viewCount, 0)
        cls.assertEqual(cls.doi.attributes.referenceCount, 1)
        cls.assertEqual(cls.doi.attributes.created, "2011-08-10T12:58:08Z")
        cls.assertEqual(cls.doi.attributes.registered, "2005-04-08T04:11:40Z")
        cls.assertEqual(cls.doi.attributes.published, None)
        cls.assertEqual(cls.doi.attributes.updated, "2022-07-05T01:06:26Z")
