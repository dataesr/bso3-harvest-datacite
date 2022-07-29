import json
from pathlib import Path
from domain.model.datas import *
from unittest import TestCase


class TestAttributes(TestCase):
    doi = None

    @classmethod
    def setUpClass(self):
        path_file = Path.cwd() / "tests/unit_test/test_doi_dataclasses/dcdump-test.json"
        with path_file.open("r", encoding="utf-8") as f:
            jsonstring = json.load(f)
            self.doi = Doi.from_dict_custom(jsonstring)

    def test_attributtes(self):

        self.assertEqual(self.doi.attributes.doi, "10.1594/pangaea.52464")
        self.assertListEqual(self.doi.attributes.identifiers, [])
        self.assertDictEqual(
            self.doi.attributes.creators[0].to_dict(),
            {
                "name": "Broecker, Wallace S",
                "givenName": "Wallace S",
                "familyName": "Broecker",
                "affiliation": [],
                "nameIdentifiers": [
                    "{'schemeUri': 'https://orcid.org', 'nameIdentifier': 'https://orcid.org/0000-0003-4816-0903', 'nameIdentifierScheme': 'ORCID'}"
                ],
            },
        )
        self.assertDictEqual(
            self.doi.attributes.titles[0].to_dict(),
            {
                "title": "Sedimentation rates calculated on surface sediment samples from different site of the Atlantic and Pacific Oceans (Table 1), supplement to: Broecker, Wallace S; Klas, Mieczyslawa; Clark, Elizabeth; Bonani, Georges; Ivy, Susan; Wolfli, Willy (1991): The influence of CaCO3 dissolution on core top radiocarbon ages for deep-sea sediments. Paleoceanography, 6(5), 593-608"
            },
        )
        self.assertEqual(
            self.doi.attributes.publisher,
            "PANGAEA - Data Publisher for Earth & Environmental Science",
        )
        #######rcontainer "{}" != {}
        self.assertEqual(self.doi.attributes.container, "{}")
        self.assertEqual(self.doi.attributes.publicationYear, 1991)
        self.assertDictEqual(
            self.doi.attributes.subjects[1].to_dict(),
            {"subject": "Latitude of event", "subjectScheme": "Parameter"},
        )
        self.assertListEqual(self.doi.attributes.contributors, [])
        self.assertDictEqual(
            self.doi.attributes.dates[0].to_dict(),
            {"date": "1991", "dateType": "Issued"},
        )
        self.assertEqual(self.doi.attributes.language, "en")
        self.assertDictEqual(
            self.doi.attributes.types.to_dict(),
            {
                "ris": "DATA",
                "bibtex": "misc",
                "citeproc": "dataset",
                "schemaOrg": "Dataset",
                "resourceType": "Supplementary Dataset",
                "resourceTypeGeneral": "Dataset",
            },
        )
        self.assertDictEqual(
            self.doi.attributes.relatedIdentifiers[2].to_dict(),
            {
                "relationType": "References",
                "relatedIdentifier": "10.1017/s0033822200007542",
                "relatedIdentifierType": "DOI",
            },
        )
        self.assertListEqual(self.doi.attributes.sizes, ["219 data points"])
        self.assertListEqual(self.doi.attributes.formats, ["text/tab-separated-values"])
        self.assertEqual(self.doi.attributes.version, None)
        self.assertDictEqual(
            self.doi.attributes.rightsList[0].to_dict(),
            {
                "rights": "Creative Commons Attribution 3.0 Unported",
                "rightsUri": "https://creativecommons.org/licenses/by/3.0/legalcode",
                "schemeUri": "https://spdx.org/licenses/",
                "rightsIdentifier": "cc-by-3.0",
                "rightsIdentifierScheme": "SPDX",
            },
        )
        self.assertDictEqual(
            self.doi.attributes.descriptions[1].to_dict(),
            {
                "description": "depth is the apparent mixing depth, as determined by a downcore series of radiocarbon age",
                "descriptionType": "TechnicalInfo",
            },
        )
        self.assertListEqual(
            self.doi.attributes.fundingReferences,
            [
                "{'awardUri': 'https://cordis.europa.eu/project/rcn/47700/factsheet/en', 'awardTitle': 'Quaternary Environment of the Eurasian North', 'funderName': 'Fourth Framework Programme', 'awardNumber': 'MAS3980185', 'funderIdentifier': 'https://doi.org/10.13039/100011105', 'funderIdentifierType': 'Crossref Funder ID'}"
            ],
        )
        self.assertEqual(self.doi.attributes.contentUrl, None)
        self.assertEqual(
            self.doi.attributes.schemaVersion, "http://datacite.org/schema/kernel-4"
        )
        self.assertEqual(self.doi.attributes.source, "mds")
        self.assertEqual(self.doi.attributes.state, "findable")
        self.assertEqual(self.doi.attributes.reason, None)
        self.assertEqual(self.doi.attributes.viewCount, 0)
        self.assertEqual(self.doi.attributes.referenceCount, 1)
        self.assertEqual(self.doi.attributes.created, "2011-08-10T12:58:08Z")
        self.assertEqual(self.doi.attributes.registered, "2005-04-08T04:11:40Z")
        self.assertEqual(self.doi.attributes.published, None)
        self.assertEqual(self.doi.attributes.updated, "2022-07-05T01:06:26Z")
