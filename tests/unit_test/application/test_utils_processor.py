from copy import deepcopy
import gzip
import json
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from application.utils_processor import (
    _retrieve_object_name, _create_affiliation_string, _format_string, _safe_get,
    get_abstract, get_classification_FOS, get_classification_subject,
    get_client_id, get_created, get_description, get_description_element, get_doi_element, get_doi_supplement_to,
    get_doi_version_of, get_grants, get_language, get_licenses,
    get_matched_affiliations, get_methods, get_publicationYear, get_publisher,
    get_registered, get_resourceType, get_resourceTypeGeneral,
    get_ror_or_orcid, get_title, get_updated, json_line_generator, listify, trim_null_values,
    write_doi_files, gzip_cli, _parse_url_and_retrieve_last_part)
from config.global_config import config_harvester
from tests.unit_test.fixtures.utils_processor import *

TESTED_MODULE = "application.utils_processor"


class TestUtilsProcessor(TestCase):
    @classmethod
    def setUpClass(cls):
        input_file_path = fixture_path / "sample.ndjson"
        cls.standard_doi = next(json_line_generator(input_file_path)).get('data')[0]

    def test_get_matched_affiliations(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[48]
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        this_doi_df = sample_affiliations[sample_affiliations["doi"] == doi["id"]]
        affiliation = (doi["attributes"]["creators"] + doi["attributes"]["contributors"])[1]["affiliation"][0]
        aff_str = _create_affiliation_string(affiliation, exclude_list=["affiliationIdentifierScheme"])
        aff_ror = get_ror_or_orcid(affiliation, "affiliationIdentifierScheme", "ROR", "affiliationIdentifier")
        aff_name = affiliation.get('name')
        expected_matched_affiliations = {
            'name': 'Lawrence Berkeley National Laboratory',
            'ror': None,
            'detected_countries': ['us'],
            'detected_ror': [],
            'detected_grid': [],
            'detected_rnsr': []
        }
        # When
        (matched_affiliations, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr)\
         = get_matched_affiliations(this_doi_df[this_doi_df['affiliation_str'] == aff_str], aff_ror, aff_name)
        # Then
        self.assertEqual(matched_affiliations, expected_matched_affiliations)
        self.assertEqual(creator_or_contributor, 'creators')
        self.assertEqual(is_publisher_fr, False)
        self.assertEqual(is_clientId_fr, False)
        self.assertEqual(is_countries_fr, False)
  
    @patch(f"{TESTED_MODULE}.append_to_es_index_sourcefile")
    def test_write_doi_files_and_enrich_doi(self, mock_append):
        # Given
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        fr_doi_file_name = f"{_format_string(sample_affiliations.doi.values[0])}.json"
        output_dir = fixture_path / "doi_files"
        expected_fr_doi_file = fixture_path / "expected_fr_doi.json"
        with expected_fr_doi_file.open("r", encoding="utf-8") as f:
            expected_content = json.load(f)
        is_fr = (sample_affiliations.is_publisher_fr | sample_affiliations.is_clientId_fr | sample_affiliations.is_countries_fr)
        # When
        write_doi_files(
            sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir), index_name='datacite_fr'
        )
        # Then
        output_files = [file.name for file in output_dir.glob("*.json")]
        fr_doi_file = next(
            file for file in output_dir.glob("*.json") if file.name == fr_doi_file_name
        )
        self.assertEqual(sorted(expected_output_files), sorted(output_files))
        with fr_doi_file.open("r", encoding="utf-8") as f:
            content = json.load(f)

        mock_append.assert_called_with(content, expected_creators, expected_contributors, expected_fr_reasons, expected_fr_reasons_concat)
        self.assertEqual(content, expected_content)
        shutil.rmtree(output_dir)

    @patch(f"{TESTED_MODULE}.append_to_file")
    def test_append_to_es_index_sourcefile(self, mock_append):
        # Given
        sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
        output_dir = fixture_path / "doi_files"
        is_fr = (sample_affiliations.is_publisher_fr | sample_affiliations.is_clientId_fr | sample_affiliations.is_countries_fr)
        with open(fixture_path / "expected_append.json", 'r') as f:
            expected_append = json.load(f)
        # When
        write_doi_files(
            sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir), index_name='datacite_fr'
        )
        # Then
        mock_append.assert_called_with(_str=json.dumps(expected_append), file=config_harvester["es_index_sourcefile"])
        shutil.rmtree(output_dir)

    def test_gzip_cli_compress(self):
        # Given
        expected_file_compressed = file_to_compress + ".gz"
        with open(file_to_compress, "rb") as f:
            expected_file_content = f.read()
        # When
        gzip_cli(file_to_compress)
        # Then
        self.assertTrue(os.path.exists(expected_file_compressed))
        with gzip.open(expected_file_compressed, "rb") as f:
            actual_file_content = f.read()
        self.assertEqual(expected_file_content, actual_file_content)

    def test_gzip_cli_decompress(self):
        # Given
        expected_file = os.path.splitext(file_to_decompress)[0]
        with gzip.open(file_to_decompress, "rb") as f:
            expected_file_content = f.read()
        # When
        gzip_cli(file_to_decompress, decompress=True)
        # Then
        self.assertTrue(os.path.exists(expected_file))
        with open(expected_file, "rb") as f:
            actual_file_content = f.read()
        self.assertEqual(expected_file_content, actual_file_content)

    def test_parse_url(self):
        # Given
        url = "https://orcid.org/0000-0002-7285-027X"
        expected = "0000-0002-7285-027X"
        # When
        result = _parse_url_and_retrieve_last_part(url)
        # Then
        self.assertEqual(result, expected)

    def test_parse_url_no_splitter(self):
        # Given
        url = "0000-0002-7285-027X"
        expected = "0000-0002-7285-027X"
        # When
        result = _parse_url_and_retrieve_last_part(url)
        # Then
        self.assertEqual(result, expected)

    def test_parse_url_default(self):
        # Given
        url = ""
        expected = ""
        # When
        result = _parse_url_and_retrieve_last_part(url)
        # Then
        self.assertEqual(result, expected)

    def test_trim_null_values(self):
        # Given
        _dict = {
            "a": {},
            "b": "",
            "c": [],
            "d": None,
            "e": {"a": None},
            "f": {"a": "a"},
            "g": "g",
            "h": [None],
            "i": ["i"],
        }
        expected = {
            "f": {"a": "a"},
            "g": "g",
            "h": [None],
            "i": ["i"],
        }
        # When
        result = trim_null_values(_dict)
        # Then
        self.assertEqual(expected, result)

    def test_listify(self):
        # Given
        list_str = "['a', 'b']"
        expected = ['a', 'b']
        # When
        result = listify(list_str)
        # Then
        self.assertEqual(expected, result)

    def test_listify_w_list(self):
        # Given
        list_str = ['a', 'b']
        expected = ['a', 'b']
        # When
        result = listify(list_str)
        # Then
        self.assertEqual(expected, result)

    def test_listify_default(self):
        # Given
        list_str = "not a list"
        expected = []
        # When
        result = listify(list_str)
        # Then
        self.assertEqual(expected, result)

    
class TestGetters(TestCase):
    @classmethod
    def setUpClass(cls):
        input_file_path = fixture_path / "sample.ndjson"
        cls.standard_doi = next(json_line_generator(input_file_path)).get('data')[0]

    def test_safe_get(self):
        # Given
        default_value = ""
        expected_publisher = "The Global Biodiversity Information Facility"
        # When
        publisher = _safe_get(default_value, TestGetters.standard_doi, "attributes", "publisher")
        # Then
        self.assertEqual(publisher, expected_publisher)

    def test_safe_get_KeyError_handling(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        del doi["attributes"]
        default_value = ""
        # When
        publisher = _safe_get(default_value, doi, "attributes", "publisher")
        # Then
        self.assertEqual(publisher, default_value)

    def test_safe_get_TypeError_handling(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"] = []
        default_value = ""
        # When
        publisher = _safe_get(default_value, doi, "attributes", "publisher")
        # Then
        self.assertEqual(publisher, default_value)

    def test_safe_get_value_eq_None(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["publisher"] = None
        default_value = ""
        # When
        publisher = _safe_get(default_value, doi, "attributes", "publisher")
        # Then
        self.assertEqual(publisher, default_value)

    def test_retrieve_object_name(self):
        # Given
        expected_name = "Occdownload Gbif.Org"
        # When
        name = _retrieve_object_name(creator)
        # Then
        self.assertEqual(name, expected_name)

    def test_retrieve_object_name_wo_name(self):
        # Given
        creator_no_name = deepcopy(creator)
        del creator_no_name['name']
        expected_name = "Jeremie Mouginot"
        # When
        name = _retrieve_object_name(creator_no_name)
        # Then
        self.assertEqual(name, expected_name)

    def test_retrieve_object_name_w_only_givenName(self):
        # Given
        creator_w_only_givenName = deepcopy(creator)
        del creator_w_only_givenName['name']
        del creator_w_only_givenName['familyName']
        expected_name = "Jeremie"
        # When
        name = _retrieve_object_name(creator_w_only_givenName)
        # Then
        self.assertEqual(name, expected_name)

    def test_retrieve_object_name_w_only_familyName(self):
        # Given
        creator_w_only_familyName = deepcopy(creator)
        del creator_w_only_familyName['name']
        del creator_w_only_familyName['givenName']
        expected_name = "Mouginot"
        # When
        name = _retrieve_object_name(creator_w_only_familyName)
        # Then
        self.assertEqual(name, expected_name)

    def test_retrieve_object_name_default(self):
        # Given
        creator_w_nothing = deepcopy(creator)
        del creator_w_nothing['name']
        del creator_w_nothing['givenName']
        del creator_w_nothing['familyName']
        expected_name = ""
        # When
        name = _retrieve_object_name(creator_w_nothing)
        # Then
        self.assertEqual(name, expected_name)

    def test_get_classification_subject(self):
        # Given
        expected_classification_subject = ['GBIF', 'biodiversity', 'species occurrences']
        # When
        classification_subject = get_classification_subject(TestGetters.standard_doi)
        # Then
        self.assertEqual(classification_subject, expected_classification_subject)

    def test_get_classification_subject_KeyError_handling(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        del doi["attributes"]["subjects"]
        expected_classification_subject = []
        # When
        classification_subject = get_classification_subject(doi)
        # Then
        self.assertEqual(classification_subject, expected_classification_subject)

    def test_get_classification_subject_empty_list(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["subjects"] = []
        expected_classification_subject = []
        # When
        classification_subject = get_classification_subject(doi)
        # Then
        self.assertEqual(classification_subject, expected_classification_subject)

    def test_get_classification_subject_list_w_None(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["subjects"] = [None]
        expected_classification_subject = []
        # When
        classification_subject = get_classification_subject(doi)
        # Then
        self.assertEqual(classification_subject, expected_classification_subject)

    def test_get_classification_subject_list_w_None_element(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["subjects"] = [{"subject": None}]
        expected_classification_subject = []
        # When
        classification_subject = get_classification_subject(doi)
        # Then
        self.assertEqual(classification_subject, expected_classification_subject)

    def test_get_classification_FOS(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[23]
        expected_classification_FOS = ['Biological sciences']
        # When
        classification_FOS = get_classification_FOS(doi)
        # Then
        self.assertEqual(classification_FOS, expected_classification_FOS)

    def test_get_classification_FOS_KeyError_handling(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[23]
        del doi["attributes"]["subjects"]
        expected_classification_FOS = []
        # When
        classification_FOS = get_classification_FOS(doi)
        # Then
        self.assertEqual(classification_FOS, expected_classification_FOS)

    def test_get_classification_FOS_empty_list(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[23]
        doi["attributes"]["subjects"] = []
        expected_classification_FOS = []
        # When
        classification_FOS = get_classification_FOS(doi)
        # Then
        self.assertEqual(classification_FOS, expected_classification_FOS)

    def test_get_classification_FOS_list_w_None(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[23]
        doi["attributes"]["subjects"] = [None]
        expected_classification_FOS = []
        # When
        classification_FOS = get_classification_FOS(doi)
        # Then
        self.assertEqual(classification_FOS, expected_classification_FOS)

    def test_get_classification_FOS_list_w_None_element(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[23]
        doi["attributes"]["subjects"] = [
            {
                "subject": None,
                "subjectScheme": "Fields of Science and Technology (FOS)"
            }
        ]
        expected_classification_FOS = []
        # When
        classification_FOS = get_classification_FOS(doi)
        # Then
        self.assertEqual(classification_FOS, expected_classification_FOS)

    def test_get_description_element(self):
        # Given
        expected_abstract = 'A dataset containing 46 species occurrences available in GBIF matching the query: { "TaxonKey" : [ "is Leuciscus danilewskii (Kessler, 1877)" ] } The dataset includes 46 records from 10 constituent datasets: 2 records from iNaturalist Research-grade Observations. 20 records from Fish occurrence in middle Volga and upper Don regions (Russia). 1 records from CAS Ichthyology (ICH). 1 records from INSDC Sequences. 1 records from Field Museum of Natural History (Zoology) Fish Collection. 1 records from NMNH Extant Specimen Records (USNM, US). 9 records from Records of protected animals species in Ukraine. 1 records from Fishbase. 8 records from ZFMK Ichthyology collection. 2 records from Museo Nacional de Ciencias Naturales, Madrid: MNCN_ICTIO. Data from some individual datasets included in this download may be licensed under less restrictive terms.'
        # When
        abstract = get_description_element(TestGetters.standard_doi, "Abstract")
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_description_element_KeyError_handling(self):
        # Given
        doi = TestGetters.standard_doi
        del doi["attributes"]["descriptions"]
        expected_abstract = ""
        # When
        abstract = get_description_element(doi, "Abstract")
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_description_element_empty_list(self):
        # Given
        doi = TestGetters.standard_doi
        doi["attributes"]["descriptions"] = []
        expected_abstract = ""
        # When
        abstract = get_description_element(doi, "Abstract")
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_description_element_list_w_None(self):
        # Given
        doi = TestGetters.standard_doi
        doi["attributes"]["descriptions"] = [None]
        expected_abstract = ""
        # When
        abstract = get_description_element(doi, "Abstract")
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_description_element_list_w_None_element(self):
        # Given
        doi = TestGetters.standard_doi
        doi["attributes"]["descriptions"] = [
            {
                "description": None,
                "descriptionType": "Abstract"
            }
        ]
        expected_abstract = ""
        # When
        abstract = get_description_element(doi, "Abstract")
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_grants(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        expected_grants = [{'name': 'NASA'}]
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_grants_KeyError_handling(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        del doi["attributes"]["fundingReferences"]
        expected_grants = []
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_grants_empty_list(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        doi["attributes"]["fundingReferences"] = []
        expected_grants = []
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_grants_list_w_None(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        doi["attributes"]["fundingReferences"] = [None]
        expected_grants = []
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_grants_list_w_None_element(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        doi["attributes"]["fundingReferences"] = [{"funderName": None}]
        expected_grants = []
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_doi_element(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        expected_doi_element = ['10.5281/zenodo.6510179']
        # When
        doi_element = get_doi_element(doi, "IsVersionOf")
        # Then
        self.assertEqual(doi_element, expected_doi_element)

    def test_get_doi_element_KeyError_handling(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        del doi["attributes"]["relatedIdentifiers"]
        expected_doi_element = []
        # When
        doi_element = get_doi_element(doi, "IsVersionOf")
        # Then
        self.assertEqual(doi_element, expected_doi_element)

    def test_get_doi_element_empty_list(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        doi["attributes"]["relatedIdentifiers"] = []
        expected_doi_element = []
        # When
        doi_element = get_doi_element(doi, "IsVersionOf")
        # Then
        self.assertEqual(doi_element, expected_doi_element)

    def test_get_doi_element_list_w_None(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        doi["attributes"]["relatedIdentifiers"] = [None]
        expected_doi_element = []
        # When
        doi_element = get_doi_element(doi, "IsVersionOf")
        # Then
        self.assertEqual(doi_element, expected_doi_element)

    def test_get_doi_element_list_w_None_element(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        doi["attributes"]["relatedIdentifiers"] = [
            {
                "relatedIdentifier": None,
                "relationType": "IsVersionOf",
                "relatedIdentifierType": "DOI"
            }
        ]
        expected_doi_element = []
        # When
        doi_element = get_doi_element(doi, "IsVersionOf")
        # Then
        self.assertEqual(doi_element, expected_doi_element)

    def test_get_title(self):
        # Given
        expected_title = "Occurrence Download"
        # When
        title = get_title(TestGetters.standard_doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_title_KeyError_handling(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        del doi["attributes"]["titles"]
        expected_title = ""
        # When
        title = get_title(doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_title_empty_list(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["titles"] = []
        expected_title = ""
        # When
        title = get_title(doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_title_list_w_None(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["titles"] = [None]
        expected_title = ""
        # When
        title = get_title(doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_title_list_w_None_element(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["titles"] = [{"title": None}]
        expected_title = ""
        # When
        title = get_title(doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_registered(self):
        # Given
        expected_registered = "2022-06-03T21:43:46Z"
        # When
        registered = get_registered(TestGetters.standard_doi)
        # Then
        self.assertEqual(registered, expected_registered)

    def test_get_created(self):
        # Given
        expected_created = "2022-06-03T21:43:46Z"
        # When
        created = get_created(TestGetters.standard_doi)
        # Then
        self.assertEqual(created, expected_created)

    def test_get_licenses(self):
        # Given
        expected_license = "cc-by-nc-4.0"
        # When
        license = get_licenses(TestGetters.standard_doi)
        # Then
        self.assertEqual(license, expected_license)

    def test_get_licenses_KeyError_handling(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        del doi["attributes"]["rightsList"]
        expected_license = ""
        # When
        license = get_licenses(doi)
        # Then
        self.assertEqual(license, expected_license)

    def test_get_licenses_empty_list(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["rightsList"] = []
        expected_license = ""
        # When
        license = get_licenses(doi)
        # Then
        self.assertEqual(license, expected_license)

    def test_get_licenses_list_w_None(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["rightsList"] = [None]
        expected_license = ""
        # When
        license = get_licenses(doi)
        # Then
        self.assertEqual(license, expected_license)

    def test_get_licenses_list_w_None_element(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["rightsList"] = [{"rightsIdentifier": None}]
        expected_license = ""
        # When
        license = get_licenses(doi)
        # Then
        self.assertEqual(license, expected_license)

    def test_get_resourceType(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[7]
        expected_resourceType = "poster"
        # When
        resourceType = get_resourceType(doi)
        # Then
        self.assertEqual(resourceType, expected_resourceType)

    def test_get_resourceType_list(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[7]
        doi["attributes"]["types"]["resourceType"] = ["POSTER"]
        expected_resourceType = "['poster']"
        # When
        resourceType = get_resourceType(doi)
        # Then
        self.assertEqual(resourceType, expected_resourceType)

    def test_get_resourceTypeGeneral(self):
        # Given
        expected_resourceTypeGeneral = "dataset"
        # When
        resourceTypeGeneral = get_resourceTypeGeneral(TestGetters.standard_doi)
        # Then
        self.assertEqual(resourceTypeGeneral, expected_resourceTypeGeneral)

    def test_get_resourceTypeGeneral_list(self):
        # Given
        doi = deepcopy(TestGetters.standard_doi)
        doi["attributes"]["types"]["resourceTypeGeneral"] = ["POSTER"]
        expected_resourceTypeGeneral = "['poster']"
        # When
        resourceTypeGeneral = get_resourceTypeGeneral(doi)
        # Then
        self.assertEqual(resourceTypeGeneral, expected_resourceTypeGeneral)

    def test_get_language(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[1]
        expected_language = "en"
        # When
        language = get_language(doi)
        # Then
        self.assertEqual(language, expected_language)

    def test_get_publicationYear(self):
        # Given
        expected_publicationYear = "2022"
        # When
        publicationYear = get_publicationYear(TestGetters.standard_doi)
        # Then
        self.assertEqual(publicationYear, expected_publicationYear)

    def test_get_updated(self):
        # Given
        expected_updated = "2022-06-03T21:43:46Z"
        # When
        updated = get_updated(TestGetters.standard_doi)
        # Then
        self.assertEqual(updated, expected_updated)

    def test_get_publisher(self):
        # Given
        expected_publisher = "The Global Biodiversity Information Facility"
        # When
        publisher = get_publisher(TestGetters.standard_doi)
        # Then
        self.assertEqual(publisher, expected_publisher)

    def test_get_client_id(self):
        # Given
        expected_client_id = "gbif.gbif"
        # When
        client_id = get_client_id(TestGetters.standard_doi)
        # Then
        self.assertEqual(client_id, expected_client_id)

