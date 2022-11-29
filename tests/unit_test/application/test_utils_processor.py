import json
import shutil
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd

from application.utils_processor import (_concat_affiliation_of_creator_or_contributor, _format_string, get_client_id, get_created, get_language, get_license, get_matched_affiliations, get_publicationYear, get_publisher, get_registered, get_resourceType, get_resourceTypeGeneral, get_ror_or_orcid, get_title, get_abstract,
                                         get_classification_FOS,
                                         get_classification_subject,
                                         get_description,
                                         get_doi_supplement_to,
                                         get_doi_version_of, get_grants,
                                         get_methods, get_updated, json_line_generator,
                                         write_doi_files)
from config.global_config import config_harvester
from tests.unit_test.fixtures.utils_processor import *

TESTED_MODULE = "application.utils_processor"


class TestProcessor(TestCase):
    @classmethod
    def setUpClass(cls):
        input_file_path = fixture_path / "sample.ndjson"
        cls.standard_doi = next(json_line_generator(input_file_path)).get('data')[0]

    # def test_get_matched_affiliations(self):
    #     # Given
    #     sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
    #     is_fr = (sample_affiliations.is_publisher_fr | sample_affiliations.is_clientId_fr | sample_affiliations.is_countries_fr)
    #     doi = TestProcessor.standard_doi
    #     affiliation = next(next(doi["attributes"]["creators"] + doi["attributes"]["contributors"]).get("affiliation"))
    #     aff_str = _concat_affiliation_of_creator_or_contributor(affiliation, exclude_list=["affiliationIdentifierScheme"])
    #     aff_ror = get_ror_or_orcid(affiliation, "affiliationIdentifierScheme", "ROR", "affiliationIdentifier")
    #     aff_name = affiliation.get('name')
    #     this_doi = sample_affiliations["doi"] == doi["id"]
    #     # When
    #     (matched_affiliations, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr)\
    #      = get_matched_affiliations(sample_affiliations[this_doi], aff_str, aff_ror, aff_name)
    #     # Then

    
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
            sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir)
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
            sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir)
        )
        # Then
        mock_append.assert_called_with(_str=json.dumps(expected_append), file=config_harvester["es_index_sourcefile"])
        shutil.rmtree(output_dir)

    def test_get_classification_subject(self):
        # Given
        expected_classification_subject = ['GBIF', 'biodiversity', 'species occurrences']
        # When
        classification_subject = get_classification_subject(TestProcessor.standard_doi)
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

    def test_get_abstract(self):
        # Given
        expected_abstract = 'A dataset containing 46 species occurrences available in GBIF matching the query: { "TaxonKey" : [ "is Leuciscus danilewskii (Kessler, 1877)" ] } The dataset includes 46 records from 10 constituent datasets: 2 records from iNaturalist Research-grade Observations. 20 records from Fish occurrence in middle Volga and upper Don regions (Russia). 1 records from CAS Ichthyology (ICH). 1 records from INSDC Sequences. 1 records from Field Museum of Natural History (Zoology) Fish Collection. 1 records from NMNH Extant Specimen Records (USNM, US). 9 records from Records of protected animals species in Ukraine. 1 records from Fishbase. 8 records from ZFMK Ichthyology collection. 2 records from Museo Nacional de Ciencias Naturales, Madrid: MNCN_ICTIO. Data from some individual datasets included in this download may be licensed under less restrictive terms.'
        # When
        abstract = get_abstract(TestProcessor.standard_doi)
        # Then
        self.assertEqual(abstract, expected_abstract)

    def test_get_description(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        expected_description = 'If you use this software, please cite it using these metadata.'
        # When
        description = get_description(doi)
        # Then
        self.assertEqual(description, expected_description)

    def test_get_methods(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[75]
        expected_methods = 'Participant Sixteen volunteers (15 males and 1 female; mean ± SD age, 28.6 ± 6.6 y; weight, 66.0 ± 12.0 kg) without a history of neurological or orthopedic disorders were included in this study. Each participant was tested using two of the four experimental protocols (Fig. 1B). Eight of them participated in Experiments 1 and 3, while the other eight participated in Experiments 2 and 4, with the order of participation randomly distributed among subjects to overcome any ordering effects. All participants were naïve to the purpose of the study and provided written informed consent before participation. All experimental procedures were approved by the local Ethics Committee of the School of Arts and Sciences of the University of Tokyo and were conducted following the Declaration of Helsinki. Experiment The experiments consisted of walking under one of two physical conditions (either with an “aiding” the force field or an “impeding” force field, see Fig. 1A). Force fields were applied to the participants via a belt stranded around the torso near the COM, which was then attached to the counterweight (2 kg) through two carabiners, a cable, and two low friction pulleys. Subjects were pulled horizontally forward in the aiding force field, while a backward pull was applied in the impeding force field. The participants were instructed to walk on a split-belt treadmill (Bertec, Columbus, OH, USA) with two separate belts, and the speed of each belt was controlled independently. In the present experiment, the treadmill was operated under one of two conditions, tied (two belts moving together at the same speed) or split (separately at different speeds), using a custom-written computer program written in Lab-VIEW (National Instruments, Austin, Texas, USA). The speeds were set at 0.75 ms-1 for both belts under the tied, and while in the split, the belt on the left side was 0.5 ms-1 and another on the right was 1.0 ms-1 (ratio, 1:2). The limb on the slower (left) side speed of the treadmill under the split was defined as the “slow limb” and the limb on the faster (right) side speed as the “fast limb.” The experimental protocols consisted of baseline, adaptation, re-adaptation, and three washout periods, as dictated by the protocol (Fig. 1B). Participants always accompanied one of the two force fields (impeding or aiding) throughout the experiments. During the baseline, the treadmill was tied and participants walked with the impeding and aiding force fields for 1 min each. The treadmill was then operated in a split and the participants underwent a 10-min adaptation, followed by a 1-min catch trial (washout 1) to walk on the treadmill in tied. The treadmill was then returned to split, and the participants again underwent adaptation to walking on a split-belt (re-adaptation) for 5 min, which was again followed by a 1-min catch trial (washout 2) to walk on a tied belt. The force fields in the two catch trial periods (washouts 1 and 2) were different in direction (impeding washout 1 and aiding in washout 2 or vice versa) to address both the degree of adaptation by evaluating the magnitude of the aftereffect and how it could transfer to walking with the opposite force field. In Experiment 1, for example, the degree of adaptation was tested by assessing the magnitude of the aftereffect while walking with the aiding force field on the tied belt during washout 1 (catch trial) after adapting to walk on the split-belt with the force field in the same (aiding) direction. The transfer of adaptation, on the other hand, was tested by walking with the impeding force field in washout 2 (post-adaptation) after adapting to walking (re-adaptation period) with the force field in the opposite (aiding) direction. Given that the emergence of the aftereffect is not stable but can decay throughout the experiments (Ogawa et al. 2015; 2018) (4,12), the order of exposure to the washout periods with different force fields was alternated depending on the experiments (between experiments 1 and 2, 3, and 4, respectively) to overcome possible ordering effects. In Experiments 1 and 3, subjects underwent an extra washout of three periods to walk on a tied belt with force fields in the same direction as the adaptation periods, but were different from those during the washout 2 period. The purpose of this additional washout period was to evaluate the degree to which the adaptation acquired through walking on the split-belt could be maintained (or washed out) after walking with a force field in a different direction. Between each testing period, upon changing the belt speeds and/or direction of the force field, there was a 15-s time interval in which subjects stepped on platforms on both sides of the treadmill. They were then allowed to step on the treadmill with the left leg when a sufficient belt speed was reached and the appropriate force fields were mounted. During the experiments, subjects were instructed to walk while watching a wall approximately 3 m in front of them and not to look down at the belts. They were allowed to hold onto the handrails mounted on either side of the treadmill in case of risk of falling. All subjects completed the test sessions without holding on. To ensure safety, one experimenter stood on the treadmill. Data recording and analysis Force sensors mounted underneath each treadmill belt were used to determine the dimensional ground reaction force (GRF) components: mediolateral (Fx), anteroposterior (Fy), and vertical (Fz). Force signals were sampled at 1 kHz, stored on a computer via an analog-to-digital converter, and low-pass filtered at a cut-off frequency of 8 Hz (Power Lab; AD Instruments, Sydney, Australia). The magnitude of the GRF components was evaluated for each stride cycle. The timing of foot contact and toe-off for each stride cycle was determined based on the vertical Fz component of the GRF for both fast and slow sides using custom-written software (VEE Pro 9.3, Agilent Technologies, Santa Clara, CA, USA). To address the degree of adaptation and transfer of motor patterns across walking with force fields in opposite directions, the degree of asymmetry in the anteroposterior (Fy) component of the GRF was calculated for each stride cycle of walking. As depicted in Fig. 2A, this GRF component includes anterior (breaking) and posterior propulsive components that appear at different phases during the gait cycle. For each component, the peak amplitude during each gait cycle was calculated as the absolute value for both the fast and slow sides (upper panels of Figs. 3 and 4). The degree of asymmetry, which represents the difference in the absolute values, was then calculated by subtracting the value of the slow limb from that of the fast limb on a stride-by-stride basis (lower panels of Figs. 3 and 4). As exposure to different force fields (aiding and impeding) influences the magnitude of the GRF components during walking, it does not allow for direct comparisons of the degree of asymmetry between walking with different force fields. In addition, to consider the influence of the natural walking movement of the subjects, which is not perfectly symmetrical and allows for comparisons between different force fields, the obtained degree of asymmetry underwent a normalization process. For both the anterior and posterior components of the GRF, the degree of asymmetry obtained in the washout periods (1, 2, and 3) was subtracted from the mean values of those under the respective baseline with different force fields. The normalized values were then divided into bins of 5 s and averaged for each bin. Statistics Two-way analysis of variance (ANOVA) with repeated measures was used to test for statistically significant differences in the degree of asymmetry (in terms of both acquisition and transfer of adaptation) between walking with different force fields (either aiding or impeding) and different time periods of the experiment ( initial or final phase of washout periods). When ANOVA revealed significant results, Bonferroni’s post hoc comparisons were performed to identify significant differences between variables. In addition, to test whether the adaptation acquired during walking with one force field was washed out (or maintained) by walking with the other force field, a paired Student’s t-test was performed to compare the degree of asymmetry between the final phase of washout 2 and the initial phase of washout 3 periods. A paired Student’s t-test was used to compare the magnitude of the GRF components and the cadence between walking under the two force conditions. Data are presented as mean ± standard error of the mean (SEM) values. Statistical significance was set at P &lt; 0.05.'
        # When
        methods = get_methods(doi)
        # Then
        self.assertEqual(methods, expected_methods)

    def test_get_grants(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[64]
        expected_grants = [{'name': 'NASA'}]
        # When
        grants = get_grants(doi)
        # Then
        self.assertEqual(grants, expected_grants)

    def test_get_doi_supplement_to(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[91]
        expected_doi_supplement_to = ['10.1364/ao.459256']
        # When
        doi_supplement_to = get_doi_supplement_to(doi)
        # Then
        self.assertEqual(doi_supplement_to, expected_doi_supplement_to)

    def test_get_doi_version_of(self):
        # Given
        input_file_path = fixture_path / "sample.ndjson"
        doi = next(json_line_generator(input_file_path)).get('data')[26]
        expected_doi_version_of = ['10.5281/zenodo.6510179']
        # When
        doi_version_of = get_doi_version_of(doi)
        # Then
        self.assertEqual(doi_version_of, expected_doi_version_of)

    def test_get_title(self):
        # Given
        expected_title = "Occurrence Download"
        # When
        title = get_title(TestProcessor.standard_doi)
        # Then
        self.assertEqual(title, expected_title)

    def test_get_registered(self):
        # Given
        expected_registered = "2022-06-03T21:43:46Z"
        # When
        registered = get_registered(TestProcessor.standard_doi)
        # Then
        self.assertEqual(registered, expected_registered)

    def test_get_created(self):
        # Given
        expected_created = "2022-06-03T21:43:46Z"
        # When
        created = get_created(TestProcessor.standard_doi)
        # Then
        self.assertEqual(created, expected_created)

    def test_get_license(self):
        # TODO Find an example with license
        # Given
        expected_license = "cc-by-nc-4.0"
        # When
        license = get_license(TestProcessor.standard_doi)
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

    def test_get_resourceTypeGeneral(self):
        # Given
        expected_resourceTypeGeneral = "dataset"
        # When
        resourceTypeGeneral = get_resourceTypeGeneral(TestProcessor.standard_doi)
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
        publicationYear = get_publicationYear(TestProcessor.standard_doi)
        # Then
        self.assertEqual(publicationYear, expected_publicationYear)

    def test_get_updated(self):
        # Given
        expected_updated = "2022-06-03T21:43:46Z"
        # When
        updated = get_updated(TestProcessor.standard_doi)
        # Then
        self.assertEqual(updated, expected_updated)

    def test_get_publisher(self):
        # Given
        expected_publisher = "The Global Biodiversity Information Facility"
        # When
        publisher = get_publisher(TestProcessor.standard_doi)
        # Then
        self.assertEqual(publisher, expected_publisher)

    def test_get_client_id(self):
        # Given
        expected_client_id = "gbif.gbif"
        # When
        client_id = get_client_id(TestProcessor.standard_doi)
        # Then
        self.assertEqual(client_id, expected_client_id)

