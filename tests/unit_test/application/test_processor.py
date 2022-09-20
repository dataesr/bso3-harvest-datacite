from pathlib import Path
from unittest import TestCase
import os
import glob
import pandas as pd
from tests.unit_test.application.test_global_config import test_config_harvester
from application.processor import Processor

TESTED_MODULE = "application.processor"


class TestProcessor(TestCase):
    processor = None

    @classmethod
    def setUpClass(cls):
        cls.processor = Processor(test_config_harvester)

    def test_init_processor_return_list_of_files_and_target_directory(self):
        expected_number_of_files = 1

        # Given processor in SetUpClass

        # expect
        self.assertEqual(len(self.processor.list_of_files), expected_number_of_files)

    def test_init_processor_given_one_dump_file_containing_four_dois_when_call_process_files_expect_four_dois(
            self,
    ):
        expected_number_of_dois_processed = 4

        fileList = glob.glob(str(self.processor.target_directory / '*.csv'))

        for filePath in fileList:
            os.remove(filePath)

        # expect
        global_number_of_processed_dois, processed_files_and_status = self.processor.process()

        self.assertEqual(global_number_of_processed_dois, expected_number_of_dois_processed)

    def test_init_processor_with_custom_target_repository_expect_create_new_directory(self):
        current_directory = self.processor.target_directory
        parent_directory = current_directory.parent

        # Given processor in SetUpClass
        expect_target_directory = Path(parent_directory, "test_dois")

        # expect
        self.assertEqual(self.processor.target_directory, expect_target_directory)

    def test_init_processor_given_one_dump_file_containing_dois_with_only_two_same_affiliations_in_different_creator_produce_only_one_affiliation_in_global_affiliation_file(
            self):
        current_directory = self.processor.target_directory
        parent_directory = current_directory.parent

        # Given processor in SetUpClass
        target_target_directory = Path(parent_directory, "test_dois")
        expected_number_global_affiliation = 2

        fileList = glob.glob(str(target_target_directory / '*.csv'))

        for filePath in fileList:
            os.remove(filePath)

        self.processor.process()

        global_affiliation = pd.read_csv(target_target_directory / self.processor.global_affiliation_file_path,
                                         sep=",",
                                         names=['doi_publisher', 'doi_client_id', 'affiliation'],
                                         header=None)

        # expect
        self.assertEqual(global_affiliation.shape[0], expected_number_global_affiliation)

    def test_init_processor_given_one_dump_file_containing_dois_with_only_two_same_affiliations_in_different_creator_produce_two_affiliation_in_detailed_affiliation_file(
            self):
        current_directory = self.processor.target_directory
        parent_directory = current_directory.parent

        # Given processor in SetUpClass
        target_target_directory = Path(parent_directory, "test_dois")
        expected_number_global_affiliation = 4

        fileList = glob.glob(str(target_target_directory / '*.csv'))

        for filePath in fileList:
            os.remove(filePath)

        self.processor.process()

        global_affiliation = pd.read_csv(target_target_directory / self.processor.detailed_affiliation_file_path,
                                         sep=",",
                                         names=['doi_publisher', 'doi_client_id', 'affiliation'],
                                         header=None)

        # expect
        self.assertEqual(global_affiliation.shape[0], expected_number_global_affiliation)
