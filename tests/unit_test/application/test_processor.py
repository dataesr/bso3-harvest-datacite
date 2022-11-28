from pathlib import Path
from unittest import TestCase
from unittest.mock import patch, Mock
import os
import glob
import pandas as pd

from adapters.databases.process_state_repository import ProcessStateRepository
from application.utils_processor import _list_files_in_directory
from tests.unit_test.application.test_global_config import test_config_harvester
from application.processor import Processor
from adapters.databases.mock_postgres_session import MockPostgresSession

TESTED_MODULE = "application.processor"


class TestProcessor(TestCase):
    process_state_repository = None
    mock_postgres_session = None
    processor = None

    @classmethod
    @patch(f'adapters.databases.process_state_table.ProcessStateTable.__table__.exists', Mock(return_value=True))
    def setUp(self):
        host: str = "fake_host"
        port: int = 0
        username: str = "fake_username"
        password: str = "fake_password"
        database_name: str = "fake_db_name"

        self.mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        self.process_state_repository = ProcessStateRepository(self.mock_postgres_session)
        
        self.processor = Processor(test_config_harvester, 0,
                                    _list_files_in_directory(test_config_harvester['raw_dump_folder_name'], "*" + test_config_harvester['datacite_file_extension']),
                                  self.process_state_repository)

    def tearDown(self):
        for f in glob.glob(f"{test_config_harvester['processed_dump_folder_name']}/*"):
            os.remove(f)

    def test_init_processor_return_list_of_files_and_target_directory(self):
        expected_number_of_files = 1

        expected_number_of_files = len(_list_files_in_directory(test_config_harvester['raw_dump_folder_name'], "*" + test_config_harvester['datacite_file_extension']))
        # Given processor in SetUpClass

        # expect
        self.assertEqual(len(self.processor.list_of_files_in_partition), expected_number_of_files)

    def test_init_processor_given_one_dump_file_containing_four_dois_when_call_process_files_expect_four_dois(
            self,
    ):
        expected_number_of_dois_processed = 4

        # expect
        global_number_of_processed_dois, processed_files_and_status = self.processor.process_list_of_files_in_partition()

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
        # Given processor in SetUpClass
        expected_number_global_affiliation = 2

        self.processor.process_list_of_files_in_partition()

        global_affiliation = pd.read_csv(self.processor.partition_consolidated_affiliation_file_path,
                                         sep=",",
                                         names=['doi_publisher', 'doi_client_id', 'affiliation'],
                                         header=None)

        # expect
        self.assertEqual(global_affiliation.shape[0], expected_number_global_affiliation)

    def test_init_processor_given_one_dump_file_containing_dois_with_only_two_same_affiliations_in_different_creator_produce_two_affiliation_in_detailed_affiliation_file(
            self):
        # Given processor in SetUpClass
        expected_number_detailed_affiliation = 4

        self.processor.process_list_of_files_in_partition()
        detailed_affiliation = pd.read_csv(self.processor.partition_detailed_affiliation_file_path,
                                         sep=",",
                                         names=['doi_publisher', 'doi_client_id', 'affiliation'],
                                         header=None)

        # expect
        self.assertEqual(detailed_affiliation.shape[0], expected_number_detailed_affiliation)
