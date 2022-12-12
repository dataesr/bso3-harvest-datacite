from pathlib import Path
from unittest import TestCase
from unittest.mock import patch, Mock
from application.utils_processor import _list_files_in_directory, _create_file
from tests.unit_test.application.test_global_config import test_config_harvester
from application.processor import PartitionsController

TESTED_MODULE = "application.processor"

class TestPartitionsController(TestCase):

    @classmethod
    def setUp(self):
        self.file_prefix = "20220101"

        for i in range(5):
            partition_consolidated_affiliations_file_name = f"partition_consolidated_affiliations_{i}.csv"
            partition_detailed_affiliations_file_name = f"partition_detailed_affiliations_{i}.csv"

            _create_file(test_config_harvester['processed_dump_folder_name'],
                         partition_consolidated_affiliations_file_name)
            _create_file(test_config_harvester['processed_dump_folder_name'], partition_detailed_affiliations_file_name)

            files = [
                Path(Path(test_config_harvester[
                              'processed_dump_folder_name']) / partition_consolidated_affiliations_file_name),
                Path(Path(
                    test_config_harvester['processed_dump_folder_name']) / partition_detailed_affiliations_file_name)
            ]

            for affiliation_file in files:
                with open(affiliation_file, "w") as file:
                    file.write("zenodo,cern.zenodo,@actions,[],False,False,False,10.5281/zenodo.6612534,"
                               "10.5281_zenodo.6612534,creators,Actions-User,[],[],[]")

        self.processor_controller = PartitionsController(test_config_harvester, "20220101")

    def test_init_processor_controller_with_five_partitions_expect_five_files_in_folder(self):
        # expect
        self.assertEqual(5,
                         len(_list_files_in_directory(test_config_harvester['processed_dump_folder_name'],
                                                      "partition_consolidated_affiliations_*.csv")))
        self.assertEqual(5,
                         len(_list_files_in_directory(test_config_harvester['processed_dump_folder_name'],
                                                      "partition_detailed_affiliations_*.csv")))

    def test_init_processor_controller_expect_detailed_file_and_consolidated_file_created(self):
        # Given five files created by processor
        # expect
        self.assertEqual(Path(Path(test_config_harvester[
                                       'processed_dump_folder_name']) / f"{self.file_prefix}_{test_config_harvester['detailed_affiliation_file_name']}")
                         , self.processor_controller.global_detailed_affiliation_file_path)

        self.assertEqual(Path(Path(test_config_harvester[
                                       'processed_dump_folder_name']) / f"{self.file_prefix}_{test_config_harvester['global_affiliation_file_name']}")
                         , self.processor_controller.global_consolidated_affiliation_file_path)