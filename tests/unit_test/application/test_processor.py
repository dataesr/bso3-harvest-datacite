from pathlib import Path
from unittest import TestCase

from application.processor import Processor

TESTED_MODULE = "application.processor"


class TestProcessor(TestCase):
    dump_folder = ""
    processor = None

    @classmethod
    def setUpClass(cls):
        cls.dump_folder = Path.cwd() / "tests/unit_test/application"
        cls.processor = Processor(str(cls.dump_folder.absolute()), "test_dois")

    def test_init_processor_return_list_of_files_and_target_directory(self):
        expected_number_of_files = 1

        # Given processor in SetUpClass

        # expect
        self.assertEqual(len(self.processor.list_of_files), expected_number_of_files)

    def test_init_processor_given_one_dump_file_containing_five_dois_when_call_process_files_expect_five_dois_files(
            self,
    ):
        expected_number_of_files = 5

        # when
        self.processor.process()

        # expect
        self.assertEqual(len(list(self.processor.target_directory.glob("*.json"))), expected_number_of_files)

    def test_init_processor_with_custom_target_repository_expect_create_new_directory(self):
        current_directory = self.processor.target_directory
        parent_directory = current_directory.parent

        # Given processor in SetUpClass
        expect_target_directory = Path(parent_directory, "test_dois")

        # expect
        self.assertEqual(self.processor.target_directory, expect_target_directory)
