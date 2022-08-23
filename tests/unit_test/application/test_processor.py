from calendar import c
from pathlib import Path
import shutil
from unittest import TestCase, skip
from unittest.mock import patch
from shutil import rmtree
from types import SimpleNamespace

from datetime import datetime

from application.processor import Processor

TESTED_MODULE = "application.processor"


class TestProcessor(TestCase):

    dump_folder = ""
    processor = None

    @classmethod
    def setUpClass(self):
        self.dump_folder = Path.cwd() / "tests/unit_test/application/"
        self.processor = Processor(self.dump_folder, "test_dois")

    def test_init_processor_return_list_of_files_and_target_directory(self):

        expected_number_of_files = 1

        # Given processor in SetUpClass

        # expect
        assert len(self.processor.list_of_files) == expected_number_of_files

    def test_init_processor_given_one_dump_file_containing_four_dois_when_call_process_files_expect_four_dois_files(
        self,
    ):

        expected_number_of_files = 5

        # when
        self.processor.process()

        # expect

        assert (
            len(list(self.processor.target_directory.glob("**/*.json"))) == expected_number_of_files
        )

    def test_init_processor_with_custom_target_repository_create_expect_new_directory(self):

        current_directory = Path.cwd()
        parent_directory = current_directory.parent

        # Given processor in SetUpClass
        expect_target_directory = Path(parent_directory, "/test_dois")

        # expect
        assert self.processor.target_directory == expect_target_directory

        shutil.rmtree(expect_target_directory)
