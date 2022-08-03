from unittest import TestCase
from unittest.mock import patch

from datetime import datetime

from application.harvester import Harvester

TESTED_MODULE = "application.harvester"


class TestHarvester(TestCase):
    @classmethod
    def setUpClass(self):
        self.target_directory: str = "fake_directory"
        self.start_date: datetime = datetime.now()
        self.end_date: datetime = datetime.now()
        self.interval: str = "minute"
        self.max_requests: int = 16777216
        self.file_prefix: str = "dcdump-"
        self.workers: int = 4
        self.sleep_duration: int = "3m0s"

        self.harvester: Harvester = Harvester(None)

    def test_given_different_inputs_when_using_a_while_with_selectInterval_then_should_get_results_expected(self):
        # Given after setUpClass
        inputs: list = ["minute", "day", "hour", "week", "anything"]

        results_expected: list = ["e", "d", "h", "w", "e"]

        # When
        results: list = []

        for input in inputs:
            results.append(self.harvester.selectInterval(input))

        # Then
        assert results == results_expected

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_when_using_executeDcdump_and_returncode_of_run_returns_0_then_run_is_called_once(self, mock_run):
        # Given after setUpClass
        mock_run.return_value.returncode = 0

        # When
        self.harvester.executeDcdump(self.target_directory, self.start_date, self.end_date, self.interval, self.max_requests, self.file_prefix, self.workers, self.sleep_duration)

        # Then
        mock_run.assert_called_once()

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_when_using_executeDcdump_and_returncode_of_run_returns_2_then_run_is_called_once_and_raise_error_exception(self, mock_run):
        # Given after setUpClass
        exception_msg_expected: str = "error"

        mock_run.return_value.returncode = 2
        mock_run.return_value.stdout = exception_msg_expected

        # When
        with self.assertRaises(Exception) as context:
            self.harvester.executeDcdump(self.target_directory, self.start_date, self.end_date, self.interval, self.max_requests, self.file_prefix, self.workers, self.sleep_duration)

        # Then
        mock_run.assert_called_once()
        self.assertTrue(exception_msg_expected in str(context.exception))
