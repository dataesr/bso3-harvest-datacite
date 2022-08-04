from unittest import TestCase
from unittest.mock import patch

from types import SimpleNamespace

from datetime import datetime

from adapters.databases.mock_harvest_state_repository import MockHarvestStateRepository

from application.harvester import Harvester

TESTED_MODULE = "application.harvester"


class TestHarvesterExecution(TestCase):
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

    def setUp(self):
        self.mock_harvest_state_repository: MockHarvestStateRepository = MockHarvestStateRepository(None)

        self.harvester: Harvester = Harvester(self.mock_harvest_state_repository)

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
    @patch(f"{TESTED_MODULE}.Path")
    def test_given_inputs_and_returncode_of_run_returns_0_when_using_executeDcdump_then_run_and_Path_are_called_once(self, mock_path, mock_run):
        # Given after setUpClass
        mock_run.return_value.returncode = 0

        # When
        self.harvester.executeDcdump(self.target_directory, self.start_date, self.end_date, self.interval, self.max_requests, self.file_prefix, self.workers, self.sleep_duration)

        # Then
        mock_path.assert_called_once()
        mock_run.assert_called_once()

    @patch(f"{TESTED_MODULE}.run")
    @patch(f"{TESTED_MODULE}.Path")
    def test_given_inputs_and_returncode_of_run_returns_2_when_using_executeDcdump_then_run_and_Path_are_called_once_and_raise_error_exception(self, mock_path, mock_run):
        # Given after setUpClass
        exception_msg_expected: str = "error"

        mock_run.return_value.returncode = 2
        mock_run.return_value.stdout = exception_msg_expected

        # When
        with self.assertRaises(Exception) as context:
            self.harvester.executeDcdump(self.target_directory, self.start_date, self.end_date, self.interval, self.max_requests, self.file_prefix, self.workers, self.sleep_duration)

        # Then
        mock_path.assert_called_once()
        mock_run.assert_called_once()
        self.assertTrue(exception_msg_expected in str(context.exception))

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_and_returncode_of_run_returns_0_and_stdout_with_msg_2_intervals_when_using_getNumberSlices_then_return_2_and_run_called_once(self, mock_run):
        # Given after setUpClass
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = '... level=info msg="2 intervals"'

        number_of_slices_expected: int = 2

        # When
        number_of_slices: int = self.harvester.getNumberSlices(self.start_date, self.end_date, self.interval)

        # Then
        mock_run.assert_called_once()
        assert number_of_slices == number_of_slices_expected

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_and_returncode_of_run_returns_2_when_using_getNumberSlices_then_run_called_once_and_raise_exception(self, mock_run):
        # Given after setUpClass
        exception_msg_expected: str = "error"

        mock_run.return_value.returncode = 2
        mock_run.return_value.stdout = exception_msg_expected

        # When
        with self.assertRaises(Exception) as context:
            self.harvester.getNumberSlices(self.start_date, self.end_date, self.interval)

        # Then
        mock_run.assert_called_once()
        self.assertTrue(exception_msg_expected in str(context.exception))

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_and_returncode_of_run_returns_2_and_stdout_returns_50_when_using_getNumberDownloaded_then_run_is_called_4_times_and_should_get_50(self, mock_run):
        # Given after setUpClass
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "50 "

        number_downloaded_expected: int = 50
        number_mock_run_calls_expected: int = 4

        # When
        number_downloaded: int = self.harvester.getNumberDownloaded(self.target_directory, self.file_prefix, self.start_date, self.end_date)

        # Then
        assert mock_run.call_count == number_mock_run_calls_expected
        assert number_downloaded == number_downloaded_expected

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_and_returncode_of_run_returns_2_and_stdout_returns_error_when_using_getNumberDownloaded_then_run_is_called_once_and_should_raise_exception_error(self, mock_run):
        # Given after setUpClass
        exception_msg_expected: str = "error"

        mock_run.return_value.returncode = 2
        mock_run.return_value.stdout = exception_msg_expected

        # When
        with self.assertRaises(Exception) as context:
            self.harvester.getNumberDownloaded(self.target_directory, self.file_prefix, self.start_date, self.end_date)

        # Then
        mock_run.assert_called_once()
        self.assertTrue(exception_msg_expected in str(context.exception))

    @patch(f"{TESTED_MODULE}.run")
    def test_given_inputs_and_returncode_of_run_returns_2_for_the_2nd_call_and_stdout_returns_error_when_using_getNumberDownloaded_then_run_is_called_2_times_and_should_raise_exception_error(
        self, mock_run
    ):
        # Given after setUpClass
        exception_msg_expected: str = "error"

        mock_run.side_effect = [
            SimpleNamespace(returncode=0, stdout="not important"),
            SimpleNamespace(returncode=2, stdout=exception_msg_expected),
        ]

        number_mock_run_calls_expected: int = 2

        # When
        with self.assertRaises(Exception) as context:
            self.harvester.getNumberDownloaded(self.target_directory, self.file_prefix, self.start_date, self.end_date)

        # Then
        assert mock_run.call_count == number_mock_run_calls_expected
        self.assertTrue(exception_msg_expected in str(context.exception))

    @patch(f"{TESTED_MODULE}.Harvester.selectInterval")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberSlices")
    @patch(f"{TESTED_MODULE}.Harvester.executeDcdump")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberDownloaded")
    def test_given_inputs_and_all_working_correctly_when_using_download_then_all_function_are_called_once_and_update_get_some_expected_args_and_download_should_return_true(
        self, mock_getNumberDownloaded, mock_executeDcdump, mock_getNumberSlices, mock_selectInterval
    ):
        # Given after setUpClass and setUp
        mock_selectInterval.return_value = "e"
        mock_getNumberSlices.return_value = 50
        mock_getNumberDownloaded.return_value = 50

        nb_calls_create_expected: int = 1
        nb_calls_update_expected: int = 1
        update_args_call_expected: list = [({"number_missed": 0, "status": "done", "number_slices": 50}, {"id": 1})]

        # When
        downloading: bool = self.harvester.download(self.target_directory, self.start_date, self.end_date, self.interval)

        # Then
        mock_selectInterval.assert_called_once()
        mock_getNumberSlices.assert_called_once()
        mock_executeDcdump.assert_called_once()
        mock_getNumberDownloaded.assert_called_once()
        assert self.mock_harvest_state_repository.nb_calls_create == nb_calls_create_expected
        assert self.mock_harvest_state_repository.nb_calls_update == nb_calls_update_expected
        assert self.mock_harvest_state_repository.update_args_calls == update_args_call_expected
        assert downloading

    @patch(f"{TESTED_MODULE}.Harvester.selectInterval")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberSlices")
    @patch(f"{TESTED_MODULE}.Harvester.executeDcdump")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberDownloaded")
    def test_given_inputs_and_numberSlices_return_50_and_numberDownloaded_return_25_when_using_download_then_all_function_are_called_once_and_update_get_some_expected_args_with_status_error_and_download_should_return_true(
        self, mock_getNumberDownloaded, mock_executeDcdump, mock_getNumberSlices, mock_selectInterval
    ):
        # Given after setUpClass and setUp
        mock_selectInterval.return_value = "e"
        mock_getNumberSlices.return_value = 50
        mock_getNumberDownloaded.return_value = 25

        nb_calls_create_expected: int = 1
        nb_calls_update_expected: int = 1
        update_args_call_expected: list = [({"number_missed": 25, "status": "error", "number_slices": 50}, {"id": 1})]

        # When
        downloading: bool = self.harvester.download(self.target_directory, self.start_date, self.end_date, self.interval)

        # Then
        mock_selectInterval.assert_called_once()
        mock_getNumberSlices.assert_called_once()
        mock_executeDcdump.assert_called_once()
        mock_getNumberDownloaded.assert_called_once()
        assert self.mock_harvest_state_repository.nb_calls_create == nb_calls_create_expected
        assert self.mock_harvest_state_repository.nb_calls_update == nb_calls_update_expected
        assert self.mock_harvest_state_repository.update_args_calls == update_args_call_expected
        assert downloading

    @patch(f"{TESTED_MODULE}.Harvester.selectInterval")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberSlices")
    @patch(f"{TESTED_MODULE}.Harvester.executeDcdump")
    @patch(f"{TESTED_MODULE}.Harvester.getNumberDownloaded")
    def test_given_inputs_when_using_download_then_all_function_are_not_called_except_create_once_and_download_should_return_false(
        self, mock_getNumberDownloaded, mock_executeDcdump, mock_getNumberSlices, mock_selectInterval
    ):
        # Given after setUpClass and setUp
        self.mock_harvest_state_repository.create_same_job_already_exists = True

        nb_calls_mock_selectInterval_expected: int = 0
        nb_calls_mock_getNumberSlices_expected: int = 0
        nb_calls_mock_executeDcdump_expected: int = 0
        nb_calls_mock_getNumberDownloaded_expected: int = 0
        nb_calls_create_expected: int = 1
        nb_calls_update_expected: int = 0
        update_args_call_expected: list = []

        # When
        downloading: bool = self.harvester.download(self.target_directory, self.start_date, self.end_date, self.interval)

        # Then
        assert mock_selectInterval.call_count == nb_calls_mock_selectInterval_expected
        assert mock_getNumberSlices.call_count == nb_calls_mock_getNumberSlices_expected
        assert mock_executeDcdump.call_count == nb_calls_mock_executeDcdump_expected
        assert mock_getNumberDownloaded.call_count == nb_calls_mock_getNumberDownloaded_expected
        assert self.mock_harvest_state_repository.nb_calls_create == nb_calls_create_expected
        assert self.mock_harvest_state_repository.nb_calls_update == nb_calls_update_expected
        assert self.mock_harvest_state_repository.update_args_calls == update_args_call_expected
        assert not downloading
