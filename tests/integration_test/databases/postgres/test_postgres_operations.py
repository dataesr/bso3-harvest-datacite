from datetime import datetime
from unittest import TestCase

from adapters.databases.postgres_session import PostgresSession
from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.harvest_state_table import HarvestStateTable

from os import getenv

from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.databases.process_state_table import ProcessStateTable


class TestPostgresOperations(TestCase):
    @classmethod
    def setUpClass(self):
        # Given
        host: str = getenv("DB_POSTGRES_HOST")
        port: int = getenv("DB_POSTGRES_PORT")
        username: str = getenv("DB_POSTGRES_USER")
        password: str = getenv("DB_POSTGRES_PASSWORD")
        database_name: str = getenv("DB_POSTGRES_NAME")

        # create session
        self.postgres_session: PostgresSession = PostgresSession(host, port, username, password, database_name)

        # create repositories
        self.harvest_state_repository: HarvestStateRepository = HarvestStateRepository(self.postgres_session)
        self.process_state_repository: ProcessStateRepository = ProcessStateRepository(self.postgres_session)

    def setUp(self):
        # create tables
        HarvestStateTable.createTable(self.postgres_session.getEngine())
        ProcessStateTable.createTable(self.postgres_session.getEngine())

    def tearDown(self):
        # drop tables
        HarvestStateTable.dropTable(self.postgres_session.getEngine())
        ProcessStateTable.dropTable(self.postgres_session.getEngine())

    def test_given_repository_and_empty_database_when_using_create_and_get_without_filter_then_should_have_1_result(
            self):
        # Given (in setups)
        number_missed: int = None
        number_slices: int = None
        start_date: datetime = datetime.now()
        end_date: datetime = datetime.now()
        status: str = "done"
        current_directory: str = "/tmp"
        processed: bool = False

        harvest_state: HarvestStateTable = HarvestStateTable(
            number_missed=number_missed, number_slices=number_slices, start_date=start_date, end_date=end_date,
            status=status, current_directory=current_directory, processed=processed
        )

        harvest_state_expected: HarvestStateTable = harvest_state
        number_of_rows_returned_expected: int = 1
        is_inserted_expected: bool = True

        # When
        is_inserted: bool = self.harvest_state_repository.create(harvest_state)
        result_request: list = self.harvest_state_repository.get()

        # Then
        self.assertEqual(is_inserted, is_inserted_expected)
        self.assertEqual(len(result_request), number_of_rows_returned_expected)
        self.assertEqual(result_request[0], harvest_state_expected)

    def test_given_repository_and_empty_database_when_using_create_2_objects_and_update_missed_with_filter_id_get_without_filter_then_should_have_2_results_updated(
            self):
        # Given (in setups)
        number_missed: int = None
        number_slices: int = None
        start_date: datetime = datetime.now()
        end_date: datetime = datetime.now()
        status: str = "done"
        current_directory1: str = "/tmp"
        current_directory2: str = "/tmp2"
        processed: bool = False

        harvest_state_1: HarvestStateTable = HarvestStateTable(
            number_missed=number_missed, number_slices=number_slices, start_date=start_date, end_date=end_date,
            status=status, current_directory=current_directory1, processed=processed
        )
        harvest_state_2: HarvestStateTable = HarvestStateTable(
            number_missed=number_missed, number_slices=number_slices, start_date=start_date, end_date=end_date,
            status=status, current_directory=current_directory2, processed=processed
        )

        number_of_rows_returned_expected: int = 2
        is_inserted_expected_1: bool = True
        is_inserted_expected_2: bool = True

        # When
        is_inserted_1: bool = self.harvest_state_repository.create(harvest_state_1)
        is_inserted_2: bool = self.harvest_state_repository.create(harvest_state_2)

        self.harvest_state_repository.update(values_args={"number_missed": 1}, where_args={"id": harvest_state_2.id})

        result_request: list = self.harvest_state_repository.get()

        # Then
        self.assertEqual(is_inserted_1, is_inserted_expected_1)
        self.assertEqual(is_inserted_2, is_inserted_expected_2)
        self.assertEqual(len(result_request), number_of_rows_returned_expected)
        self.assertEqual(result_request[0], harvest_state_1)
        self.assertEqual(result_request[1].id, harvest_state_2.id)
        self.assertEqual(result_request[1].number_missed, 1)

    def test_given_repository_and_empty_database_when_using_create_new_processstate_and_get_without_filter_then_should_have_1_result(
            self):
        # Given (in setups)
        file_name: str = "dcdump-20200729000000-20200729235959.ndjson"
        file_path: str = "../sample-dump/dcdump-20200729000000-20200729235959.ndjson"
        number_of_dois: int = 10
        number_of_dois_with_null_attributes: int = 9
        number_of_non_null_dois: int = 1
        process_date: datetime.now()
        processed: bool

        process_state: ProcessStateTable = ProcessStateTable(
            process_date=datetime.now(), processed=True, file_name=file_name, file_path=file_path,
            number_of_dois=number_of_dois, number_of_non_null_dois=number_of_non_null_dois, number_of_dois_with_null_attributes=number_of_dois_with_null_attributes
        )

        process_state_expected: ProcessStateTable = process_state
        number_of_rows_returned_expected: int = 1
        is_inserted_expected: bool = True

        # When
        is_inserted: bool = self.process_state_repository.create(process_state)
        result_request: list = self.process_state_repository.get()

        # Then
        self.assertEqual(is_inserted, is_inserted_expected)
        self.assertEqual(len(result_request), number_of_rows_returned_expected)
        self.assertEqual(result_request[0], process_state_expected)
