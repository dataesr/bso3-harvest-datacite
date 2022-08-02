from datetime import datetime
from unittest import TestCase, result

from adapters.databases.postgres_session import PostgresSession
from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.harvest_state_table import HarvestStateTable

from os import getenv


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

    def setUp(self):
        # create tables
        HarvestStateTable.createTable(self.postgres_session.getEngine())

    def tearDown(self):
        # drop tables
        HarvestStateTable.dropTable(self.postgres_session.getEngine())

    def test_given_repository_and_empty_database_when_using_create_and_get_without_filter_then_should_have_1_result(self):
        # Given (in setups)
        number_missed: int = 0
        start_date: datetime = datetime.now()
        end_date: datetime = datetime.now()
        status: str = "done"
        current_directory: str = "/tmp"

        harvest_state: HarvestStateTable = HarvestStateTable(number_missed, start_date, end_date, status, current_directory)

        harvest_state_expected: HarvestStateTable = harvest_state
        number_of_rows_returned_expected: int = 1
        is_inserted_expected: bool = True

        # When
        is_inserted: bool = self.harvest_state_repository.create(harvest_state)
        result_request: list = self.harvest_state_repository.get()

        # Then
        assert is_inserted == is_inserted_expected
        assert len(result_request) == number_of_rows_returned_expected
        assert result_request[0].__eq__(harvest_state_expected)

    def test_given_repository_and_empty_database_when_using_create_2_objects_and_update_missed_with_filter_id_get_without_filter_then_should_have_2_results_updated(self):
        # Given (in setups)
        number_missed: int = 0
        start_date: datetime = datetime.now()
        end_date: datetime = datetime.now()
        status: str = "done"
        current_directory1: str = "/tmp"
        current_directory2: str = "/tmp2"

        harvest_state_1: HarvestStateTable = HarvestStateTable(number_missed, start_date, end_date, status, current_directory1)
        harvest_state_2: HarvestStateTable = HarvestStateTable(number_missed, start_date, end_date, status, current_directory2)

        number_of_rows_returned_expected: int = 2
        is_inserted_expected_1: bool = True
        is_inserted_expected_2: bool = True

        # When
        is_inserted_1: bool = self.harvest_state_repository.create(harvest_state_1)
        is_inserted_2: bool = self.harvest_state_repository.create(harvest_state_2)

        self.harvest_state_repository.update(values_args={"number_missed": 1}, where_args={"id": harvest_state_2.id})

        result_request: list = self.harvest_state_repository.get()

        # Then
        assert is_inserted_1 == is_inserted_expected_1
        assert is_inserted_2 == is_inserted_expected_2
        assert len(result_request) == number_of_rows_returned_expected
        assert result_request[0].__eq__(harvest_state_1)
        assert result_request[1].id == harvest_state_2.id and result_request[1].number_missed == 1
