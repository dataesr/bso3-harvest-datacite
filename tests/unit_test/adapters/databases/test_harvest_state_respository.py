from unittest import TestCase

from adapters.databases.harvest_state_respository import HarvestStateRepository
from adapters.databases.mock_postgres_session import MockPostgresSession

TESTED_MODULE = 'adapters.databases.harvest_state_repository'


class TestHarvestStateRepositoryGet(TestCase):
    def test_given_mock_postgres_session_and_a_harvest_state_repository_when_using_get_from_repository_then_all_methods_of_the_repository_are_called_once_except_getEngine_called_0_time(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        harvest_state_repository = HarvestStateRepository(mock_postgres_session)

        nb_calls_init_expected: int = 1
        nb_calls_getEngine_expected: int = 0
        nb_calls_getSession_expected: int = 1
        nb_calls_sessionScope_expected: int = 1

        # When
        harvest_state_repository.get()

        # Then
        assert mock_postgres_session.nb_calls_init == nb_calls_init_expected
        assert mock_postgres_session.nb_calls_getEngine == nb_calls_getEngine_expected
        assert mock_postgres_session.nb_calls_getSession == nb_calls_getSession_expected
        assert mock_postgres_session.nb_calls_sessionScope == nb_calls_sessionScope_expected


class TestHarvestStateRepositoryUpdate(TestCase):
    def test_given_mock_postgres_session_and_a_harvest_state_repository_when_using_update_from_repository_with_empty_arg_dict_then_all_methods_of_the_repository_are_called_once_except_getEngine_called_0_time(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        harvest_state_repository = HarvestStateRepository(mock_postgres_session)

        nb_calls_init_expected: int = 1
        nb_calls_getEngine_expected: int = 0
        nb_calls_getSession_expected: int = 1
        nb_calls_sessionScope_expected: int = 1

        elements_to_update: dict = {}

        # When
        harvest_state_repository.update(elements_to_update)

        # Then
        assert mock_postgres_session.nb_calls_init == nb_calls_init_expected
        assert mock_postgres_session.nb_calls_getEngine == nb_calls_getEngine_expected
        assert mock_postgres_session.nb_calls_getSession == nb_calls_getSession_expected
        assert mock_postgres_session.nb_calls_sessionScope == nb_calls_sessionScope_expected


    def test_given_mock_postgres_session_and_a_harvest_state_repository_when_using_update_from_repository_with_empty_arg_dict_then_all_methods_of_the_repository_are_called_once_except_getEngine_called_0_time(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        harvest_state_repository = HarvestStateRepository(mock_postgres_session)

        nb_calls_init_expected: int = 1
        nb_calls_getEngine_expected: int = 0
        nb_calls_getSession_expected: int = 1
        nb_calls_sessionScope_expected: int = 1

        elements_to_update: dict = {'number_missed': 0}

        # When
        harvest_state_repository.update(elements_to_update)

        # Then
        assert mock_postgres_session.nb_calls_init == nb_calls_init_expected
        assert mock_postgres_session.nb_calls_getEngine == nb_calls_getEngine_expected
        assert mock_postgres_session.nb_calls_getSession == nb_calls_getSession_expected
        assert mock_postgres_session.nb_calls_sessionScope == nb_calls_sessionScope_expected


    def test_given_mock_postgres_session_and_a_harvest_state_repository_when_using_update_from_repository_with_wrong_element_in_dict_arg_then_raise_specific_exception(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        harvest_state_repository = HarvestStateRepository(mock_postgres_session)

        nb_calls_init_expected: int = 1
        nb_calls_getEngine_expected: int = 0
        nb_calls_getSession_expected: int = 0
        nb_calls_sessionScope_expected: int = 0

        elements_to_update: dict = {'key_nonexistant': 'value'}

        exception_msg_expected: str = "Element to update not available in HarvestStateTable : 'key_nonexistant'"

        # When
        with self.assertRaises(Exception) as context:
            harvest_state_repository.update(elements_to_update)

        self.assertTrue(exception_msg_expected in str(context.exception))

        # Then
        assert mock_postgres_session.nb_calls_init == nb_calls_init_expected
        assert mock_postgres_session.nb_calls_getEngine == nb_calls_getEngine_expected
        assert mock_postgres_session.nb_calls_getSession == nb_calls_getSession_expected
        assert mock_postgres_session.nb_calls_sessionScope == nb_calls_sessionScope_expected

    def test_given_mock_postgres_session_and_a_harvest_state_repository_when_using_update_from_repository_with_wrong_type_element_in_dict_arg_then_raise_specific_exception(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        mock_postgres_session = MockPostgresSession(host, port, username, password, database_name)

        harvest_state_repository = HarvestStateRepository(mock_postgres_session)

        nb_calls_init_expected: int = 1
        nb_calls_getEngine_expected: int = 0
        nb_calls_getSession_expected: int = 0
        nb_calls_sessionScope_expected: int = 0

        elements_to_update: dict = {'date_debut': 'toto'}

        exception_msg_expected: str = "Element type to update not correct for HarvestStateTable : got '<class 'str'>' and expected '<class 'datetime.datetime'>' for 'date_debut' attribute"
        # When
        with self.assertRaises(Exception) as context:
            harvest_state_repository.update(elements_to_update)

        self.assertTrue(exception_msg_expected in str(context.exception))

        # Then
        assert mock_postgres_session.nb_calls_init == nb_calls_init_expected
        assert mock_postgres_session.nb_calls_getEngine == nb_calls_getEngine_expected
        assert mock_postgres_session.nb_calls_getSession == nb_calls_getSession_expected
        assert mock_postgres_session.nb_calls_sessionScope == nb_calls_sessionScope_expected