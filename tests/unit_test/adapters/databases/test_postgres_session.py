from unittest import TestCase
from unittest.mock import patch

from adapters.databases.postgres_session import PostgresSession

from contextlib import _GeneratorContextManager

TESTED_MODULE = 'adapters.databases.postgres_session'


class TestPostgresSession(TestCase):
    @classmethod
    @patch(f"{TESTED_MODULE}.create_engine")
    @patch(f"{TESTED_MODULE}.Session")
    def setUpClass(self, mock_create_engine, mock_session):
        # Given
        self.host: str = "fake_host"
        self.port: int = 0
        self.username: str = 'fake_username'
        self.password: str = 'fake_password'
        self.database_name: str = 'fake_db_name'

        self.mock_create_engine = mock_create_engine
        self.mock_session = mock_session

        # Given / When
        self.postgres_session: PostgresSession = PostgresSession(
            self.host, self.port, self.username, self.password, self.database_name)

    def test_given_correct_args_when_init_is_called_then_create_engine_and_session_are_called_once(self):
        # Given after setUpClass

        # When after setUpClass

        # Then
        self.mock_create_engine.assert_called_once()
        self.mock_session.assert_called_once()

    def test_given_a_postgres_session_when_getSession_is_called_then_the_result_is_not_none(self):
        # Given after setUpClass

        # When after setUpClass
        result_get_session = self.postgres_session.getSession()

        # Then
        assert result_get_session is not None

    def test_given_a_postgres_session_when_getEngine_is_called_then_the_result_is_not_none(self):
        # Given after setUpClass

        # When after setUpClass
        result_get_engine = self.postgres_session.getEngine()

        # Then
        assert result_get_engine is not None

    def test_given_a_postgres_session_when_sessionScope_is_called_then_the_result_is_a_generator_class(self):
        # Given after setUpClass

        # When after setUpClass
        generator = self.postgres_session.sessionScope()

        # Then
        assert isinstance(generator, _GeneratorContextManager)
