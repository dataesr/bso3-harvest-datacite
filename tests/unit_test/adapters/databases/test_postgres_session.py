from unittest import TestCase
from unittest.mock import MagicMock, patch

from adapters.databases.postgres_session import PostgresSession

TESTED_MODULE = 'adapters.databases.postgres_session'

class TestMongoSession(TestCase):
    @patch(f"{TESTED_MODULE}.create_engine")
    def test_init_and_get_session(self, mock_create_engine):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        #When
        session: PostgresSession = PostgresSession(host, port, username, password, database_name)
        get_session_result = type(session.getSession())

        #Then
        mock_create_engine.assert_called_once()
        assert get_session_result != None 

