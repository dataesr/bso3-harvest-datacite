from unittest import TestCase
from unittest.mock import patch

from adapters.databases.mongo_session import MongoSession
from pymongo import MongoClient

TESTED_MODULE = 'adapters.databases.mongo_session'

class TestMongoSession(TestCase):
    @patch(f"{TESTED_MODULE}.MongoClient.__init__")
    def test_init_and_get_session(self, mock_MongoClient_init):
        # Given
        
        #host: str = f"{getenv('DB_MONGO_HOST')}:{getenv('DB_MONGO_PORT')}"
        #username: str = getenv('DB_MONGO_USER')
        #password: str = getenv('DB_MONGO_PASSWORD')
        #authMechanism: str = getenv('DB_MONGO_AUTH_MECH')

        host: str = "fake_host:0"
        username: str = 'fake_username'
        password: str = 'fake_password'
        authMechanism: str = 'fake_auth_mechanisme'

        mock_MongoClient_init.return_value = None

        get_session_result_type_expected: type = MongoClient

        #When
        session: MongoSession = MongoSession(host, username, password, authMechanism=authMechanism)
        get_session_type_result = type(session.getSession())

        #Then
        mock_MongoClient_init.assert_called_once()
        assert get_session_type_result == get_session_result_type_expected

