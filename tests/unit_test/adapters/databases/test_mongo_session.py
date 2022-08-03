from unittest import TestCase
from unittest.mock import patch

from adapters.databases.mongo_session import MongoSession
from pymongo import MongoClient

TESTED_MODULE = "adapters.databases.mongo_session"


class TestMongoSession(TestCase):
    @patch.object(MongoClient, "__init__")
    @patch.object(MongoClient, "__getitem__")
    def test_init_and_get_session(self, mock_MongoClient__getitem__, mock_MongoClient__init__):
        # Given

        # host: str = f"{getenv('DB_MONGO_HOST')}:{getenv('DB_MONGO_PORT')}"
        # username: str = getenv('DB_MONGO_USER')
        # password: str = getenv('DB_MONGO_PASSWORD')
        # authMechanism: str = getenv('DB_MONGO_AUTH_MECH')

        host: str = "fake_host:0"
        username: str = "fake_username"
        password: str = "fake_password"
        authMechanism: str = "fake_auth_mechanisme"
        database_name: str = "fake_database"

        mock_MongoClient__init__.return_value = None
        mock_MongoClient__getitem__.return_value = None

        # When
        session: MongoSession = MongoSession(host, username, password, database_name, authMechanism=authMechanism)
        get_session_type_result = type(session.getSession())

        # Then
        mock_MongoClient__init__.assert_called_once()
        mock_MongoClient__getitem__.assert_called_once()
