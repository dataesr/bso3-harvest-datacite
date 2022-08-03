from unittest import TestCase
from unittest.mock import patch
from pymongo import MongoClient
from os import getenv
from adapters.databases.mongo_db_repository import DefaultDocument
from adapters.databases.mongo_session import MongoSession
from marshmallow import Schema
from marshmallow.fields import Str
from pymongo.database import Database
from pymongo.collection import Collection


class SampleSchema(Schema):
    field_name = Str(required=True)
    value = Str(required=True)


class SampleModel(DefaultDocument):
    meta = {
        "database": getenv("DB_MONGO_NAME"),
        "collection": "sample_model",
        "schema": SampleSchema,
    }


TESTED_MODULE = "adapters.databases.mongo_db_repository"


class TestMongoCrud(TestCase):
    @classmethod
    @patch(f"{TESTED_MODULE}.MongoClient.__init__", return_value=None)
    def setUpClass(self, mock_MongoClient_init):

        self.host: str = "fake_host"
        self.username: str = "fake_username"
        self.password: str = "fake_password"
        self.authMechanism: str = "fake_authMechanism"
        self.database_name: str = "fake_database"
        self.collection_name: str = "fake_collection_name"

        # Given / When
        self.mongo_session: MongoSession = MongoSession(
            self.host,
            self.username,
            self.password,
            self.database_name,
            authMechanism=self.authMechanism,
        )
        self.sampleModel: SampleModel = SampleModel(self.mongo_session)

        self.mock_MongoClient_init = mock_MongoClient_init

    def test_given_a_mongo_session_and_a_sample_model_when_get_database_is_called_then_get_a_database_instance(
        self,
    ):
        # Given after setUpClass

        # When after setUpClass
        get_database_result_type_expected: type = Database

        result_get_database = type(self.sampleModel.get_database())

        assert result_get_database == get_database_result_type_expected

    def test_given_a_mongo_session_and_a_sample_model_when_get_database_is_called_then_get_a_collection_instance(
        self,
    ):
        # Given after setUpClass

        # When after setUpClass
        get_collection_result_type_expected: type = Collection

        result_get_collection = type(self.sampleModel.get_collection())
        assert result_get_collection == get_collection_result_type_expected
