from unittest import TestCase
from unittest.mock import patch
from adapters.databases.mongo_db_repository import DefaultDocument
from adapters.databases.mongo_session import MongoSession
from marshmallow import Schema
from marshmallow.fields import Str
from bson import ObjectId


class SampleSchema(Schema):
    field_name = Str(required=True)
    value = Str(required=True)
    new_field = Str()


class SampleModel(DefaultDocument):
    meta = {
        "collection": "sample_model",
        "schema": SampleSchema,
    }


TESTED_MODULE = "adapters.databases.mongo_db_repository"


class TestMongoCrud(TestCase):
    @classmethod
    @patch("pymongo.collection.Collection")
    @patch("pymongo.database.Database")
    def setUpClass(self, mock_database, mock_collection):

        self.host: str = "fake_host"
        self.username: str = "fake_username"
        self.password: str = "fake_password"
        self.authMechanism: str = "SCRAM-SHA-256"
        self.database_name: str = "fake_database"

        self.mongo_session: MongoSession = MongoSession(
            self.host,
            self.username,
            self.password,
            self.database_name,
            authMechanism=self.authMechanism,
        )

        self.mock_collection = mock_collection
        self.mock_database = mock_database

        self.sampleModel: SampleModel = SampleModel(self.mongo_session.getSession())

    def test_given_a_mongo_collection_database_and_a_sample_model_when_get_collection_is_called_then_get_a_collection_instance(
        self,
    ):
        # Given after setUpClass
        result_get_collection = type(self.sampleModel.get_collection())

        # Then
        assert result_get_collection is not None

    @patch("pymongo.collection.Collection.insert_one")
    def test_given_a_mongo_collection_database_and_a_sample_model_when_create_document_is_called_then_return_a_mongo_object(
        self, mongo_insert_one
    ):
        # Given after setUpClass

        mongo_insert_one.return_value = None

        # When
        result_get_document = self.sampleModel.create(
            **{
                "field_name": "field_name",
                "value": "value",
            }
        )
        # Then
        assert result_get_document is not None

    @patch("pymongo.collection.Collection.find_one")
    def test_given_a_mongo_collection_database_and_a_sample_model_when_get_document_is_called_then_return_a_mongo_object(
        self, mongo_find_one
    ):
        # Given after setUpClass

        mongo_find_one.return_value = None

        # When
        result_get_document = self.sampleModel.get(id="54f112defba522406c9cc208")

        # Then
        assert result_get_document is not None
