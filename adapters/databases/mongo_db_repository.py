from email.policy import strict
from locale import Error
from bson import ObjectId
import marshmallow
from numpy import False_
from adapters.databases.mongo_session import MongoSession
from pymongo.database import Database
from pymongo.collection import Collection
from domain.databases.abstract_collection_repository import AbstractMongoCollectionRepository


class DefaultDocument(AbstractMongoCollectionRepository):

    meta = {}
    session: MongoSession
    database: Database = None
    collection: Collection = None

    def __init__(self, session: MongoSession) -> None:
        self.session = session.getSession()
        self.database = self.get_database()
        self.collection = self.get_collection()

    def get_collection(self) -> Collection:
        collection_name = self.meta.get("collection", None)
        if collection_name is None:
            raise Error("No collection name provided")
        return self.database[collection_name]

    def get_database(self) -> Database:
        database_name = self.meta.get("database", None)
        print(f"database {database_name}")

        if database_name is None:
            raise Error("No Database name provided")
        return self.session[database_name]

    def validate_schema(self, params):
        try:
            schema = self.meta.get("schema")
            return schema(strict=False).load(params).data
        except marshmallow.exceptions.ValidationError as error:
            raise Exception(error)

    def create(self, **kwargs):

        result = self.collection.insert_one(kwargs)
        return self.get(_id=result.inserted_id)

    def get(self, **kwargs):
        if "id" in kwargs:
            kwargs["_id"] = (
                ObjectId(kwargs.pop("id")) if type(kwargs["id"]) is str else kwargs.pop("id")
            )
        return self.collection.find_one(kwargs)

    def delete(self, id):
        return self.collection.delete_one({"_id": id})

    def update(self, id, **kwargs):
        doc = self.get(id=id)

        for key, value in kwargs.items():
            path_add(doc, key, value, create_path=True)

        updated_doc = self.validate_schema(doc)

        result = self.collection.update_one({"_id": ObjectId(id)}, {"$set": updated_doc})
        return self.get(id=id) if result.acknowledged else None

    def list(self):
        return list(self.collection.find({}))
