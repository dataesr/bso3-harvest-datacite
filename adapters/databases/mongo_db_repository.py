# Inspired from https://github.com/greendad/pymongo-example-repo/blob/master/src/base_document.py
from locale import Error
from bson import ObjectId
import marshmallow
from pymongo.database import Database
from pymongo.collection import Collection
from domain.databases.abstract_collection_repository import AbstractMongoCollectionRepository


class DefaultDocument(AbstractMongoCollectionRepository):

    meta = {}
    database: Database
    collection: Collection = None

    def __init__(self, database: Database) -> None:
        self.database = database
        self.collection = self.get_collection()

    def get_collection(self) -> Collection:
        collection_name = self.meta.get("collection", None)
        if collection_name is None:
            raise Error("No collection name provided")
        return self.database[collection_name]

    def validate_schema(self, params):
        try:
            schema = self.meta.get("schema")
            return schema().load(params)
        except marshmallow.exceptions.ValidationError as error:
            raise Exception(error)

    def create(self, **kwargs):
        doc = self.validate_schema(kwargs)
        result = self.collection.insert_one(doc)
        return self.get(_id=result.inserted_id)

    def get(self, **kwargs):
        if "id" in kwargs:
            kwargs["_id"] = (
                ObjectId(kwargs.pop("id")) if type(kwargs["id"]) is str else kwargs.pop("id")
            )
        return self.collection.find_one(kwargs)

    def delete(self, id):
        return self.collection.delete_one({"_id": id})

    def update(self, id, **update_dictionary):
        doc = self.get(id=id)
        doc.update(update_dictionary)
        updated_document = self.validate_schema(doc)

        result = self.collection.update_one({"_id": ObjectId(id)}, {"$set": updated_document})
        return self.get(id=id) if result.acknowledged else None

    def list(self):
        return list(self.collection.find({}))
