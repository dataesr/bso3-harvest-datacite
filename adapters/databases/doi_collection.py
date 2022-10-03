from adapters.databases.mongo_db_repository import DefaultDocument
from adapters.databases.mongo_session import MongoSession
from marshmallow import Schema
from config.global_config import get_mongo_config
from marshmallow.fields import Str, List, Nested


class MatchedAffiliationsSchema(Schema):
    countries = List(Str())
    ror = List(Str())
    grid = List(Str())
    rnsr = List(Str())


class DoiSchema(Schema):
    doi = Str(required=True)
    matched_affiliations_list = List(Nested(MatchedAffiliationsSchema))
    clientId = Str(required=True)
    publisher = Str(required=True)
    update_date = Str(required=True)


class DoiCollection(DefaultDocument):
    meta = {"collection": "dois", "schema": DoiSchema}


def get_mongo_repo():
    mongo_session: MongoSession = MongoSession(**get_mongo_config())
    return DoiCollection(mongo_session.getSession())
