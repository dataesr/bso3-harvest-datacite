from pymongo import MongoClient
from domain.databases.abstract_session import AbstractSession
from pymongo.database import Database


class MongoSession(AbstractSession):
    session: Database

    def __init__(self, host: str, username: str, password: str, database_name: str, authMechanism: str):
        mongo_client = MongoClient(host=host, username=username, password=password, authMechanism=authMechanism)
        self.session = mongo_client[database_name]

    def getSession(self) -> Database:
        return self.session
