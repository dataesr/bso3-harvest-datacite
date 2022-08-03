from pymongo import MongoClient
from domain.databases.abstract_session import AbstractSession
from pymongo.database import Database


class MongoSession(AbstractSession):
    session: MongoClient
    database: Database

    def __init__(
        self, host: str, username: str, password: str, database_name: str, authMechanism: str
    ):
        self.session = MongoClient(
            host=host, username=username, password=password, authMechanism=authMechanism
        )
        self.session = self.session[database_name]

    def getSession(self) -> MongoClient:
        return self.session
