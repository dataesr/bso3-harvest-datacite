from pymongo import MongoClient
from domain.databases.abstract_session import AbstractSession

class MongoSession(AbstractSession):
    session: MongoClient

    def __init__(self, host: str, username: str, password: str, authMechanism: str):
        self._session = MongoClient( host=host,
                                username=username,
                                password=password,
                                authMechanism=authMechanism)
    
    def getSession(self) -> MongoClient:
        return self._session
