from abc import ABCMeta, abstractmethod
from typing import Collection, Dict
from pymongo.database import Database
from adapters.databases.mongo_session import MongoSession
from domain.databases.abstract_session import AbstractSession
from domain.model.harvest_state import HarvestState


class AbstractMongoCollectionRepository(metaclass=ABCMeta):
    session: MongoSession
    database: Database
    collection: Collection
    
    @abstractmethod
    def __init__(self, database: Database):
        raise NotImplementedError

    @abstractmethod
    def update(self):
        raise NotImplementedError

    @abstractmethod
    def get_collection(self):
        raise NotImplementedError

    @abstractmethod
    def validate_schema(self):
        raise NotImplementedError

    @abstractmethod
    def create(self):
        raise NotImplementedError

    @abstractmethod
    def get(self):
        raise NotImplementedError

    @abstractmethod
    def delete(self):
        raise NotImplementedError

    @abstractmethod
    def list(self):
        raise NotImplementedError
