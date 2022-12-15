from abc import ABCMeta, abstractmethod

from domain.databases.abstract_harvest_state_repository import AbstractHarvestStateRepository
from domain.databases.abstract_session import AbstractSession


class AbstractHarvester(metaclass=ABCMeta):
    harvest_state_repository: AbstractHarvestStateRepository
    postgres_session: AbstractSession

    @abstractmethod
    def __init__(self, harvest_state_repository: AbstractHarvestStateRepository, postgres_session: AbstractSession):
        raise NotImplementedError

    @abstractmethod
    def download(self):
        raise NotImplementedError
