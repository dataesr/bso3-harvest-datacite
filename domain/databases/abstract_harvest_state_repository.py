from abc import ABCMeta, abstractmethod
from domain.databases.abstract_session import AbstractSession
from domain.model.harvest_state import HarvestState


class AbstractHarvestStateRepository(metaclass=ABCMeta):
    session: AbstractSession

    @abstractmethod
    def __init__(self, session: AbstractSession):
        raise NotImplementedError

    @abstractmethod
    def get(self) -> HarvestState:
        raise NotImplementedError

    @abstractmethod
    def update(self):
        raise NotImplementedError
