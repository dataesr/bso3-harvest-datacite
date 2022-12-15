from abc import ABCMeta, abstractmethod
from domain.databases.abstract_session import AbstractSession
from domain.model.process_state import ProcessState
from typing import List


class AbstractProcessStateRepository(metaclass=ABCMeta):
    session: AbstractSession

    @abstractmethod
    def __init__(self, session: AbstractSession):
        raise NotImplementedError

    @abstractmethod
    def create(self):
        raise NotImplementedError

    @abstractmethod
    def get(self) -> List[ProcessState]:
        raise NotImplementedError

    @abstractmethod
    def update(self):
        raise NotImplementedError
