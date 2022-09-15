from abc import ABCMeta, abstractmethod

from domain.databases.abstract_session import AbstractSession


class AbstractProcessor(metaclass=ABCMeta):

    source_folder: str

    @abstractmethod
    def __init__(self, source_folder: str):
        raise NotImplementedError

    @abstractmethod
    def process(self):
        raise NotImplementedError
