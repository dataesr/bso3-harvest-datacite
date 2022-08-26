from abc import ABCMeta, abstractmethod


class AbstractCache(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def add(self, key, val):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError
