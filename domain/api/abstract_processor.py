from abc import ABCMeta, abstractmethod


class AbstractProcessor(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def process_partition(self):
        raise NotImplementedError
