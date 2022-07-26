from abc import ABCMeta, abstractmethod


class AbstractSession(metaclass=ABCMeta):
    session: any

    @abstractmethod
    def getSession(self) -> any:
        pass
