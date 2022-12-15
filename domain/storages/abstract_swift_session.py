from abc import ABCMeta, abstractmethod


class AbstractSwiftSession(metaclass=ABCMeta):
    session: any

    @abstractmethod
    def getSession(self) -> any:
        pass
