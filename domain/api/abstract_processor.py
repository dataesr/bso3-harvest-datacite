from abc import ABCMeta, abstractmethod


class AbstractProcessor(metaclass=ABCMeta):
    source_folder: str

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def process_list_of_files_in_partition(self):
        raise NotImplementedError
