from abc import ABCMeta, abstractmethod
from typing import List


class AbstractAffiliationMatcher(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, base_url: str):
        raise NotImplementedError

    @abstractmethod
    def get_affiliation(self, match_type: str, affiliation_string: str):
        raise NotImplementedError

    @abstractmethod
    def get_affiliations_list(self, match_types: List[str], affiliations_list: List[str]):
        raise NotImplementedError
