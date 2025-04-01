from functools import lru_cache
import unicodedata as ud
from typing import List

import requests
from domain.api.abstract_affiliation_matcher import AbstractAffiliationMatcher
from project.server.main.logger import get_logger

logger = get_logger(__name__)

CACHE_SIZE = 100_000

class AffiliationMatcher(AbstractAffiliationMatcher):
    french_publishers = [
            "Recherche Data Gouv",
            "Université de Lorraine",
            "NAKALA",
            "CIRAD",
            "INRAE",
            "DataSuds",
        ]
    french_alpha2 = ["fr", "gp", "gf", "mq", "re", "yt", "pm", "mf", "bl", "wf", "tf", "nc", "pf"]
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {"Content-type": "application/json"}
        self.french_publishers = list(map(self._normalizer, self.french_publishers))

    def get_version(self):
        return requests.post(
                    url=f"{self.base_url}/match",
                    headers=self.headers,
                    json={
                        "type": "country",
                        "query": "Department of Medical Genetics, Hotel Dieu de France, Beirut, Lebanon"
                    }).json()["version"]

    @lru_cache(maxsize=CACHE_SIZE)
    def get_affiliation(self, match_type: str, affiliation_string: str):
        """
        Calls affiliation matcher to determine the countries/ror/grid/rnsr mentionned in the string.
        Uses a cache to avoid repeating frequent queries.
        """
        if not isinstance(affiliation_string, str):
            return []
        try:
            return requests.post(
                    url=f"{self.base_url}/match",
                    headers=self.headers,
                    json={"type": match_type, "query": affiliation_string},
                ).json()["results"]
        except:
            logger.exception(
                f"Error during get affiliation {{'type': {match_type}, 'query': {affiliation_string}}}",
                exc_info=True)
            return []

    def is_publisher_fr(self, publisher: str) -> bool:
        """Matches for french publishers according to the business rules
        from https://github.com/Barometre-de-la-Science-Ouverte/bso3-harvest-datacite/blob/dcdump/business_rules.csv"""
        normalized_publisher = self._normalizer(publisher)
        return normalized_publisher in self.french_publishers

    def is_clientId_fr(self, clientId: str) -> bool:
        """Matches for french clientId according to the business rules
        from https://github.com/Barometre-de-la-Science-Ouverte/bso3-harvest-datacite/blob/dcdump/business_rules.csv"""
        return self._normalizer(clientId).startswith("inist.")

    def _normalizer(self, _str: str) -> str:
        """Returns a lower case, non-accentuated, ascii version of the string"""
        return ud.normalize("NFKD", _str).encode("ascii", "ignore").decode().lower()

    def is_countries_fr(self, affiliations: List) -> bool:
        """Matches affiliation detected by affiliation matcher against alpha2 code for french territories"""
        return len(set(self.french_alpha2) & set(affiliations)) != 0
