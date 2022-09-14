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

    def get_affiliations_list(self, match_types: List[str], affiliations_list: List[str]):
        return requests.post(
            url=f"{self.base_url}/match_list",
            headers=self.headers,
            json={"match_types": match_types, "affiliations": affiliations_list},
        )

    @lru_cache(maxsize=CACHE_SIZE)
    def get_affiliation(self, match_type: str, affiliation_string: str):
        try:
            return requests.post(
                    url=f"{self.base_url}/match",
                    headers=self.headers,
                    json={"type": match_type, "query": affiliation_string},
                ).json()["results"]
        except:
            logger.exception(
                f"Error during get affiliation {{'type': match_type, 'query': affiliation_string}}",
                exc_info=True)
            return []


    def _normalizer(self, _str):
        return ud.normalize("NFKD", _str).encode("ascii", "ignore").decode().lower()

    def is_affiliation_fr(self, publisher: str, clientId: str, affiliations: List):
        # Implement business rules from https://github.com/Barometre-de-la-Science-Ouverte/bso3-harvest-datacite/blob/dcdump/business_rules.csv
        # Match for publishers
        normalized_publisher = self._normalizer(publisher)
        for french_publisher in self.french_publishers:
            if self._normalizer(french_publisher) in normalized_publisher:
                return True
        # Match for clientId
        if self._normalizer(clientId).startswith("inist."):
            return True
        # Affiliation matcher
        return len(set(self.french_alpha2) & set(affiliations)) != 0


class AffiliationMatcherDemo(AffiliationMatcher):
    def __init__(self):
        super().__init__(base_url="https://affiliation-matcher.staging.dataesr.ovh")
        self.match_type = ["country"]  # "grid" "rnsr" "ror"
        self.affiliations_list = [
            "Department of Medical Genetics, Hotel Dieu de France, Beirut, Lebanon",
            "IPAG Institut de Planétologie et d'Astrophysique de Grenoble",
        ]

    def get_affiliation_demo(self):
        print(self.affiliations_list[0])
        return self.get_affiliation(self.match_type[0], self.affiliations_list[0])

    def get_affiliations_list_demo(self):
        return self.get_affiliations_list(self.match_type, self.affiliations_list)
