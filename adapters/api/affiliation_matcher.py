from typing import List

import requests
from domain.api.abstract_affiliation_matcher import AbstractAffiliationMatcher


class AffiliationMatcher(AbstractAffiliationMatcher):
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {"Content-type": "application/json"}

    def get_affiliations_list(self, match_types: List[str], affiliations_list: List[str]):
        return requests.post(
            url=f"{self.base_url}/match_list",
            headers=self.headers,
            json={"match_types": match_types, "affiliations": affiliations_list},
        )

    def get_affiliation(self, match_type: str, affiliation_string: str):
        return requests.post(
            url=f"{self.base_url}/match",
            headers=self.headers,
            json={"type": match_type, "query": affiliation_string},
        ).json()["results"]


class AffiliationMatcherDemo(AffiliationMatcher):
    def __init__(self):
        super().__init__("https://affiliation-matcher.staging.dataesr.ovh")
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
