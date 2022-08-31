from unittest.mock import patch
from unittest import TestCase
from adapters.api.affiliation_matcher import AffiliationMatcher
from adapters.api.mock_cache import MockCache

TESTED_MODULE = "adapters.api.affiliation_matcher"


class TestAffiliationMatcher(TestCase):
    def setUp(self):
        self.cache_size = 1
        self.mock_cache = MockCache(nb_items_max=self.cache_size)
        self.affiliation_matcher = AffiliationMatcher("fake_url", self.mock_cache)
        self.match_type = "match_type"
        self.affiliation_string = "affiliation_string"

    @patch(f"{TESTED_MODULE}.requests.post")
    def test_get_affiliation_first_request(self, mock_post):
        # Given
        # When
        self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # Then
        mock_post.assert_called_with(
            url=f"{self.affiliation_matcher.base_url}/match",
            headers=self.affiliation_matcher.headers,
            json={"type": self.match_type, "query": self.affiliation_string},
        )
        assert self.mock_cache.nb_cache_retrieval == 0
        assert len(self.mock_cache.cache.values()) == 1

    @patch(f"{TESTED_MODULE}.requests.post")
    def test_get_affiliation_cached_request(self, mock_post):
        # Given
        self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # When
        self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # Then
        mock_post.assert_called_once()
        assert self.mock_cache.nb_cache_retrieval == 1
        assert len(self.mock_cache.cache.values()) == 1
