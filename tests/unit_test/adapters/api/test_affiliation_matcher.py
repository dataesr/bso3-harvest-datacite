from unittest.mock import patch
from unittest import TestCase
from adapters.api.affiliation_matcher import AffiliationMatcher

TESTED_MODULE = "adapters.api.affiliation_matcher"


class TestAffiliationMatcher(TestCase):
    def setUp(self):
        self.affiliation_matcher = AffiliationMatcher("fake_url")
        self.match_type = "match_type"
        self.affiliation_string = "affiliation_string"

    def tearDown(self):
        self.affiliation_matcher.get_affiliation.cache_clear()

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
        self.assertEqual(self.affiliation_matcher.get_affiliation.cache_info().hits, 0)
        self.assertEqual(self.affiliation_matcher.get_affiliation.cache_info().currsize, 1)

    @patch(f"{TESTED_MODULE}.requests.post")
    def test_get_affiliation_cached_request(self, mock_post):
        # Given
        self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # When
        self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # Then
        mock_post.assert_called_once()
        self.assertEqual(self.affiliation_matcher.get_affiliation.cache_info().currsize, 1)
        self.assertEqual(self.affiliation_matcher.get_affiliation.cache_info().hits, 1)
