from unittest.mock import MagicMock, Mock, patch
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

    def test_is_publisher_fr(self):
        # Given
        french_publisher = "Recherche Data Gouv"
        not_french_publisher = "Not Recherche Data Gouv"
        # When
        publisher_should_be_fr = self.affiliation_matcher.is_publisher_fr(french_publisher)
        publisher_should_not_be_fr = self.affiliation_matcher.is_publisher_fr(not_french_publisher)
        # Then
        self.assertEqual(publisher_should_be_fr, True)
        self.assertEqual(publisher_should_not_be_fr, False)

    def test_is_clientId_fr(self):
        # Given
        french_clientId = "inist."
        not_french_clientId = "Not inist."
        # When
        clientId_should_be_fr = self.affiliation_matcher.is_clientId_fr(french_clientId)
        clientId_should_not_be_fr = self.affiliation_matcher.is_clientId_fr(not_french_clientId)
        # Then
        self.assertEqual(clientId_should_be_fr, True)
        self.assertEqual(clientId_should_not_be_fr, False)

    def test_is_countries_fr(self):
        # Given
        french_countries = ["fr"]
        not_french_countries = ["not fr"]
        # When
        countries_should_be_fr = self.affiliation_matcher.is_countries_fr(french_countries)
        countries_should_not_be_fr = self.affiliation_matcher.is_countries_fr(not_french_countries)
        # Then
        self.assertEqual(countries_should_be_fr, True)
        self.assertEqual(countries_should_not_be_fr, False)

    @patch(f"{TESTED_MODULE}.requests.post")
    def test_get_version(self, mock_post):
        # Given
        # When
        self.affiliation_matcher.get_version()
        # Then
        mock_post.assert_called_with(
            url=f"{self.affiliation_matcher.base_url}/match",
            headers=self.affiliation_matcher.headers,
            json={
                "type": "country",
                "query": "Department of Medical Genetics, Hotel Dieu de France, Beirut, Lebanon"
            },
        )

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

    @patch(f"{TESTED_MODULE}.requests.post", MagicMock(side_effect=Exception()))
    def test_get_affiliation_exception(self):
        # Given
        # When
        affiliation = self.affiliation_matcher.get_affiliation(self.match_type, self.affiliation_string)
        # Then
        self.assertEqual(affiliation, [])
