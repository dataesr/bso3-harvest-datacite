from unittest import TestCase
from adapters.api.cache import Cache

TESTED_MODULE = "adapters.api.cache"


class TestCache(TestCase):
    def setUp(self):
        self.cache_size = 1
        self.cache = Cache(nb_items_max=1)

    def test_add_item(self):
        # Given
        # When
        self.cache.add("key", "value")
        # Then
        assert len(self.cache.cache.values()) == 1

    def test_get_item(self):
        # Given
        expected_value = "value"
        self.cache.add("key", "value")
        # When
        returned_value = self.cache.get("key")
        # Then
        assert returned_value == expected_value

    def test_cache_size_exceeded(self):
        # Given
        self.cache.add("key_0", "value_0")
        expected_value = "value_1"
        self.cache.add("key_1", "value_1")
        # When
        returned_value_0 = self.cache.get("key_0")
        returned_value_1 = self.cache.get("key_1")
        # Then
        self.assertIsNone(returned_value_0)
        assert returned_value_1 == expected_value
