from adapters.api.cache import Cache


class MockCache(Cache):
    nb_cache_retrieval = 0

    def __init__(self, nb_items_max=100_000):
        self.cache = {}
        self.entries = []
        self.nb_items_max = nb_items_max

    def hash_key(self, val):
        return hash(val)

    def add(self, key, val):
        if len(self.cache) >= self.nb_items_max:
            oldest_item_key = self.entries.pop(0)
            del self.cache[oldest_item_key]
        hashed_key = self.hash_key(key)
        self.entries.append(hashed_key)
        self.cache[hashed_key] = val

    def get(self, key):
        key = self.hash_key(key)
        if key in self.cache:
            self.nb_cache_retrieval += 1
            return self.cache[key][1]
        return None
