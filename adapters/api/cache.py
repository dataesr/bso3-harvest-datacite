from datetime import datetime, timedelta

from domain.api.abstract_cache import AbstractCache


class Cache(AbstractCache):

    def __init__(self, default_time=60):
        self.cache = {}
        self.default_time = timedelta(seconds=default_time)

    def hash_key(self, val):
        return hash(val)

    def add(self, key, val, timeout=None):
        expire = datetime.now() + (
            timedelta(seconds=timeout) if timeout else self.default_time
        )
        self.cache[self.hash_key(key)] = expire, val

    def get(self, key):
        key = self.hash_key(key)
        if key in self.cache:
            if datetime.now() < self.cache[key][0]:
                return self.cache[key][1]
            else:
                self.cache.pop(key)
        return None
