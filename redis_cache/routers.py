import random
from itertools import cycle


class BaseRouter(object):
    def __init__(self, cache, unified_keyspace=True):
        self.cache = cache

        self.unified_keyspace = unified_keyspace

    def get_client(self, versioned_key, for_write=False):
        raise NotImplementedError('Subclasses must define this method.')


class RoundRobinRouter(BaseRouter):

    def _get_reader(self):
        readers = cycle(self.cache.readers)
        while True:
            yield next(readers)

    def _get_writer(self):
        writers = cycle(self.cache.writers)
        while True:
            yield next(writers)

    def get_client(self, versioned_key, for_write=False):
        if for_write:
            return next(self._get_writer())
        else:
            return next(self._get_reader())


class RandomRouter(BaseRouter):

    def get_client(self, versioned_key, for_write=False):
        if for_write:
            return random.choice(self.cache.writers)
        else:
            return random.choice(self.cache.readers)
