from collections import defaultdict

from django.core.cache.backends.base import BaseCache
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from django.core.cache.backends.base import InvalidCacheBackendError
from django.core.exceptions import ImproperlyConfigured
from django.utils.functional import cached_property

try:
    import redis
except ImportError:
    raise InvalidCacheBackendError(
        "Redis cache backend requires the 'redis-py' library"
    )

from redis_cache.constants import KEY_EXPIRED
from redis_cache.constants import KEY_NON_VOLATILE
from redis_cache.utils import chunker
from redis_cache.utils import import_class


class ServerConfig:

    def __init__(
            self,
            host='localhost',
            port=6379,
            db=1,
            unix_socket_path=None,
            password=None,
            for_write=True,
            for_read=True,
            **client_kwargs):

        # Connection parameters
        self.host = host
        self.port = port
        self.db = db
        self.unix_socket_path = unix_socket_path

        # Security parameters
        self.password = password

        self.client_kwargs = client_kwargs

        # Meta parameters.
        self.for_write = for_write
        self.for_read = for_read

    def get_client(self):
        return redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            unix_socket_path=self.unix_socket_path,
            **self.client_kwargs
        )

    @cached_property
    def client(self):
        return self.get_client()


class RedisCache(BaseCache):
    """
    CACHES = {
        'default': {
            'LOCATION': [
                {
                    'host': 'localhost',
                    'port': 6379,
                    'db': 1,
                    'password': 'yadayada',
                    'unix_sociate_path': None,
                    'ssl': True,
                    'ssl_keyfile': '...',
                    'ssl_certfile': '...',
                    'ssl_cert_reqs': '...',
                    'ssl_ca_certs': '...',
                    'for_write': True,
                    'for_read': True,
                }
                'redis://[:password]@localhost:6380/0',
                'rediss://[:password]@localhost:6381/0',
                'unix://[:password]@/path/to/socket.sock?db=0',
            ],
            'OPTIONS': {
                'COMPRESSOR': ('<module path>', {<kwargs>}),
                'SERIALIZER': ('<module path>', {<kwargs>}),
                'ROUTER': ('<module path>', {<kwargs>}),
                # Default config applies to all server connections where the server config does
                # not specify a value for the parameter.
                'DEFAULT_CONFIG': {
                    'db': 1,
                    'password': 'yadayada',
                    'unix_sociate_path': None,
                    'for_write': True,
                    'for_read': True,
                },
            },
        },
    }
    """

    def __init__(self, server, params):
        """
        Connect to Redis, and set up cache backend.

        """
        super(RedisCache, self).__init__(params)
        self.server = server
        self.params = params or {}
        self.options = params.get('OPTIONS', {})
        self.configs = self.get_server_configs(server)
        self.clients = []
        self.writers = []
        self.readers = []

        for config in self.configs:
            if config.for_write:
                self.writers.append(config.client)
            if config.for_read:
                self.readers.append(config.client)
            self.clients = config.client

    def __getstate__(self):
        return {'params': self.params, 'server': self.server}

    def __setstate__(self, state):
        self.__init__(**state)

    def get_server_configs(self, server):
        """Returns a list of servers given the server argument passed in from Django."""
        if isinstance(server, str):
            servers = [location.strip() for location in server.split(',')]
        elif hasattr(server, '__iter__'):
            servers = server
        else:
            raise ImproperlyConfigured(
                '"server" must be an iterable or string'
            )
        configs = []
        default_config = self.options.get('DEFAULT_CONFIG', {
            'host': 'localhost',
            'port': 6379,
            'db': 1,
            'unix_socket_path': None,
            'password': '',
        })
        for server in servers:
            config = default_config.copy()
            if isinstance(server, dict):
                config.update(server)
            elif isinstance(server, str):
                # config.update(parse_connection_kwargs(server))
                pass
            configs.append(ServerConfig(**config))
        return configs

    @cached_property
    def serializer(self):
        default = ('redis_cache.serializers.PickleSerializer', {})
        path, kwargs = self.options.get('SERIALIZER', default)
        cls = import_class(path)
        return cls(**kwargs)

    @cached_property
    def compressor(self):
        default = ('redis_cache.compressors.NoopCompressor', {})
        path, kwargs = self.options.get('COMPRESSOR', default)
        cls = import_class(path)
        return cls(**kwargs)

    @cached_property
    def router(self):
        default = ('redis_cache.routers.RoundRobinRouter', {})
        path, kwargs = self.options.get('ROUTER', default)
        cls = import_class(path)
        return cls(self, **kwargs)

    def get_client(self, versioned_key, for_write=False):
        return self.router.get_client(versioned_key, for_write=for_write)

    def serialize(self, value):
        return self.serializer.serialize(value)

    def deserialize(self, value):
        return self.serializer.deserialize(value)

    def compress(self, value):
        return self.compressor.compress(value)

    def decompress(self, value):
        return self.compressor.decompress(value)

    def get_value(self, original):
        try:
            value = int(original)
        except (ValueError, TypeError):
            decompressed_value = self.decompress(original)
            deserialized_value = self.deserialize(decompressed_value)
            return deserialized_value
        else:
            return value

    def prep_value(self, value):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        serialized_value = self.serialize(value)
        compressed_value = self.compress(serialized_value)
        return compressed_value

    def make_keys(self, keys, version=None):
        return [self.make_key(key, version=version) for key in keys]

    def get_timeout(self, timeout):
        if timeout is DEFAULT_TIMEOUT:
            timeout = self.default_timeout

        if timeout is not None:
            timeout = int(timeout)

        return timeout

    def partition_keys(self, versioned_keys, for_write=False):
        client_to_keys = defaultdict(list)
        for versioned_key in versioned_keys:
            client = self.get_client(versioned_key, for_write=for_write)
            client_to_keys[client].append(versioned_key)
        return client_to_keys

    ####################
    # Django cache api #
    ####################

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        timeout = self.get_timeout(timeout)
        return self._set(client, versioned_key, self.prep_value(value), timeout, _add_only=True)

    def _get(self, client, versioned_key, default=None):
        value = client.get(versioned_key)
        if value is None:
            return default
        value = self.get_value(value)
        return value

    def get(self, key, default=None, version=None):
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=False)
        return self._get(client, versioned_key, default=default)

    def _set(self, client, versioned_key, value, timeout, _add_only=False):
        if timeout is not None and timeout < 0:
            return False
        elif timeout == 0:
            return client.expire(versioned_key, 0)
        return client.set(versioned_key, value, nx=_add_only, ex=timeout)

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        """Persist a value to the cache, and set an optional expiration time."""
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        timeout = self.get_timeout(timeout)
        result = self._set(client, versioned_key, self.prep_value(value), timeout, _add_only=False)
        return result

    def delete(self, key, version=None):
        """Remove a key from the cache."""
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        return client.delete(versioned_key)

    def delete_many(self, keys, version=None):
        """
        Remove multiple keys at once.
        """
        versioned_keys = [self.make_key(key, version=version) for key in keys]
        for client, _versioned_keys in self.partition_keys(versioned_keys, for_write=True).items():
            client.delete(*_versioned_keys)

    def clear(self, version=None):
        """Flush cache keys."""
        if version is None:
            for client in self.writers:
                client.flushdb()
        else:
            self.delete_pattern('*', version=version)

    def get_many(self, keys, version=None):
        recovered_data = {}

        versioned_key_to_key = {self.make_key(key, version=version): key for key in keys}
        partitioned_keys = self.partition_keys(versioned_key_to_key.keys())
        for client, versioned_keys in partitioned_keys.items():
            # Only try to mget if we actually received any keys to get
            if versioned_key_to_key:
                results = client.mget(versioned_keys)

                for key, value in zip(versioned_keys, results):
                    if value is None:
                        continue
                    recovered_data[versioned_key_to_key[key]] = self.get_value(value)

        return recovered_data

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        """Set a bunch of values in the cache at once from a dict of key/value
        pairs. This is much more efficient than calling set() multiple times.

        If timeout is given, that timeout will be used for the key; otherwise
        the default cache timeout will be used.
        """
        timeout = self.get_timeout(timeout)
        versioned_key_to_key = {self.make_key(key, version=version): key for key in data.keys()}

        partitioned_keys = self.partition_keys(versioned_key_to_key.keys(), for_write=True)
        for client, versioned_keys in partitioned_keys.items():
            # Use pipeline instead of `mset`, so we can set the timeout on the keys.
            pipeline = client.pipeline()
            for versioned_key in versioned_keys:
                value = self.prep_value(data[versioned_key_to_key[versioned_key]])
                self._set(pipeline, versioned_key, value, timeout)
            pipeline.execute()

    def incr(self, key, delta=1, version=None):
        """Add delta to value in the cache. If the key does not exist, raise a
        `ValueError` exception.
        """
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        exists = client.exists(versioned_key)
        if not exists:
            raise ValueError("Key '%s' not found" % key)

        value = client.incr(versioned_key, delta)

        return value

    def incr_version(self, key, delta=1, version=None):
        """Adds delta to the cache version for the supplied key. Returns the new version."""
        if version is None:
            version = self.version

        versioned_old_key = self.make_key(key, version=version)
        versioned_new_key = self.make_key(key, version=version + delta)
        old_key_client = self.get_client(versioned_old_key, for_write=False)
        new_key_client = self.get_client(versioned_new_key, for_write=True)
        if old_key_client == new_key_client:
            try:
                old_key_client.rename(versioned_old_key, versioned_new_key)
            except redis.ResponseError:
                raise ValueError("Key '%s' not found" % key)
        else:
            ttl = old_key_client.ttl(versioned_old_key)
            value = old_key_client.get(versioned_old_key)
            writeable_old_key_client = self.get_client(versioned_old_key, for_write=True)
            writeable_old_key_client.delete(versioned_old_key)
            new_key_client.set(versioned_new_key, value, ex=ttl)
        return version + delta

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        """Reset the timeout of a key to `timeout` seconds."""
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        return client.expire(versioned_key, timeout)

    def has_key(self, key, version=None):
        """Returns True if the key is in the cache and has not expired."""
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=False)
        return client.exists(versioned_key)

    #####################
    # Extra api methods #
    #####################

    def ttl(self, key, version=None):
        """Returns the 'time-to-live' of a key.  If the key is not volitile,
        i.e. it has not set expiration, then the value returned is None.
        Otherwise, the value is the number of seconds remaining.  If the key
        does not exist, 0 is returned.
        """
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=False)
        ttl = client.ttl(versioned_key)
        if ttl == KEY_NON_VOLATILE:
            return None
        elif ttl == KEY_EXPIRED:
            return 0
        else:
            return ttl

    def delete_pattern(self, pattern, version=None):
        pattern = self.make_key(pattern, version=version)
        for client in self.writers:
            iterator = client.scan_iter(match=pattern)
            for keys in chunker(iterator, 10000):
                client.delete(*keys)

    def lock(self, key, timeout=None, sleep=0.1, blocking_timeout=None, thread_local=True):
        client = self.get_client(key, for_write=True)
        return client.lock(
            key,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local
        )

    def get_or_set(
            self,
            key,
            func,
            timeout=DEFAULT_TIMEOUT,
            lock_timeout=None,
            stale_cache_timeout=None,
            version=None):
        """Get a value from the cache or call ``func`` to set it and return it.

        This implementation is slightly more advanced that Django's.  It provides thundering herd
        protection, which prevents multiple threads/processes from calling the value-generating
        function too much.

        There are three timeouts you can specify:

        ``timeout``: Time in seconds that value at ``key`` is considered fresh.
        ``lock_timeout``: Time in seconds that the lock will stay active and prevent other threads
            or processes from acquiring the lock.
        ``stale_cache_timeout``: Time in seconds that the stale cache will remain after the key has
            expired. If ``None`` is specified, the stale value will remain indefinitely.

        """
        if not callable(func):
            raise Exception("Must pass in a callable")

        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)

        lock_key = "__lock__" + versioned_key
        fresh_key = "__fresh__" + versioned_key

        is_fresh = self._get(client, fresh_key)
        value = self._get(client, versioned_key)

        if is_fresh:
            return value

        timeout = self.get_timeout(timeout)
        lock = self.lock(lock_key, timeout=lock_timeout)

        acquired = lock.acquire(blocking=False)

        if acquired:
            try:
                value = func()
            except Exception:
                raise
            else:
                key_timeout = (
                    None if stale_cache_timeout is None else timeout + stale_cache_timeout
                )
                pipeline = client.pipeline()
                pipeline.set(versioned_key, self.prep_value(value), key_timeout)
                pipeline.set(fresh_key, 1, timeout)
                pipeline.execute()
            finally:
                lock.release()

        return value

    def persist(self, key, version=None):
        """Remove the timeout on a key.

        Equivalent to setting a timeout of None in a set command.

        Returns True if successful and False if not.
        """
        versioned_key = self.make_key(key, version=version)
        client = self.get_client(versioned_key, for_write=True)
        return client.persist(versioned_key)
