from redis_cache.utils import parse_connection_kwargs


schemes = [
    'redis://[:password]@localhost:6379/0',
    'rediss://[:password]@localhost:6379/0',
    'unix://[:password]@/path/to/socket.sock?db=0',
]
expected_keys = {'host', 'port', 'db', 'password', 'connection_class', 'unix_socket_path'}
for scheme in schemes:
    absent_keys = expected_keys - parse_connection_kwargs(scheme).keys()
    assert len(absent_keys) == 0, (scheme, absent_keys)