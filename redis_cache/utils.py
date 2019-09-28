import importlib
import itertools
import warnings
from functools import lru_cache
from urllib.parse import parse_qs
from urllib.parse import urlparse

from django.core.exceptions import ImproperlyConfigured

from redis.connection import Connection
from redis.connection import SSLConnection
from redis.connection import UnixDomainSocketConnection
from redis.connection import URL_QUERY_ARGUMENT_PARSERS


def chunker(iterable, chunksize=1000):
    iterator = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(iterator, chunksize))
        if not chunk:
            return
        yield chunk


@lru_cache(maxsize=None)
def import_class(path):
    module_name, class_name = path.rsplit('.', 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise ImproperlyConfigured('Could not find module "%s"' % module_name)
    else:
        try:
            return getattr(module, class_name)
        except AttributeError:
            raise ImproperlyConfigured('Cannot import "%s"' % class_name)


def parse_connection_kwargs(url, db=None, **kwargs):
    """
    Return a connection pool configured from the given URL.

    For example::

        redis://[:password]@localhost:6379/0
        rediss://[:password]@localhost:6379/0
        unix://[:password]@/path/to/socket.sock?db=0

    Three URL schemes are supported:

    - ```redis://``
      <https://www.iana.org/assignments/uri-schemes/prov/redis>`_ creates a
      normal TCP socket connection
    - ```rediss://``
      <https://www.iana.org/assignments/uri-schemes/prov/rediss>`_ creates
      a SSL wrapped TCP socket connection
    - ``unix://`` creates a Unix Domain Socket connection

    There are several ways to specify a database number. The parse function
    will return the first specified option:
        1. A ``db`` querystring option, e.g. redis://localhost?db=0
        2. If using the redis:// scheme, the path argument of the url, e.g.
           redis://localhost/0
        3. The ``db`` argument to this function.

    If none of these options are specified, db=0 is used.

    The ``decode_components`` argument allows this function to work with
    percent-encoded URLs. If this argument is set to ``True`` all ``%xx``
    escapes will be replaced by their single-character equivalents after
    the URL has been parsed. This only applies to the ``hostname``,
    ``path``, and ``password`` components.

    Any additional querystring arguments and keyword arguments will be
    passed along to the ConnectionPool class's initializer. The querystring
    arguments ``socket_connect_timeout`` and ``socket_timeout`` if supplied
    are parsed as float values. The arguments ``socket_keepalive`` and
    ``retry_on_timeout`` are parsed to boolean values that accept
    True/False, Yes/No values to indicate state. Invalid types cause a
    ``UserWarning`` to be raised. In the case of conflicting arguments,
    querystring arguments always win.

    """
    schemes = ('redis://', 'rediss://', 'unix://')
    raw_url = url

    url = urlparse(url)
    url_options = {
        'unix_socket_path': None
    }

    for name, value in parse_qs(url.query).items():
        if value and len(value) > 0:
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    url_options[name] = parser(value[0])
                except (TypeError, ValueError):
                    warnings.warn(UserWarning(
                        "Invalid value for `%s` in connection URL." % name
                    ))
            else:
                url_options[name] = value[0]

    password = url.password
    path = url.path
    hostname = url.hostname

    # We only support redis://, rediss:// and unix:// schemes.
    if url.scheme == 'unix':
        url_options.update({
            'host': None,
            'port': None,
            'password': password,
            'unix_socket_path': path,
            'connection_class': UnixDomainSocketConnection,
        })

    elif url.scheme in ('redis', 'rediss'):
        url_options.update({
            'host': hostname,
            'port': int(url.port or 6379),
            'password': password,
        })

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if 'db' not in url_options and path:
            try:
                url_options['db'] = int(path.replace('/', ''))
            except (AttributeError, ValueError):
                pass

        if url.scheme == 'rediss':
            url_options['connection_class'] = SSLConnection
        else:
            url_options['connection_class'] = Connection
    else:
        valid_schemes = ', '.join(schemes)
        raise ValueError(
            "'%s' is not a valid Redis URL. A Redis URL must specify one of the following schemes "
            "(%s)" % (
                raw_url,
                valid_schemes
            )
        )

    # last shot at the db value
    url_options['db'] = int(url_options.get('db', db or 0))

    # update the arguments from the URL values
    kwargs.update(url_options)

    # backwards compatability
    if 'charset' in kwargs:
        warnings.warn(DeprecationWarning(
            '"charset" is deprecated. Use "encoding" instead'))
        kwargs['encoding'] = kwargs.pop('charset')
    if 'errors' in kwargs:
        warnings.warn(DeprecationWarning(
            '"errors" is deprecated. Use "encoding_errors" instead'))
        kwargs['encoding_errors'] = kwargs.pop('errors')

    return kwargs
