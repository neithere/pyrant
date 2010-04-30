# -*- coding: utf-8 -*-
"""
A pure-Python implementation of Tokyo Tyrant protocol.
Python 2.4+ is required.

More information about Tokyo Cabinet:
    http://1978th.net/tokyocabinet/

More information about Tokyo Tyrant:
    http://1978th.net/tokyotyrant/

Usage example (note the automatically managed support for table database)::

    >>> import pyrant
    >>> TEST_HOST, TEST_PORT = '127.0.0.1', 1983
    >>> t = pyrant.Tyrant(host=TEST_HOST, port=TEST_PORT)    # default port is 1978
    >>> if t.table_enabled:
    ...     t['key'] = {'name': 'foo'}
    ...     print t['key']['name']
    ... else:
    ...     t['key'] = 'foo'
    ...     print t['key']
    foo
    >>> del t['key']
    >>> print t['key']
    Traceback (most recent call last):
        ...
    KeyError: 'key'

"""

import itertools as _itertools
import uuid

# pyrant
import exceptions
import protocol
import query
import utils


__version__ = '0.6.2'
__all__ = ['Tyrant']


# Constants
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 1978


class Tyrant(object):
    """A Python dictionary API for Tokyo Tyrant.

    :param host: Tyrant host address
    :param port: Tyrant port number
    :param separator: if set, will be used to get/put lists as values. For table
        databases the separator applies to column values.
    :param literal: if set, returned data is not encoded to Unicode (default is
        False)

    Usage::

        >>> import pyrant
        >>> t = pyrant.Tyrant(host=TEST_HOST, port=TEST_PORT)

        # remove anything that could be left from previous time
        >>> t.clear()

        # make sure there are zero records in the database
        >>> len(t)
        0

    :class:`Tyrant` provides pythonic syntax and data pythonification, while the
    the lower-lever :class:`~pyrant.protocol.TyrantProtocol` closely follows the
    orginal Tokyo Tyrant API and only converts incoming data to strings
    (Unicode). You decide which to use. Generally you would want more pythonic
    API (:class:`Tyrant`) for most cases and the lower-level interface to reduce
    overhead or to fix broken data which cannot be properly converted by means
    of the higher-level API.

    It is also important that Tokyo Cabinet has a great query extension for table
    databases. This extension is supported by :class:`pyrant.query.Query` which
    only requires :class:`~pyrant.protocol.TyrantProtocol` to work and can be
    easily accessed via :attr:`Tyrant.query`::

        >>> t.query
        []

    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, separator=None,
                 literal=False):
        """
        The pythonic interface for Tokyo Tyrant. Mimics dict API.
        """
        # keep the protocol public just in case anyone needs a specific option
        self.proto = protocol.TyrantProtocol(host, port)

        self.separator = separator
        if not separator and self.table_enabled:
            self.separator = protocol.TABLE_COLUMN_SEP

        self.literal = literal

    def __contains__(self, key):
        try:
            self.proto.vsiz(key)
        except exceptions.TyrantError:
            return False
        else:
            return True

    def __delitem__(self, key):
        try:
            return self.proto.out(key)
        except exceptions.TyrantError:
            raise KeyError(key)

    def __getitem__(self, key):
        try:
            elem = self.proto.get(key, self.literal)
            return utils.to_python(elem, self.db_type, self.separator)
        except exceptions.TyrantError:
            raise KeyError(key)

    def get(self, key, default=None):
        """
        Returns value for `key`. If no record is found, returns `default`.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self):
        return iter(self.iterkeys())

    def __len__(self):
        return self.proto.rnum()

    def __repr__(self):
        return u'<Tyrant %s:%s>' % (self.proto.host, self.proto.port)

    def __setitem__(self, key, value):
        """
        Sets given value for given primary key in the database.
        Additional types conversion is only done if the value is a dictionary.
        """
        if isinstance(value, dict):
            flat = list(_itertools.chain(*((k, utils.from_python(v)) for
                                            k,v in value.iteritems())))
            args = [key] + flat
            self.proto.misc('put', args)
        else:
            if isinstance(value, (list, tuple)):
                assert self.separator, "Separator is not set"
                prepared_value = self.separator.join(value)
            else:
                prepared_value = value
            self.proto.put(key, prepared_value)

    @property
    def db_type(self):
        stats = self.get_stats()
        assert 'type' in stats and stats['type'], ('statistics must provide a '
                                                   'valid database type')
        return stats['type']

    @property
    def db_path(self):
        stats = self.get_stats()
        assert 'path' in stats, 'statistics must provide a database path'
        return stats['path']

    @property
    def table_enabled(self):
        """
        Returns True is current database type is TDB so TDB-specific extensions
        are enabled.
        """
        return self.db_type == protocol.DB_TABLE

    def call_func(self, func, key, value, record_locking=False,
                  global_locking=False):
        """
        Calls specific function.
        """
        # TODO: write better documentation *OR* move this method to lower level
        opts = ((record_locking and protocol.TyrantProtocol.RDBXOLCKREC) |
                (global_locking and protocol.TyrantProtocol.RDBXOLCKGLB))
        return self.proto.ext(func, opts, key, value)

    def clear(self):
        """
        Removes all records from the remote database.
        """
        self.proto.vanish()

    def concat(self, key, value, width=None):
        """
        Concatenates columns of the existing record.
        """
        # TODO: write better documentation, provide example code
        if width is None:
            self.proto.putcat(key, value)
        else:
            self.proto.putshl(key, value, width)

    def generate_key(self):
        """
        Returns a unique primary key for given database. Tries to obtain the
        key using database's built-in function `genuid`. If `genuid` fails to
        provide the key, a UUID is generated instead.
        """
        try:
            return self.proto.genuid()
        except ValueError:
            return uuid.uuid4()

    def get_size(self, key):
        """
        Returns the size of the value for `key`.
        """
        try:
            return self.proto.vsiz(key)
        except exceptions.TyrantError:
            raise KeyError(key)

    def get_stats(self):
        """
        Returns the status message of the database as dictionary.
        """
        return utils.csv_to_dict(self.proto.stat())

    def iterkeys(self):
        """
        Iterates keys using remote operations.
        """
        self.proto.iterinit()
        try:
            while True:
                yield self.proto.iternext()
        except exceptions.TyrantError:
            pass

    def keys(self):
        """
        Returns the list of keys in the database.
        """
        return list(self.iterkeys())

    def itervalues(self):
        for k, v in self.iteritems():
            yield v

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        """
        Returns a generator with key/value pairs. The data is read from the
        database in chunks to alleviate the issues of a) too many database
        hits, and b) too heavy memory usage when only a part of the list is
        actually used.
        """
        CHUNK_SIZE = 1000
        chunk = []
        for key in self.iterkeys():
            chunk.append(key)
            if CHUNK_SIZE <= len(chunk):
                for k,v in self.multi_get(chunk):
                    yield k,v
                chunk = []
        if chunk:
            for k,v in self.multi_get(chunk):
                yield k,v

    def items(self):
        return list(self.iteritems())

    def has_key(self, key):
        return key in self

    def setdefault(self, key, value):
        """
        >>> t.setdefault('foo', {'one': 'one'})
        {u'one': u'one'}
        >>> t.setdefault('foo', {'two': 'two'})
        {u'one': u'one'}

        """
        if not key in self:
            self[key] = value
        return self[key]

    def update(self, mapping=None, **kwargs):
        """
        Updates given objects from a dict, list of key and value pairs or a
        list of named params. Usage::

            mapping = (
                ('john', dict(name='John')),
            )
            t.update(mapping, mary={'name': 'Mary'})

        See built-in `dict.update` method for more information.
        """
        data = dict(mapping or {}, **kwargs)
        self.multi_set(data)

    def multi_del(self, keys, no_update_log=False):
        """
        Removes given records from the database.
        """
        # TODO: write better documentation: why would user need the no_update_log param?
        opts = (no_update_log and protocol.TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        self.proto.misc('outlist', keys, opts)

    def multi_get(self, keys):
        """
        Returns records that match given keys. Missing keys are silently
        ignored, i.e. the number of results may be lower than the number of
        requested keys. The records are returned as key/value pairs. Usage::

            >>> g = t.multi_get(['foo', 'bar', 'galakteko opasnoste'])
            >>> g
            [('foo', {'one': 'one'}), ('bar', {'two': 'two'})]

        :param keys: the list of keys.

        """
        # TODO: write better documentation: why would user need the no_update_log param?
        assert hasattr(keys, '__iter__'), 'expected iterable, got %s' % keys
        prep_val = lambda v: utils.to_python(v, self.db_type, self.separator)

        keys = list(keys)
        data = self.proto.misc('getlist', keys, 0)
        data_keys = data[::2]
        data_vals = (prep_val(x) for x in data[1::2])
        return zip(data_keys, data_vals)

    def multi_set(self, items, no_update_log=False):
        """
        Stores given records in the database.
        """
        opts = (no_update_log and protocol.TyrantProtocol.RDBMONOULOG or 0)
        ready_pairs = []
        for key, value in items.iteritems():
            if isinstance(value, dict):
                # make flat list of interleaved key/value pairs
                new_value = []
                for pair in value.items():
                    new_value.extend(pair)
                value = new_value
            if hasattr(value, '__iter__'):
                assert self.separator, 'Separator is not set'
                value = self.separator.join(value)
            ready_pairs.extend((key, value))

        self.proto.misc('putlist', ready_pairs, opts)

    def prefix_keys(self, prefix, maxkeys=None):
        """
        Get forward matching keys in a database.
        The return value is a list object of the corresponding keys.
        """
        # TODO: write better documentation: describe purpose, provide example code
        if maxkeys is None:
            maxkeys = len(self)

        return self.proto.fwmkeys(prefix, maxkeys)

    def sync(self):
        """
        Synchronizes updated content with the database.
        """
        # TODO: write better documentation: when would user need this?
        self.proto.sync()

    @property
    def query(self):
        """
        Returns a :class:`~pyrant.Query` object for the database.
        """
        if not self.table_enabled:
            raise TypeError('Query only works with table databases but %s is a '
                            '%s database.' % (self.db_path, self.db_type))
        return query.Query(self.proto, self.db_type, self.literal)
