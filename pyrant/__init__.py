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

import itertools
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
        if not isinstance(key, (str, unicode)):
            raise TypeError('Primary key must be a string, got %s "%s"'
                            % (type(key).__name__, key))
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
            # check if there are no keys that would become empty strings
            if not all(unicode(k) for k in value):
                raise KeyError('Empty keys are not allowed (%s).' % repr(value))

            # EXPLAIN why the 'from_python' conversion is necessary, as there
            # is no straight forward way of restoring the python objects. What
            # about limiting the allowed keys and values to string only, an
            # raise exception on any other object type?
            flat = list(itertools.chain(*((k, utils.from_python(v)) for
                                           k,v in value.iteritems())))
            args = [key] + flat
            self.proto.misc('put', args)  # EXPLAIN why is this hack necessary?
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

    def multi_add(self, values, chunk_size=1000, no_update_log=False):
        """
        Adds given values as new records and returns a list of automatically
        generated primary keys for these records. Wrapper for
        :meth:`~pyrant.Tyrant.multi_set`.

        :param values: any iterable; in fact, you can pass a generator and it
            will be processed in chunks in order to save on resources and
            cut down the database hit at the same time.
        :param chunk_size: size of chunks in which the data will be fed to the
            database. If set to zero, data is fed to database in one chunk.

        """
        keys = self._multi_add(values=values, chunk_size=chunk_size,
                               no_update_log=no_update_log)
        return list(keys)

    def _multi_add(self, values, chunk_size=1000, no_update_log=False):
        assert hasattr(values, '__iter__'), 'values must be an iterable'
        chunk = []
        for value in values:
            'value', value
            key = self.generate_key()
            yield key
            chunk.append((key, value))
            if 0 < chunk_size < len(chunk):
                # chunk has grown big, need to feed the database
                self.multi_set(chunk, no_update_log=no_update_log)
                chunk = []
        else:
            # feed the remnants
            self.multi_set(chunk, no_update_log=no_update_log)

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
        Stores the given records in the database. The records may be given
        as an iterable sequence. Usage::

           >>> t.multi_set([('foo', {'one': 'one'}), ('bar', {'two': 'two'})])

        which is equevalent with the call::

           >>> t.multi_set({'foo': {'one': 'one'}, 'bar':{'two': 'two'}})

        :param items: the sequence of records to be stored.

        """
        # TODO: add here and in other places (in this module and protocol)
        # notes on the no-update-log param:
        #
        # One thing worth noting is  that you can choose replication option
        # when you call from TT. By default, all your operation will be written
        # to update logging files. If you call with RDBMONOULOG option, then it
        # won't record update log nor replication. If you are doing get or
        # search operation which does not require replication, you should call
        # with RDBMONOULOG.
        # If you want to initialise or optimise, but not replicate tcrdbsync,
        # tcrdboptimize, tcrdbvanish, then you should call it with RDBMONOULOG,
        # too.
        #  You can do quite a lot if you call "misc" from Lua extensions.
        #
        # source:
        # http://tokyocabinetwiki.pbworks.com/37_hidden_features_of_the_misc_method

        opts = (no_update_log and protocol.TyrantProtocol.RDBMONOULOG or 0)
        ready_pairs = []
        # HACK To allow for items to be given in a sequence, as e.g. the list returned from multi_get.
        if isinstance(items, dict):
            iterator = items.iteritems()
        else:
            iterator = iter(items)
        for key, value in iterator:
            if isinstance(value, dict):
                # make flat list of interleaved key/value pairs
                new_value = []
                for pair in value.items():
                    new_value.extend(pair)  # EXPLAIN why is utils.from_python() not used here?
                value = new_value
            if hasattr(value, '__iter__'):
                assert self.separator, 'Separator is not set'
                strings = (str(x) for x in value)
                value = self.separator.join(strings)
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

        Tokyo Cabinet is not durable, as data committed to the database is not
        immediately flushed to disk. Unwritten data may lost in the event of
        e.g. a system crash.

        Use `sync` to force a flush of unwritten data to disk, but be aware that
        this also locks the writer process and blocks queries.

        The better approach is to use database replication (copy the data to
        another database instance) and backup often.
        """
        self.proto.sync()

    @property
    def query(self):
        """
        Returns a :class:`~pyrant.Query` object for the database.

        .. note:: Only available for Table Databases.

        """
        if not self.table_enabled:
            raise TypeError('Query only works with table databases but %s is a '
                            '%s database.' % (self.db_path, self.db_type))
        return query.Query(self.proto, self.db_type, self.literal)
