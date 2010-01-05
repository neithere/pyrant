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
    >>> t = pyrant.Tyrant(host='127.0.0.1', port=1983)    # default port is 1978
    >>> if t.dbtype != pyrant.DBTYPETABLE:
    ...     t['key'] = 'foo'
    ...     print t['key']
    ... else:
    ...     t['key'] = {'name': 'foo'}
    ...     print t['key']['name']
    foo
    >>> del t['key']
    >>> print t['key']
    Traceback (most recent call last):
        ...
    KeyError: 'key'

"""

import itertools as _itertools
from exceptions import TyrantError
from protocol import TyrantProtocol
from query import Query
from utils import to_python


__version__ = '0.1.0'
__all__ = ['Tyrant', 'TyrantError', 'TyrantProtocol']


# Constants
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 1978


class Tyrant(dict):
    """A Python dictionary API for Tokyo Tyrant.

    :param host: Tyrant host address
    :param port: Tyrant port number
    :param separator: if set, will be used to get/put lists as values
    :param literal: if set, returned data is not encoded to Unicode
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, separator=None,
                 literal=False):
        """
        Acts like a python dictionary.
        """
        # We want to make protocol public just in case anyone need any
        # specific option
        self.proto = TyrantProtocol(host, port)
        self.dbtype = self.get_stats()['type']
        self.separator = separator
        if not separator and self.dbtype=="table":
            self.separator = "\x00" #Default separator for tables
        self.literal = literal

    def __contains__(self, key):
        try:
            self.proto.vsiz(key)
        except TyrantError:
            return False
        else:
            return True

    def __delitem__(self, key):
        try:
            return self.proto.out(key)
        except TyrantError:
            raise KeyError(key)

    def __getitem__(self, key):
        try:
            elem = self.proto.get(key, self.literal)
            return utils.to_python(elem, self.dbtype, self.separator)
        except TyrantError:
            raise KeyError(key)

    def get(self, key, default=None):
        """Returns value for `key`. If no record is found, returns `default`."""
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self):
        return iter(self.iterkeys())

    def __len__(self):
        return self.proto.rnum()

    def __repr__(self):
        return object.__repr__(self)

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            flat = _itertools.chain([key], *value.iteritems())
            self.proto.misc('put', list(flat))

        elif isinstance(value, (list, tuple)):
            assert self.separator, "Separator is not set"

            flat = self.separator.join(value)
            self.proto.put(key, flat)

        else:
            self.proto.put(key, value)


    def call_func(self, func, key, value, record_locking=False,
                  global_locking=False):
        """Calls specific function."""
        # TODO: write better documentation *OR* move this method to lower level
        opts = ((record_locking and TyrantProtocol.RDBXOLCKREC) |
                (global_locking and TyrantProtocol.RDBXOLCKGLB))
        return self.proto.ext(func, opts, key, value)

    def clear(self):
        """Removes all records from the remote database."""
        self.proto.vanish()

    def concat(self, key, value, width=None):
        """Concatenates columns of the existing record."""
        # TODO: write better documentation, provide example code
        if width is None:
            self.proto.putcat(key, value)
        else:
            self.proto.putshl(key, value, width)

    def get_size(self, key):
        """Returns the size of the value for `key`."""
        try:
            return self.proto.vsiz(key)
        except TyrantError:
            raise KeyError(key)

    def get_stats(self):
        """Returns the status message of the database as dictionary."""
        return utils.csv_to_dict(self.proto.stat())

    def iterkeys(self):
        """Iterates keys using remote operations."""
        self.proto.iterinit()
        try:
            while True:
                yield self.proto.iternext()
        except TyrantError:
            pass

    def keys(self):
        """Returns the list of keys in the database."""
        return list(self.iterkeys())

    def update(self, dict=None, **kwargs):
        """
        Updates given objets from a dict, list of key and value pairs or a list of named params.

        See update method in python built-in object for more info
        """
        data = {}
        if dict:
            data.update(dict, **kwargs)
        else:
            data.update(**kwargs)
        self.multi_set(data)

    def multi_del(self, keys, no_update_log=False):
        """Removes given records from the database."""
        # TODO: write better documentation: why would user need the no_update_log param?
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        self.proto.misc("outlist", keys, opts)

    def multi_get(self, keys, no_update_log=False):
        """Returns a list of records that match given keys."""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)

        rval = self.proto.misc("getlist", keys, opts)

        if len(rval) <= len(keys):
            # 1.1.10 protocol, may return invalid results
            if len(rval) < len(keys):
                raise KeyError("Missing a result, unusable response in 1.1.10")

            return rval

        # 1.1.11 protocol returns interleaved key, value list
        d = dict((rval[i], to_python(rval[i + 1], self.dbtype,
                                       self.separator)) \
                    for i in xrange(0, len(rval), 2))
        return d

    def multi_set(self, items, no_update_log=False):
        """Stores given records in the database."""
        opts = (no_update_log and TyrantProtocol.RDBMONOULOG or 0)
        lst = []
        for k, v in items.iteritems():
            if isinstance(v, (dict)):
                new_v = []
                for kk, vv in v.items():
                    new_v.append(kk)
                    new_v.append(vv)
                v = new_v
            if isinstance(v, (list, tuple)):
                assert self.separator, "Separator is not set"

                v = self.separator.join(v)
            lst.extend((k, v))

        self.proto.misc("putlist", lst, opts)

    def prefix_keys(self, prefix, maxkeys=None):
        """Get forward matching keys in a database.
        The return value is a list object of the corresponding keys.
        """
        # TODO: write better documentation: describe purpose, provide example code
        if maxkeys is None:
            maxkeys = len(self)

        return self.proto.fwmkeys(prefix, maxkeys)

    def sync(self):
        """Synchronizes updated content with the database."""
        # TODO: write better documentation: when would user need this?
        self.proto.sync()

    @property
    def query(self):
        """Returns a :class:`~pyrant.Query` object for the database."""
        return Query(self.proto, self.dbtype, self.literal)
