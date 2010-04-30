# -*- coding: utf-8 -*-
"""
Protocol implementation for `Tokyo Tyrant <http://1978th.net/tokyotyrant/>`_.

Let's assume some defaults for our sandbox::

    >>> TEST_HOST = '127.0.0.1'
    >>> TEST_PORT = 1983    # default port is 1978

"""

import math
import socket
import struct

import exceptions


# Pyrant constants
MAGIC_NUMBER = 0xc8
ENCODING = 'UTF-8'
ENCODING_ERROR_HANDLING = 'strict'    # set to 'replace' or 'ignore' if needed

# Table Types
DB_BTREE  = 'B+ tree'
DB_TABLE  = 'table'
DB_MEMORY = 'on-memory hash'
DB_HASH   = 'hash'

TABLE_COLUMN_SEP = '\x00'

def _ulen(expr):
    "Returns length of the string in bytes."
    return len(expr.encode(ENCODING)) if isinstance(expr, unicode) else len(expr)

def _pack(code, *args):
    # Craft string that we'll use to send data based on args type and content
    buf = ''
    fmt = '>BB'
    largs = []
    for arg in args:
        if isinstance(arg, int):
            fmt += 'I'
            largs.append(arg)

        elif isinstance(arg, str):
            buf += arg

        elif isinstance(arg, unicode):
            buf += arg.encode(ENCODING)

        elif isinstance(arg, long):
            fmt += 'Q'
            largs.append(arg)

        elif isinstance(arg, (list, tuple)):
            for v in arg:
                if isinstance(v, unicode):
                    v = v.encode(ENCODING)
                else:
                    v = str(v)
                buf += "%s%s" % (struct.pack(">I", len(v)), v)

    return "%s%s" % (struct.pack(fmt, MAGIC_NUMBER, code, *largs), buf)


class _TyrantSocket(object):
    """
    Socket logic. We use this class as a wrapper to raw sockets.
    """

    def __init__(self, host, port, timeout=None):
        self._sock = socket.socket()
        if not timeout is None:
            self._sock.settimeout(timeout)
        self._sock.connect((host, port))
        self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def __del__(self):
        self._sock.close()

    def send(self, *args, **kwargs):
        """
        Packs arguments and sends the buffer to the socket.
        """
        sync = kwargs.pop('sync', True)
        # Send message to socket, then check for errors as needed.
        self._sock.sendall(_pack(*args))
        if not sync:
            return

        fail_code = ord(self.get_byte())
        if fail_code:
            raise exceptions.get_for_code(fail_code)

    def recv(self, bytes):
        """
        Retrieves given number of bytes from the socket and returns them as
        string.
        """
        d = ''
        while len(d) < bytes:
            c = self._sock.recv(min(8192, bytes - len(d)))
            if not c:
                raise socket.error('server disconnected unexpectedly')
            d += c
        return d

    def get_byte(self):
        """
        Retrieves one byte from the socket and returns it.
        """
        return self.recv(1)

    def get_int(self):
        """
        Retrieves an integer (4 bytes) from the socket and returns it.
        """
        return struct.unpack('>I', self.recv(4))[0]

    def get_long(self):
        """
        Retrieves a long integer (8 bytes) from the socket and returns it.
        """
        return struct.unpack('>Q', self.recv(8))[0]

    def get_str(self):
        """
        Retrieves a string (n bytes, which is an integer just before string)
        from the socket and returns it.
        """
        return self.recv(self.get_int())

    def get_unicode(self):
        """
        Retrieves a unicode string from the socket and returns it. This method
        uses :meth:`get_str`, which in turn makes use of :meth:`get_int`.
        """
        return self.get_str().decode(ENCODING, ENCODING_ERROR_HANDLING)

    def get_double(self):
        """
        Retrieves two long integers (16 bytes) from the socket and returns them.
        """
        intpart, fracpart = struct.unpack('>QQ', self.recv(16))
        return intpart + (fracpart * 1e-12)

    def get_strpair(self):
        """
        Retrieves a pair of strings (n bytes, n bytes which are 2 integers just
        before the pair) and returns them as a tuple of strings.
        """
        klen = self.get_int()
        vlen = self.get_int()
        return self.recv(klen), self.recv(vlen)


class TyrantProtocol(object):
    """
    A straightforward implementation of the Tokyo Tyrant protocol. Provides all
    low level constants and operations. Provides a level of abstraction that is
    just enough to communicate with server from Python using Tyrant API.

    More sophisticated APIs can be built on top of this class. Two of them are
    included in pyrant: the dict-like API (:class:`~pyrant.Pyrant`) and the
    query API (:class:`~pyrant.query.Query`).

    Let's connect to a sanbdox Tyrant server::

        >>> from pyrant import protocol
        >>> p = protocol.TyrantProtocol(host=TEST_HOST, port=TEST_PORT)

        # remove anything that could be left from previous time
        >>> p.vanish()

        # make sure there are zero records in the database
        >>> p.rnum()
        0

    """

    # Protocol commands

    PUT       = 0x10
    PUTKEEP   = 0x11
    PUTCAT    = 0x12
    PUTSHL    = 0x13
    PUTNR     = 0x18
    OUT       = 0x20
    GET       = 0x30
    MGET      = 0x31
    VSIZ      = 0x38
    ITERINIT  = 0x50
    ITERNEXT  = 0x51
    FWMKEYS   = 0x58
    ADDINT    = 0x60
    ADDDOUBLE = 0x61
    EXT       = 0x68
    SYNC      = 0x70
    VANISH    = 0x72
    COPY      = 0x73
    RESTORE   = 0x74
    SETMST    = 0x78
    RNUM      = 0x80
    SIZE      = 0x81
    STAT      = 0x88
    MISC      = 0x90

    # Query conditions

    RDBQCSTREQ   = 0     # string is equal to
    RDBQCSTRINC  = 1     # string is included in
    RDBQCSTRBW   = 2     # string begins with
    RDBQCSTREW   = 3     # string ends with
    RDBQCSTRAND  = 4     # string includes all tokens in
    RDBQCSTROR   = 5     # string includes at least one token in
    RDBQCSTROREQ = 6     # string is equal to at least one token in
    RDBQCSTRRX   = 7     # string matches regular expressions of
    RDBQCNUMEQ   = 8     # number is equal to
    RDBQCNUMGT   = 9     # number is greater than
    RDBQCNUMGE   = 10    # number is greater than or equal to
    RDBQCNUMLT   = 11    # number is less than
    RDBQCNUMLE   = 12    # number is less than or equal to
    RDBQCNUMBT   = 13    # number is between two tokens of
    RDBQCNUMOREQ = 14    # number is equal to at least one token in
    RDBQCFTSPH   = 15    # full-text search with the phrase of
    RDBQCFTSAND  = 16    # full-text search with all tokens in
    RDBQCFTSOR   = 17    # full-text search with at least one token in
    RDBQCFTSEX   = 18    # full-text search with the compound expression of

    RDBQCNEGATE  = 1 << 24    # negation flag
    RDBQCNOIDX   = 1 << 25    # no index flag

    # Order types

    RDBQOSTRASC  = 0    # string ascending
    RDBQOSTRDESC = 1    # string descending
    RDBQONUMASC  = 2    # number ascending
    RDBQONUMDESC = 3    # number descending

    # Operation types

    TDBMSUNION = 0    # union
    TDBMSISECT = 1    # intersection
    TDBMSDIFF  = 2    # difference

    # Miscellaneous operation options

    RDBMONOULOG = 1    # omission of update log

    # Scripting extension options

    RDBXOLCKREC = 1    # record locking
    RDBXOLCKGLB = 2    # global locking

    # Index types (for table databases)

    TDBITLEXICAL = 0    # lexical string
    TDBITDECIMAL = 1 # decimal string
    TDBITTOKEN = 2 # token inverted index
    TDBITQGRAM = 3 # q-gram inverted index
    TDBITOPT = 9998 # optimize index
    TDBITVOID = 9999 # remove index
    TDBITKEEP = 1 << 24 # keep existing index

    def __init__(self, host, port, timeout=None):
        # connect to the remote database
        self._sock = _TyrantSocket(host, port, timeout)
        # expose connection info (not used internally)
        self.host = host
        self.port = port

    def put(self, key, value):
        """
        Unconditionally sets key to value::

            >>> p.put(u'foo', u'bar\x00baz')
            >>> p.rnum()
            1
            >>> p.put('fox', u'box\x00quux')
            >>> p.rnum()
            2

        """
        self._sock.send(self.PUT, _ulen(key), _ulen(value), key, value)

    def putkeep(self, key, value):
        """
        Sets key to value if key does not already exist.
        """
        self._sock.send(self.PUTKEEP, _ulen(key), _ulen(value), key, value)

    def putcat(self, key, value):
        """
        Appends value to the existing value for key, or sets key to value if it
        does not already exist.
        """
        self._sock.send(self.PUTCAT, _ulen(key), _ulen(value), key, value)

    def putshl(self, key, value, width):
        """
        Equivalent to::

            self.putcat(key, value)
            self.put(key, self.get(key)[-width:])

        """
        self._sock.send(self.PUTSHL, _ulen(key), _ulen(value), width, key,
                        value)

    def putnr(self, key, value):
        """
        Sets key to value without waiting for a server response.
        """
        self._sock.send(self.PUTNR, _ulen(key), _ulen(value), key, value,
                        sync=False)

    def out(self, key):
        """
        Removes key from server.
        """
        self._sock.send(self.OUT, _ulen(key), key)

    def genuid(self):
        """
        Generates and returns a unique primary key. Raises `ValueError` if the
        database could not return sensible data.
        """
        res = self.misc('genuid', [])
        if not len(res) == 1 or not res[0]:
            raise ValueError('Could not generate primary key: got "%s"' % res)
        return res[0]

    def get(self, key, literal=False):
        """
        Returns the value of `key` as stored on the server::

            >>> p.get(u'foo')
            u'bar\x00baz'
            >>> p.get(u'fox')
            u'box\x00quux'

        """
        self._sock.send(self.GET, _ulen(key), key)
        return self._sock.get_str() if literal else self._sock.get_unicode()

    def getint(self, key):
        """
        Returns an integer for given `key`. Value must be set by
        :meth:`~pyrant.protocol.TyrantProtocol.addint`.
        """
        return self.addint(key)

    def getdouble(self, key):
        """
        Returns a double for given key. Value must be set by
        :meth:`~adddouble`.
        """
        return self.adddouble(key)

    def mget(self, keys):
        """
        Returns key,value pairs from the server for the given list of keys::

            >>> p.mget(['foo', 'fox'])
            [('foo', 'bar\x00baz'), ('fox', 'box\x00quux')]

        """
        self._sock.send(self.MGET, len(keys), keys)
        numrecs = self._sock.get_int()
        return [self._sock.get_strpair() for i in xrange(numrecs)]

    def vsiz(self, key):
        """
        Returns the size of a value for given key.
        """
        self._sock.send(self.VSIZ, _ulen(key), key)
        return self._sock.get_int()

    def iterinit(self):
        """
        Begins iteration over all keys of the database.

            >>> p.iterinit()    # now we can call iternext()

        """
        self._sock.send(self.ITERINIT)

    def iternext(self):
        """
        Returns the next key after ``iterinit`` call. Raises an exception which
        is subclass of :class:`~pyrant.protocol.TyrantError` on iteration end::

            >>> p.iternext()  # assume iterinit() was already called
            u'foo'
            >>> p.iternext()
            u'fox'
            >>> p.iternext()
            Traceback (most recent call last):
                ...
            InvalidOperation

        """
        self._sock.send(self.ITERNEXT)
        return self._sock.get_unicode()

    def fwmkeys(self, prefix, maxkeys=-1):
        """
        Get up to the first maxkeys starting with prefix
        """
        self._sock.send(self.FWMKEYS, _ulen(prefix), maxkeys, prefix)
        numkeys = self._sock.get_int()
        return [self._sock.get_unicode() for i in xrange(numkeys)]

    def addint(self, key, num=0):
        """
        Adds given integer to existing one. Stores and returns the sum.
        """
        self._sock.send(self.ADDINT, _ulen(key), num, key)
        return self._sock.get_int()

    def adddouble(self, key, num=0.0):
        """
        Adds given double to existing one. Stores and returns the sum.
        """
        fracpart, intpart = math.modf(num)
        fracpart, intpart = int(fracpart * 1e12), int(intpart)
        self._sock.send(self.ADDDOUBLE, _ulen(key), long(intpart),
                        long(fracpart), key)
        return self._sock.get_double()

    def ext(self, func, opts, key, value):
        """
        Calls ``func(key, value)`` with ``opts``.

        :param opts: a bitflag that can be `RDBXOLCKREC` for record locking
            and/or `RDBXOLCKGLB` for global locking.
        """
        self._sock.send(self.EXT, len(func), opts, _ulen(key), _ulen(value),
                        func, key, value)
        return self._sock.get_unicode()

    def sync(self):    # TODO: better documentation (why would someone need this?)
        """
        Synchronizes the updated contents of the remote database object with the
        file and the device.
        """
        self._sock.send(self.SYNC)

    def vanish(self):
        """
        Removes all records from the database.
        """
        self._sock.send(self.VANISH)

    def copy(self, path):
        """
        Hot-copies the database to given path.
        """
        self._sock.send(self.COPY, _ulen(path), path)

    def restore(self, path, msec):
        """
        Restores the database from `path` at given timestamp (in `msec`).
        """
        self._sock.send(self.RESTORE, _ulen(path), msec, path)

    def setmst(self, host, port):
        """
        Sets master to `host`:`port`.
        """
        self._sock.send(self.SETMST, len(host), port, host)

    def rnum(self):
        """
        Returns the number of records in the database.
        """
        self._sock.send(self.RNUM)
        return self._sock.get_long()

    def add_index(self, name, kind=None, keep=False):
        """
        Sets index on given column. Returns `True` if index was successfully
        created.

        :param name: column name for which index should be set.
        :param kind: index type, one of: `lexical`, `decimal`, `token`,
            `q-gram`.
        :param keep: if True, index is only created if it did not yet exist.
            Default is False, i.e. any existing index is reset.

        .. note:: we have chosen not to mimic the original API here because it
            is a bit too confusing. Instead of a single cumbersome function
            Pyrant provides three: :meth:`~add_index`, :meth:`~optimize_index`
            and :meth:`~drop_index`. They all do what their names suggest.

        """
        # TODO: replace "kind" with keyword arguments
        TYPES = {
            'lexical': self.TDBITLEXICAL,
            'decimal': self.TDBITDECIMAL,
            'token':   self.TDBITTOKEN,
            'q-gram':  self.TDBITQGRAM,
        }
        kind = 'lexical' if kind is None else kind
        assert kind in TYPES, 'unknown index type "%s"' % kind
        type_code = TYPES[kind]
        if keep:
            type_code |= self.TDBITKEEP
        try:
            self.misc('setindex', [name, type_code])
        except exceptions.InvalidOperation:
            return False
        else:
            return True

    def optimize_index(self, name):
        """
        Optimizes index for given column. Returns `True` if the operation was
        successfully performed. In most cases the operation fails when the
        index does not exist. You can add index using :meth:`~add_index`.
        """
        try:
            self.misc('setindex', [name, self.TDBITOPT])
        except exceptions.InvalidOperation:
            return False
        else:
            return True

    def drop_index(self, name):
        """
        Removes index for given column. Returns `True` if the operation was
        successfully performed. In most cases the operation fails when the
        index doesn't exist. You can add index using :meth:`~add_index`.
        """
        try:
            self.misc('setindex', [name, self.TDBITVOID])
        except exceptions.InvalidOperation:
            return False
        else:
            return True

    def size(self):
        """
        Returns the size of the database in bytes.
        """
        self._sock.send(self.SIZE)
        return self._sock.get_long()

    def stat(self):
        """
        Returns some statistics about the database.
        """
        self._sock.send(self.STAT)
        return self._sock.get_unicode()

    def search(self, conditions, limit=10, offset=0,
               order_type=0, order_column=None, opts=0,
               ms_conditions=None, ms_type=None, columns=None,
               out=False, count=False, hint=False):
        """
        Returns list of keys for elements matching given ``conditions``.

        :param conditions: a list of tuples in the form ``(column, op, expr)``
            where `column` is name of a column and `op` is operation code (one of
            TyrantProtocol.RDBQC[...]). The conditions are implicitly combined
            with logical AND. See `ms_conditions` and `ms_type` for more complex
            operations.
        :param limit: integer. Defaults to 10.
        :param offset: integer. Defaults to 0.
        :param order_column: string; if defined, results are sorted by this
            column using default or custom ordering method.
        :param order_type: one of TyrantProtocol.RDBQO[...]; if defined along
            with `order_column`, results are sorted by the latter using given
            method. Default is RDBQOSTRASC.
        :param opts: a bitflag (see
            :meth:`~pyrant.protocol.TyrantProtocol.misc`
        :param ms_conditions: MetaSearch conditions.
        :param ms_type: MetaSearch operation type.
        :param columns: iterable; if not empty, returns only given columns for
            matched records.
        :param out: boolean; if True, all items that correspond to the query are
            deleted from the database when the query is executed.
        :param count: boolean; if True, the return value is the number of items
            that correspond to the query.
        :param hint: boolean; if True, the hint string is added to the return
            value.
        """

        # TODO: split this function into separate functions if they return
        # different results:
        #
        # - search      = misc('search', [])        --> list of keys
        # - searchget   = misc('search', ['get'])   --> list of items
        # - searchout   = misc('search', ['out'])   --> boolean
        # - searchcount = misc('search', ['count']) --> integer
        #
        # Some functions should be of course left as keywords for the
        # above-mentioned functions:
        #
        # - addcond     = misc('search', ['addcond...'])
        # - setorder    = misc('search', ['setorder...'])
        # - setlimit    = misc('search', ['setlimit...'])
        # - hint        = misc('search', ['hint'])
        # - metasearch stuff, including functions 'mstype', 'addcond' and 'next'.
        #
        # See http://1978th.net/tokyotyrant/spex.html#tcrdbapi

        # sanity check
        assert limit  is None or 0 <= limit, 'wrong limit value "%s"' % limit
        assert offset is None or 0 <= offset, 'wrong offset value "%s"' % offset
        if offset and not limit:
            # this is required by TDB API. Could be worked around somehow?
            raise ValueError('Offset cannot be specified without limit.')
        assert ms_type in (None, self.TDBMSUNION, self.TDBMSISECT, self.TDBMSDIFF)
        assert order_type in (self.RDBQOSTRASC, self.RDBQOSTRDESC,
                              self.RDBQONUMASC, self.RDBQONUMDESC)

        # conditions
        args = ['addcond\x00%s\x00%d\x00%s' % cond for cond in conditions]

        # MetaSearch support (multiple additional queries, one Boolean operation)
        if ms_type is not None and ms_conditions:
            args += ['mstype\x00%s' % ms_type]
            for conds in ms_conditions:
                args += ['next']
                args += ['addcond\x00%s\x00%d\x00%s' % cond for cond in conds]

        # return only selected columns
        if columns:
            args += ['get\x00%s' % '\x00'.join(columns)]

        # set order in query
        if order_column:
            args += ['setorder\x00%s\x00%d' % (order_column, order_type)]

        # set limit and offset
        if limit:   # and 0 <= offset:
            # originally this is named setlimit(max,skip).
            # it is *not* possible to specify offset without limit.
            args += ['setlimit\x00%d\x00%d' % (limit, offset)]

        # drop all records yielded by the query
        if out:
            args += ['out']

        if count:
            args += ['count']

        if hint:
            args += ['hint']

        return self.misc('search', args, opts)

    def misc(self, func, args, opts=0):
        """
        Executes custom function.

        :param func: the function name (see below)
        :param opts: a bitflag (see below)

        Functions supported by all databases:

        * `putlist` stores records. It receives keys and values one after
          the other, and returns an empty list.
        * `outlist` removes records. It receives keys, and returns
          an empty list.
        * `getlist` retrieves records. It receives keys, and returns values.

        Functions supported by the table database (in addition to mentioned above):

        * `setindex`
        * `search`
        * `genuid`.

        Possible options:

        * :const:`TyrantProtocol.RDBMONOULOG` to prevent writing to the update log.
        """
        try:
            self._sock.send(self.MISC, len(func), opts, len(args), func, args)
        finally:
            numrecs = self._sock.get_int()

        return [self._sock.get_unicode() for i in xrange(numrecs)]
