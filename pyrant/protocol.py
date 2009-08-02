"""
Protocol implementation for Tyrant 
<http://tokyocabinet.sourceforge.net/tyrantdoc/>
"""

import math
import socket
import struct

class TyrantError(Exception):
    """
    Tyrant error, socket and communication errors are not included here.
    """

# pyrant constants
MAGIC_NUMBER = 0xc8
ENCODING = 'UTF-8'


def _ulen(expr):
    return len(expr.encode(ENCODING)) \
            if isinstance(expr, unicode) else len(expr)


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
                v = str(v)
                buf += "%s%s" % (struct.pack(">I", len(v)), v)

    return "%s%s" % (struct.pack(fmt, MAGIC_NUMBER, code, *largs), buf)


class _TyrantSocket(object):
    # Socket logic. We use this class as a wrapper to raw sockets.

    def __init__(self, host, port):
        self._sock = socket.socket()
        self._sock.connect((host, port))
        self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)

    def __del__(self):
        self._sock.close()

    def send(self, *args, **kwargs):
        """Pack arguments and send the buffer to the socket"""
        sync = kwargs.pop('sync', True)
        # Send message to socket, then check for errors as needed.
        self._sock.sendall(_pack(*args))
        if not sync:
            return

        fail_code = ord(self.get_byte())
        if fail_code:
            raise TyrantError(fail_code)

    def recv(self, bytes):
        """Get given bytes from socket"""
        d = ''
        while len(d) < bytes:
            d += self._sock.recv(min(8192, bytes - len(d)))
        return d

    def get_byte(self):
        """Get 1 byte from socket."""
        return self.recv(1)

    def get_int(self):
        """Get an integer (4 bytes) from socket."""
        return struct.unpack('>I', self.recv(4))[0]

    def get_long(self):
        """Get a long (8 bytes) from socket."""
        return struct.unpack('>Q', self.recv(8))[0]

    def get_str(self):
        """Get a string (n bytes, which is an integer just before string)."""
        return self.recv(self.get_int())

    def get_unicode(self):
        """Get a unicode."""
        return self.get_str().decode(ENCODING)

    def get_double(self):
        """Get 2 long numbers (16 bytes) from socket"""
        intpart, fracpart = struct.unpack('>QQ', self.recv(16))
        return intpart + (fracpart * 1e-12)

    def get_strpair(self):
        """Get string pair (n bytes, n bytes which are 2 integers just 
        before pair)"""
        klen = self.get_int()
        vlen = self.get_int()
        return self.recv(klen), self.recv(vlen)


class TyrantProtocol(object):
    """Tyrant protocol raw implementation. There are all low level constants
    and operations. You can use it if you need that atomicity in your requests
    """

    # Protocol commands
    PUT = 0x10
    PUTKEEP = 0x11
    PUTCAT = 0x12
    PUTSHL = 0x13
    PUTNR = 0x18
    OUT = 0x20
    GET = 0x30
    MGET = 0x31
    VSIZ = 0x38
    ITERINIT = 0x50
    ITERNEXT = 0x51
    FWMKEYS = 0x58
    ADDINT = 0x60
    ADDDOUBLE = 0x61
    EXT = 0x68
    SYNC = 0x70
    VANISH = 0x72
    COPY = 0x73
    RESTORE = 0x74
    SETMST = 0x78
    RNUM = 0x80
    SIZE = 0x81
    STAT = 0x88
    MISC = 0x90

    # Query conditions
    RDBQCSTREQ = 0    # string is equal to 
    RDBQCSTRINC = 1   # string is included in 
    RDBQCSTRBW = 2    # string begins with 
    RDBQCSTREW = 3    # string ends with 
    RDBQCSTRAND = 4   # string includes all tokens in 
    RDBQCSTROR = 5    # string includes at least one token in 
    RDBQCSTROREQ = 6  # string is equal to at least one token in 
    RDBQCSTRRX = 7    # string matches regular expressions of 
    RDBQCNUMEQ = 8    # number is equal to 
    RDBQCNUMGT = 9    # number is greater than 
    RDBQCNUMGE = 10   # number is greater than or equal to 
    RDBQCNUMLT = 11   # number is less than 
    RDBQCNUMLE = 12   # number is less than or equal to 
    RDBQCNUMBT = 13   # number is between two tokens of 
    RDBQCNUMOREQ = 14 # number is equal to at least one token in 
    RDBQCNEGATE = 15  # negation flag 
    RDBQCNOIDX = 16   # no index flag 

    # Order
    RDBQOSTRASC = 0   # string ascending 
    RDBQOSTRDESC = 1  # string descending 
    RDBQONUMASC = 2   # number ascending 
    RDBQONUMDESC = 3  # number descending 

    # Opts
    RDBMONOULOG = 1
    RDBXOLCKREC = 1
    RDBXOLCKGLB = 2

    conditionsmap = {
        # String conditions
        'seq': RDBQCSTREQ,
        'scontains': RDBQCSTRINC,
        'sstartswith': RDBQCSTRBW,
        'sendswith': RDBQCSTREW,
        'smatchregex': RDBQCSTRRX,
        
        # Numbers conditions
        'neq': RDBQCNUMEQ,
        'ngt': RDBQCNUMGT,
        'nge': RDBQCNUMGE,
        'nlt': RDBQCNUMLT,
        'nle': RDBQCNUMLE,

        # Multiple conditions
        'scontains_or': RDBQCSTROR,
        'seq_or': RDBQCSTROREQ,
        'neq_or': RDBQCNUMOREQ
    }

    def __init__(self, host, port):
        self._sock = _TyrantSocket(host, port)

    def put(self, key, value):
        """Unconditionally set key to value
        """
        self._sock.send(self.PUT, _ulen(key), _ulen(value), key, value)

    def putkeep(self, key, value):
        """Set key to value if key does not already exist
        """
        self._sock.send(self.PUTKEEP, _ulen(key), _ulen(value), key, value)

    def putcat(self, key, value):
        """Append value to the existing value for key, or set key to
        value if it does not already exist
        """
        self._sock.send(self.PUTCAT, _ulen(key), _ulen(value), key, value)

    def putshl(self, key, value, width):
        """Equivalent to::

            self.putcat(key, value)
            self.put(key, self.get(key)[-width:])
        """
        self._sock.send(self.PUTSHL, _ulen(key), _ulen(value), width, key,
                        value)

    def putnr(self, key, value):
        """Set key to value without waiting for a server response
        """
        self._sock.send(self.PUTNR, _ulen(key), _ulen(value), key, value, 
                         sync=False)

    def out(self, key):
        """Remove key from server
        """
        self._sock.send(self.OUT, _ulen(key), key)

    def get(self, key, literal=False):
        """Get the value of a key from the server
        """
        self._sock.send(self.GET, _ulen(key), key)
        return self._sock.get_str() if literal else self._sock.get_unicode()

    def getint(self, key):
        """Get an integer for given key. Must been added by addint"""
        self._sock.send(self.GET, _ulen(key), key)
        val = self._sock.get_str()
        return struct.unpack('I', val)[0]

    def getdouble(self, key):
        """Get a double for given key. Must been added by adddouble"""
        self._sock.send(self.GET, _ulen(key), key)
        val = self._sock.get_str()
        intpart, fracpart = struct.unpack('>QQ', val)
        return intpart + (fracpart * 1e-12)

    def mget(self, klst):
        """Get key,value pairs from the server for the given list of keys
        """
        self._sock.send(self.MGET, len(klst), klst)
        numrecs = self._sock.get_int()
        return [self._sock.get_strpair() for i in xrange(numrecs)]

    def vsiz(self, key):
        """Get the size of a value for key
        """
        self._sock.send(self.VSIZ, _ulen(key), key)
        return self._sock.get_int()

    def iterinit(self):
        """Begin iteration over all keys of the database
        """
        self._sock.send(self.ITERINIT)

    def iternext(self):
        """Get the next key after iterinit
        """
        self._sock.send(self.ITERNEXT)
        return self._sock.get_unicode()

    def fwmkeys(self, prefix, maxkeys):
        """Get up to the first maxkeys starting with prefix
        """
        self._sock.send(self.FWMKEYS, _ulen(prefix), maxkeys, prefix)
        numkeys = self._sock.get_int()
        return [self._sock.get_unicode() for i in xrange(numkeys)]

    def addint(self, key, num):
        """Sum given integer to existing one
        """
        self._sock.send(self.ADDINT, _ulen(key), num, key)
        return self._sock.get_int()

    def adddouble(self, key, num):
        """Sum given double to existing one
        """
        fracpart, intpart = math.modf(num)
        fracpart, intpart = int(fracpart * 1e12), int(intpart)
        self._sock.send(self.ADDDOUBLE, _ulen(key), long(intpart), 
                        long(fracpart), key)
        return self._sock.get_double()

    def ext(self, func, opts, key, value):
        """Call func(key, value) with opts

        opts is a bitflag that can be RDBXOLCKREC for record locking
        and/or RDBXOLCKGLB for global locking"""
        self._sock.send(self.EXT, len(func), opts, _ulen(key), _ulen(value),
                        func, key, value)
        return self._sock.get_unicode()

    def sync(self):
        """Synchronize the database
        """
        self._sock.send(self.SYNC)

    def vanish(self):
        """Remove all records
        """
        self._sock.send(self.VANISH)

    def copy(self, path):
        """Hot-copy the database to path
        """
        self._sock.send(self.COPY, _ulen(path), path)

    def restore(self, path, msec):
        """Restore the database from path at timestamp (in msec)
        """
        self._sock.send(self.RESTORE, _ulen(path), msec, path)

    def setmst(self, host, port):
        """Set master to host:port
        """
        self._sock.send(self.SETMST, len(host), port, host)

    def rnum(self):
        """Get the number of records in the database
        """
        self._sock.send(self.RNUM)
        return self._sock.get_long()

    def size(self):
        """Get the size of the database
        """
        self._sock.send(self.SIZE)
        return self._sock.get_long()

    def stat(self):
        """Get some statistics about the database
        """
        self._sock.send(self.STAT)
        return self._sock.get_unicode()

    def search(self, conditions, limit=10, offset=0, 
               order_type=0, order_field=None, opts=0):
        """Search table elements. args should be (field, opt, expr) tuple
        """
        args = ["addcond\x00%s\x00%d\x00%s" % cond for cond in conditions]

        # Set order in query
        if order_field:
            args += ['setorder\x00%s\x00%d' % (order_field, order_type)]

        # Set limit and offset
        if limit > 0 and offset >= 0:
            args += ['setlimit\x00%d\x00%d' % (limit, offset)]

        return self.misc('search', args, opts)

    def misc(self, func, args, opts=0):
        """All databases support "putlist", "outlist", and "getlist".
        "putlist" is to store records. It receives keys and values one after
        the other, and returns an empty list.
        "outlist" is to remove records. It receives keys, and returns an empty
        list.
        "getlist" is to retrieve records. It receives keys, and returns values

        Table database supports "setindex", "search", "genuid".
        opts is a bitflag that can be:
            RDBMONOULOG to prevent writing to the update log
        """
        try:
            self._sock.send(self.MISC, len(func), opts, len(args), func, args)
        finally:
            numrecs = self._sock.get_int()
        
        return [self._sock.get_unicode() for i in xrange(numrecs)]


