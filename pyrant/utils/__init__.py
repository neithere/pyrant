# -*- coding: utf-8 -*-

import warnings
from pyrant.protocol import DB_TABLE, TABLE_COLUMN_SEP


def pairwise(elems):
    """
    Splits given iterable in pairs. Returns a generator. If number of elems is
    odd, None is added to the end of list::

        >>> from pyrant.utils import pairwise
        >>> list(pairwise(['a', 'b', 'c', 'd']))
        [('a', 'b'), ('c', 'd')]
        >>> list(pairwise(['a', 'b', 'c']))
        [('a', 'b'), ('c', None)]

    """
    assert hasattr(elems, '__iter__')
    elems = list(elems)
    if len(elems) % 2:
        elems.append(None)
    for i in xrange(0, len(elems), 2):
        yield elems[i], elems[i+1]

def from_python(value):
    """
    Returns value prepared for storage. This is required for search because
    some Python types cannot be converted to string and back without changes
    in semantics, e.g. bools (True-->"True"-->True and False-->"False"-->True)
    and NoneType (None-->"None"-->"None")::

        >>> from pyrant.utils import from_python

        >>> bool(None) == bool(str(from_python(None)))
        True
        >>> bool(True) == bool(str(from_python(True)))
        True
        >>> bool(False) == bool(str(from_python(False)))
        True

    Such behaviour is achieved this way::

        >>> from_python('text')
        'text'
        >>> from_python(0)
        0
        >>> from_python(123)
        123
        >>> from_python(True)
        1
        >>> from_python(False)
        ''
        >>> from_python(None)
        ''

    Note that we don't convert the value to bytes here, it's done by
    pyrant.protocol._pack.
    """
    if value is None:
        return ''
    if isinstance(value, bool):
        return 1 if value else ''
    return value

def to_python(value, db_type, sep=None):
    """
    Returns pythonic representation of a database record::

        >>> from pyrant.protocol import DB_HASH, DB_TABLE
        >>> from pyrant.utils import to_python

        # hash database

        >>> to_python('foo', DB_HASH)
        'foo'
        >>> to_python('foo\x00bar', DB_HASH)
        'foo\x00bar'
        >>> to_python('foo\x00bar, baz, quux', DB_HASH, sep=', ')
        ['foo\x00bar', 'baz', 'quux']

        # table database

        >>> to_python('foo', DB_TABLE)
        {'foo': None}
        >>> to_python('foo\x00bar', DB_TABLE)
        {'foo': 'bar'}
        >>> to_python('foo\x00bar, baz, quux', DB_TABLE, sep=', ')
        {'foo': ['bar', 'baz', 'quux']}
        >>> to_python('foo\x00bar\x00baz\x00quux, 123', DB_TABLE, sep=', ')
        {'foo': 'bar', 'baz': ['quux', '123']}

    Note that despite nasty black magic (with incessant fire- and fairyworks)
    takes place here, still we cannot fully restore the true pythonic meaning
    of the ancient writings in runes called "bytes". Only those who possess some
    complementary knowledge called `metadata` or `models`_ can give stringified
    values their true intimate names, whether to Python types they belong or to
    custom classes like datetime.date. May code bless you with a segfault.

    .. _models: http://pypi.python.org/pypi/pymodels
    """
    if db_type == DB_TABLE:
        # Split element by \x00 which is the column separator
        elems = value.split(TABLE_COLUMN_SEP)
        if not elems or not elems[0]:
            return {}
        #elems_len = len(elems)
        #if elems_len % 2:
        #    warnings.warn(u'odd number of key/value pairs in table record: %s'
        #                  % value, Warning)
        #    return {}
        return dict((k, _elem_to_python(v, sep)) for k,v in pairwise(elems))
    else:
        return _elem_to_python(value, sep)

def _elem_to_python(elem, sep):
    if sep and sep in elem:
        return elem.split(sep)
    else:
        return elem

def csv_to_dict(lines):
    return dict(line.split('\t', 1) for line in lines.splitlines() if line)
