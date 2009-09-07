# -*- coding: utf-8 -*-

from protocol import DB_TABLE


def to_python(elem, dbtype, sep=None):
    """Returns pythonic representation of a database record."""
    if dbtype == DB_TABLE:
        # Split element by \x00 which is the column separator
        elems = elem.split('\x00')
        if not elems[0]:
            return None

        return dict((elems[i], elems[i + 1]) \
                        for i in xrange(0, len(elems), 2))
    elif sep and sep in elem:
        return elem.split(sep)

    return elem
