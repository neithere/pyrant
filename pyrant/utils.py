# -*- coding: utf-8 -*-

from protocol import DB_TABLE, TABLE_COLUMN_SEP


def to_python(elem, dbtype, sep=None):
    """Returns pythonic representation of a database record."""
    if dbtype == DB_TABLE:
        # Split element by \x00 which is the column separator
        elems = elem.split(TABLE_COLUMN_SEP)
        if elems[0]:
            return dict((elems[i], elems[i+1]) for i in xrange(0, len(elems), 2))
        else:
            return
    
    if sep and sep in elem:
        return elem.split(sep)

    return elem

def csv_to_dict(lines):
    return dict(line.split('\t', 1) for line in lines.splitlines() if line)
