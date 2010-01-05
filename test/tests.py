# -*- coding: utf-8 -*-

"""
>>> TEST_HOST = '127.0.0.1'
>>> TEST_PORT = 1983    # default port is 1978

"""

class TestProtocol(object):
    """
    >>> from pyrant import protocol
    >>> p = protocol.TyrantProtocol(host=TEST_HOST, port=TEST_PORT)
    >>> p.vanish()
    >>> p.rnum()
    0

    """
    def add_item(self):
        """
        >>> p.put(u'foo', u'bar\0baz')      # TDB assumed
        >>> p.rnum()
        1
        >>> p.put('fox', u'box\0quux')
        >>> p.rnum()
        2

        """

    def get_item(self):
        """
        >>> p.get(u'foo')
        u'bar\x00baz'
        >>> p.get(u'fox')
        u'box\x00quux'

        """

    def iterate_over_items(self):
        """
        >>> p.iterinit()
        >>> p.iternext()
        u'foo'
        >>> p.iternext()
        u'fox'
        >>> p.iternext()
        Traceback (most recent call last):
            ...
        TyrantError: 1

        """

    def get_multiple_items_at_once(self):
        """
        >>> p.mget(['foo', 'fox'])
        [('foo', 'bar\x00baz'), ('fox', 'box\x00quux')]

        """

    def remove_item(self):
        """
        >>> p.out('fox')
        >>> p.get('fox')
        Traceback (most recent call last):
            ...
        TyrantError: 1

        """


class TestTyrant(object):
    """
    >>> import pyrant
    >>> from pyrant import Tyrant
    >>> t = Tyrant(host=TEST_HOST, port=TEST_PORT)
    >>> t.clear()

    """
    def basic_usage(self):
        """
        >>> if t.dbtype != pyrant.protocol.DB_TABLE:
        ...     t['key'] = 'foo'
        ...     print t['key']
        ... else:
        ...     t['key'] = {'name': 'foo'}
        ...     print t['key']['name']
        foo
        >>> del t['key']
        >>> t['key']
        Traceback (most recent call last):
            ...
        KeyError: 'key'

        """

    def get_default(self):
        """
        >>> t['foo'] = {'a': 'z', 'b': 'y'}
        >>> t.get('foo', {}) == {u'a': u'z', u'b': u'y'}
        True
        >>> t.get('bar', {})
        {}

        """


class TestTable(TestTyrant):
    """
    # Table extension for Tyrant

    >>> t.dbtype == pyrant.protocol.DB_TABLE
    True

    """

    def query_filter(self):
        """
        >>> t['i'] = {'name': 'Reflejo', 'test': 0}
        >>> t['you'] = {'name': 'Fulano', 'test': 1}
        >>> res = t.query.filter(name='Reflejo', test=0)
        >>> key, data = res[0]
        >>> key
        u'i'
        >>> data['name']
        u'Reflejo'
        >>> res = t.query.filter(name='Reflejo') | t.query.filter(name='Fulano')
        >>> len(res)
        2

        """

    def query_exclude(self):
        """
        >>> q = t.query.exclude(name='Fulano')
        >>> [x[0] for x in q]
        [u'foo', u'i']

        """

    def query_order(self):
        """
        >>> q = t.query.order_by('test')
        >>> [x[0] for x in q]
        [u'i', u'you', u'foo']
        >>> q = q.order_by('-test')
        >>> [x[0] for x in q]
        [u'you', u'i', u'foo']

        """

    def query_values(self):
        """
        >>> t.query.values('name')
        [u'Reflejo', u'Fulano']
        >>> t.query.values('test')
        [u'1', u'0']
        >>> t.query.values('camelot')
        []

        """

    def query_stat(self):
        """
        >>> t.query.stat() == {u'a': 1, u'test': 2, u'b': 1, u'name': 2}
        True

        """
