# -*- coding: utf-8 -*-

class TestBase:
    """
    >>> import pyrant
    >>> from pyrant import Tyrant, Q
    >>> t = Tyrant(host='127.0.0.1', port=1983)    # default port is 1978
    >>> t.clear()

    """
    def basic(self):
        """
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

    def get_default(self):
        """
        >>> t['foo'] = {'a': 'z', 'b': 'y'}
        >>> print t.get('foo', {})
        {u'a': u'z', u'b': u'y'}
        >>> print t.get('bar', {})
        {}

        """

class TestTable(TestBase):
    """
    >>> t.dbtype == pyrant.DBTYPETABLE
    True

    """

    def query_filter(self):
        """
        >>> t['i'] = {'name': 'Reflejo', 'test': 0}
        >>> t['you'] = {'name': 'Fulano', 'test': 1}
        >>> res = t.query.filter(Q(name='Reflejo'), Q(test=0))
        >>> key, data = res[0]
        >>> key
        u'i'
        >>> data['name']
        u'Reflejo'
        >>> res = t.query.filter(Q(name='Reflejo') | Q(name='Fulano'))
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
        >>> q = t.query.order('test')
        >>> [x[0] for x in q]
        [u'i', u'you', u'foo']
        >>> q = q.order('-#test')
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
        >>> t.query.stat()
        {u'a': 1, u'test': 2, u'b': 1, u'name': 2}

        """
