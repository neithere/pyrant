# -*- coding: utf-8 -*-

""" Query classes for Tokyo Tyrant implementation.
"""

import copy

from protocol import TyrantProtocol
from utils import to_python


MAX_RESULTS = 1000


class Q(object):
    """Condition object. You can | this type to ORs conditions,
    but you cannot use operand "&", to do this just add more :class:`~pyrant.Q`s
    to your :meth:`~pyrant.Query.filter`.
    """
    # TODO: write better documentation: provide example code

    def __init__(self, **kwargs):
        assert kwargs, "You need to specify at least one condition"

        # FIXME all kwargs are ignored except for the last pair!!
        for kw, val in kwargs.iteritems():
            nameop = kw.split('__')
            self.name = nameop[0]
            if isinstance(val, (tuple, list)):
                if val and isinstance(val[0], int):
                    self._op = 'n'
                else:
                    self._op = 's'

                self._op += nameop[1] if len(nameop) > 1 else 'eq_or'
                self.expr = " ".join(map(str, val))
            else:
                self._op = 's' if isinstance(val, (str, unicode)) else 'n'
                self._op += nameop[1] if len(nameop) > 1 else 'eq'
                self.expr = val

        self.negate = False

    def __or__(self, q):
        assert isinstance(q, Q), "Unsupported operand type(s) for |"

        op = '%s_or' % q._op

        assert op in TyrantProtocol.conditionsmap, ('Expected one of ("%s"), '
            'got "%s"' % ('", "'.join(TyrantProtocol.conditionsmap.keys()), op))

        assert q._op == self._op, ('Cannot apply disjunction to Q objects with '
                                   'different operations (%s vs. %s)' % (q._op, self._op))

        # FIXME this is actually a bug:
        assert q.name == self.name, ('Cannot apply disjunction to Q objects with '
                                     'different column names')

        if q._op == self._op and op in TyrantProtocol.conditionsmap:
            qcopy = q._clone()
            qcopy._op = op
            qcopy.expr = "%s,%s" % (q.expr , self.expr)

            return qcopy
        else:
            raise TypeError("Unsoported operand for |. You can only do this "\
                            "on contains or eq")

    @property
    def op(self):
        op = TyrantProtocol.conditionsmap[self._op]
        return op | TyrantProtocol.RDBQCNEGATE if self.negate else op

    def __repr__(self):
        return "%s [%s] %s" % (self.name, self.op, self.expr)

    def _clone(self):
        return copy.copy(self)


class Query(object):
    """Query table operations. This is a lazy object
    that abstract all queries for tyrant protocol.
    """

    def __init__(self, proto, dbtype, literal=False, conditions=None):
        if conditions:
            assert isinstance(conditions, list) and \
                   all(isinstance(c,Q) for c in conditions), \
                   'Expected a list of Q instances, got %s' % conditions
        self._conditions = conditions or []
        self._order = None
        self._order_t = 0
        self._cache = {}
        self._proto = proto
        self._dbtype = dbtype
        self.literal = literal

    def _clone(self):
        conditions = [q._clone() for q in self._conditions]
        query = Query(self._proto, self._dbtype, literal=self.literal,
                      conditions=conditions)
        return query

    @staticmethod
    def _decorate(k, v):
        return (k, v)

    def _to_python(self, key):
        elem = self._proto.get(key, self.literal)
        elem = to_python(elem, self._dbtype)
        return self._decorate(key, elem)

    def order(self, name):
        """Defines order in which results should be retrieved.

        :param name: the column name. If prefixed with ``-``, direction is changed
            from ascending (default) to descending.
            If prefixed with ``#``, values are treated as numbers.

        Examples::

            q.order('name')       # ascending
            q.order('-name')      # descending
            q.order('-#ranking')  # descending, numeric

        """
        if name.startswith('-'):
            if name.startswith('-#'):
                order, order_t = name[2:], TyrantProtocol.RDBQONUMDESC
            else:
                order, order_t = name[1:], TyrantProtocol.RDBQOSTRDESC
        elif name.startswith('#'):
            order, order_t = name[1:], TyrantProtocol.RDBQONUMASC
        else:
            order, order_t = name, TyrantProtocol.RDBQOSTRASC

        query = self._clone()

        if self._order == order and self._order_t == order_t:
            # provide link to existing cache
            query._cache = self._cache
        query._order = order
        query._order_t = order_t

        return query

    def exclude(self, *args, **kwargs):
        """Antipode of :meth:`~pyrant.Query.filter`."""
        return self._filter(True, args, kwargs)

    def filter(self, *args, **kwargs):    # TODO: provide full list of lookups
        """Returns a clone of the Query object with given conditions applied.

        Conditions can be specified as :class:`~pyrant.Q` objects and/or
        keyword arguments.

        Supported keyword lookups are:

            * __eq: Equals (default) to expression
            * __lt: Less than expression
            * __le: Less or equal to expression
            * __gt: Greater than expression
            * __ge: Greater or equal to expression

        Usage:

            connect to a remote table database:

            >>> t = Tyrant()
            >>> t.get_stats()['type']
            u'table'

            stuff some data into the storage:

            >>> t['a'] = {'name': 'Foo', 'price': 1}
            >>> t['b'] = {'name': 'Bar', 'price': 2}
            >>> t['c'] = {'name': 'Foo', 'price': 3}

            find everything with price > 1:

            >>> [x[0] for x in t.query.filter(price__gt=1)]
            ['b', 'c']

            find everything with name "Foo":

            >>> [x[0] for x in t.query.filter(name='Foo')]
            ['a', 'c']

            chain queries:

            >>> cheap_items = t.query.filter(price__lt=3)
            >>> cheap_bars = cheap_items.filter(name='Bar')
            >>> [x[0] for x in cheap_items]
            ['a', 'b']
            >>> [x[0] for x in cheap_bars]
            ['b']

        """
        return self._filter(False, args, kwargs)

    def _filter(self, negate, args, kwargs):
        query = self._clone()

        # Iterate arguments. Should be instances of Q
        for cond in args:
            assert isinstance(cond, Q), "Arguments must be instances of Q"
            q = cond._clone()
            q.negate = q.negate ^ negate
            query._conditions.append(q)

        # Generate Q with arguments as needed
        for name, expr in kwargs.iteritems():
            q = Q(**{name: expr})
            q.negate = negate
            query._conditions.append(q)

        return query

    def values(self, key):
        "Returns a list of unique values for given key."
        collected = {}
        for _, data in self[:]:
            for k,v in data.iteritems():
                if k == key and v not in collected:
                    collected[v] = 1
        return collected.keys()

    def stat(self):
        "Returns statistics on key usage."
        collected = {}
        for _, data in self[:]:
            for k in data:
                collected[k] = collected.setdefault(k, 0) + 1
        return collected

    def __len__(self):
        return len(self[:])

    def __repr__(self):
        # Do the query using getitem
        return str(self[:])

    def __getitem__(self, k):
        # Retrieve an item or slice from the set of results.
        if not isinstance(k, (slice, int, long)):
            raise TypeError("ResultSet indices must be integers")

        # Check slice integrity
        assert (not isinstance(k, slice) and (k >= 0)) \
            or (isinstance(k, slice) and (k.start is None or k.start >= 0) \
            and (k.stop is None or k.stop >= 0)), \
            "Negative indexing is not supported."

        if isinstance(k, slice):
            offset = k.start or 0
            limit = (k.stop - offset) if k.stop is not None else MAX_RESULTS
        else:
            offset = k
            limit = 1

        cache_key = "%s_%s" % (offset, limit)
        if cache_key in self._cache:
            return self._cache[cache_key]

        conditions = [(c.name, c.op, c.expr) for c in self._conditions]

        # Do the search.
        keys = self._proto.search(conditions, limit, offset,
                                  order_type=self._order_t,
                                  order_field=self._order)

        # Since results are keys, we need to query for actual values
        if isinstance(k, slice):
            ret = [self._to_python(key) for key in keys]
        else:
            ret = self._to_python(keys[0])

        self._cache[cache_key] = ret

        return ret
