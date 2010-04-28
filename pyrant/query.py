# -*- coding: utf-8 -*-

"""
Query classes for Tokyo Tyrant API implementation.
"""

import copy
import warnings

from protocol import TyrantProtocol
import utils


MAX_RESULTS = 1000


class Query(object):
    """
    A lazy abstraction for queries via Tyrant protocol.

    You will normally instantiate Query this way::

        >>> from pyrant import Tyrant
        >>> t = Tyrant(host='localhost', port=1983)
        >>> query = t.query

    .. note:: queries do *not* cache the results. You can always do that
        yourself by assigning them to a variable in your code. However, do not
        confuse *query object* with its *results*! The queries are lazy. This
        means that they are not evaluated until you iterate over them or
        request a slice. So the code ``foo = t.query`` assigns a query object
        to the variable ``foo``, while ``foo = list(t.query)`` or
        ``foo = t.query[:]`` assigns the *results* (i.e. a list) to that
        variable.

    """

    def __init__(self, proto, db_type, literal=False, conditions=None,
                 columns=None, ms_type=None, ms_conditions=None):
        if conditions:
            assert isinstance(conditions, list) and \
                   all(isinstance(c, Condition) for c in conditions), \
                   'Expected a list of Condition instances, got %s' % conditions
        self.literal = literal
        self._conditions = conditions or []
        self._ordering = Ordering()
        #self._cache = {}
        self._proto = proto
        self._db_type = db_type
        self._columns = columns
        self._ms_type = ms_type
        self._ms_conditions = ms_conditions

    #
    # PYTHON MAGIC METHODS
    #

    def __and__(self, other):
        return self.intersect(other)

    def __contains__(self, key):
        keys = self._do_search()
        return key in keys

    def __getitem__(self, k):
        # Retrieve an item or slice from the set of results.

        # XXX do we really need to cache the data? What if there are a couple
        #     millions items and the user wants to just iterate over them in
        #     order to calculate an aggregate? I think cache should be either
        #     kept small or turned off by default or completely removed.
        #     The user can always easily cache the data explicitly by keeping
        #     the results in a variable (i.e. "records = query[:]").

        if not isinstance(k, (slice, int, long)):
            raise TypeError("Query indices must be integers")

        # Check slice integrity
        if isinstance(k, slice):
            for x in k.start, k.stop:
                if x is not None and x < 0:
                    raise ValueError('Negative indexing is not supported')
            if k.start and not k.stop:
                raise ValueError('Offset without limit is not supported')
            if k.start and k.start == k.stop:
                raise ValueError('Zero-length slices are not supported')
        else:
            if k < 0:
                raise ValueError('Negative indexing is not supported')

        if isinstance(k, slice):
            offset = k.start or 0
            limit = k.stop - (k.start or 0) if k.start else k.stop
        else:
            offset = k
            limit = 1

        #cache_key = "%s_%s" % (offset, limit)
        #if cache_key in self._cache:
        #    return self._cache[cache_key]

        defaults = {
            'conditions': [c.prepare() for c in self._conditions],
            'limit': limit,
            'offset': offset,
        }

        keys = self._do_search(**defaults)

        # Since results are keys, we need to query for actual values
        if isinstance(k, slice):
            if self._columns:
                # in column mode we get values instead of keys
                ret = [self._to_python(value) for value in keys]
            else:
                print 'using mget on %d keys' % len(keys)
                ret = self._proto.mget(keys)
                ret = [(k, self._to_python(v)) for k,v in ret]
        else:
            if self._columns:
                ret = self._to_python(keys[0])
            else:
                print 'using get'
                k = keys[0]
                v = self._proto.get(k)
                ret = (k, self._to_python(v))

        #self._cache[cache_key] = ret

        return ret

    def __len__(self):
        return len(self[:])

    def __or__(self, other):
        return self.union(other)

    def __repr__(self):
        # Do the query using getitem
        return str(self[:])

    def __sub__(self, other):
        return self.minus(other)

    #
    # PRIVATE METHODS
    #

    def _add_to_metasearch(self, other, method):
        """
        Returns a Query instance made from current one and the ``other`` using
        given ``method``.
        """
        query = self._clone()
        assert isinstance(other, Query), "This function needs other Query object type"
        assert query._ms_type in (None, method), "You can not mix union with intersect or minus"
        if not query._ms_conditions:
            query._ms_conditions = []
        other = other._clone()
        query._ms_conditions.append(other._conditions)
        query._ms_type = method
        return query

    def _clone(self):
        defaults = {
            'literal': self.literal,
            'conditions': [c._clone() for c in self._conditions],
            'ms_type': self._ms_type,
        }

        if self._ms_conditions:
            defaults.update(
                ms_conditions = [[query._clone() for query in conds]
                                    for conds in self._ms_conditions],
            )

        if self._columns:
            defaults.update(columns=self._columns[:])

        return Query(self._proto, self._db_type, **defaults)

    def _do_search(self, conditions=None, limit=None, offset=None,
                   out=False, count=False, hint=False):
        """
        Returns keys of items that correspond to the Query instance.
        """
        defaults = {
            'out': out,
            'count': count,
            'hint': hint,
            'conditions': conditions or [c.prepare() for c in self._conditions],
            'limit': limit,
            'offset': offset,
        }
        if self._columns:
            defaults.update(columns=self._columns[:])
        if self._ordering:
            defaults.update(
                order_column = self._ordering.name,
                order_type   = self._ordering.type,
            )
        if self._ms_conditions:
            defaults.update(    # FIXME make this more readable
                ms_type = self._ms_type,
                ms_conditions = [[c.prepare() for c in ms_c] for ms_c in self._ms_conditions]
            )

        return self._proto.search(**defaults)

    def _filter(self, negate, args, kwargs):
        query = self._clone()

        # Iterate arguments. Should be instances of Q
        for cond in args:
            assert isinstance(cond, Condition), "Arguments must be instances of Q"
            c = cond._clone()
            c.negate = c.negate ^ negate
            query._conditions.append(c)

        # Generate Condition objects with arguments as needed
        for name, expr in kwargs.iteritems():
            c = Condition(name, expr)
            c.negate = negate
            query._conditions.append(c)

        return query

    def _to_python(self, elem):
        return utils.to_python(elem, self._db_type)

    #
    # PUBLIC API
    #

    def columns(self, *names):
        """
        Returns a Query instance which will only retrieve specified columns
        per item. Expects names of columns to fetch. If none specified or '*'
        is in the names, all available columns are fetched.

        .. note:: in this mode Query returns *only dictionaries* without
            primary keys.

        Usage::

            query.columns()                # fetches whole items
            query.columns('*')             # same as above
            query.columns('name', 'age')   # only fetches data for these columns

        """
        query = self._clone()
        query._columns = None
        if names:
            query._columns = []
            for name in names:
                if isinstance(names, (tuple, list)):
                    query._columns.extend(names)
                elif isinstance(args, (str, unicode)):
                    if names == '*':
                        query._columns = None
                        break
                    query._columns.append(names)
                else:
                    raise TypeError("%s is not supported for describing columns" % arg)
        return query

    def count(self):
        """
        Returns the number of matched items.
        """
        return int(self._do_search(count=True)[0])

    def delete(self, quick=False):
        """
        Deletes all matched items from the database. Returns `True` on success
        or `False` if the operation could not be performed.

        .. warning:: current implementation is inefficient due to a bug on a
            lower level (probably within Pyrant). The underlying function does
            not tell us whether the operation was successful, so we perform an
            additional query. This may substantially decrease performance in
            some rare cases. A workaround is to use the param `quick`.

        :param quick: if `True`, the method always returns `None` and does not
            check whether the operation was successful. Useful if you call this
            method *very* frequently. Default is `False`. Please note that this
            param will be deprecated after the underlying code is fixed so the
            method will always return a boolean.

        """
        # FIXME this is broken: lower level always returns empty list, not sure why
        response = self._do_search(out=True)
        # assert 1 == len(response)
        # return True if response[0] == 'true' else False

        # XXX emulating the proper response
        # TODO: deprecate the `confirm` param
        if quick:
            return not bool(self._do_search(count=True))
        else:
            return None

    def exclude(self, *args, **kwargs):
        """
        Antipode of :meth:`~filter`.
        """
        return self._filter(True, args, kwargs)

    def filter(self, *args, **kwargs):    # TODO: provide full list of lookups
        """
        Returns a clone of the Query object with given conditions applied.

        Conditions can be specified as keyword arguments in this form::

            t.query.filter(name__is='John', age__gte=50)

        Supported keyword lookups and appropriate expression types are:

            * `between`: (list of numbers)
            * `contains`: (string or list of strings)
            * `contains_any` (list of strings)
            * `endswith`: (string)
            * `exists`: (boolean)
            * `gt`: (number)
            * `gte`: (number)
            * `in`: (list of strings or numbers)
            * `is`: (string, list of strings or a number)
            * `like`: (string or list of strings)
            * `like_any`: (list of strings)
            * `lt` (number)
            * `lte` (number)
            * `matches` (string)
            * `search` (string)
            * `startswith` (string)

        If a column name is provided with no lookup, exact match (`is`) is
        assumed.

        Connect to a remote table database::

            >>> t.table_enabled
            True

        Stuff some data into the storage::

            >>> t['a'] = {'name': 'Foo', 'price': 1}
            >>> t['b'] = {'name': 'Bar', 'price': 2}
            >>> t['c'] = {'name': 'Foo', 'price': 3}

        Find everything with price > 1::

            >>> for k, v in t.query.filter(price__gt=1):
            ...     print k
            b
            c

        Find everything with name "Foo"::

            >>> for k, v in t.query.filter(name='Foo'):
            ...     print k
            a
            c

        Chain queries::

            >>> cheap_items = t.query.filter(price__lt=3)
            >>> cheap_bars = cheap_items.filter(name='Bar')
            >>> for k, v in cheap_items:
            ...     print k
            a
            b
            >>> for k, v in cheap_bars:
            ...     print k
            b

        """
        return self._filter(False, args, kwargs)

    def hint(self):
        """
        Returns the hint string.

        .. warning:: currently this executes the query and does not cache its
            results. If you fetch the results before or after calling this
            method, the search will be made twice.

        """
        # TODO: the results should be cached and accessible via __getitem__
        results = self._do_search(hint=True)    # list of keys + hint string
        return results[-1]

    def intersect(self, other):
        """
        Returns a Query instance with items matched by both this query and the
        `other` one. Semantically equivalent to "a AND b".
        """
        return self._add_to_metasearch(other, TyrantProtocol.TDBMSISECT)

    def minus(self, other):
        """
        Returns a Query instance with items matched by either this query or
        the `other` but not both.
        """
        return self._add_to_metasearch(other, TyrantProtocol.TDBMSDIFF)

    def order_by(self, name, numeric=False):
        """
        Defines order in which results should be retrieved.

        :param name: the column name. If prefixed with ``-``, direction changes
            from ascending (default) to descending.
        :param numeric: if True, values are treated as numbers. Default is False.

        Examples::

            q.order_by('name')     # ascending
            q.order_by('-name')    # descending
            q.order_by('-price', numeric=True)

        """
        query = self._clone()

        # handle "name"/"-name"
        if name.startswith('-'):
            name = name[1:]
            direction = Ordering.DESC
        else:
            direction = Ordering.ASC

        query._ordering = Ordering(name, direction, numeric)

        #if self._ordering == query._ordering:
        #    # provide link to existing cache
        #    query._cache = self._cache

        return query

    def stat(self):
        """
        Returns statistics on key usage.
        """
        collected = {}
        for _, data in self[:]:
            for k in data:
                collected[k] = collected.setdefault(k, 0) + 1
        return collected

    def union(self, other):
        """
        Returns a Query instance which items are matched either by this query
        or the `other` one or both of them. Sematically equivalent to "a OR b".
        """
        return self._add_to_metasearch(other, TyrantProtocol.TDBMSUNION)

    def values(self, key):
        """
        Returns a list of unique values for given key.
        """
        # TODO: use self.columns()
        collected = {}
        for _, data in self[:]:
            for k,v in data.iteritems():
                if k == key and v not in collected:
                    collected[v] = 1
        return collected.keys()


class Lookup(object):
    """
    Lookup definition.
    """
    has_custom_value = False
    min_args = None
    max_args = None

    def __init__(self, constant, iterable=False, string=False, numeric=False,
                 boolean=False, value=None, min_args=None, max_args=None):
        self.boolean = boolean
        self.iterable = iterable
        self.numeric = numeric
        self.string = string

        self.operator = getattr(TyrantProtocol, constant)

        # custom value; only used if "has_custom_value" is True
        self.value = value

        if min_args or max_args:
            assert iterable, 'number of arguments can be specified only for iterables'
        self.min_args = min_args
        self.max_args = max_args

    def accepts(self, value):
        """
        Returns True if given value is acceptable for this lookup definition.
        """
        if self.iterable:
            if not hasattr(value, '__iter__'):
                return False
            if value:
                value = value[0]
        if self.boolean:
            if not isinstance(value, bool):
                return False
        if self.numeric:
            if not isinstance(value, (int, float)):
                return False
        if self.string:
            if not isinstance(value, basestring):
                return False
        return True

    def validate(self, value):
        """
        Checks if value does not only look acceptable, but is also valid. Returns
        the value.
        """
        if hasattr(value, '__iter__'):
            if self.min_args and len(value) < self.min_args:
                raise ValueError('expected at least %d arguments' % self.min_args)
            if self.max_args and self.max_args < len(value):
                raise ValueError('expected at most %d arguments' % self.max_args)
        return value


class ExistanceLookup(Lookup):
    has_custom_value = True


class Condition(object):
    """
    Representation of a query condition. Maps lookups to protocol constants.
    """

    # each lookup has 1..n definitions that can be used to a) check if the
    # lookup suits the expression, and b) to construct the condition in terms
    # of low-level API.
    LOOKUP_DEFINITIONS = {
        'between':      [Lookup('RDBQCNUMBT', iterable=True, numeric=True,
                                min_args=2, max_args=2)],
        'contains':     [Lookup('RDBQCSTRINC', string=True),
                         Lookup('RDBQCSTRAND', iterable=True, string=True)],
        'contains_any': [Lookup('RDBQCSTROR', iterable=True, string=True)],
        'endswith':     [Lookup('RDBQCSTREW', string=True)],
        'exists':       [ExistanceLookup('RDBQCSTRRX', boolean=True, value='')],
        'gt':           [Lookup('RDBQCNUMGT', numeric=True)],
        'gte':          [Lookup('RDBQCNUMGE', numeric=True)],
        'in':           [Lookup('RDBQCSTROREQ', iterable=True, string=True),
                         Lookup('RDBQCNUMOREQ', iterable=True, numeric=True)],
        'is':           [Lookup('RDBQCNUMEQ', numeric=True),
                         Lookup('RDBQCSTREQ')],
        'like':         [Lookup('RDBQCFTSPH', string=True),
                         Lookup('RDBQCFTSAND', iterable=True, string=True)],
        'like_any':     [Lookup('RDBQCFTSOR', iterable=True, string=True)],
        'lt':           [Lookup('RDBQCNUMLT', numeric=True)],
        'lte':          [Lookup('RDBQCNUMLE', numeric=True)],
        'matches':      [Lookup('RDBQCSTRRX', string=True)],
        'search':       [Lookup('RDBQCFTSEX', string=True)],
        'startswith':   [Lookup('RDBQCSTRBW', string=True)],
    }
    # default lookup (if none provided by the user)
    LOOKUP_DEFINITIONS[None] = LOOKUP_DEFINITIONS['is']

    def __init__(self, lookup, expr, negate=False):
        name, lookup = self._parse_lookup(lookup)
        self.name = name
        self.lookup = lookup
        self.expr = expr
        self.negate = negate

    def __repr__(self):
        return u'<%s %s%s "%s">' % (self.name, ('not ' if self.negate else ''),
                                     self.lookup, self.expr)

    def _clone(self):
        return copy.copy(self)

    def _parse_lookup(self, lookup):
        """
        Expects lookup ("foo", "foo__contains").
        Returns column name and the normalized operator name.
        """
        if '__' in lookup:
            col_name, op_name = lookup.split('__', 1)
        else:
            col_name, op_name = lookup, 'is'
        return col_name, op_name

    def prepare(self):
        """
        Returns search-ready triple: column name, operator code, expression.
        """

        if not self.lookup in self.LOOKUP_DEFINITIONS:
            available_lookups = ', '.join(str(x) for x in self.LOOKUP_DEFINITIONS)
            raise NameError('Unknown lookup "%s". Available are: %s' %
                            (self.lookup, available_lookups))

        definitions = self.LOOKUP_DEFINITIONS[self.lookup]

        for definition in definitions:
            if definition.accepts(self.expr):
                try:
                    value = definition.validate(self.expr)
                except ValueError, e:
                    raise ValueError(u'Bad lookup %s__%s=%s: %s' % (
                                     self.name,
                                     self.lookup,
                                     (self.expr if hasattr(self.expr,'__iter__') else u'"%s"'%self.expr),
                                     unicode(e)))

                op = definition.operator

                # deal with negation: it can be external ("exclude(...)") or
                # internal ("foo__exists=False")
                negate = self.negate
                if definition.has_custom_value:
                    if isinstance(value, bool) and not value:
                        # if the value is substituted and only provided to define
                        # the expected result of a test (yes/no), we must modify
                        # our internal negation state according to the value
                        negate = not negate
                    value = definition.value
                if negate:
                    op = op | TyrantProtocol.RDBQCNEGATE

                # boolean values are stored as integers
                value = utils.from_python(value)

                # flatten list (TC can search tokens)
                if hasattr(value, '__iter__'):
                    value = ', '.join(unicode(x) for x in value)

                return self.name, op, value

        raise ValueError(u'could not find a definition for lookup "%s" suitable'
                         u' for value "%s"' % (self.lookup, self.expr))


class Ordering(object):
    """
    Representation of ordering policy for a query. Accepts column name,
    sorting direction (ascending or descending) and sorting method
    (alphabetic or numeric) and selects the appropriate protocol constant.
    Default sorting settings are: ascending + alphabetic.
    """
    ASC, DESC = 0, 1
    ALPHABETIC, NUMERIC = 0, 1

    PROTOCOL_MAP = {
        DESC: {
            NUMERIC:    TyrantProtocol.RDBQONUMDESC,
            ALPHABETIC: TyrantProtocol.RDBQOSTRDESC
        },
        ASC: {
            NUMERIC:    TyrantProtocol.RDBQONUMASC,
            ALPHABETIC: TyrantProtocol.RDBQOSTRASC,
        }
    }

    def __init__(self, name=None, direction=None, numeric=False):
        self.name = name
        self.direction = direction or self.ASC
        self.method = self.NUMERIC if numeric else self.ALPHABETIC

    def __eq__(self, other):
        """
        Returns True if key attributes of compared instances are the same.
        """
        if not isinstance(other, type(self)):
            raise TypeError('Expected %s instance, got %s' % type(self), other)
        for attr in 'name', 'direction', 'method':
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __nonzero__(self):
        return bool(self.name)

    def __repr__(self):
        return u'<Order by %s (%s, %s)>' % (
            self.name,
            'desc' if self.direction else 'asc',
            'numeric' if self.method else 'alphabetic',
        )

    @property
    def type(self):
        return self.PROTOCOL_MAP[self.direction][self.method]
