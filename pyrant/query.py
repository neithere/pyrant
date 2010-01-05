# -*- coding: utf-8 -*-

"""
Query classes for Tokyo Tyrant API implementation.
"""

import copy
import warnings

from protocol import TyrantProtocol
from utils import to_python


MAX_RESULTS = 1000


class Query(object):
    """
    A lazy abstraction for queries via Tyrant protocol.
    """

    def __init__(self, proto, dbtype, literal=False, conditions=None,
                 columns=None, ms_type=None, ms_conditions=None):
        if conditions:
            assert isinstance(conditions, list) and \
                   all(isinstance(c, Condition) for c in conditions), \
                   'Expected a list of Condition instances, got %s' % conditions
        self.literal = literal
        self._conditions = conditions or []
        self._ordering = Ordering()
        self._cache = {}
        self._proto = proto
        self._dbtype = dbtype
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
        if not isinstance(k, (slice, int, long)):
            raise TypeError("Query indices must be integers")

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

        defaults = {
            'conditions': [c.prepare() for c in self._conditions],
            'limit': limit,
            'offset': offset,
        }
        
        keys = self._do_search(**defaults)

        # Since results are keys, we need to query for actual values
        if isinstance(k, slice):
            if self._columns:
                ret = [self._to_python_dict(key) for key in keys]
            else:
                ret = [self._to_python(key) for key in keys]
        else:
            if self._columns:
                ret = self._to_python_dict(keys[0])
            else:
                ret = self._to_python(keys[0])

        self._cache[cache_key] = ret

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
        
        return Query(self._proto, self._dbtype, **defaults)
    
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

        # Generate Q with arguments as needed
        for name, expr in kwargs.iteritems():
            c = Condition(name, expr)
            c.negate = negate
            query._conditions.append(c)

        return query

    def _parse_conditions(self, c):
        warnings.warn(DeprecationWarning('_parse_conditions is deprecated, use Condition.prepare() instead'))
        return (c.name, c.op, c.expr)

    def _to_python(self, key):
        elem = self._proto.get(key, self.literal)
        elem = to_python(elem, self._dbtype)
        return key, elem

    def _to_python_dict(self, val):
        vals = val.split("\x00")
        return dict(zip(vals[:-1:2], vals[1::2]))
    
    #
    # PUBLIC API
    #
    
    def columns(self, *names):
        """
        Returns a Query instance which will only retrieve specified columns
        per item. Expects names of columns to fetch. If none specified or '*'
        is in the names, all available columns are fetched.
        
        Usage::
        
            query.columns()                # fetches whole items
            query.columns('*')             # same as above
            query.columns('name', 'age')   # only fetches data for these columns
        
        NOTE: in this mode Query returns *only dictionaries*, no primary keys!
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

    def delete(self):
        """
        Deletes all matched items from the database.
        """
        keys = self._do_search(out=True)
        if self._columns:
            ret = [self._to_python_dict(key) for key in keys]
        else:
            ret = [self._to_python(key) for key in keys]
        return ret

    def exclude(self, *args, **kwargs):
        """
        Antipode of :meth:`~pyrant.Query.filter`.
        """
        return self._filter(True, args, kwargs)

    def filter(self, *args, **kwargs):    # TODO: provide full list of lookups
        """
        Returns a clone of the Query object with given conditions applied.

        Conditions can be specified as keyword arguments in this form::
        
            t.query.filter(name__is='John', age__gte=50)

        Supported keyword lookups and appropriate expression types are:

            * `any`: 
            * `is`: Equals (default) to expression
            * `lt`: Less than expression
            * `lte`: Less or equal to expression
            * `gt`: Greater than expression
            * `gte`: Greater or equal to expression
            * 'any' (list of strings)
            * 'between' (list of numbers)
            * 'contains' (string)
            * 'endswith' (string)
            * 'exists' (boolean)
            * 'gt' (number)
            * 'gte' (number)
            * 'in' (list of strings or numbers)
            * 'is' (string, list of strings or a number)
            * 'like' (string or list of strings)
            * 'like_any' (list of strings)
            * 'lt' (number)
            * 'lte' (number)
            * 'matches' (string)
            * 'search' (string)
            * 'startswith' (string)

        If a column name is provided with no lookup, exact match (`is`) is
        assumed.

        Usage::

            connect to a remote table database:

            >>> t = Tyrant()
            >>> t.get_stats()['type']
            u'table'

            stuff some data into the storage:

            >>> t['a'] = {'name': 'Foo', 'price': 1}
            >>> t['b'] = {'name': 'Bar', 'price': 2}
            >>> t['c'] = {'name': 'Foo', 'price': 3}

            find everything with price > 1:

            >>> for x in t.query.filter(price__gt=1):
            ...     print x[0]
            b
            c

            find everything with name "Foo":

            >>> for x in t.query.filter(name='Foo'):
            ...     print x
            a
            c

            chain queries:

            >>> cheap_items = t.query.filter(price__lt=3)
            >>> cheap_bars = cheap_items.filter(name='Bar')
            >>> for x in cheap_items:
            ...     print x
            a
            b
            >>> for x in cheap_bars:
            ...     print x
            b

        """
        return self._filter(False, args, kwargs)
    
    def hint(self):
        # TODO: documentation
        return self._do_search(hint=True)
    
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
    
    def order(self, name):
        """
        DEPRECATED, see order_by.
        """
        
        warnings.warn(DeprecationWarning(
            'Method Query.order is deprecated, use Query.order_by instead.')
        )
        
        numeric = False
        if '#' in name:
            numeric = True
            name = ''.join(name.split('#'))
        return self.order_by(name, numeric)

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

        if self._ordering == query._ordering:
            # provide link to existing cache
            query._cache = self._cache

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
    
    def __init__(self, constant, iterable=False, string=False, numeric=False,
                 boolean=False, value=None):
        self.boolean = boolean
        self.iterable = iterable
        self.numeric = numeric
        self.string = string
        
        self.operator = getattr(TyrantProtocol, constant)
        
        # custom value; only used if "has_custom_value" is True
        self.value = value
    
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
        'between':      [Lookup('RDBQCNUMBT', iterable=True, numeric=True)],
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
                op = definition.operator
                if self.negate:
                    op = op | TyrantProtocol.RDBQCNEGATE
                value = self.expr
                if definition.has_custom_value:
                    value = definition.value
                # flatten list (TC can search tokens) 
                if hasattr(value, '__iter__'):
                    value = ', '.join(unicode(x) for x in value)
                return self.name, op, value
            
        raise ValueError(u'could not find a definition for lookup "%s" suitable'
                         u' for value "%s"' % (self.lookup, self.expr))


# TODO: remove this previously deprecated class
class Q(Condition):
    def __init__(self, *args, **kwargs):
        
        warnings.warn(
            DeprecationWarning('Q class in deprecated, use Condition instead.')
        )
        
        super(Q, self).__init__()

    def __or__(self, q):
        
        warnings.warn(
            DeprecationWarning('Q class in deprecated, use Condition instead.')
        )
        
        return self
        

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
