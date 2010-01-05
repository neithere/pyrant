# -*- coding: utf-8 -*-

# python
import os
try:
    set
except NameError:
    from sets import Set as set

# testing
import unittest
from nose import *

# the app
from pyrant import TyrantError, Tyrant, Query


class TestTyrant(unittest.TestCase):
    TYRANT_HOST = '127.0.0.1'
    TYRANT_PORT = 1983
    TYRANT_FILE = os.path.abspath('test123.tct')
    TYRANT_PID = os.path.abspath('test123.pid')
    TYRANT_LUA = os.path.dirname(__file__) + '/test.lua'

    def setUp(self):
        assert not os.path.exists(self.TYRANT_FILE), 'Cannot proceed if test database already exists'
        cmd = 'ttserver -dmn -host %(host)s -port %(port)s -pid %(pid)s -ext %(lua)s %(file)s#idx=id:lex#idx=store:qgr#idx=color:tok#idx=stock:dec#'
        cmd = cmd % {'host': self.TYRANT_HOST, 'port': self.TYRANT_PORT,
                'pid': self.TYRANT_PID, 'file': self.TYRANT_FILE, 'lua': self.TYRANT_LUA}
        os.popen(cmd).read()
        self.t = Tyrant(host=self.TYRANT_HOST, port=self.TYRANT_PORT)
        self.t.clear() #Clear dirty data
        self.t.sync()
        self._set_test_data()
        self.q = self.t.query

    def tearDown(self):
        del self.t
        cmd = 'ps -e -o pid,command | grep "ttserver" | grep "\-port %s"' % self.TYRANT_PORT
        line = os.popen(cmd).read()
        try:
            pid = int(line.strip().split(' ')[0])
        except:
            'Expected "pid command" format, got %s' % line

        #os.popen('kill %s' % pid)
        os.unlink(self.TYRANT_FILE)
        try:
            os.unlink("%s.idx.store.qgr" % self.TYRANT_FILE)
            os.unlink("%s.idx.stock.dec" % self.TYRANT_FILE)
            os.unlink("%s.idx.id.lex" % self.TYRANT_FILE)
            os.unlink("%s.idx.color.tok" % self.TYRANT_FILE)
        except:
            pass

    def __init__(self, methodName="runTest"):
        unittest.TestCase.__init__(self, methodName)
        fields = [
            ("id", lambda _:_),
            ("store", lambda _:_),
            ("color", lambda _:_),
            ("price", lambda x:x), #TODO: See how to set as double
            ("stock", lambda _:_),
        ]
        raw_data = """
apple\tConvenience Store\tred\t1.20\t120
blueberry\tFarmer's Market\tblue\t1.12\t92
peach\tShopway\tyellow\t2.30\t300
pear\tFarmer's Market\tyellow\t0.80\t58
raspberry\tShopway\tred\t1.50\t12
strawberry\tFarmer's Market\tred\t3.15\t214
        """
        data = {}

        for line in raw_data.splitlines():
            f = line.split("\t")
            d = {}
            if len(f) < len(fields):
                continue
            for i in xrange(len(fields)):
                d[fields[i][0]] = fields[i][1](f[i])
            data[d["id"]] = d

        self.data = data

    def _set_test_data(self):
        self.t.update(self.data)

    def test_exact_match(self):
        #Test implicit __is operator
        apple = self.q.filter(id="apple")[:]
        assert len(apple) == 1
        assert apple[0][1] == self.data["apple"]

        #Test explicit __is lookup
        pear = self.q.filter(id__is="pear")[:]
        assert len(pear) == 1
        assert pear[0][1] == self.data["pear"]

        #Test many results
        shopway = self.q.filter(store="Shopway")[:]
        assert len(shopway) == 2
        for k, v in shopway:
            assert self.data[k]["store"] == "Shopway"
        #Test limit keys
        color = self.q.filter(color="red")[:1]
        assert len(color) == 1
        for k, v in color:
            assert self.data[k]["color"] == "red"
        #Test and query
        shopway_red = self.q.filter(color="red", store="Shopway")[:]
        assert len(shopway_red) == 1
        assert shopway_red[0][0] == "raspberry"
        #Test chained and filter
        shopway = self.q.filter(store="Shopway")
        shopway_red = shopway.filter(color="red")[:]
        assert len(shopway_red) == 1
        assert shopway_red[0][0] == "raspberry"
        #Test exclude
        shopway_not_red = shopway.exclude(color="red")[:]
        assert len(shopway_not_red) == 1
        assert shopway_not_red[0][0] == "peach"

    def test_numeric(self):
        #Numeric or decimal means integers. Search over floats or doubles are crazy
        bad_stock = self.q.filter(stock__lt=100)
        assert len(bad_stock) == 3
        for k, v in bad_stock:
            assert k in "blueberry pear raspberry".split()
        stock_300 = self.q.filter(stock=300)
        assert len(stock_300) == 1
        assert stock_300[0][0] == "peach"
        stock_58 = self.q.filter(stock__is=58)
        assert len(stock_58) == 1
        assert stock_58[0][0] == "pear"
        good_stock = self.q.filter(stock__gt=100)
        assert len(good_stock) == 3
        for k, v in good_stock:
            assert k in "apple peach strawberry".split()
        
        middle_stock = self.q.filter(stock__gte=58, stock__lte=120)
        assert len(middle_stock) == 3
        for k, v in middle_stock:
            assert k in "pear blueberry apple"
        
        middle_stock_between = self.q.filter(stock__between=[58, 120])
        assert list(middle_stock) == list(middle_stock_between)

    def test_string_contains(self):
        with_s = self.q.filter(id__contains="s")
        assert len(with_s) == 2
        for k, v in with_s:
            assert k in "raspberry strawberry".split()

    def test_string_startswith(self):
        start = self.q.filter(id__startswith="pe")
        assert len(start) == 2
        for k, v in start:
            assert k in "peach pear".split()

    def test_string_endswith(self):
        ends = self.q.filter(id__endswith="berry")
        assert len(ends) == 3
        for k, v in ends:
            assert k in "blueberry raspberry strawberry".split()

    def test_string_matchregex(self):
        regex = self.q.filter(id__matches=".ea.*")
        assert len(regex) == 2
        for k, v in regex:
            assert k in "peach pear".split()

    def test_token_eq_or(self):
        #Test string token
        yellow_blue = self.q.filter(color__in=["blue", "yellow"])
        assert len(yellow_blue) == 3
        for k, v in yellow_blue:
            assert k in "blueberry peach pear".split()

        #Test numeric token
        some_stocks = self.q.filter(stock__in=[12, 120])
        assert len(some_stocks) == 2
        for k, v in some_stocks:
            assert k in "apple raspberry".split()
        

    def test_token_contains_or(self):
        market_store = self.q.filter(store__contains_any=["Market", "Store"])
        assert len(market_store) == 4
        for k, v in market_store:
            assert k in "apple blueberry pear strawberry".split()

    def test_token_contains_and(self):
        store_convenience = self.q.filter(store__contains=["Store", "Convenience"])
        assert len(store_convenience) == 1
        assert store_convenience[0][0] == "apple"

    def test_qgr_like(self):
        market = self.q.filter(store__like="market")
        assert len(market) == 3
        for k, v in market:
            assert k in "blueberry pear strawberry".split()

        #Like is not like any
        market = self.q.filter(store__like="market store")
        assert len(market) == 0

    def test_qgr_like_all(self):
        store = self.q.filter(store__like="nience store".split())
        assert len(store) == 1
        assert store[0][0] == "apple"
        store = self.q.filter(store__like="market store".split())
        assert len(store) == 0

    def test_qgr_like_any(self):
        market_store = self.q.filter(store__like_any="market store".split())
        assert len(market_store) == 4
        for k, v in market_store:
            assert k in "apple blueberry pear strawberry".split()

    def test_qgr_search(self):
        market_store = self.q.filter(store__search="market || store")
        assert len(market_store) == 4
        for k, v in market_store:
            assert k in "apple blueberry pear strawberry".split()

        market_store = self.q.filter(store__search="market && store")
        assert len(market_store) == 0

    def test_order(self):
        #Gets some fruits
        fruits = self.q.filter(id__in=["apple", "blueberry", "peach"])
        
        #Order by name
        named_fruits = fruits.order_by("id")
        assert named_fruits[0][0] == "apple"
        assert named_fruits[1][0] == "blueberry"
        assert named_fruits[2][0] == "peach"

        #Order by name desc
        named_fruits_desc = fruits.order_by("-id")
        assert named_fruits_desc[0][0] == "peach"
        assert named_fruits_desc[1][0] == "blueberry"
        assert named_fruits_desc[2][0] == "apple"

        #Order by stock
        stock_fruits = fruits.order_by("stock", numeric=True)
        assert stock_fruits[0][0] == "blueberry"
        assert stock_fruits[1][0] == "apple"
        assert stock_fruits[2][0] == "peach"

        #Order by stock desc
        stock_fruits_desc = fruits.order_by("-stock", numeric=True)
        assert stock_fruits_desc[0][0] == "peach"
        assert stock_fruits_desc[1][0] == "apple"
        assert stock_fruits_desc[2][0] == "blueberry"

    def test_values(self):
        assert self.q.values("color") == [u'blue', u'yellow', u'red']
        assert self.q.values("store") == [u'Shopway', u"Farmer's Market", u'Convenience Store']

    def test_stat(self):
        assert self.q.stat() == {u'color': 6, u'price': 6, u'id': 6, u'store': 6, u'stock': 6}
        self.t.clear()
        self.t["prueba"] = dict(color="rojo", precio="3")
        self.t["test"] = dict(color="red", price="3")
        assert self.t.query.stat() == {u'color': 2, u'price': 1, u'precio': 1}

    def test_operator_or(self):
        #TODO: Q | Q
        not_blue = self.q.filter(color="red") | self.q.filter(color="yellow")
        assert len(not_blue) == 5
        assert "blueberry" not in not_blue

        complex_or = self.q.filter(color="blue") | self.q.filter(store="Shopway")
        print complex_or
        assert len(complex_or) == 3

    def test_columns(self):
        q = self.q.filter(id="apple")
        assert q.columns("id", "store")[0] == dict(id="apple", store="Convenience Store")
        assert q.columns(*"id color".split())[0] == dict(id="apple", color="red")
        assert q.columns("price", "stock")[:] == [dict(price="1.20", stock="120")]

    def test_union(self):
        q_apple = self.q.filter(id="apple")
        q_pear = self.q.filter(id="pear")
        q_red = self.q.filter(color="red")
        def get_ids(q):
            res = q.columns("id")[:]
            return set([d["id"] for d in res])
        assert get_ids(q_apple.union(q_pear)) == set("apple pear".split())
        assert get_ids(q_apple | q_pear) == set("apple pear".split())
        assert get_ids(q_pear | q_red) == set("apple pear raspberry strawberry".split())

    def test_intersect(self):
        q_apple = self.q.filter(id="apple")
        q_pear = self.q.filter(id="pear")
        q_red = self.q.filter(color="red")
        def get_ids(q):
            res = q.columns("id")[:]
            return set([d["id"] for d in res])
        assert get_ids(q_apple.intersect(q_pear)) == set([])
        assert get_ids(q_apple & q_pear) == set([])
        assert get_ids(q_apple & q_red) == set(["apple"])

    def test_intersect(self):
        q_apple = self.q.filter(id="apple")
        q_pear = self.q.filter(id="pear")
        q_red = self.q.filter(color="red")
        def get_ids(q):
            res = q.columns("id")[:]
            return set([d["id"] for d in res])
        assert get_ids(q_apple.minus(q_pear)) == set(["apple"])
        assert get_ids(q_apple - q_pear) == set(["apple"])
        assert get_ids(q_red - q_apple) == set("raspberry strawberry".split())

    def test_delete(self):
        assert "apple" in self.t
        self.q.filter(id="apple").delete()
        assert "apple" not in self.t
        assert "pear" in self.t
        assert self.q.filter(id="pear").columns("color").delete() == [{u'color': u'yellow'}]
        assert "pear" not in self.t
        
    def test_count(self):
        q_red = self.q.filter(color="red")
        assert q_red.count() == 3
        del self.t["apple"]
        assert q_red.count() == 2

    def test_hint(self):
        q_apple = self.q.filter(id="apple")
        q_red = self.q.filter(color="red")
        assert "HINT" in q_red.hint()[-1]
        assert "HINT" in q_apple.hint()[-1]
