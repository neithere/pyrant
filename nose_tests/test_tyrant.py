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
from pyrant import Tyrant
from pyrant import exceptions


class TestTyrant(unittest.TestCase):
    TYRANT_HOST = '127.0.0.1'
    TYRANT_PORT = 1983
    TYRANT_FILE = os.path.abspath('test123.tct')
    TYRANT_PID = os.path.abspath('test123.pid')
    TYRANT_LUA = os.path.dirname(__file__) + '/test.lua'

    def setUp(self):
        assert not os.path.exists(self.TYRANT_FILE), 'Cannot proceed if test database already exists'
        cmd = 'ttserver -dmn -host %(host)s -port %(port)s -pid %(pid)s -ext %(lua)s %(file)s'
        cmd = cmd % {'host': self.TYRANT_HOST, 'port': self.TYRANT_PORT,
                'pid': self.TYRANT_PID, 'file': self.TYRANT_FILE, 'lua': self.TYRANT_LUA}
        os.popen(cmd).read()
        self.t = Tyrant(host=self.TYRANT_HOST, port=self.TYRANT_PORT)
        self.t.clear() #Clear dirty data
        self.t.sync()
        self._set_test_data()

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

    def _set_test_data(self):
        self.t["apple"] = dict(store="Convenience Store", color="red")
        self.t["blueberry"] = dict(store="Farmer's Market", color="blue")
        self.t["peach"] = dict(store="Shopway", color="yellow")
        self.t["pear"] = dict(store="Farmer's Market", color="yellow")
        self.t["raspberry"] = dict(store="Shopway", color="red")
        self.t["strawberry"] = dict(store="Farmer's Market", color="red")

    def test___contains__(self):
        assert "apple" in self.t
        assert "melon" not in self.t

    def test___delitem__(self):
        assert "apple" in self.t
        del self.t["apple"]
        assert "apple" not in self.t
        def fail():
            del self.t["melon"]
        self.assertRaises(KeyError, fail)

    def test___getitem__(self):
        assert self.t["apple"] == dict(store="Convenience Store", color="red")
        assert self.t["blueberry"] == dict(store="Farmer's Market", color="blue")
        def fail():
            return self.t["melon"]
        self.assertRaises(KeyError, fail)

    def test_get(self):
        assert self.t.get("apple") == dict(store="Convenience Store", color="red")
        assert self.t.get("blueberry") == dict(store="Farmer's Market", color="blue")
        assert self.t.get("melon", None) == None

    def test___len__(self):
        assert len(self.t) == 6

    def test___setitem__(self):
        assert self.t.get("apple") == dict(store="Convenience Store", color="red")
        self.t["apple"] = dict(store="Bah", color="yellow")
        assert self.t.get("apple") == dict(store="Bah", color="yellow")
        self.t["melon"] = dict(store="VillaConejos", color="green")
        assert self.t.get("melon") == dict(store="VillaConejos", color="green")

    def test_call_func(self):
        assert self.t.call_func("test_ext", "key", "value") == u"test: key=value"
        fake_func = lambda: self.t.call_func("invented_function", "key", "value")
        self.assertRaises(exceptions.InvalidOperation, fake_func)

    def test_clear(self):
        assert len(self.t) == 6
        self.t.clear()
        assert len(self.t) == 0

    def test_concat(self):
        self.fail("Code and doc revision needed")

    def test_get_size(self):
        self.assertRaises(KeyError, lambda:self.t.get_size("melon"))
        assert self.t.get_size("apple") == 34 #More usefull in not table dbtype. 34 is magic

    def test_get_stats(self):
        stats = self.t.get_stats()
        assert stats["rnum"] == "6"
        assert stats["type"] == "table"

    def test_iterkeys(self):
        keys = set("apple blueberry peach pear raspberry strawberry".split())
        g = self.t.iterkeys()
        assert hasattr(g, '__iter__')
        assert not hasattr(g, '__len__')
        db_keys = set([key for key in g])
        assert keys == db_keys

    def test_keys(self):
        assert self.t.keys() == "apple blueberry peach pear raspberry strawberry".split() #BTree and Tables are ordered

    def test_iteritems(self):
        g = self.t.iteritems()
        assert hasattr(g, '__iter__')
        assert not hasattr(g, '__len__')
        lst = list(g)
        assert len(lst) == 6
        assert 'apple' in dict(lst)

    def test_items(self):
        assert dict(self.t.iteritems()) == dict(self.t.items())

    def test_itervalues(self):
        g = self.t.itervalues()
        assert hasattr(g, '__iter__')
        assert not hasattr(g, '__len__')
        lst = list(g)
        assert len(lst) == 6
        assert 'color' in lst[0]
        assert lst[0]['color'] == 'red'

    def test_values(self):
        assert dict(self.t.itervalues()) == dict(self.t.values())

    def test_update(self):
        assert "melon" not in self.t
        assert "tomatoe" not in self.t
        #Update from named params
        self.t.update(
            melon = dict(store="VillaConejos", color="green"),
            tomatoe = dict(store="Bah de Perales", color="red")
        )
        assert "melon" in self.t
        assert "tomatoe" in self.t
        self.t.clear()
        #Update from a key and value list
        self.t.update([
                ("melon", dict(store="VillaConejos", color="green")),
                ("tomatoe", dict(store="Bah de Perales", color="red"))
        ])
        assert "melon" in self.t
        assert "tomatoe" in self.t
        self.t.clear()
        #Update from another dict
        self.t.update(dict(
            melon = dict(store="VillaConejos", color="green"),
            tomatoe = dict(store="Bah de Perales", color="red")
        ))
        assert "melon" in self.t
        assert "tomatoe" in self.t

    def test_multi_del(self):
        assert len(self.t) == 6
        self.t.multi_del(["apple", "pear"])
        assert len(self.t) == 4
        assert "apple" not in self.t
        assert "pear" not in self.t

    def test_multi_get(self):
        fruits = self.t.multi_get("apple melon pear".split())
        assert len(fruits) == 2
        assert "apple" in fruits
        assert "pear" in fruits

    def test_multi_set(self):
        self.t.multi_set(dict(
            melon = dict(store="VillaConejos", color="green"),
            tomatoe = dict(store="Bah de Perales", color="red")
        ))
        assert "melon" in self.t
        assert "tomatoe" in self.t
        assert self.t["melon"] == dict(store="VillaConejos", color="green")
        assert self.t["tomatoe"] == dict(store="Bah de Perales", color="red")

    def test_prefix_keys(self):
        fruits_a = self.t.prefix_keys("a")
        assert len(fruits_a) == 1
        fruits_p = self.t.prefix_keys("p")
        assert len(fruits_p) == 2
        fruits_p1 = self.t.prefix_keys("p", 1)
        assert len(fruits_p1) == 1
        fruits_m = self.t.prefix_keys("m")
        assert len(fruits_m) == 0

    def test_sync(self):
        #I don't know if sync system call is performed, but i can test if the
        # function call returns an error.
        self.t.sync()

    def test_query(self):
        pass

    def test_unicode(self):
        item_with_unicode_value = {'name': u'Андрей'}
        item_with_unicode_key = {u'имя': 'Andrey'}
        ascii_pk = 'primary key'
        unicode_pk = u'первичный ключ'

        self.t[ascii_pk] = item_with_unicode_value
        assert self.t[ascii_pk] == item_with_unicode_value

        self.t[ascii_pk] = item_with_unicode_key
        assert self.t[ascii_pk] == item_with_unicode_key

        self.t[unicode_pk] = item_with_unicode_value
        assert self.t[unicode_pk] == item_with_unicode_value
