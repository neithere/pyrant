# -*- coding: utf-8 -*-

# python
import os

# testing
import unittest
from nose import *

# the app
from pyrant import protocol, exceptions


class TestProtocol(unittest.TestCase):
    TYRANT_HOST = '127.0.0.1'
    TYRANT_PORT = 1983
    TYRANT_FILE = os.path.abspath('test123.tct')
    TYRANT_FILE_COPY = os.path.abspath('test321.tct')
    TYRANT_PID = os.path.abspath('test123.pid')
    TYRANT_LUA = os.path.dirname(__file__) + '/test.lua'

    def setUp(self):
        assert not os.path.exists(self.TYRANT_FILE), 'Cannot proceed if test database already exists'
        cmd = 'ttserver -dmn -host %(host)s -port %(port)s -pid %(pid)s -ext %(lua)s %(file)s'
        cmd = cmd % {'host': self.TYRANT_HOST, 'port': self.TYRANT_PORT,
                'pid': self.TYRANT_PID, 'file': self.TYRANT_FILE, 'lua': self.TYRANT_LUA}
        os.popen(cmd).read()
        self.p = protocol.TyrantProtocol(host=self.TYRANT_HOST, port=self.TYRANT_PORT)
        self.p.vanish()

    def tearDown(self):
        cmd = 'ps -e -o pid,command | grep "ttserver" | grep "\-port %s"' % self.TYRANT_PORT
        line = os.popen(cmd).read()
        try:
            pid = int(line.strip().split(' ')[0])
        except:
            'Expected "pid command" format, got %s' % line

        #os.popen('kill %s' % pid)
        os.unlink(self.TYRANT_FILE)

    def test_add_item(self):
        self.p.put(u'foo', u'bar\0baz')
        assert self.p.rnum() == 1
        self.p.put('fox', u'box\0quux')
        assert self.p.rnum() == 2

    def test_get_item(self):
        self.test_add_item() #Put data fields
        assert self.p.get(u'foo') == u'bar\x00baz'
        assert self.p.get(u'fox') == u'box\x00quux'

    def test_iterate_over_items(self):
        self.test_add_item() #Put data fields
        self.p.iterinit()
        assert self.p.iternext() == u'foo'
        assert self.p.iternext() == u'fox'
        self.assertRaises(exceptions.InvalidOperation, self.p.iternext)

    def test_get_multiple_items_at_once(self):
        self.test_add_item() #Put data fields
        assert self.p.mget(['foo', 'fox']) == [('foo', 'bar\x00baz'), ('fox', 'box\x00quux')]

    def test_remove_item(self):
        self.test_add_item() #Put data fields
        self.p.out('fox')
        def getter(key):
            def inner():
                self.p.get(key)
            return inner
        self.assertRaises(exceptions.InvalidOperation, getter('fox'))
        assert self.p.get(u'foo') == u'bar\x00baz' #Don't Vanish de DB

    def test_putkeep(self):
        self.test_add_item() #Put data fields
        def pk(key, value):
            def inner():
                self.p.putkeep(key, value)
            return inner
        self.assertRaises(exceptions.InvalidOperation,
                          pk('fox', 'old_value\x00not_stablished'))
        assert self.p.rnum() == 2
        assert self.p.get(u'fox') == u'box\x00quux'
        self.p.putkeep('new_fox', 'new_value\x00stablished')
        assert self.p.rnum() == 3
        assert self.p.get(u'new_fox') == u'new_value\x00stablished'

    def test_putcat(self):
        self.test_add_item() #Put data fields

        self.p.putcat("lala", "key\x00value")
        self.p.putcat("fox", "key\x00value")
        assert self.p.get('lala') == "key\x00value"
        assert self.p.get('fox') == u'box\x00quux\x00key\x00value'

    def test_putshl(self):
        self.test_add_item()
        self.p.putshl("fox", "key\x00value", len("box\x00quux\x00"))
        assert self.p.get("fox") == "key\x00value"
        self.p.putshl("lala", "key\x00value", len("key\x00value"))
        assert self.p.get("lala") == "key\x00value"  #An entry is first rotated and next append

    def test_putnr(self):
        #TODO: How to test if is waiting or not?
        self.p.putnr("lala", "key\x00value")
        assert self.p.get("lala") == "key\x00value" #XXX: This must fail or not?

    def test_out(self):
        self.test_add_item()
        def out(key):
            def inner():
                self.p.out(key)
            return inner
        def get(key):
            def inner():
                return self.p.get(key)
            return inner
        self.assertRaises(exceptions.InvalidOperation, get("not_existant_key"))
        self.assertRaises(exceptions.InvalidOperation, out("not_existant_key"))
        assert get("fox")() == "box\x00quux"
        out("fox")()
        self.assertRaises(exceptions.InvalidOperation, get("fox"))

    def test_get(self):
        pass #Tested in other tests

    def test_getint(self):
        self.test_add_item()
        def getint(key):
            def inner():
                return self.p.getint(key)
            return inner
        assert getint("fox")() == 0 #Is not a integer. Becomes setted to 0
        assert getint("lala")() == 0 #Don't exists. Becommes setted to 0
        self.p.addint("number", 3)
        assert getint("number")() == 3

    def test_getdouble(self):
        self.test_add_item()
        def getdouble(key):
            def inner():
                return self.p.getdouble(key)
            return inner
        assert getdouble("fox")() == 0.0 #Is not a double. Becomes setted to 0
        assert getdouble("lala")() == 0.0 #Don't exists. Becommes setted to 0
        self.p.adddouble("number", 3.0)
        assert self.p.adddouble("number", 3.0) == 6.0
        assert getdouble("number")() == 6.0

    def test_mget(self):
        self.test_add_item()
        ret = self.p.mget(["foo", "fox", "not_exists"])
        assert ('foo', 'bar\x00baz') in ret
        assert ('fox', 'box\x00quux') in ret
        assert len(ret) == 2
        assert self.p.mget(["ne", "not_exists", "none"]) == []
        assert self.p.mget([]) == []
        self.assertRaises(TypeError, self.p.mget) #A non List argument must fail
        self.assertRaises(TypeError, lambda:self.p.mget(9)) #A non List argument must fail

    def test_vsiz(self):
        self.test_add_item()
        assert self.p.vsiz("foo") == len('bar\x00baz') +1
        assert self.p.vsiz("fox") == len('box\x00quux') +1
        self.assertRaises(exceptions.InvalidOperation, lambda:self.p.vsiz("not_exists"))

    def test_iter(self):
        self.test_add_item()
        self.p.iterinit()
        assert self.p.iternext() in ("foo", "fox")
        assert self.p.iternext() in ("foo", "fox")
        self.assertRaises(exceptions.InvalidOperation, self.p.iternext) #Cursor exhausted

    def test_fwmkeys(self):
        self.test_add_item()
        assert len(self.p.fwmkeys("fo", -1)) == 2
        assert len(self.p.fwmkeys("fo", 1)) == 1 #Testing maxkeys
        assert len(self.p.fwmkeys("fox", -1)) == 1
        assert len(self.p.fwmkeys("not_found", -1)) == 0
        assert len(self.p.fwmkeys("not_found")) == 0 #Optional parameter

    def test_ext(self):
        assert self.p.ext("test_ext", 0, "key", "value") == u"test: key=value"

    def test_sync(self):
        self.test_add_item()
        self.p.sync() #This not test really that a sync call is performed

    def test_vanish(self):
        self.test_add_item()
        assert self.p.rnum() == 2
        self.p.vanish()
        assert self.p.rnum() == 0

    def test_copy(self):
        self.test_add_item()
        self.p.copy(self.TYRANT_FILE_COPY) #First time works
        self.p.copy(self.TYRANT_FILE_COPY) #Second time works
        cmd = 'tctmgr list %s' % self.TYRANT_FILE_COPY
        keys = os.popen(cmd).read().split()
        assert "foo" in keys
        assert "fox" in keys
        os.unlink(self.TYRANT_FILE_COPY)

    def test_restore(self):
        #TODO: I don't know how to test this
        pass

    def test_setmst(self):
        #TODO: Test not done yet
        #The test must be separated (slave need update log)
        pass

    def test_size(self):
        prev_size = self.p.size()
        self.test_add_item()
        next_size = self.p.size()
        assert next_size > prev_size #Size does not correspond with file size or keys + vals size

    def test_stat(self):
        def parse_stats(stats):
            return dict(map(lambda x: x.split("\t", 1), stats.splitlines()))
        stats = parse_stats(self.p.stat())
        assert stats["type"] == protocol.DB_TABLE
        assert stats["rnum"] == "0"
        self.test_add_item()
        stats = parse_stats(self.p.stat())
        assert stats["type"] == protocol.DB_TABLE
        assert stats["rnum"] == "2"

    def test_search(self):
        #TODO: Not done yet
        pass

    def test_misc(self):
        #TODO: Not done yet
        pass
