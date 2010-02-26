#!/usr/bin/python -O
# -*- coding: utf-8 -*-


import os
import unittest
import doctest


TESTS_DIRS = ('pyrant',)

# A sandbox Tyrant instance parametres:
TYRANT_HOST = '127.0.0.1'
TYRANT_PORT = '1983'    # default is 1978 so we avoid clashes
TYRANT_FILE = os.path.abspath('test123.tct')
TYRANT_PID  = os.path.abspath('test123.pid')


def _start_tyrant():
    assert not os.path.exists(TYRANT_FILE), 'Cannot proceed if test database already exists'
    cmd = 'ttserver -dmn -host %(host)s -port %(port)s -pid %(pid)s %(file)s'
    os.popen(cmd % {'host': TYRANT_HOST, 'port': TYRANT_PORT,
                    'pid': TYRANT_PID, 'file': TYRANT_FILE}).read()
    if __debug__:
        print 'Sandbox Tyrant started...'

def _stop_tyrant():
    cmd = 'ps -e -o pid,command | grep "ttserver" | grep "\-port %s"' % TYRANT_PORT
    line = os.popen(cmd).read()
    try:
        pid = int(line.strip().split(' ')[0])
    except ValueError:
        'Expected "pid command" format, got %s' % line

    os.popen('kill %s' % pid)
    if __debug__:
        print 'Sandbox Tyrant stopped.'

    if os.path.exists(TYRANT_FILE):
        os.unlink(TYRANT_FILE)
        if __debug__:
            print 'Sandbox database %s deleted.' % TYRANT_FILE

def _add_files_to(suite):
    def _inner(_, dirname, fnames):
        for f in fnames:
            if f.endswith('.py'):
                file_path = os.path.join(dirname, f)
                suite.addTest(doctest.DocFileSuite(file_path, optionflags=doctest.ELLIPSIS))
    return _inner

def _test():

    # run the sandbox Tyrant instance
    _start_tyrant()

    # set up suite
    suite = unittest.TestSuite()

    # collect files for testing
    for directory in TESTS_DIRS:
        os.path.walk(directory, _add_files_to(suite), None)

    # run tests on all files collected
    runner = unittest.TextTestRunner()
    runner.run(suite)

    # kill the sandbox Tyrant instance
    _stop_tyrant()

if __name__ == '__main__':
    _test()
