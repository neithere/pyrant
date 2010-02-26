#!/bin/sh

echo "Running doctests..."

python -O run_doctests.py

kill `cat test123.pid`

echo "Running nose tests..."

nosetests nose_tests

kill `cat test123.pid`
