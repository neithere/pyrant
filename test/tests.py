"""
Doctest logic. We keep Tyrant() Q and Tyrant visible for all tests
"""
import doctest
from pyrant import Tyrant, Q

t = Tyrant()
globs = {
    'Tyrant': Tyrant,
    'Q': Q,
    't': t
}
doctest.testfile('../pyrant/__init__.py', globs=globs, 
                 optionflags=doctest.ELLIPSIS)
