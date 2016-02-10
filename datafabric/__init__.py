import os
import unittest

from yellow_pages import YellowPages

def test():
    testsuite = unittest.TestLoader().discover(os.path.join(os.path.dirname(__file__), 'tests'))
    unittest.TextTestRunner(verbosity=2).run(testsuite)
