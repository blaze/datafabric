import os
import unittest

from shared_memory import SharedMemoryManager


def test():
    testsuite = unittest.TestLoader().discover(os.path.join(os.path.dirname(__file__), 'tests'))
    unittest.TextTestRunner(verbosity=2).run(testsuite)
