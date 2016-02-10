import unittest

import distributed
import datafabric as df

class TestDataFabric(unittest.TestCase):
    def test_allocation(self):
        smm = df.SharedMemoryManager(distributed.Executor('127.0.0.1:8786'))
        smm.allocate(['foo-%d' % i for i in range(10)], 100)

if __name__ == '__main__':
    unittest.main()
