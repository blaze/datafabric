import unittest

import distributed
import datafabric as df

class TestDataFabric(unittest.TestCase):
    def test_allocate(self):
        executor = distributed.Executor('127.0.0.1:8786')

        yp = df.YellowPages(executor)
        yp.allocate(['block{}'.format(i) for i in range(10)], 1024)
        yp.allocate(['_block{}'.format(i) for i in range(5)], 2048)

        blocks = [block[1] for block in yp.blocks()]
        self.assertTrue(len(blocks) == 15)
        for i in range(10):
            self.assertTrue('block{}'.format(i) in blocks)
        for i in range(5):
            self.assertTrue('_block{}'.format(i) in blocks)

        blocks = [block[1:] for block in yp.blocks(ip_only = False)]
        self.assertTrue(len(blocks) == 15)
        for i in range(10):
            self.assertTrue(('block{}'.format(i), 1024, 0) in blocks)
        for i in range(5):
            self.assertTrue(('_block{}'.format(i), 2048, 0) in blocks)

        yp.clear()

        self.assertTrue(not yp.blocks())

    def test_insert(self):
        executor = distributed.Executor('127.0.0.1:8786')

        yp = df.YellowPages(executor)
        yp.allocate(['block{}'.format(i) for i in range(10)], 1024)

        with self.assertRaises(LookupError):
            yp.find('x')

        yp.insert('x', 4)
        yp.find('x') # Should not raise an exception
        self.assertEqual((1024, 4, 4, 0), yp.find('x', ip_only = False)[2:])

        with self.assertRaises(LookupError):
            yp.find('y')

        yp.insert('y', 8)
        yp.find('x') # Should not raise an exception
        self.assertEqual((1024, 12, 4, 0), yp.find('x', ip_only = False)[2:])

        yp.find('y') # Should not raise an exception
        self.assertEqual((1024, 12, 8, 4), yp.find('y', ip_only = False)[2:])

        with self.assertRaises(LookupError):
            yp.find('p')
        with self.assertRaises(LookupError):
            yp.find('q')

        with self.assertRaises(ValueError):
            yp.insert('z', 2048)

        yp.clear()

        yp.allocate(['block0'], 1024)

        yp.insert('x', 4)
        yp.insert('y', 8)
        with self.assertRaises(ValueError):
            yp.insert('z', 1013)

        yp.clear()

    def test_remove(self):
        executor = distributed.Executor('127.0.0.1:8786')

        yp = df.YellowPages(executor)
        yp.allocate(['block{}'.format(i) for i in range(10)], 1024)

        yp.insert('x', 4)
        yp.find('x') # Should not raise an exception
        self.assertEqual((1024, 4, 4, 0), yp.find('x', ip_only = False)[2:])

        yp.remove('x')
        with self.assertRaises(LookupError):
            yp.find('x')

        yp.insert('x', 4)
        yp.insert('y', 8)
        yp.find('x') # Should not raise an exception
        self.assertEqual((1024, 12, 4, 4), yp.find('x', ip_only = False)[2:])
        yp.find('y') # Should not raise an exception
        self.assertEqual((1024, 12, 8, 8), yp.find('y', ip_only = False)[2:])

        yp.remove('x')
        self.assertEqual((1024, 8, 8, 8), yp.find('y', ip_only = False)[2:])
        with self.assertRaises(LookupError):
            yp.find('x')

        yp.remove('y')
        with self.assertRaises(LookupError):
            yp.find('y')

        yp.clear()

if __name__ == '__main__':
    unittest.main()
