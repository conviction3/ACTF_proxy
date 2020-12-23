from app.utils import *
import unittest


class UtilsTestSuite(unittest.TestCase):
    def test_bytes2int(self):
        b = b'1234'
        c = bytes2int(b)
        self.assertEqual(type(c), int)

