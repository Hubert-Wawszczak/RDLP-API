import unittest
from utils.singleton import singleton

class TestSingleton(unittest.TestCase):
    def setUp(self):
        # Define a simple class for testing
        @singleton
        class Dummy:
            def __init__(self, value=0):
                self.value = value
        self.Dummy = Dummy

    def test_single_instance(self):
        a = self.Dummy(1)
        b = self.Dummy(2)
        self.assertIs(a, b)
        # The value should be from the first instantiation
        self.assertEqual(a.value, 1)
        self.assertEqual(b.value, 1)

    def test_singleton_with_no_args(self):
        @singleton
        class NoArgs:
            pass
        x = NoArgs()
        y = NoArgs()
        self.assertIs(x, y)

    def test_singleton_multiple_classes(self):
        @singleton
        class A:
            pass
        @singleton
        class B:
            pass
        a1 = A()
        a2 = A()
        b1 = B()
        b2 = B()
        self.assertIs(a1, a2)
        self.assertIs(b1, b2)
        self.assertIsNot(type(a1), type(b1))

if __name__ == '__main__':
    unittest.main()