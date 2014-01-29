# test_weakref

import weakref
import unittest
import stacklesslib.weakmethod as weakmethod

class TestClass(object):
    def method(self, *args):
        return args

def func():
    pass

class TestWeakMethod(unittest.TestCase):
    def setUp(self):
        self.c = TestClass()

    def test_create(self):
        r = weakmethod.WeakMethod(TestClass().method)

    def test_weak(self):
        r = weakmethod.WeakMethod(TestClass().method)
        self.assertEqual(r(), None)

    def test_strong(self):
        r = weakmethod.WeakMethod(self.c.method)
        self.assertEqual(r(), self.c.method)
        del self.c
        self.assertEqual(r(), None)

    def test_fail(self):
        self.assertRaises(TypeError, weakmethod.WeakMethod, func)

    def test_eq(self):
        m = self.c.method
        r = weakmethod.WeakMethod(m)
        self.assertEqual(m, r())

    def test_repr(self):
        m = self.c.method
        r = weakmethod.WeakMethod(m)
        self.assertEqual(repr(m), repr(r()))

    def test_ref(self):
        m = self.c.method
        r = weakmethod.WeakMethod(m)
        weakref.ref(r)


class TestProxy(unittest.TestCase):
    """Test some properties of callable proxies that we want to emulate"""
    def setUp(self):
        def dude():
            pass
        self.dude = dude

    def test_callback(self):
        foo = [None]
        def cb(arg):
            foo[0] = arg
        p = weakref.proxy(self.dude, cb)
        del self.dude
        self.assertEqual(id(foo[0]), id(p))

    def test_hash(self):
        p = weakref.proxy(self.dude)
        self.assertRaises(TypeError, hash, p)
        del self.dude
        self.assertRaises(TypeError, hash, p)

    def test_cmp(self):
        def cb1(arg): pass
        def cb2(arg): pass
        p1 = weakref.proxy(self.dude, cb1)
        p2 = weakref.proxy(self.dude, cb2)
        self.assertNotEqual(id(p1), id(p2))
        self.assertEqual(p1, p2)
        del self.dude
        def cmp(a, b): return a == b
        self.assertRaises(weakref.ReferenceError, cmp, p1, p2)


class TestWeakMethodProxy(unittest.TestCase):
    def setUp(self):
        self.c = TestClass()

    def test_create(self):
        r = weakmethod.WeakMethodProxy(self.c.method)

    def test_weak(self):
        r = weakmethod.WeakMethodProxy(TestClass().method)
        self.assertRaises(weakref.ReferenceError, r)

    def test_strong(self):
        r = weakmethod.WeakMethodProxy(self.c.method)
        result = r(1,2,3)
        self.assertEqual(result, (1, 2, 3))
        del self.c
        self.assertRaises(weakref.ReferenceError, r)

    def test_strong_callback(self):
        foo = [None]
        def cb(arg):
            foo[0] = arg
        r = weakmethod.WeakMethodProxy(self.c.method, callback=cb)
        result = r(1,2,3)
        self.assertEqual(result, (1, 2, 3))
        self.assertEqual(foo[0], None)
        del self.c
        self.assertRaises(weakref.ReferenceError, r)
        self.assertEqual(id(foo[0]), id(r))

    def test_fallback(self):
        def fb(*arg):
            return arg+arg
        r = weakmethod.WeakMethodProxy(self.c.method, fallback=fb)
        result = r(1,2,3)
        self.assertEqual(result, (1, 2, 3))
        del self.c
        result = r(1,2,3)
        self.assertEqual(result, (1, 2, 3)*2)

    def test_hash(self):
        r = weakmethod.WeakMethodProxy(self.c.method)
        self.assertRaises(TypeError, hash, r)
        del self.c
        self.assertRaises(TypeError, hash, r)

    def test_repr(self):
        r = weakmethod.WeakMethodProxy(self.c.method)
        inner = repr(self.c.method)
        outer = repr(r)
        self.assertTrue(inner in outer)
        del self.c
        outer = repr(r)
        self.assertFalse(inner in outer)
        self.assertTrue("None" in outer)

    def test_cmp(self):
        def cb1(arg): pass
        def cb2(arg): pass
        p1 = weakmethod.WeakMethodProxy(self.c.method)
        p2 = weakmethod.WeakMethodProxy(self.c.method)
        self.assertNotEqual(id(p1), id(p2))
        self.assertEqual(p1, p2)
        del self.c
        def cmp(a, b): return a == b
        self.assertRaises(weakref.ReferenceError, cmp, p1, p2)

class TestRef(unittest.TestCase):
    def test_object(self):
        o = TestClass()
        p1 = weakref.ref(o)
        p2 = weakmethod.ref(o)
        self.assertEqual(p1, p2)

    def test_object_cb(self):
        def cb1(arg): pass
        o = TestClass()
        p1 = weakref.ref(o, cb1)
        p2 = weakmethod.ref(o, cb1)
        self.assertEqual(p1, p2)

    def test_func(self):
        def o(): pass
        p1 = weakref.ref(o)
        p2 = weakmethod.ref(o)
        self.assertEqual(p1, p2)

    def test_func(self):
        c = TestClass()
        o = c.method
        p1 = weakref.ref(o)
        p2 = weakmethod.ref(o)
        self.assertNotEqual(p1, p2)
        p3 = weakmethod.WeakMethod(o)
        self.assertNotEqual(p1, p3)

class TestProxy(unittest.TestCase):
    def test_object(self):
        o = TestClass()
        p1 = weakref.proxy(o)
        p2 = weakmethod.proxy(o)
        self.assertEqual(p1, p2)

    def test_object_cb(self):
        def cb1(arg): pass
        o = TestClass()
        p1 = weakref.proxy(o, cb1)
        p2 = weakmethod.proxy(o, cb1)
        self.assertEqual(p1, p2)

    def test_func(self):
        def o(): pass
        p1 = weakref.proxy(o)
        p2 = weakmethod.proxy(o)
        self.assertEqual(p1, p2)

    def test_func(self):
        c = TestClass()
        o = c.method
        p1 = weakref.proxy(o)
        p2 = weakmethod.proxy(o)
        self.assertNotEqual(p1, p2)
        p3 = weakmethod.WeakMethodProxy(o)
        self.assertNotEqual(p1, p3)







