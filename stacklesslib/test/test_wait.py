import unittest
import logging
import itertools
import contextlib
import weakref
import stackless
import stacklesslib.wait
import stacklesslib.app
import stacklesslib.errors
import stacklesslib.base
import stacklesslib.main

from .support import timesafe, captured_stderr

basetasklet = stackless.tasklet
class OldTasklet(stackless.tasklet):
    '''Emulate the old school tasklet'''
    def __new__(cls, func=None):
        i = basetasklet.__new__(cls, func)
        i.__init__(func)
        return i

    def __init__(kls, func=None):
        pass

    def bind(self, func):
        super(OldTasklet, self).bind(func)


@contextlib.contextmanager
def old():
    """Context manager to inherit from an emulated old style tasklet """
    oldkls = stackless.tasklet
    oldmod = stacklesslib.wait
    oldbasesa = twaitable.__bases__
    oldbasesb = vwaitable.__bases__
    stackless.tasklet = OldTasklet
    stacklesslib.wait = reload(stacklesslib.wait)
    stackless.tasklet = oldkls
    # Re create all the necessary classes
    init_waitables()
    try:
        yield
    finally:
        stacklesslib.wait = oldmod
        # restore to old definitions
        init_waitables()

def ctxtStart(ctxt):
    """Run the start part of a context manager"""
    ctxt.__enter__()
    return ctxt

def ctxtStop(ctxt):
        ctxt.__exit__(None, None, None)

def getswaitable():
    class swaitable(stacklesslib.wait.WaitSite):
        def __init__(self, t):
            super(waitable, self).__init__()
            self.t = t
            self.done = False

        def __repr__(self):
            return "<waitable t=%r>"%self.t

        def __lt__(self, o):
            return self.t < o.t

        def waitsite_signalled(self):
            return self.done

        def go(self):
            def f():
                stacklesslib.main.sleep(self.t)
                with stacklesslib.base.atomic():
                    self.done = True
                    self.waitsite_signal()
            if self.t > 0:
                stackless.tasklet(f)()
            else:
                f()
    return swaitable

def gettwaitable():
    class twaitable(stacklesslib.wait.WaitableTasklet):
        def __init__(self, t):
            super(twaitable, self).__init__(self.tf)
            self.t = t
            self.done = False

        def __repr__(self):
            return "<twaitable t=%r>"%self.t

        def __lt__(self, o):
            return self.t < o.t

        def tf(self):
            if self.t:
                stacklesslib.main.sleep(self.t)
            self.done = True

        def go(self):
            self()
            if self.t == 0:
                self.run()
    return twaitable

def getvwaitable():
    class vwaitable(stacklesslib.wait.ValueTasklet):
        def __init__(self, t):
            super(vwaitable, self).__init__(self.tf)
            self.t = t
            self.done = False

        def __repr__(self):
            return "<vwaitable t=%r>"%self.t

        def __lt__(self, o):
            return self.t < o.t

        def tf(self):
            if self.t:
                stacklesslib.main.sleep(self.t)
            self.done = True
            return self.t

        def go(self):
            self()
            if self.t == 0:
                self.run()
    return vwaitable

def init_waitables():
    globals()["swaitable"] = getswaitable()
    globals()["twaitable"] = gettwaitable()
    globals()["vwaitable"] = getvwaitable()
    globals()["waitable"] = globals()["swaitable"]
init_waitables()


def get_waitables(times):
    r = [waitable(t) for t in times]
    for t in r:
        t.go()
    return r

class WaitMixIn(object):
    @timesafe(1)
    def test_none(self):
        r = self.wait([])
        self.assertEqual(r, [])

    @timesafe(1)
    def test_one_incomplete(self):
        dude = waitable(0.001)
        self.assertFalse(dude.done)
        dude.go()
        self.assertFalse(dude.done)
        r = self.wait([dude])
        self.assertEqual(r, [dude])
        self.assertTrue(dude.done)

    @timesafe(1)
    def test_one_complete(self):
        dude = waitable(0)
        self.assertFalse(dude.done)
        dude.go()
        self.assertTrue(dude.done)
        r = self.wait([dude])
        self.assertEqual(r, [dude])
        self.assertTrue(dude.done)

    @timesafe(1)
    def test_two_incomplete(self):
        w = get_waitables([0.001, 0.002])
        r = self.wait(w)
        self.assertEqual(sorted(w), sorted(r))
        for ww in r:
            self.assertTrue(ww.done)

    @timesafe(1)
    def test_various(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.002])
        r = self.wait(w)
        self.assertEqual(sorted(w), sorted(r))
        for ww in r:
            self.assertTrue(ww.done)

    @timesafe(1)
    def test_completion_order(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.0025])
        r = self.wait(w)
        self.assertEqual(sorted(w), r)

    def test_timeout(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.1])
        r = self.wait(w, timeout=0.004)
        for o in r:
            self.assertTrue(o.done)
        self.assertEqual(len(r), 5)

class WaitTest(WaitMixIn, unittest.TestCase):
    def wait(self, objects, timeout=None):
        return stacklesslib.wait.wait(objects, timeout=timeout)

    def test_count(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.1])
        ww = stacklesslib.wait.wait(w, count=3)
        for o in ww:
            self.assertTrue(o.done)
        self.assertEqual(len(ww), 3)

    def test_count_large(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.003])
        ww = stacklesslib.wait.wait(w, count=len(w) + 1)
        for o in ww:
            self.assertTrue(o.done)
        self.assertEqual(len(ww), len(w))

    def test_count_small(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.003])
        ww = stacklesslib.wait.wait(w, count=0)
        for o in ww:
            self.assertTrue(o.done)
        self.assertEqual(len(ww), 0)


class TestIWait(WaitMixIn, unittest.TestCase):
    """Perform basic wait test plus iteration tests"""
    def wait(self, objects, timeout=None):
        return list(stacklesslib.wait.iwait(objects, timeout=timeout))

    def test_order(self):
        w = get_waitables([0.001, 0.002, 0, 0.0001, 0, 0.003, 0.025])
        r = list(itertools.islice(self.wait(w), 4))
        self.assertEqual(r, sorted(w)[:4])

    def test_timeout2(self):
        """Test that the iterative timeout is for the whole duration """
        w = get_waitables([0.001, 0.002, 0, 0.003, 0, 0.004, 0.005])
        r = list(self.wait(w, timeout = 0.0035))
        self.assertLess(len(r), len(w))

    def test_timeout_raise(self):
        w = get_waitables([0.001, 0.002, 0, 0.003, 0, 0.004, 0.005])
        i = stacklesslib.wait.iwait(w, timeout = 0.0035, raise_timeout=True)
        r = []
        def func():
            for v in i:
                r.append(v)
        self.assertRaises(stacklesslib.errors.TimeoutError, func)
        self.assertTrue(r)
        self.assertLess(len(r), len(w))

    def test_timeout_raise_zero(self):
        w = get_waitables([0, 0, 0.0, 0.001])
        i = stacklesslib.wait.iwait(w, timeout = 0, raise_timeout=True)
        r = []
        def func():
            for v in i:
                r.append(v)
        self.assertRaises(stacklesslib.errors.TimeoutError, func)
        self.assertEqual(len(r), 3)


class WaitTaskletTest(WaitTest):
    """Class that performs the same tests using a waitable tasklet"""
    def setUp(self):
        global waitable
        self.old = waitable
        waitable = twaitable
    def tearDown(self):
        global waitable
        waitable = self.old

class WaitValueTaskletTest(WaitTest):
    """Class that performs the same tests using a Value tasklet"""
    def setUp(self):
        global waitable
        self.old = waitable
        waitable = vwaitable
    def tearDown(self):
        global waitable
        waitable = self.old

class OldTaskletTest(WaitValueTaskletTest):
    @classmethod
    def setUpClass(cls):
        cls.tmp = ctxtStart(old())

    @classmethod
    def tearDownClass(cls):
        ctxtStop(cls.tmp)
        del cls.tmp

class TestWaitSite(unittest.TestCase):
    def setUp(self):
        self.ws = stacklesslib.wait.WaitSite()

    def test_cb(self):
        r = []
        def cb(ws):
            r.append(ws)
        def cb2(ws):
            r.append(ws)
        self.ws.add_done_callback(cb)
        self.ws.add_done_callback(cb2)
        self.assertFalse(r)
        self.ws.waitsite_signal()
        self.assertEqual(r, [self.ws, self.ws])


    def test_cb_error(self):
        r = []
        def cb(ws):
            1/0
            r.append(ws)
        def cb2(ws):
            r.append(ws)
        self.ws.add_done_callback(cb)
        self.ws.add_done_callback(cb2)
        self.assertFalse(r)
        with captured_stderr() as e:
            self.ws.waitsite_signal()
        self.assertEqual(r, [self.ws, ])
        self.assertIn("ZeroDivisionError", e.getvalue())

    def test_cb_error_subclass(self):
        e = []
        class WS(stacklesslib.wait.WaitSite):
            def handle_exception(self, ei):
                e.append(str(ei[1]))
        self.ws = WS()
        r = []
        def cb(ws):
            1/0
            r.append(ws)
        def cb2(ws):
            r.append(ws)

        self.ws.add_done_callback(cb)
        self.ws.add_done_callback(cb2)
        self.assertFalse(r)
        self.ws.waitsite_signal()
        self.assertEqual(r, [self.ws, ])
        self.assertIn("by zero", e[0])


class TestObserver(unittest.TestCase):
    def test_observer(self):
        w = stacklesslib.wait.WaitSite()
        o = stacklesslib.wait.Observer(w)

    def test_got_callback(self):
        w = stacklesslib.wait.WaitSite()
        o = stacklesslib.wait.Observer(w)
        self.assertFalse(o.got_callback)
        w.waitsite_signal()
        self.assertTrue(o.got_callback)

    def test_cb(self):
        w = stacklesslib.wait.WaitSite()
        o = stacklesslib.wait.Observer(w)
        oo = stacklesslib.wait.Observer(o)
        self.assertFalse(oo.got_callback)
        w.waitsite_signal()
        self.assertTrue(oo.got_callback)

    def test_weak(self):
        w = stacklesslib.wait.WaitSite()
        o = stacklesslib.wait.Observer(w)
        wr = weakref.ref(o)
        self.assertTrue(wr())
        del o
        self.assertFalse(wr())

    def test_close(self):
        w = stacklesslib.wait.WaitSite()
        o = stacklesslib.wait.Observer(w)
        self.assertFalse(o.got_callback)
        o.close()
        w.waitsite_signal()
        self.assertFalse(o.got_callback)

    def test_filter(self):
        class O(stacklesslib.wait.Observer):
            def filter(self):
                return False
        w = stacklesslib.wait.WaitSite()
        o = O(w)
        oo = stacklesslib.wait.Observer(o)
        self.assertFalse(oo.got_callback)
        w.waitsite_signal()
        self.assertFalse(oo.got_callback)

    def test_initial_call(self):
        class W(stacklesslib.wait.WaitSite):
            def waitsite_signalled(self):
                return True
        w = W()
        o = stacklesslib.wait.Observer(w)
        self.assertTrue(o.got_callback)
        o.got_callback = False
        w.waitsite_signal()
        self.assertTrue(o.got_callback)

    def test_initial_call_and_filter(self):
        class W(stacklesslib.wait.WaitSite):
            def waitsite_signalled(self):
                return True
        class O(stacklesslib.wait.Observer):
            def filter(self):
                return False
        w = W()
        o = O(w)
        oo = stacklesslib.wait.Observer(o)
        self.assertTrue(o.got_callback)
        self.assertFalse(oo.got_callback)
        w.waitsite_signal()
        self.assertTrue(o.got_callback)
        self.assertFalse(oo.got_callback)


from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()
