import unittest
import logging
import itertools
import stackless
import stacklesslib.wait
import stacklesslib.app
import stacklesslib.errors
import stacklesslib.base
import stacklesslib.main

from .support import timesafe, captured_stderr

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

waitable = swaitable

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
        r = self.wait(w, timeout=0.003)
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




from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()
