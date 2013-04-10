#testtimeout.py
"""Test various timeout aspects of stacklesslib"""
import unittest
import logging
import stackless
import time
import contextlib

from stacklesslib import main, app, util
from stacklesslib.errors import TimeoutError

#compute fluff
t0 = time.time()
t1 = time.time()
while(t1 == t0):
    t1 = time.time()
fluff = (t1-t0) * 2
# print "fluff", fluff
# typcially 2ms

class TimeoutMixin(object):
    def _notimeout(self, call, timeout=0.1):
        with self.Timer(timeout):
            with util.timeout(timeout):
                return call()

    def _timeout(self, call, timeout=0.1):
        self.assertRaises(TimeoutError, self._notimeout, call, timeout)

    @contextlib.contextmanager
    def Timer(self, delay):
        t0 = time.time()
        try:
            yield
        except TimeoutError:
            dt = time.time() - t0
            self.assertLessEqual(delay - fluff, dt)
            self.assertLessEqual(dt, delay + fluff)
            raise


class TestTimeout(TimeoutMixin, unittest.TestCase):

    def test_send(self):
        c = stackless.channel()
        self._timeout(lambda:c.send(None))

    def test_receive(self):
        c = stackless.channel()
        self._timeout(c.receive)

    def test_success(self):
        def func():
            return "hullo"
        result = self._notimeout(func)
        self.assertEqual(result, "hullo")

    def test_long_success(self):
        import time
        sleep = getattr(time, "real_sleep", time.sleep)
        def func():
            sleep(0.1)
            return "hullo"
        # should not timeou because there is no point for the tasklet
        # to do so
        result = self._notimeout(func, 0.05)
        self.assertEqual(result, "hullo")

    def test_sleep(self):
        result = self._timeout(lambda:app.sleep(0.1), 0.05)

    def test_event(self):
        e = app.Event()
        result = self._timeout(lambda:e.wait(), 0.05)

    def test_Lock(self):
        e = app.Lock()
        with e:
            result = self._timeout(lambda:e.acquire(), 0.05)


class TestTimeoutDeco(TestTimeout):
    """Using the function decorator"""
    def _notimeout(self, call, timeout=0.1):
        call2 = util.timeout_function(timeout)(call)
        with self.Timer(timeout):
            return call2()

class TestTimeoutFunc(TestTimeout):
    """Using the function call proxy"""
    def _notimeout(self, call, timeout=0.1):
        def call2():
            ok, val = util.timeout_call(call, timeout)
            if ok:
                return val
            else:
                raise TimeoutError("dude")
        with self.Timer(timeout):
            return call2()


class TestRecursion(TimeoutMixin, unittest.TestCase):
    def test_no_inner_catch(self):

        def inner():
            with util.timeout(1.0): #long timeout
                try:
                    app.sleep(2.0)
                except TimeoutError:
                    self.assertFalse("this should not have timed out")
        self._timeout(inner, 0.1)

    def test_inner_catch_reraise(self):
        def inner():
            with util.timeout(0.1): #long timeout
                app.sleep(2.0)
        # expect the inner timeout to percolate outwards
        def outer():
            with util.timeout(1.0):
                self.assertRaises(TimeoutError, inner)
                raise TimeoutError
        self.assertRaises(TimeoutError, outer)

    def test_inner_catch(self):
        def inner():
            with util.timeout(0.1): #long timeout
                app.sleep(2.0)

        # expect the inner timeout to percolate outwards
        def outer():
            with util.timeout(1.0):
                self.assertRaises(TimeoutError, inner)
                return "foo"
        self.assertEqual(outer(), "foo")

    def test_inner_same(self):
        def inner():
            with util.timeout(0.1): #long timeout
                app.sleep(2.0)

        def outer():
            with util.timeout(0.1):
                self.assertRaises(TimeoutError, inner)
                raise TimeoutError
                return "foo"
        self.assertRaises(TimeoutError, outer)
        #self.assertEqual(outer(), "foo")

    def test_three_timeouts(self):
        def one():
            with util.timeout(0.6):
                app.sleep(1.0)
        def two():
            with util.timeout(0.8):
                self.assertRaises(TimeoutError, one)
                app.sleep(1.0)

        def three():
            with util.timeout(0.1):
                self.assertRaises(TimeoutError, two)
                app.sleep(1.0)
        self.assertRaises(TimeoutError, three)

    def test_three_successes(self):
        def one():
            with util.timeout(0.6):
                return "foo"
        def two():
            with util.timeout(0.8):
                return one()
        def three():
            with util.timeout(0.1):
                return two()
        self.assertEqual("foo", three())







def run_unittests():
    import sys
    if not sys.argv[1:]:
        sys.argv.append('-v')
    # This code might
    unittest.main()


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    app.install_stackless()

    def new_tasklet(f, *args, **kwargs):
        try:
            f(*args, **kwargs)
        except Exception:
            traceback.print_exc()
    run_unittests_tasklet = stackless.tasklet(new_tasklet)(run_unittests)

    while run_unittests_tasklet.alive:
        main.mainloop.loop()