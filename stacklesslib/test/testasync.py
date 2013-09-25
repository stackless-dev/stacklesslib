#test the asyn call stuff
import stackless
import unittest
import logging
import time
import sys

from stacklesslib import main, app, util, async, futures
from stacklesslib.errors import TimeoutError
from stacklesslib.util import atomic
import random


# return a function that sleeps for a random time
def GetSleepFunc(min=0.000001, max=0.01):
    t = random.random() * (max-min) + min
    def f(*args):
        #t0 = time.clock()
        #print "sleeping for ", t
        main.sleep(t)
        #print "slept for", time.clock()-t0, "wanted", t
        return args
    return f

def GetErrFunc(exception=ZeroDivisionError, min=0.000001, max=0.01):
    t = random.random() * (max-min) + min
    def f():
        main.sleep(t)
        raise exception
    return f

def GetTask(args, min=0.000001, max=0.01):
    return async.create_future(GetSleepFunc(min, max), args)

def GetErrTask(exception=ZeroDivisionError, min=0.000001, max=0.01):
    return async.create_future(GetErrFunc(exception, min, max))

class TestTask(unittest.TestCase):
    def testTask(self):
        t = async.create_future(GetSleepFunc)

    def testWait(self):
        t = GetTask(("foo", "bar"))
        self.assertFalse(t.done())
        t.wait()
        self.assertTrue(t.done())
        self.assertEqual(t.result(), ("foo", "bar"))

    def testLateWait(self):
        t = GetTask(("foo", "bar"), max = 0.001)
        self.assertFalse(t.done())
        main.sleep(0.002)
        self.assertTrue(t.done())
        t.wait()
        self.assertTrue(t.done())
        self.assertEqual(t.result(), ("foo", "bar"))


    def testGetResult(self):
        t = GetTask(("foo", "bar"))
        self.assertFalse(t.done())
        t.wait()
        self.assertEqual(t.result(), ("foo", "bar"))
        self.assertTrue(t.done())

    def testErrorTask(self):
        t = GetErrTask()
        self.assertFalse(t.done())
        t.wait()
        self.assertTrue(t.done())
        self.assertTrue(t.exception())
        self.assertEqual(t.exception()[0], ZeroDivisionError)
        self.assertRaises(ZeroDivisionError, t.result)

    def testTimeout(self):
        t = GetErrTask(min=1)
        self.assertFalse(t.done())
        self.assertRaises(TimeoutError, t.wait, 0.001)
        self.assertFalse(t.done())



class TestAwait(unittest.TestCase):
    def setUp(self):
        self.events = []

    def assertEvents(self, l=None):
        if l is None:
            l = len(self.events)
        self.assertEqual(self.events, list(range(l)))


    @async.async
    def f1(awaiter, self):
        t = GetTask(('foo',))
        self.events.append(2)
        return awaiter.await(t)

    @async.async
    def f2(awaiter, self):
        t = GetTask(('bar',))
        self.events.append(4)
        return awaiter.await(t)

    @async.async
    def f3(awaiter, self, arg):
        self.assertEqual(arg, "hello")
        self.events.append(1)
        t1 = self.f1()
        self.events.append(3)
        t2 = self.f2()
        self.events.append(5)
        r1 = awaiter.await(t1)
        r2 = awaiter.await(t2)
        return r1[0] + r2[0]

    @async.async
    def f4(awaiter, self):
        self.events.append(0)
        t = self.f3("hello")
        self.events.append(6)
        return awaiter.await(t)

    def testAwait(self):
        t = self.f4()
        self.events.append(7)
        r = t.result()
        self.assertEqual(r, "foobar")
        self.assertEvents()

    @async.future()
    def sleep(self, d):
        main.sleep(d)

    @async.async
    def f5(awaiter, self):
        self.events.append(1)
        awaiter.await(self.sleep(0.01), 0.001)

    def testAwaitTimeout(self):
        self.events.append(0)
        t = self.f5()
        self.events.append(2)
        self.assertRaises(TimeoutError, t.result)
        self.assertEvents()


class TestFactories(unittest.TestCase):
    @async.future(futures.tasklet_executor)
    def taskletTask(self, arg):
        return arg

    def testTasklet(self):
        r = self.taskletTask("foo")
        self.assertEqual(r.result(), "foo")

    @async.future(futures.thread_executor)
    def threadTask(self, arg):
        return arg

    def testThread(self):
        r = self.threadTask("foo")
        self.assertEqual(r.result(), "foo")

from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    # stdout printing affects timing, so diagnostics need to be kept around.
    import cStringIO
    s = cStringIO.StringIO()
    old = sys.stdout
    sys.stdout = s
    try:
        unittest.main()
    finally:
        sys.stdout=old
        print s.getvalue()

#t = stackless.tasklet(top)()
#stacklesslib.main.mainloop.run()
