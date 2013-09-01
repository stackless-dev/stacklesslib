#test the asyn call stuff
import stackless
import unittest
import logging
import time

from stacklesslib import main, app, util, async
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
    return async.create_task(GetSleepFunc(min, max), args)

def GetErrTask(exception=ZeroDivisionError, min=0.000001, max=0.01):
    return async.create_task(GetErrFunc(exception, min, max))

class TestTask(unittest.TestCase):
    def testTask(self):
        t = async.create_task(GetSleepFunc)

    def testWait(self):
        t = GetTask(("foo", "bar"))
        self.assertFalse(t.ready)
        t.wait()
        self.assertTrue(t.ready)
        self.assertEqual(t.get_result(), ("foo", "bar"))

    def testLateWait(self):
        t = GetTask(("foo", "bar"), max = 0.001)
        self.assertFalse(t.ready)
        main.sleep(0.002)
        self.assertTrue(t.ready)
        t.wait()
        self.assertTrue(t.ready)
        self.assertEqual(t.get_result(), ("foo", "bar"))


    def testGetResult(self):
        t = GetTask(("foo", "bar"))
        self.assertFalse(t.ready)
        t.wait()
        self.assertEqual(t.get_result(), ("foo", "bar"))
        self.assertTrue(t.ready)

    def testErrorTask(self):
        t = GetErrTask()
        self.assertFalse(t.ready)
        self.assertFalse(t.is_exception)
        t.wait()
        self.assertTrue(t.ready)
        self.assertTrue(t.is_exception)
        self.assertEqual(t.get_exception()[0], ZeroDivisionError)
        self.assertRaises(ZeroDivisionError, t.get_result)

    def testTimeout(self):
        t = GetErrTask(min=1)
        self.assertFalse(t.ready)
        self.assertRaises(TimeoutError, t.wait, 0.001)
        self.assertFalse(t.ready)


class TestWaitAll(unittest.TestCase):
    def _getTasks(self, shuffle=True):
         tasks = [GetTask((i,), min=0.001*i, max=0.001*i) for i in range(10)]
         if shuffle:
            random.shuffle(tasks)
         return tasks

    def testWaitAll(self):
        with atomic():
            tasks = self._getTasks(False)
            for t in tasks:
                self.assertFalse(t.ready)
            async.Task.wait_all(tasks)
            for t in tasks:
                self.assertTrue(t.ready)
            for i, t in enumerate(tasks):
                self.assertEqual(t.get_result(), (i,))

    def testWaitAllTimeout(self):
        with atomic():
            tasks = self._getTasks()
            self.assertRaises(TimeoutError, async.Task.wait_all, tasks, 0.005)
            # some should have run
            self.assertGreater(sum(task.ready for task in tasks), 0)

class TestWhenAll(unittest.TestCase):
    def _getTask(self):
         tasks = [GetTask((i,), min=0.001*i, max=0.001*i) for i in range(10)]
         random.shuffle(tasks)
         return async.Task.when_all(tasks), tasks

    def testWhenAll(self):
        with atomic():
            t, tasks = self._getTask()
            self.assertFalse(t.ready)
            for i in tasks:
                self.assertFalse(i.ready)
            r = t.get_result()
            self.assertEqual(r, None)
            self.assertTrue(t.ready)
            for i in tasks:
                self.assertTrue(i.ready)

    def testWhenAllTimeout(self):
        with atomic():
            t, tasks = self._getTask()
            self.assertRaises(TimeoutError, t.get_result, 0.005)
            self.assertFalse(t.ready)


class TestWaitAny(unittest.TestCase):
    def _getTasks(self, offset=0.002):
        tasks = [GetTask((i,), min=offset + 0.001*i, max=offset + 0.001*i) for i in range(10)]
        random.shuffle(tasks)
        return tasks

    def testWaitAny(self):
        with atomic():
            tasks = self._getTasks()
            for t in tasks:
                self.assertFalse(t.ready)
            i = async.Task.wait_any(tasks)
            self.assertTrue(tasks[i].ready)
            # one is ready now
            self.assertEqual(sum(task.ready for task in tasks), 1)

    def testWaitAnyTimeout(self):
        with atomic():
            tasks = self._getTasks()

            # This timeout should still allow a task to work.
            i = async.Task.wait_any(tasks, 0.005)
            self.assertTrue(tasks[i].ready)
            self.assertEqual(sum(task.ready for task in tasks), 1)

    def testWaitAnyShortTimeout(self):
        with atomic():
            tasks = self._getTasks()

            # No task should run
            self.assertRaises(TimeoutError, async.Task.wait_any, tasks, 0.001)
            self.assertEqual(sum(task.ready for task in tasks), 0)


class TestWhenAny(unittest.TestCase):
    def _getTask(self):
        tasks = [GetTask((i,), min=0.001*i, max=0.001*i) for i in range(2, 12)]
        random.shuffle(tasks)
        return async.Task.when_any(tasks), tasks

    def testWhenAny(self):
        with atomic():
            t, tasks= self._getTask()
            self.assertFalse(t.ready)
            for i in tasks:
                self.assertFalse(i.ready)
            i = t.get_result()
            self.assertTrue(tasks[i].ready)
            # one is ready now
            self.assertEqual(sum(task.ready for task in tasks), 1)

    def testWhenAnyTimeout(self):
        with atomic():
            t, tasks= self._getTask()

            # This timeout should still allow a task to work.
            i = t.get_result()
            self.assertTrue(tasks[i].ready)
            self.assertEqual(sum(task.ready for task in tasks), 1)

    def testWhenAnyShortTimeout(self):
        with atomic():
            t, tasks= self._getTask()

            # No task should run
            self.assertRaises(TimeoutError, t.get_result, 0.001)
            self.assertFalse(t.ready)
            self.assertEqual(sum(task.ready for task in tasks), 0)

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
        r = t.get_result()
        self.assertEqual(r, "foobar")
        self.assertEvents()

    @async.task()
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
        self.assertRaises(TimeoutError, t.get_result)
        self.assertEvents()


class TestFactories(unittest.TestCase):
    @async.task(async.taskletTaskFactory)
    def taskletTask(self, arg):
        return arg

    def testTasklet(self):
        r = self.taskletTask("foo")
        self.assertEqual(r.get_result(), "foo")

    @async.task(async.threadTaskFactory)
    def threadTask(self, arg):
        return arg

    def testThread(self):
        r = self.threadTask("foo")
        self.assertEqual(r.get_result(), "foo")

    @async.task(async.dummyTaskFactory)
    def dummyTask(self, arg):
        return arg

    def testDummy(self):
        r = self.dummyTask("foo")
        self.assertEqual(r.get_result(), "foo")

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
