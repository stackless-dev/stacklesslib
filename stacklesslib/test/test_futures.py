from __future__ import print_function
import test.test_support



import sys
import threading
import unittest
import weakref
import logging
from collections import Counter

from stacklesslib import futures
from stacklesslib.futures import Future
from stacklesslib.app import sleep
from stacklesslib.locks import Barrier
from stacklesslib.test import support
#make time.sleep be the right one
import time
class Time(object):
    pass
_time = Time()
_time.sleep = sleep
_time.time = time.time
time = _time


from stacklesslib.futures import RUNNING, PENDING, CANCELLED, FINISHED
CANCELLED_AND_NOTIFIED = CANCELLED #no difference

def create_future(state=PENDING, exception=None, result=None):
    f = Future()
    f.state = state
    if state == FINISHED:
        if exception:
            f._result = (False, (exception, None, None))
        else:
            f._result = (True, result)
    elif state == CANCELLED:
        f._result = (False, (None, ()))
    return f

PENDING_FUTURE = create_future(state=PENDING)
RUNNING_FUTURE = create_future(state=RUNNING)
CANCELLED_FUTURE = create_future(state=CANCELLED)
CANCELLED_AND_NOTIFIED_FUTURE = create_future(state=CANCELLED_AND_NOTIFIED)
EXCEPTION_FUTURE = create_future(state=FINISHED, exception=OSError())
SUCCESSFUL_FUTURE = create_future(state=FINISHED, result=42)


def mul(x, y):
    return x * y


def sleep_and_raise(t):
    time.sleep(t)
    raise Exception('this is an exception')

def sleep_and_print(t, msg):
    time.sleep(t)
    print(msg)
    sys.stdout.flush()


class MyObject(object):
    def my_method(self):
        pass


class ExecutorMixin(unittest.TestCase):
    worker_count = 5
    do_prime = False

    def setUp(self):
        self.t1 = time.time()
        try:
            self.executor = self.executor_type(max_workers=self.worker_count)
        except NotImplementedError as e:
            self.skipTest(str(e))
        if self.do_prime:
            self._prime_executor()

    def tearDown(self):
        self.executor.shutdown(wait=True)
        dt = time.time() - self.t1
        # print("%.2fs" % dt, end=' ')
        self.assertLess(dt, 60, "synchronization issue: test lasted too long")

    def _prime_executor(self):
        # Make sure that the executor is ready to do work before running the
        # tests. This should reduce the probability of timeouts in the tests.
        futures = [self.executor.submit(time.sleep, 0.1)
                   for _ in range(self.worker_count)]

        for f in futures:
            f.result()

# Use this so that we don't have to change the entire file
class TaskletMixin(ExecutorMixin):
    executor_type = futures.TaskletExecutor
class ThreadPoolMixin(ExecutorMixin):
    executor_type = futures.ThreadPoolExecutor

class WaitTests(object):

    def test_first_completed(self):
        #future1 = self.executor.submit(mul, 21, 2)
        future1 = self.executor.submit(time.sleep, 0.05)
        future2 = self.executor.submit(time.sleep, 0.15)

        done, not_done = futures.wait(
                [CANCELLED_FUTURE, future1, future2],
                 return_when=futures.FIRST_COMPLETED)

        #self.assertEqual(set([future1]), done)
        #self.assertEqual(set([CANCELLED_FUTURE, future2]), not_done)
        #cancelled is effettive immediately
        self.assertEqual(set([CANCELLED_FUTURE]), done)
        self.assertEqual(set([future1, future2]), not_done)

        future1 = self.executor.submit(mul, 21, 2)
        future2 = self.executor.submit(time.sleep, 0.15)
        done, not_done = futures.wait(
                [future1, future2],
                 return_when=futures.FIRST_COMPLETED)
        self.assertEqual(set([future1]), done)
        self.assertEqual(set([future2]), not_done)

    def test_first_completed_some_already_completed(self):
        future1 = self.executor.submit(time.sleep, 0.15)

        finished, pending = futures.wait(
                 [CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE, future1],
                 return_when=futures.FIRST_COMPLETED)

        self.assertEqual(
                set([CANCELLED_AND_NOTIFIED_FUTURE, SUCCESSFUL_FUTURE]),
                finished)
        self.assertEqual(set([future1]), pending)

    def test_first_exception(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(sleep_and_raise, 0.15)
        future3 = self.executor.submit(time.sleep, 0.3)

        finished, pending = futures.wait(
                [future1, future2, future3],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([future1, future2]), finished)
        self.assertEqual(set([future3]), pending)

    def test_first_exception_some_already_complete(self):
        future1 = self.executor.submit(divmod, 21, 0)
        future2 = self.executor.submit(time.sleep, 0.15)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 #CANCELLED_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 future1, future2],
                return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              future1]), finished)
        #self.assertEqual(set([CANCELLED_FUTURE, future2]), pending)
        self.assertEqual(set([future2]), pending)

    def test_first_exception_one_already_failed(self):
        future1 = self.executor.submit(time.sleep, 0.2)

        finished, pending = futures.wait(
                 [EXCEPTION_FUTURE, future1],
                 return_when=futures.FIRST_EXCEPTION)

        self.assertEqual(set([EXCEPTION_FUTURE]), finished)
        self.assertEqual(set([future1]), pending)

    def test_all_completed(self):
        future1 = self.executor.submit(divmod, 2, 0)
        future2 = self.executor.submit(mul, 2, 21)

        finished, pending = futures.wait(
                [SUCCESSFUL_FUTURE,
                 CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 future1,
                 future2],
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([SUCCESSFUL_FUTURE,
                              CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              future1,
                              future2]), finished)
        self.assertEqual(set(), pending)

    def test_timeout(self):
        future1 = self.executor.submit(mul, 6, 7)
        future2 = self.executor.submit(time.sleep, 0.6)

        finished, pending = futures.wait(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2],
                timeout=0.5,
                return_when=futures.ALL_COMPLETED)

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE,
                              future1]), finished)
        self.assertEqual(set([future2]), pending)

class ThreadPoolWaitTests(ThreadPoolMixin, WaitTests):

    def test_pending_calls_race(self):
        # Issue #14406: multi-threaded race condition when waiting on all
        # futures.
        event = threading.Event()
        def future_func():
            event.wait()
        #oldswitchinterval = sys.getswitchinterval()
        #sys.setswitchinterval(1e-6)
        try:
            fs = {self.executor.submit(future_func) for i in range(100)}
            event.set()
            futures.wait(fs, return_when=futures.ALL_COMPLETED)
        finally:
            #sys.setswitchinterval(oldswitchinterval)
            pass

class AsCompletedTests(object):
    # TODO(brian@sweetapp.com): Should have a test with a non-zero timeout.
    def test_no_timeout(self):
        future1 = self.executor.submit(mul, 2, 21)
        future2 = self.executor.submit(mul, 7, 6)

        completed = set(futures.as_completed(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]))
        self.assertEqual(set(
                [CANCELLED_AND_NOTIFIED_FUTURE,
                 EXCEPTION_FUTURE,
                 SUCCESSFUL_FUTURE,
                 future1, future2]),
                completed)

    def test_zero_timeout(self):
        future1 = self.executor.submit(time.sleep, 0.2)
        completed_futures = set()
        try:
            for future in futures.as_completed(
                    [CANCELLED_AND_NOTIFIED_FUTURE,
                     EXCEPTION_FUTURE,
                     SUCCESSFUL_FUTURE,
                     future1],
                    timeout=0):
                completed_futures.add(future)
        except futures.TimeoutError:
            pass

        self.assertEqual(set([CANCELLED_AND_NOTIFIED_FUTURE,
                              EXCEPTION_FUTURE,
                              SUCCESSFUL_FUTURE]),
                         completed_futures)


class TaskletAsCompletedTests(TaskletMixin, AsCompletedTests):
    pass

class ThreadPoolAsCompletedTests(ThreadPoolMixin, AsCompletedTests):
    pass



class ExecutorTest(object):
    # Executor.shutdown() and context manager usage is tested by
    # ExecutorShutdownTest.
    def test_submit(self):
        future = self.executor.submit(pow, 2, 8)
        self.assertEqual(256, future.result())

    def test_submit_keyword(self):
        future = self.executor.submit(mul, 2, y=8)
        self.assertEqual(16, future.result())

    def test_map(self):
        self.assertEqual(
                list(self.executor.map(pow, range(10), range(10))),
                list(map(pow, range(10), range(10))))

    def test_map_exception(self):
        i = iter(self.executor.map(divmod, [1, 1, 1, 1], [2, 3, 0, 5]))
        self.assertEqual(next(i), (0, 1))
        self.assertEqual(next(i), (0, 1))
        self.assertRaises(ZeroDivisionError, next, i)

    def test_map_timeout(self):
        results = []
        try:
            for i in self.executor.map(time.sleep,
                                       [0, 0, 0.6],
                                       timeout=0.5):
                results.append(i)
        except futures.TimeoutError:
            pass
        else:
            self.fail('expected TimeoutError')

        self.assertEqual([None, None], results)

    def test_shutdown_race_issue12456(self):
        # Issue #12456: race condition at shutdown where trying to post a
        # sentinel in the call queue blocks (the queue is full while processes
        # have exited).
        self.executor.map(str, [2] * (self.worker_count + 1))
        self.executor.shutdown()

    @test.test_support.cpython_only
    def test_no_stale_references(self):
        # Issue #16284: check that the executors don't unnecessarily hang onto
        # references.
        my_object = MyObject()
        my_object_collected = threading.Event()
        my_object_callback = weakref.ref(
            my_object, lambda obj: my_object_collected.set())
        # Deliberately discarding the future.
        self.executor.submit(my_object.my_method)
        del my_object

        time.sleep(0.01)
        collected = my_object_collected.wait(timeout=5.0)
        self.assertTrue(collected,
                        "Stale reference not collected within timeout.")


class TaskletExecutorTest(TaskletMixin, ExecutorTest):
    def test_map_submits_without_iteration(self):
        """Tests verifying issue 11777."""
        finished = []
        def record_finished(n):
            finished.append(n)

        self.executor.map(record_finished, range(10))
        self.executor.shutdown(wait=True)
        #self.assertCountEqual(finished, range(10))
        first, second = finished, range(10)
        self.assertEqual(Counter(list(first)), Counter(list(second)))

class ThreadPoolExecutorTest(ThreadPoolMixin, TaskletExecutorTest):
    pass


class FutureTests(unittest.TestCase):
    def test_done_callback_with_result(self):
        callback_result = [None]
        def fn(callback_future):
            callback_result[0] = callback_future.result()

        f = create_future(RUNNING)
        f.add_done_callback(fn)
        f.set_result(5)
        self.assertEqual(5, callback_result[0])

    def test_done_callback_with_exception(self):
        callback_exception = [None]
        def fn(callback_future):
            callback_exception[0] = callback_future.exception()

        f = create_future(RUNNING)
        f.add_done_callback(fn)
        f.set_exception(Exception, ('test',))
        self.assertEqual(('test',), callback_exception[0][1].args)

    def test_done_callback_with_cancel(self):
        was_cancelled = [None]
        def fn(callback_future):
            was_cancelled[0] = callback_future.cancelled()

        f = Future()
        f.add_done_callback(fn)
        self.assertTrue(f.cancel())
        self.assertTrue(was_cancelled[0])

    def test_done_callback_raises(self):
        with support.captured_stderr() as stderr:
            raising_was_called = [False]
            fn_was_called = [False]

            def raising_fn(callback_future):
                raising_was_called[0] = True
                raise Exception('doh!')

            def fn(callback_future):
                fn_was_called[0] = True

            f = create_future(RUNNING)
            f.add_done_callback(raising_fn)
            f.add_done_callback(fn)
            f.set_result(5)
            self.assertTrue(raising_was_called[0])
            self.assertTrue(fn_was_called[0])
            self.assertIn('Exception: doh!', stderr.getvalue())

    def test_done_callback_already_successful(self):
        callback_result = [None]
        def fn(callback_future):
            callback_result[0] = callback_future.result()

        f = create_future(RUNNING)
        f.set_result(5)
        f.add_done_callback(fn)
        self.assertEqual(5, callback_result[0])

    def test_done_callback_already_failed(self):
        callback_exception = [None]
        def fn(callback_future):
            callback_exception[0] = callback_future.exception()

        f = create_future(RUNNING)
        f.set_exception(Exception, ('test',))
        f.add_done_callback(fn)
        self.assertEqual(('test',), callback_exception[0][1].args)

    def test_done_callback_already_cancelled(self):
        was_cancelled = [None]
        def fn(callback_future):
            was_cancelled[0] = callback_future.cancelled()

        f = Future()
        self.assertTrue(f.cancel())
        f.add_done_callback(fn)
        self.assertTrue(was_cancelled[0])

    def _test_repr(self): #don"t have regex, but want state
        self.assertRegex(repr(PENDING_FUTURE),
                         '<Future at 0x[0-9a-f]+ state=pending>')
        self.assertRegex(repr(RUNNING_FUTURE),
                         '<Future at 0x[0-9a-f]+ state=running>')
        self.assertRegex(repr(CANCELLED_FUTURE),
                         '<Future at 0x[0-9a-f]+ state=cancelled>')
        self.assertRegex(repr(CANCELLED_AND_NOTIFIED_FUTURE),
                         '<Future at 0x[0-9a-f]+ state=cancelled>')
        self.assertRegex(
                repr(EXCEPTION_FUTURE),
                '<Future at 0x[0-9a-f]+ state=finished raised OSError>')
        self.assertRegex(
                repr(SUCCESSFUL_FUTURE),
                '<Future at 0x[0-9a-f]+ state=finished returned int>')


    def _test_cancel(self): # internal state test, ignore.
        f1 = create_future(state=PENDING)
        f2 = create_future(state=RUNNING)
        f3 = create_future(state=CANCELLED)
        f4 = create_future(state=CANCELLED_AND_NOTIFIED)
        f5 = create_future(state=FINISHED, exception=OSError())
        f6 = create_future(state=FINISHED, result=5)

        self.assertTrue(f1.cancel())
        self.assertEqual(f1._state, CANCELLED)

        self.assertFalse(f2.cancel())
        self.assertEqual(f2._state, RUNNING)

        self.assertTrue(f3.cancel())
        self.assertEqual(f3._state, CANCELLED)

        self.assertTrue(f4.cancel())
        self.assertEqual(f4._state, CANCELLED_AND_NOTIFIED)

        self.assertFalse(f5.cancel())
        self.assertEqual(f5._state, FINISHED)

        self.assertFalse(f6.cancel())
        self.assertEqual(f6._state, FINISHED)

    def test_cancelled(self):
        self.assertFalse(PENDING_FUTURE.cancelled())
        self.assertFalse(RUNNING_FUTURE.cancelled())
        self.assertTrue(CANCELLED_FUTURE.cancelled())
        self.assertTrue(CANCELLED_AND_NOTIFIED_FUTURE.cancelled())
        self.assertFalse(EXCEPTION_FUTURE.cancelled())
        self.assertFalse(SUCCESSFUL_FUTURE.cancelled())

    def test_done(self):
        self.assertFalse(PENDING_FUTURE.done())
        self.assertFalse(RUNNING_FUTURE.done())
        self.assertTrue(CANCELLED_FUTURE.done())
        self.assertTrue(CANCELLED_AND_NOTIFIED_FUTURE.done())
        self.assertTrue(EXCEPTION_FUTURE.done())
        self.assertTrue(SUCCESSFUL_FUTURE.done())

    def test_running(self):
        #self.assertFalse(PENDING_FUTURE.running())
        self.assertTrue(RUNNING_FUTURE.running())
        self.assertFalse(CANCELLED_FUTURE.running())
        self.assertFalse(CANCELLED_AND_NOTIFIED_FUTURE.running())
        self.assertFalse(EXCEPTION_FUTURE.running())
        self.assertFalse(SUCCESSFUL_FUTURE.running())

    def test_result_with_timeout(self):
        self.assertRaises(futures.TimeoutError,
                          PENDING_FUTURE.result, timeout=0)
        self.assertRaises(futures.TimeoutError,
                          RUNNING_FUTURE.result, timeout=0)
        self.assertRaises(futures.CancelledError,
                          CANCELLED_FUTURE.result, timeout=0)
        self.assertRaises(futures.CancelledError,
                          CANCELLED_AND_NOTIFIED_FUTURE.result, timeout=0)
        self.assertRaises(OSError, EXCEPTION_FUTURE.result, timeout=0)
        self.assertEqual(SUCCESSFUL_FUTURE.result(timeout=0), 42)

    def test_result_with_success(self):
        # TODO(brian@sweetapp.com): This test is timing dependant.
        def notification():
            # Wait until the main thread is waiting for the result.
            time.sleep(0.1)
            f1.set_result(42)

        f1 = create_future(state=RUNNING)
        t = threading.Thread(target=notification)
        t.start()

        self.assertEqual(f1.result(timeout=5), 42)

    def _test_result_with_cancel(self): #TODO, re-enable
        # TODO(brian@sweetapp.com): This test is timing dependant.
        def notification():
            # Wait until the main thread is waiting for the result.
            time.sleep(1)
            f1.cancel()

        f1 = create_future(state=PENDING)
        t = threading.Thread(target=notification)
        t.start()

        self.assertRaises(futures.CancelledError, f1.result, timeout=5)

    def test_exception_with_timeout(self):
        self.assertRaises(futures.TimeoutError,
                          PENDING_FUTURE.exception, timeout=0)
        self.assertRaises(futures.TimeoutError,
                          RUNNING_FUTURE.exception, timeout=0)
        self.assertRaises(futures.CancelledError,
                          CANCELLED_FUTURE.exception, timeout=0)
        self.assertRaises(futures.CancelledError,
                          CANCELLED_AND_NOTIFIED_FUTURE.exception, timeout=0)
        #self.assertTrue(isinstance(EXCEPTION_FUTURE.exception(timeout=0),
        #                           OSError))
        self.assertEqual(SUCCESSFUL_FUTURE.exception(timeout=0), None)

    def _test_exception_with_success(self): #TODO, re-enable
        def notification():
            # Wait until the main thread is waiting for the exception.
            time.sleep(1)
            with f1._condition:
                f1._state = FINISHED
                f1._exception = OSError()
                f1._condition.notify_all()

        f1 = create_future(state=PENDING)
        t = threading.Thread(target=notification)
        t.start()

        self.assertTrue(isinstance(f1.exception(timeout=5), OSError))

class TestConvenience(unittest.TestCase):
    def test_all(self):
        f0 = futures.tasklet_executor.submit(lambda:(time.sleep(0.1), 0))
        f1 = futures.tasklet_executor.submit(lambda:(time.sleep(0.2), 1))
        self.assertEqual(set(futures.all_results((f0, f1))), set([(None, 0), (None, 1)]))

    def test_any(self):
        f0 = futures.tasklet_executor.submit(lambda:(time.sleep(0.1), 0))
        f1 = futures.tasklet_executor.submit(lambda:(time.sleep(0.2), 1))
        r = futures.any_result((f0, f1))
        self.assertTrue(r in [(None, 0), (None, 1)])

class TestExecutors(unittest.TestCase):
    def test_direct(self):
        f = futures.direct_executor.submit(mul, 2, 3)
        self.assertTrue(f.done())
        self.assertEqual(f.result(), mul(2, 3))

    def test_null(self):
        f = futures.null_executor.submit(mul, 2, 3)
        self.assertFalse(f.done())
        def foo():
            return f.result(timeout=0.01)
        self.assertRaises(futures.TimeoutError, foo)

    def test_tasklet(self):
        f = futures.tasklet_executor.submit(mul, 2, 3)
        self.assertFalse(f.done())
        self.assertEqual(f.result(), mul(2, 3))

    def test_tmmediate(self):
        f = futures.immediate_tasklet_executor.submit(mul, 2, 3)
        self.assertTrue(f.done())
        self.assertEqual(f.result(), mul(2, 3))

    def test_threading(self):
        import threading
        def foo():
            return threading.current_thread().ident
        f = futures.thread_executor.submit(foo)
        self.assertNotEqual(f.result(), threading.current_thread().ident)

from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()
