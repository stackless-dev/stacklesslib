#testutil.py

import unittest
import logging
import stackless
import time
import contextlib

from stacklesslib import main, app, util
from stacklesslib.errors import TimeoutError, CancelledError


class TestCallAsyncDummy(unittest.TestCase):
    def dispatcher(self):
        return util.dummy_dispatcher

    def testResult(self):
        def func(result):
            return result, result
        v = 1, 2, 3
        self.assertEqual(util.call_async(lambda:func(v), dispatcher=self.dispatcher()), (v, v))

    def testException(self):
        def func(e):
            raise e
        e = ZeroDivisionError
        self.assertRaises(e, util.call_async, lambda:func(e), dispatcher=self.dispatcher())

class TestCallAsyncTasklet(TestCallAsyncDummy):
    def dispatcher(self):
        return util.tasklet_dispatcher

    def testTasklets(self):
        def func():
            return stackless.getcurrent()

        other = util.call_async(func, dispatcher=self.dispatcher())
        self.assertNotEqual(other, stackless.getcurrent())

    def testTimeout(self):
        def func():
            return main.sleep(0.1)
        self.assertRaises(TimeoutError, util.call_async, func, dispatcher=self.dispatcher(), timeout=0.01)


    def testCancel(self):
        def func():
            me = stackless.getcurrent()
            def killer():
                me.kill()
            stackless.tasklet(killer)()
            return main.sleep(0.1)
        self.assertRaises(CancelledError, util.call_async, func, dispatcher=self.dispatcher(), timeout=0.01)


from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()

