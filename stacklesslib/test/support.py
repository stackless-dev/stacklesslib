#support.py
# unittest support functions

from time import clock
import sys
import unittest
import functools

import stackless
import stacklesslib.errors
from stacklesslib.util import timeout
from stacklesslib import app, main

# This is the total timeout for running tests.  Adjust as needed.
TIMEOUT = 10

def timesafe(t=1.0):
    """Decorate a unittest with this to make it error after 't' seconds"""
    def helper(func):
        @functools.wraps(func)
        def testmethod(self):
            try:
                with timeout(t):
                    func(self)
            except stacklesslib.errors.TimeoutError:
                self.fail("test case timed out")
        return testmethod
    return helper

class StacklessTestSuite(unittest.TestSuite):
    def run(self, results):
        err = []
        def tasklet_run():
            try:
                unittest.TestSuite.run(self, results)
            except:
                err.append(sys.exc_info())
        app.install_stackless()
        tasklet = stackless.tasklet(tasklet_run)()
        deadline = clock() + TIMEOUT
        while tasklet.alive:
            main.mainloop.loop()
            if clock() > deadline:
                raise stacklesslib.errors.TimeoutError("unittest took too long.  Deadlock?")
        if err:
            try:
                raise err[0][0], err[0][1], err[0][2]
            finally:
                err = None


def load_tests(loader, tests, pattern): # test loader protocol
    suite = StacklessTestSuite()
    suite.addTests(tests)
    return suite
