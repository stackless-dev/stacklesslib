#support.py
# unittest support functions

import sys
import unittest

import stackless
from stacklesslib import app, main

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
        while tasklet.alive:
            main.mainloop.loop()
        if err:
            try:
                raise err[0][0], err[0][1], err[0][2]
            finally:
                err = None

def load_tests(loader, tests, pattern): # test loader protocol
    suite = StacklessTestSuite()
    suite.addTests(tests)
    return suite
