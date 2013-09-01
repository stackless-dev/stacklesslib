import unittest

import stackless

import stacklesslib.main
import stacklesslib.app


class TestMainLoop(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        self.checkLeftThingsClean()

    def checkLeftThingsClean(self):
        #self.assertEqual(len(stacklesslib.main.event_queue), 0)
        return True

    def testPreemptiveRun(self):
        """
        Create a tasklet and run it pre-emptively, ensuring that we
        get the tasklet returned from 'run_tasklets' when it is
        interrupted.
        """

        t = stackless.tasklet(ArbitraryFunc)()
        t.run()

        while t.alive:
            stacklesslib.main.mainloop.pump(0)
            ret = stacklesslib.main.mainloop.run_tasklets(100)
            if ret is None and t.alive:
                continue
            self.assertEqual(ret, t)
            break
        else:
            self.fail("Tasklet was not interrupted")

    def testCooperativeRun(self):
        """
        Create a tasklet and run it cooperatively, ensuring we never
        get it interrupted and returned from run_tasklets.
        """

        t = stackless.tasklet(ArbitraryFunc)()
        t.run()

        while t.alive:
            stacklesslib.main.mainloop.pump(0)
            ret = stacklesslib.main.mainloop.run_tasklets()
            self.assertFalse(ret)


def ArbitraryFunc():
    sum = 0
    for i in range(1000):
        for j in range(1000):
            sum += 10
        stacklesslib.app.sleep(0)

def load_tests(loader, tests, pattern): # test loader protocol
    suite = unittest.TestSuite()
    suite.addTests(tests)
    stacklesslib.app.install_stackless()
    return suite


if __name__ == '__main__':
    unittest.main()
