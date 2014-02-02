import unittest
import threading
import stackless
import stacklesslib.threadpool
import stacklesslib.errors
import time

class TestCallOnThread(unittest.TestCase):
    def test_different_thread(self):
        r = []
        def func():
            r.append(threading.current_thread().ident)
        stacklesslib.threadpool.call_on_thread(func)
        self.assertNotEqual(threading.current_thread().ident, r[0])


    def test_result(self):
        def func():
            return "hello"
        r = stacklesslib.threadpool.call_on_thread(func)
        self.assertEqual(r, "hello")

    def test_args(self):
        def func(a, b):
            return a + b
        r = stacklesslib.threadpool.call_on_thread(func, ("hello", "dolly"))
        self.assertEqual(r, "hellodolly")

    def test_kwargs(self):
        def func(a, b="", c=""):
            return a + b + c
        r = stacklesslib.threadpool.call_on_thread(func, ("hello", " "), {"c" : "dude"})
        self.assertEqual(r, "hello dude")

    def test_timeout(self):
        def func():
            time.sleep(0.01)
        self.assertRaises(stacklesslib.errors.TimeoutError, stacklesslib.threadpool.call_on_thread, func, timeout=0.001)

    def test_on_orphaned_not_called(self):
        def func():
            pass
        c = []
        def orp():
            c.append(True)
        stacklesslib.threadpool.call_on_thread(func, on_orphaned=orp)
        self.assertEqual(c, [])

    def test_on_orphaned_called(self):
        c = []
        def func():
            c.append(1)
            time.sleep(0.01)
        def orp():
            c.append(2)
        def task():
            stacklesslib.threadpool.call_on_thread(func, on_orphaned=orp)
        t = stackless.tasklet(task)()
        # wait until call is in progress
        while not c:
            stackless.schedule()
            time.sleep(0.001)
        t.kill()
        self.assertEqual(c, [1, 2])

    def test_pool(self):
        class pool(stacklesslib.threadpool.DummyThreadPool):
            def start_thread(self, target):
                self.started = True
                return super(pool, self).start_thread(target)
        p = pool()
        self.assertFalse(hasattr(p, "started"))
        stacklesslib.threadpool.call_on_thread(lambda:None, pool=p)
        self.assertTrue(p.started)

class TestSimpleThreadPool(unittest.TestCase):
    def test_max_threads(self):
        p = stacklesslib.threadpool.SimpleThreadPool(3)
        c = [0]
        n = [0]
        m = [0]
        l = threading.Lock()
        def func():
            with l:
                n[0] += 1
                if n[0] > m[0]:
                    m[0] = n[0]
            time.sleep(0.001)
            with l:
                n[0] -= 1
                c[0] += 1

        for i in range(10):
            p.submit(func)
        while c[0] < 10:
            time.sleep(0.001)
        p.shutdown()
        self.assertEqual(n[0], 0)
        self.assertEqual(m[0], 3)
            

    
    

        

from .support import load_tests

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    unittest.main()