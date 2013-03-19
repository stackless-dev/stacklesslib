"""
Threadpool classes.  These are used when we want to dispatch work to happen on "real" threads.
"""

import collections
import threading

from . import locks, main
from .util import call_async

#defeat monkeypatching of the "threading" module
if hasattr(threading, "real_threading"):
    _realthreading = threading.real_threading
    _RealThread = threading.real_threading.Thread
else:
    _realthreading = threading
    _RealThread = threading.Thread


class dummy_threadpool(object):
    """
    A dummy threadpool which always starts a new thread for each request
    """
    def __init__(self, stack_size=None):
        self.stack_size = stack_size

    def stop(self):
        pass

    def start_thread(self, target):
        stack_size = self.stack_size
        if stack_size is not None:
            prev_stacksize = _realthreading.stack_size()
            _realthreading.stack_size(stack_size)
        try:
            thread = _RealThread(target=target)
            thread.start()
            return thread
        finally:
            if stack_size is not None:
                _realthreading.stack_size(prev_stacksize)

    def submit(self, job):
        self.start_thread(job)

class simple_threadpool(dummy_threadpool):
    def __init__(self, stack_size=None, n_threads=1):
        super(simple_threadpool, self).__init__(stack_size)
        self.threads_max = n_threads
        self.threads_n = 0          # threads running
        self.threads_executing = 0  # threads performing work
        self.cond = _realthreading.Condition()
        self.queue = collections.deque()

    def stop(self):
        with self.cond:
            self.threads_max = 0
            self.cond.notify_all()

    def submit(self, job):
        with self.cond:
            ready = self.threads_n - self.threads_executing
            if not ready and self.threads_n < self.threads_max:
                self.threads_n += 1
                try:
                    self.start_thread(self._threadfunc)
                except:
                    self.threads_n -= 1
                    raise
            self.queue.append(job)
            self.cond.notify()

    def _threadfunc(self):
        def predicate():
            return self.threads_n > self.threads_max or self.queue

        with self.cond:
            try:
                # Wait for quit or job
                while True:
                    self.cond.wait_for(predicate)
                    if self.threads_n > self.threads_max:
                        return
                    job = self.queue.popleft()

                    # Execute job
                    self.threads_executing += 1
                    try:
                        with locks.released(self.cond):
                            job()
                    finally:
                        self.threads_executing -= 1
                        job = None
            finally:
                self.threads_n -= 1

def call_on_thread(function, args=(), kwargs={}, stack_size=None, pool=None, timeout=None):
    """Run the given function on a different thread and return the result
       This function blocks on a channel until the result is available.
       Ideal for performing OS type tasks, such as saving files or compressing
    """
    if not pool:
        pool = dummy_threadpool(stack_size)
    # Wrap the function so that it will wake up the main thread when it is done
    def wrapped():
        try:
            return function(*args, **kwargs)
        finally:
            main.mainloop.interrupt_wait()
    def dispatcher(function):
        pool.submit(function)
    return call_async(wrapped, dispatcher, timeout=timeout)
