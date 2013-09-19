#futures.py

import sys
import traceback
import time
import stackless
from .errors import TimeoutError, CancelledError
from . import util
from .util import timeout as _timeout
from .util import tasklet_run, atomic
from weakref import WeakSet

class TaskletExecutorBase(object):
    """Base class for TaskFactories"""
    def submit(self, fn, *args, **kwargs):
        pass

    def map(self, fn, *iterables, **kwds):
        timeout = kwds.pop("timeout", None)
        if kwds:
            raise TypeError
        t = util.Timeouts(timeout)
        futures = [self.submit(fn, *args) for args in zip(*iterables)]
        # map cannot be a generator, because then it is not called until
        # the iterator is accessed.  Instead, let it return an iterator
        # itself.
        def generator():
            for f in futures:
                with t.timeout():
                    result = f.result()
                # never yield out of timeout context
                yield result
        return generator()

    def shutdown(self):
        raise NotImplementedError

    @staticmethod
    def execution_wrapper(future, fn, args, kwargs):
        try:
            try:
                result = fn(*args, **kwargs)
            except TaskletExit:
                future.set_cancel()
            except BaseException:
                future.set_exception(*sys.exc_info())
            else:
                future.set_result(result)
        except:
            print >> sys.stderr, "Unhandled exception in ", callable
            traceback.print_exc()

class StaticTaskletExecutor(TaskletExecutorBase):
    @classmethod
    def submit(cls, fn, *args, **kwargs):
        tasklet = stackless.tasklet(cls.execution_wrapper)
        future = Future(tasklet)
        tasklet(future, fn, args, kwargs)
        return future

# create a module static instance of the above
executor = StaticTaskletExecutor()

class WaitingExecutorMixIn(object):
    def __init__(self, max_workers=None):
        super(WaitingExecutorMixIn, self).__init__()
        self.futures = WeakSet()

    def submit(self, fn, *args, **kwargs):
        if self.futures is None:
            raise RuntimeError
        future = super(WaitingExecutorMixIn, self).submit(fn, *args, **kwargs)
        self.futures.add(future)
        return future

    def shutdown(self, wait=True):
        if self.futures:
            futures = set(self.futures)
            self.futures = None
            if wait:
                _wait(futures)

    def __enter__(self):
        pass
    def __exit__(self, *args):
        self.shutdown(True)

class TaskletExecutor(WaitingExecutorMixIn, StaticTaskletExecutor):
    pass





class Event(stackless.channel):
    """A channel that sends an event if anyone is listening"""
    def __init__(self):
        self.preference = 0
    def set(self, val=None):
        with atomic():
            if self.balance < 0:
                self.send(val)
    def wait(self, timeout=None):
        if timeout is None:
            return self.receive()
        with _timeout(timeout):
            return self.receive()

class Future(object):
    """A tasklet based future object"""

    def __init__(self, tasklet=None):
        self._result = None
        self.callbacks = []
        self.tasklet = tasklet

    def cancel(self):
        if self.tasklet:
            self.tasklet.kill()
        else:
            self.set_cancelled() #this is mostly for unittests
        #kill is immediate.  So, if the tasklet died, we are back here.
        return self.cancelled()

    def cancelled(self):
        return self._result == (False, None)

    def running(self):
        return not self.done()

    def done(self):
        """True if the task has completed execution"""
        return self._result is not None

    def result(self, timeout=None):
        """Wait for the execution of the task and return its result or raise
           its exception.
        """
        self.wait(timeout)
        success, result = self._result
        if success:
            return result
        if result:
            raise result[0], result[1], result[2]
        raise CancelledError

    def exception(self, timeout=None):
        """Wait for the execution of the task and return its result or raise
           its exception.
        """
        self.wait(timeout)
        success, result = self._result
        if not success:
            if result:
                return result
            raise CancelledError

    def wait(self, timeout):
        """Wait until the future has finished or been cancelled"""
        with atomic():
            if not self._result:
                e = Event()
                self.add_done_callback(e.set)
                e.wait(timeout)

    def add_done_callback(self, cb):
        """Append a callback when the event is ready"""
        if self._result:
            self._cb(cb)
        else:
            self.callbacks.append(cb)

    def _on_ready(self):
        for cb in self.callbacks:
            self._cb(cb)

    def _cb(self, cb):
        try:
            cb(self)
        except Exception:
            traceback.print_exc()

    def set_result(self, result):
        assert self._result is None
        self._result = (True, result)
        self._on_ready()

    def set_exception(self, exc, val=None, tb=None):
        assert self._result is None
        if val is None:
            val = exc
            exc = type(exc)
        elif isinstance(val, tuple):
                val = exc(*val)
        self._result = (False, (exc, val, tb))
        self._on_ready()

    def set_cancelled(self):
        assert self._result is None
        self._result = (False, None)
        self._on_ready()


FIRST_COMPLETED = 0
FIRST_EXCEPTION = 1
ALL_COMPLETED = 2

def wait(fs, timeout=None, return_when=ALL_COMPLETED):
    done = set()
    not_done = set()
    e = Event()
    with atomic():
        # depending on mode, set up callback function
        if return_when == FIRST_COMPLETED:
            def when_done(f):
                not_done.remove(f)
                done.add(f)
                e.set()
            do_wait = not done
        elif return_when == FIRST_EXCEPTION:
            def when_done(f):
                not_done.remove(f)
                done.add(f)
                # "canceled" does not constitute an exception in this sense
                if not not_done or (not f.cancelled() and f.exception()):
                    e.set()
            if not not_done:
                do_wait = False #if not done is empty
        elif return_when == ALL_COMPLETED:
            def when_done(f):
                not_done.remove(f)
                done.add(f)
                if not not_done:
                    e.set()
            do_wait = not_done
        else:
            raise ValueError(return_when)

        # Classify futures, and look for exceptions
        do_wait = True
        for f in fs:
            if f.done():
                done.add(f)
                if do_wait and (not f.cancelled() and f.exception()):
                    do_wait = False # for FIRST_EXCEPTION
            else:
                f.add_done_callback(when_done)
                not_done.add(f)

        if return_when == FIRST_COMPLETED:
            do_wait = not done
        elif return_when == FIRST_EXCEPTION:
            if not not_done:
                do_wait = False
        else:
            do_wait = not_done

        if do_wait:
            try:
                e.wait(timeout)
            except TimeoutError:
                pass
    #duplicate the sets, so that future events won't modify them.
    return set(done), set(not_done)
_wait = wait #to resolve naming conflicts

def as_completed(fs, timeout=None):
    t = util.Timeouts(timeout)
    incomplete = 0
    channel = util.qchannel() #so that sender doesn't block if receiver goes away
    with atomic():
        for f in fs:
            if f.done():
                yield f
            else:
                f.add_done_callback(channel.send)
                incomplete += 1
    for i in xrange(incomplete):
        with t.timeout():
            f = channel.receive()
        # Never yield out of the timeout context
        yield f
