#futures.py

import sys
import traceback
import collections
import stackless
from .errors import TimeoutError, CancelledError
from . import util, threadpool
from .util import timeout as _timeout
from .util import atomic
from weakref import WeakSet

class TaskletExecutorBase(object):
    """Base class for TaskFactories"""

    def submit(self, fn, *args, **kwargs):
        # Get rid of the annoying varargs api and allow a
        # pre-existing future to be passed in.
        return self.submit_future(Future(), (fn, args, kwargs))

    def submit_args(self, fn, args=(), kwargs={}):
        return self.submit_future(Future(), (fn, args, kwargs))

    def map(self, fn, *iterables, **kwds):
        timeout = kwds.pop("timeout", None)
        if kwds:
            raise TypeError
        t = util.Timeouts(timeout)
        futures = [self.submit_future(Future(), (fn, args)) for args in zip(*iterables)]
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

    @staticmethod
    def execute_future(future, fn, args, kwargs={}):
        """Execute the job and future on the current tasklet"""
        future.execute(fn, args, kwargs)

    def __enter__(self):
        pass
    def __exit__(self, *args):
        self.shutdown(True)

    def shutdown(self, wait=True):
        pass

class ThreadPoolExecutorBase(TaskletExecutorBase):
    """Runs futures on a given threadpool"""
    def __init__(self, pool):
        self.pool = pool

    def submit_future(self, future, job):
        def job_function():
            self.execute_future(future, *job)
        self.pool.submit(job_function)
        return future

    def shutdown(self, wait=True):
        self.pool.shutdown(wait)

class SimpleTaskletExecutor(TaskletExecutorBase):
    def submit_future(self, future, job):
        stackless.tasklet(self.execute_future)(future, *job)
        return future

class ImmediateTaskletExecutor(TaskletExecutorBase):
    def submit_future(self, future, job):
        stackless.tasklet(self.execute_future)(future, *job).run()
        return future

# create a module static instances of the above
thread_executor = ThreadPoolExecutorBase(threadpool.DummyThreadPool())
tasklet_executor = SimpleTaskletExecutor()
immediate_tasklet_executor = ImmediateTaskletExecutor()


class WaitingExecutorMixIn(object):
    """This mixin keeps track of issued futures so that we can wait for them all wholesale"""
    def __init__(self, *args, **kwargs):
        self.futures = WeakSet()

    def submit_future(self, future, job):
        if self.futures is None:
            raise RuntimeError
        future = super(WaitingExecutorMixIn, self).submit_future(future, job)
        self.futures.add(future)
        return future

    def shutdown(self, wait=True):
        if self.futures:
            futures = set(self.futures)
            if wait:
                _wait(futures)
            self.futures = None
        super(WaitingExecutorMixIn, self).shutdown(wait)

class BoundedExecutorMixIn(object):
    """This mixin allows the caller to put a limit on the number active futures"""
    def __init__(self, max_workers=None):
        self.max_workers = max_workers
        self.n_workers = 0
        self.jobs = collections.deque()

    def submit_future(self, future, job):
        with atomic():
            if self.max_workers == None or self.n_workers < self.max_workers:
                self.n_workers += 1
                try:
                    future = super(BoundedExecutorMixIn, self).submit_future(future, job)
                except:
                    self.n_workers -= 1
                    raise
            else:
                future = Future()
                self.jobs.append((future, job))
            return future

    def execute_future(self, future, fn, args, kwargs={}):
        try:
            super(BoundedExecutorMixIn, self).execute_future(future, fn, args, kwargs)
        finally:
            self.n_workers -= 1
            self.pump()

    def pump(self):
        with atomic():
            if self.jobs and self.n_workers < self.max_workers:
                future, job = self.jobs.popleft()
                self.submit_future(future, job)


# create a proper threadpool executor::
class ThreadPoolExecutor(WaitingExecutorMixIn, ThreadPoolExecutorBase):
    def __init__(self, max_workers=None):
        WaitingExecutorMixIn.__init__(self)
        pool = threadpool.SimpleThreadPool(n_threads = max_workers)
        ThreadPoolExecutorBase.__init__(self, pool)

# and a generate tasklet executor
class TaskletExecutor(WaitingExecutorMixIn, BoundedExecutorMixIn, SimpleTaskletExecutor):
    def __init__(self, max_workers=None):
        WaitingExecutorMixIn.__init__(self, max_workers)
        BoundedExecutorMixIn.__init__(self)
        SimpleTaskletExecutor.__init__(self)



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

# internal future states
PENDING = 'PENDING'
RUNNING = 'RUNNING'
# The future was cancelled by the user...
CANCELLED = 'CANCELLED'
# ...and _Waiter.add_cancelled() was called by a worker.
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
FINISHED = 'FINISHED'

class Future(object):
    """A tasklet based future object"""

    def __init__(self):
        self.state = PENDING
        self._result = None
        self.callbacks = []
        self.tasklet = None

    def execute(self, fn, args=(), kwargs={}):
        """Execute job and future on the current tasklet"""
        try:
            try:
                if self.attach(): # associate with this tasklet if needed.
                    self.set_result(fn(*args, **kwargs))
            except TaskletExit as e:
                self.set_cancel(e.args)
            except BaseException:
                self.set_exception(*sys.exc_info())
        except:
            print >> sys.stderr, "Unhandled exception in ", callable
            traceback.print_exc()

    def attach(self):
        with atomic():
            assert self.state in (PENDING, CANCELLED)
            if self.state is PENDING:
                self.state = RUNNING
                self.tasklet = stackless.getcurrent()
                return True # there was no cancel, go ahead and execute

    def cancel(self, args=()):
        with atomic():
            if self.tasklet:
                self.tasklet.raise_exception(TaskletExit, *args)
            # kill will cause cancel to happen too, but if it is
            # on a different thread, then that will happen later.
            self.set_cancelled(args)
            # We can always cancel tasklet-based futures because
            # they can be killed while running
        return True

    def cancelled(self):
        return self.state is CANCELLED

    def running(self):
        return self.state is RUNNING

    def done(self):
        """True if the task has completed execution"""
        return self.state in (CANCELLED, FINISHED)

    def result(self, timeout=None):
        """Wait for the execution of the task and return its result or raise
           its exception.
        """
        self.wait(timeout)
        success, result = self._result
        if success:
            return result
        if result:
            if self.state is FINISHED:
                raise result[0], result[1], result[2]
            assert self.state is CANCELLED
            raise CancelledError(*result[1])

    def exception(self, timeout=None):
        """Wait for the execution of the task and return its result or raise
           its exception.
        """
        self.wait(timeout)
        success, result = self._result
        if not success:
            if self.state is FINISHED:
                return result
            assert self.state is CANCELLED
            raise CancelledError(*result[1])

    def wait(self, timeout=None):
        """Wait until the future has finished or been cancelled"""
        with atomic():
            if not self.done():
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
        with atomic():
            if self._result is None:
                assert self.state == RUNNING
                self._result = (True, result)
                self.state = FINISHED
                self._on_ready()
            else:
                # the only race should be with the cancelled state
                assert self.state == CANCELLED

    def set_exception(self, exc, val=None, tb=None):
        with atomic():
            if self._result is None:
                assert self.state == RUNNING
                if val is None:
                    val = exc
                    exc = type(exc)
                elif isinstance(val, tuple):
                    val = exc(*val)
                self._result = (False, (exc, val, tb))
                self.state = FINISHED
                self._on_ready()
            else:
                assert self.state == CANCELLED

    def set_cancelled(self, args=()):
        with atomic():
            if self._result is None:
                assert self.state in (RUNNING, PENDING)
                self._result = (False, (None, args))
                self.state = CANCELLED
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
        elif return_when == FIRST_EXCEPTION:
            def when_done(f):
                not_done.remove(f)
                done.add(f)
                # "canceled" does not constitute an exception in this sense
                if not not_done or (not f.cancelled() and f.exception()):
                    e.set()
        elif return_when == ALL_COMPLETED:
            def when_done(f):
                not_done.remove(f)
                done.add(f)
                if not not_done:
                    e.set()
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


# Convenience functions to gather all or any result from a set of futures
def all_results(fs, timeout=None):
    with util.timeout(timeout):
        return [f.result() for f in fs]

def any_result(fs, timeout=None):
    return next(as_completed(fs, timeout)).result()
