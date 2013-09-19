#async.py
# Async execution library, similar to C# Async

import sys
import traceback
import stackless
import contextlib
from .util import atomic
from .util import timeout as timeout_ctxt

try:
    import threading
    _threading = getattr(threading, "real_threading", threading)
except ImportError:
    _threading = None

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
        with timeout_ctxt(timeout):
            return self.receive()

class Task(object):
    """Base for all tasks"""

    def __init__(self):
        self._result = None
        self.callbacks = []

    @property
    def ready(self):
        """True if the task has completed execution"""
        return self._result is not None

    @property
    def result(self):
        """The task successful result or None"""
        return self._result[1] if self._result and self._result[0] else None

    @property
    def exception(self):
        """The task exception tuple or None"""
        return self._result[1] if self._result and not self._result[0] else None

    def re_raise(self):
        """Re-raise the exception raised by the task, or return None"""
        e = self.exception
        if e:
            try:
                raise e[0], e[1], e[2]
            finally:
                # Clear the traceback to reduce cyclic links
                self.clear_exception()

    def clear_exception(self):
        if self.exception:
            self._result = (False, (None, None, None))

    def when_ready(self, cb):
        """Append a callback when the event is ready"""
        self.callbacks.append(cb)

    def _on_ready(self):
        for cb in self.callbacks:
            cb(self)

    def wait(self, timeout=None):
        """Wait untli task has completed. Returns True if it completed successfully,
           False if it raised an exception
        """
        with atomic():
            if not self._result:
                e = Event()
                self.when_ready(e.set)
                e.wait(timeout)
        return self._result[0]


    def reap(self, timeout=None):
        """Wait for the execution of the task and return its result or raise
           its exception.
        """
        self.wait(timeout)
        self.re_raise()
        return self.result

    def post_result(self, result):
        assert self.result is None
        self._result = (True, result)
        self._on_ready()

    def post_exception(self, exc, val=None, tb=None):
        assert self.result is None
        self._result = (False, (exc, val, tb))
        self._on_ready()

    @classmethod
    def reap_all(cls, tasks, timeout=None):
        """Get results of all tasks.  If one or more raise
           an exception, an arbitrary re-raised.
        """
        with timeout_ctxt(timeout):
            return [t.get_result() for t in tasks]

    @classmethod
    def reap_any(cls, tasks, timeout=None):
        """Return the result of an arbitrary taskl.  The result is returned
           as a tuple (i, result), where i is the index in the original list.
           Can re-raise an exception that was found on an arbitrary task.
        """
        return cls.wait_any(tasks, timeout).reap()

    @classmethod
    def wait_all(cls, tasks, timeout=None):
        with timeout_ctxt(timeout):
            for t in tasks:
                t.wait()

    @classmethod
    def wait_any(cls, tasks, timeout=None):
        """Wait until one of the tasks is ready.  Returns the
           ready task.
        """
        e = Event()
        with atomic():
            # See if anyone is ready, otherwise, add a callback to our event
            for t in tasks:
                if t.ready:
                    return t
                def cb(t):
                    e.set(t)
                t.when_ready(cb)
            # wait for our event
            return e.wait(timeout)

    # class methpods returning tasks that wait/reap all/any
    @classmethod
    def waiter_all(cls, tasks):
        return create_task(cls.wait_all, (tasks,))

    @classmethod
    def waiter_any(cls, tasks):
        return create_task(cls.wait_any, (tasks,))

    @classmethod
    def reaper_all(cls, tasks):
       return create_task(cls.reap_all, (tasks,))

    @classmethod
    def reaper_any(cls, tasks):
        return create_task(cls.reap_any, (tasks,))

class DummyTask(Task):
    """A dummy task object.  Work performed on a dummy is
       simply executed directly as a function call.
    """

class TaskletTask(Task):
    """ A Task object which work is performed by a tasklet """
    def __init__(self, tasklet):
        super(TaskletTask, self).__init__()
        self.tasklet = tasklet

class ThreadTask(Task):
    """ A task object that runs on a separate thread"""
    def __init__(self, thread):
        super(ThreadTask, self).__init__()
        self.thread = thread



class TaskFactory(object):
    """Base class for TaskFactories"""
    def task_wrapper(self, task, callable, args, kwargs):
        try:
            try:
                result = callable(*args, **kwargs)
            except:
                task.post_exception(*sys.exc_info())
            else:
                task.post_result(result)
        except:
            print >> sys.stderr, "Unhandled exception in ", callable
            traceback.print_exc()

class DummyTaskFactory(TaskFactory):
    """Just executes the task, and wraps the result"""
    def create_task(self, callable, args=(), kwargs={}):
        task = DummyTask()
        self.task_wrapper(task, callable, args, kwargs)
        return task

class TaskletTaskFactory(TaskFactory):
    """Creates a tasklet to run the task"""
    def create_task(self, callable, args=(), kwargs={}):
        callee = stackless.tasklet(self.task_wrapper)
        task = TaskletTask(callee)
        callee(task, callable, args, kwargs)
        return task

class ThreadTaskFactory(TaskFactory):
    """Creates tasks that run on a new thread"""
    def create_task(self, callable, args=(), kwargs={}):
        task = None
        def helper():
            """helper to pass "task" into the target"""
            self.task_wrapper(task, callable, args, kwargs)
        callee = _threading.Thread(target=helper)
        task = ThreadTask(callee)
        callee.start()
        return task

dummyTaskFactory = DummyTaskFactory()
taskletTaskFactory = TaskletTaskFactory()
threadTaskFactory = ThreadTaskFactory()


def create_task(callable, args=(), kwargs={}, factory=taskletTaskFactory):
    """Create a task, given a function and arguments"""
    return factory.create_task(callable, args, kwargs)

def task(factory=taskletTaskFactory):
    """Wraps a function so that it will be executed as a task"""
    def decorator(func):
        @contextlib.wraps(func)
        def helper(*args, **kwargs):
            return create_task(func, args, kwargs, factory)
        return helper
    return decorator

# Now comes the "await" functionality.  This is a special kind of task
# that has an Awaiter instance in its first argument which contols
# execution flow with its caller

class Awaiter(object):
    """This class provides the "await" method and is used by
       Async functions to yield to their caller
    """
    def __init__(self, caller):
        self.caller = caller
        self.caller_continued = False

    def _continue_caller(self):
        if not self.caller_continued:
            self.caller.run()
            self.caller_continued = True

    def await(self, task, timeout=None):
        with atomic():
            if task.ready:
                # get the result without switching
                return task.reap()
            # optionally continue the caller
            self._continue_caller()
            # wait for the result, possibly tasklet blocking
            return task.reap(timeout)


def async_call_helper(task, awaiter, callable, args, kwargs):
    # remove the caller from the runnables queue.  There is a window here where other tasklets
    # might run, we need perhaps a primitive to perform this task
    try:
        awaiter.caller.remove()
        try:
            result = callable(awaiter, *args, **kwargs)
        except:
            task.post_exception(*sys.exc_info())
        else:
            task.post_result(result)
        finally:
            with atomic(): #for _continue_caller
                awaiter._continue_caller()
    except:
        print >> sys.stderr, "Unhandled exception in ", callable
        traceback.print_exc()

class AsyncTask(TaskletTask):
    """A task representing the Async function"""

def call_async(callable, args=(), kwargs={}):
    awaiter = Awaiter(stackless.getcurrent())
    callee = stackless.tasklet(async_call_helper)
    task = AsyncTask(callee)
    with atomic():
        callee(task, awaiter, callable, args, kwargs)
        try:
            # here, a run(remove=True) or a switch() primitive would be useful
            callee.run()
        finally:
            # need this here, in case caller gets awoken by other means, e.g. exception
            awaiter.caller_continued = True
    return task


def async(func):
    """Wraps a function as an async function, taking an Awaiter as the first argument"""
    @contextlib.wraps(func)
    def helper(*args, **kwargs):
        return call_async(func, args, kwargs)
    return helper

