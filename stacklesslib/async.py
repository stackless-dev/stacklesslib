#async.py
# Async execution library, similar to C# Async

import sys
import traceback
import stackless
import contextlib
from .util import atomic, timeout

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
    def wait(self, delay=None):
        if delay is None:
            return self.receive()
        with timeout(delay):
            return self.receive()

class Task(object):
    """Base for all tasks"""

    def __init__(self):
        self.result = None
        self.callbacks = []

    @property
    def ready(self):
        return self.result is not None

    @property
    def is_exception(self):
        return self.result is not None and not self.result[0]

    def when_ready(self, cb):
        """Append a callback when the event is ready"""
        self.callbacks.append(cb)

    def on_ready(self):
        for cb in self.callbacks:
            cb()

    def wait(self, timeout=None):
        with atomic():
            if self.result:
                return
            e = Event()
            self.when_ready(e.set)
            e.wait(timeout)

    def get_result(self, timeout=None):
        self.wait(timeout)
        if self.result[0]:
            return self.result[1]
        # Consider using "AggregateException" here
        raise self.result[1][0], self.result[1][1], self.result[1][2]

    def get_exception(self, timeout=None):
        self.wait(timeout)
        if self.result[0]:
            return None
        return self.result[1]

    def post_result(self, result):
        assert self.result is None
        self.result = (True, result)
        self.on_ready()

    def post_exception(self, exc, val=None, tb=None):
        assert self.result is None
        self.result = (False, (exc, val, tb))
        self.on_ready()

    @classmethod
    def wait_all(cls, tasks, delay=None):
        with timeout(delay):
            for t in tasks:
                t.wait()

    @classmethod
    def wait_any(cls, tasks, delay=None):
        e = Event()
        with atomic():
            for i, t in enumerate(tasks):
                if t.ready:
                    return i
                def get_cb(i):
                    def cb():
                        e.set(i)
                    return cb
                t.when_ready(get_cb(i))
            return e.wait(delay)

    @classmethod
    def when_all(cls, tasks):
        return create_task(cls.wait_all, (tasks,))

    @classmethod
    def when_any(cls, tasks):
        return create_task(cls.wait_any, (tasks,))

    @classmethod
    def get_results(cls, tasks):
        try:
            return [t.get_result() for t in tasks]
        except:
            e = sys.exc_info()
            try:
                for t in tasks:
                    t.forget()
                raise e[0], e[1], e[2]
            finally:
                e = None #clear traceback cycle

class DummyTask(Task):
    """A dummy task object"""

class TaskletTask(Task):
    """ A Task object which work performed by a tasklet """
    def __init__(self, tasklet):
        super(TaskletTask, self).__init__()
        self.tasklet = tasklet

class ThreadTask(Task):
    """ A task object that runs on a thread"""
    def __init__(self, thread):
        super(ThreadTask, self).__init__()
        self.thread = thread


class AwaitManager(object):
    """This class provides the "await" method and is used by
       Async functions to yield to their caller
    """
    def __init__(self, caller):
        self.caller = caller
        self.caller_continued = False

    def _continue_caller(self):
        if not self.caller_continued:
            self.caller_continued = True
            self.caller.run()

    def await(self, task, delay=None):
        with atomic():
            if task.ready:
                # get the result without switching
                return task.get_result()
            # optionally continue the caller
            self._continue_caller()
            # wait for the result, possibly tasklet blocking
            return task.get_result(delay)


def async_call_helper(task, await_manager, callable, args, kwargs):
    # remove the caller from the runnables queue.  There is a window here where other tasklets
    # might run, we need perhaps a primitive to perform this task
    try:
        await_manager.caller.remove()
        try:
            result = callable(await_manager, *args, **kwargs)
        except:
            task.post_exception(*sys.exc_info())
        else:
            task.post_result(result)
        finally:
            with atomic():
                await_manager._continue_caller()
    except:
        print >> sys.stderr, "Unhandled exception in ", callable
        traceback.print_exc()

def call_async(callable, args=(), kwargs={}):
    await_manager = AwaitManager(stackless.getcurrent())
    callee = stackless.tasklet(async_call_helper)
    task = TaskletTask(callee)
    with atomic():
        callee(task, await_manager, callable, args, kwargs)
        try:
            # here, a run(remove=True) or a switch() primitive would be useful
            callee.run()
        finally:
            # need this here, in case caller gets awoken by other means, e.g. exception
            await_manager.caller_continued = True
    return task

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

def async(func):
    """Wraps a function as an async function, taking an AwaitHelper as the first argument"""
    @contextlib.wraps(func)
    def helper(*args, **kwargs):
        return call_async(func, args, kwargs)
    return helper

def task(factory=taskletTaskFactory):
    """Wraps a function so that it will be executed as a task"""
    def decorator(func):
        """Wraps a function as a task"""
        @contextlib.wraps(func)
        def helper(*args, **kwargs):
            return create_task(func, args, kwargs, factory)
        return helper
    return decorator
