#async.py
# Async execution library, similar to C# Async

import sys
import traceback
import stackless
import contextlib
from .util import atomic
from . import futures


# Helpers to run create futures and decorate functions as being Future functions
def create_future(callable, args=(), kwargs={}, executor=futures.tasklet_executor):
    """Create a task, given a function and arguments"""
    return executor.submit_args(callable, args, kwargs)

def future(executor=futures.tasklet_executor):
    """Wraps a function so that it will be executed as a task"""
    def decorator(func):
        @contextlib.wraps(func)
        def helper(*args, **kwargs):
            return create_future(func, args, kwargs, executor)
        return helper
    return decorator

# Now comes the "await" functionality.  This is a special kind of future method
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

    def await(self, future, timeout=None):
        with atomic():
            if future.done():
                # get the result without switching
                return future.result()
            # optionally continue the caller
            self._continue_caller()
            # wait for the result, possibly tasklet blocking
            return future.result(timeout)


def async_call_helper(future, awaiter, callable, args, kwargs):
    # remove the caller from the runnables queue.  There is a window here where other tasklets
    # might run, we need perhaps a primitive to perform this task
    try:
        awaiter.caller.remove()
        try:
            future.execute(callable, (awaiter,) + args, kwargs)
        finally:
            with atomic(): #for _continue_caller
                awaiter._continue_caller()
    except:
        print >> sys.stderr, "Unhandled exception in ", callable
        traceback.print_exc()

def call_async(callable, args=(), kwargs={}):
    awaiter = Awaiter(stackless.getcurrent())
    callee = stackless.tasklet(async_call_helper)
    future = futures.Future()
    with atomic():
        callee(future, awaiter, callable, args, kwargs)
        try:
            # here, a run(remove=True) or a switch() primitive would be useful
            callee.run()
        finally:
            # need this here, in case caller gets awoken by other means, e.g. exception
            awaiter.caller_continued = True
    return future


def async(func):
    """Wraps a function as an async function, taking an Awaiter as the first argument"""
    @contextlib.wraps(func)
    def helper(*args, **kwargs):
        return call_async(func, args, kwargs)
    return helper

