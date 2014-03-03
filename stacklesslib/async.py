#async.py
"""
This module provides functionality similar to the C# Async features.
There are two main parts to this:

First, it allows functions to be declared to be futures.  the
stacklesslib.futures module is used for this.

Secondly, functions can be declared to be ``async``.  They are then
turned into futures that get an additional first argument, the
``awaiter`` instance.  ``awaiter.await`` can then be used to automatically
wait for a future and transfer control up to the blocker.

This provides for a depth first future architecture that provides conventional
execution order until the results of a future are required.

For background on C# Async, see e.g.
http://msdn.microsoft.com/en-us/library/hh191443.aspx
"""

import sys
import traceback
import stackless
import contextlib
from .util import atomic
from . import futures


# Helpers to run create futures and decorate functions as being Future functions
def create_future(callable, args=(), kwargs={}, executor=futures.tasklet_executor):
    """Create a future, given a function and arguments"""
    return executor.submit_args(callable, args, kwargs)

def future(executor=futures.tasklet_executor):
    """Wraps a function so that it will be executed as a future"""
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
    def _continue_caller(self):
        with atomic():
            if not self.caller_continued:
                self.caller.run()
                self.caller_continued = True

    def await(self, future, timeout=None):
        """
        wait for the future's result and return it.  If the future is not
        ready, control will be transfered to the caller of the Async funcion.
        """
        with atomic():
            if future.done():
                # get the result without switching
                return future.result()
        # optionally continue the caller (it may have continued already)
        self._continue_caller()
        # wait for the result, possibly tasklet blocking
        return future.result(timeout)

    def call_async(self, function, args=(), kwargs={}):
        """
        Invoke the async call on the function
        """
        self.caller = stackless.getcurrent()
        self.caller_continued = False
        callee = stackless.tasklet()
        future = futures.Future()
        with atomic():
            try:
                if hasattr(callee, 'switch'):
                    # Explicitly switch to tasklet, pausing ourselves.
                    callee.bind(self.async_call_helper, (future, function, args, kwargs))
                    callee.switch()
                else:
                    # Run callee, which will then pause us.
                    callee.bind(self.old_async_call_helper, (future, function, args, kwargs))
                    callee.run()
            finally:
                # need this here, in case caller gets awoken by other means, e.g. exception
                self.caller_continued = True
        return future

    def async_call_helper(self, future, function, args, kwargs):
        """
        Run future in the rcurrent tasklet and then continue execution
        of the caller.
        """
        try:
            try:
                future.execute(function, (self,) + args, kwargs)
            finally:
                self._continue_caller()
        except:
            print >> sys.stderr, "Unhandled exception in ", function
            traceback.print_exc()

    def old_async_call_helper(self, future, function, args, kwargs):
        """
        Same as async_call_helper, for versions of stackless without
        the tasklet.switch() method.
        """
        try:
            self.caller.remove()
            try:
                future.execute(function, (self,) + args, kwargs)
            finally:
                self._continue_caller()
        except:
            print >> sys.stderr, "Unhandled exception in ", function
            traceback.print_exc()


def async(func):
    """Wraps a function as an async function, taking an Awaiter as the first argument"""
    @contextlib.wraps(func)
    def helper(*args, **kwargs):
        return Awaiter().call_async(func, args, kwargs)
    return helper
