"""
This file implements waiting according to the "wait protocol"
The wait protocol simply means implementing the "add_done_callback"

A WaitSite class is provided, as well as a WaitableTasklet and
ValueTasklet.

Objects that conform to the wait protocol also include stacklesslib.locks.Event and
stacklesslib.futures.future.
"""
import sys
import traceback
import itertools
import stackless
import stacklesslib.util
from stacklesslib.base import atomic
from stacklesslib.errors import TimeoutError


class WaitSite(object):
    """
    This class implements the interface for a waitable object, the
    "add_done_callback" function.
    """
    def __init__(self):
        self._callbacks = []

    def waitsite_signalled(self):
        """Override this to reflect the state of the object"""
        return False

    def waitsite_signal(self):
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks:
            self._cb(cb)

    # This function is synonomous with the function in futures
    def add_done_callback(self, cb):
        """
        Add a callback.  May result in an immediate call of "cb(self)"
        if object is already signalled.
        """
        # immediate callback if already signalled
        if self.waitsite_signalled():
            self._cb(cb)
        else:
            self._callbacks.append(cb)

    def _cb(self, cb):
        try:
            cb(self)
        except Exception:
            self.handle_exception(sys.exc_info())

    def handle_exception(self, ei):
        """Handle an exception in the callback.  Can be overridden."""
        traceback.print_exception(*ei)


class WaitableTasklet(stackless.tasklet, WaitSite):
    """A tasklet that implements the wait interface"""
    def __init__(self, func, args=None, kwargs=None):
        WaitSite.__init__(self)
        self.__done__ = False
        self.bind(func, args, kwargs)

    def bind(self, func, args=None, kwargs=None):
        f = func
        if func:
            def helper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                finally:
                    self.__done__ = True
                    self.waitsite_signal()
            f = helper
        super(WaitableTasklet, self).bind(f, args, kwargs)

    def waitsite_signalled(self):
        return self.__done__

    def join(timeout=None):
        wait([self], timeout)


class ValueTasklet(WaitableTasklet):
    """A WaitableTasklet that holds on to its return value or exit exception"""
    def __init__(self, func, args=None, kwargs=None):
        super(ValueTasklet, self).__init__(func, args, kwargs)
        self.__value__ = None
        self.__error__ = None

    def bind(self, func, args=None, kwargs=None):
        f = func
        if func:
            def helper(*args, **kwargs):
                try:
                    self.__value__ = func(*args, **kwargs)
                except TaskletExit as e:
                    self.__value__ = e  # considered a success
                except:
                    self.__error__ = sys.exc_info()
            f = helper
        super(ValueTasklet, self).bind(f, args, kwargs)

    def get(block=True, timeout=None):
        with atomic():
            if not self.__done__ and block:
                self.join(timeout)
            if not self.__done__:
                raise TimeoutError
            if self.__error__:
                raise self.__error__[0], self.__error__[1], self.__error__[2]
            return self.__value__


def iwait(objects, timeout=None):
    """
    A generator that returns objects as they become ready.  The total waiting
    time will not exceed "timeout" if provided.
    """
    channel = stacklesslib.util.qchannel()
    count = 0
    with atomic():
        for obj in objects:
            obj.add_done_callback(channel.send)
            count += 1

    if timeout is not None:
        timeouts = stacklesslib.util.Timeouts(timeout)
        try:
            for i in xrange(count):
                with timeouts.timeout():
                    yield channel.receive()
        except TimeoutError:
            pass
    else:
        for i in xrange(count):
            yield channel.receive()

def wait(objects, timeout=None, count=None):
    """
    Wait for objects, for at most "timeout" seconds or until "count" objects
    are ready.  Returns a list containing the ready objects in the order they became ready.
    """
    if count is None:
        return list(iwait(objects, timeout))
    return list(itertools.islice(iwait(objects, timeout), count))