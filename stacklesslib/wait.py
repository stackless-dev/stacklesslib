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
    'add_done_callback()' and 'remove_done_callback()' methods.
    """
    def __init__(self):
        self._callbacks = []

    def waitsite_signalled(self):
        """Override this to reflect the state of the object"""
        return False

    def waitsite_signal(self):
        callbacks, self._callbacks = self._callbacks, []
        for cb in callbacks[:]:
            self._cb(cb)

    # This function is synonomous with the function in futures
    def add_done_callback(self, cb):
        """
        Add a callback.  May result in an immediate call of "cb(self)"
        if object is already signalled.
        """
        # immediate callback if already signalled
        self._callbacks.append(cb)
        if self.waitsite_signalled():
            self._cb(cb)

    def remove_done_callback(self, cb):
        """
        Remove the callback previously registered with 'add_done_callback()'
        """
        try:
            self._callbacks.remove(cb)
        except ValueError:
            pass

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
    def __new__(cls, *args, **kwds):
        # Compatibility with old version of stackless that required
        # that no extra arguments were passed to __new__
        return stackless.tasklet.__new__(cls)

    def __init__(self, func, args=None, kwargs=None):
        WaitSite.__init__(self)
        self._done = False
        if func or args or kwargs:
            self.bind(func, args, kwargs)

    def bind(self, func, args=None, kwargs=None):
        f = func
        if func:
            def helper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                finally:
                    self._done = True
                    self.waitsite_signal()
            f = helper
        # compatibility with old stackless
        if args or kwargs:
            super(WaitableTasklet, self).bind(f, args, kwargs)
        else:
            super(WaitableTasklet, self).bind(f)

    def waitsite_signalled(self):
        return self._done

    def join(self, timeout=None):
        wait([self], timeout)


class ValueTasklet(WaitableTasklet):
    """A WaitableTasklet that holds on to its return value or exit exception"""
    def __init__(self, func, args=None, kwargs=None):
        super(ValueTasklet, self).__init__(func, args, kwargs)
        self._value = None
        self._error = None

    def bind(self, func, args=None, kwargs=None):
        f = func
        if func:
            def helper(*args, **kwargs):
                try:
                    self._value = func(*args, **kwargs)
                except TaskletExit as e:
                    self._value = e  # considered a success
                except:
                    self._error = sys.exc_info()
            f = helper
        super(ValueTasklet, self).bind(f, args, kwargs)

    def get(self, block=True, timeout=None):
        with atomic():
            if not self._done and block:
                self.join(timeout)
            if not self._done:
                raise TimeoutError
            if self._error:
                raise self._error[0], self._error[1], self._error[2]
            return self._value


def iwait(objects, timeout=None, raise_timeout=False):
    """
    A generator that returns objects as they become ready.  The total waiting
    time will not exceed "timeout" if provided.
    """
    channel = stacklesslib.util.qchannel()
    count = 0
    callbacks = {}
    def get_cb(obj):
        def cb(waitable):
            waitable.remove_done_callback(callbacks.pop(waitable))
            channel.send(waitable)
        callbacks[obj] = cb
        return cb

    try:
        with atomic():
            for obj in objects:
                obj.add_done_callback(get_cb(obj))
                count += 1

        if timeout is not None:
            # handle 0 timeouts by not blocking at all
            if timeout == 0:
                with atomic():
                    while channel.balance > 0:
                        count -= 1
                        yield channel.receive()
                    if count and raise_timeout:
                        raise TimeoutError()
            else:
                timeouts = stacklesslib.util.Timeouts(timeout)
                try:
                    for i in xrange(count):
                        with timeouts.timeout():
                            yield channel.receive()
                except TimeoutError:
                    if raise_timeout:
                        raise
        else:
            for i in xrange(count):
                yield channel.receive()
    finally:
        for obj, cb in callbacks.items():
            obj.remove_done_callback(cb)

def wait(objects, timeout=None, count=None):
    """
    Wait for objects, for at most "timeout" seconds or until "count" objects
    are ready.  Returns a list containing the ready objects in the order they became ready.
    """
    if count is None:
        return list(iwait(objects, timeout))
    return list(itertools.islice(iwait(objects, timeout), count))