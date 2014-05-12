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
from stacklesslib.errors import TimeoutError, CancelledError
from stacklesslib.weakmethod import WeakMethodProxy


class WaitSite(object):
    """
    This class implements the interface for a waitable object, the
    'add_done_callback()' and 'remove_done_callback()' methods.
    """
    def __init__(self):
        self._callbacks = []

    def waitsite_signalled(self):
        """
        This function is consulted when a callback is added and if it returns
        True, an immediate callback is performed, rather than scheduling one later.
        If a waitsite is stateful, this should be redefined to reflect its state.
        """
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


class Observer(WaitSite):
    """
    An observer observes another waitable.  With subclassing this can be used
    to add state logic.  This class acts as a proxy.
    """
    def __init__(self, observed):
        super(Observer, self).__init__()
        self.got_callback = False
        self.cb = WeakMethodProxy(self.callback)
        # We use a weak method here, so that the callback doesn't keep us alive.
        self.observed = observed
        observed.add_done_callback(self.cb)

    def __del__(self):
        self.close()

    def close(self):
        if self.observed is not None:
            self.observed.remove_done_callback(self.cb)
            self.observed = None

    def callback(self, target):
        self.got_callback = True
        if self.observed is not None:
            if self.filter():
                self.waitsite_signal()

    def waitsite_signalled(self):
        # A callback on add only occurs if a callback already occurred and filter approves
        return self.got_callback and self.filter()

    def filter(self):
        """
        Decides if a callback from the observed should cause a callback
        to any waiters.
        """
        return True


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
        """
        Wait for the tasklet to finish.  Returns True unless the timeout expired.
        """
        return bool(wait([self], timeout))


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
                    # Convert TaskletExit to CancelledError
                    self._error = CancelledError, CancelledError(e),  sys.exc_info()[2] # considered a success
                except:
                    self._error = sys.exc_info()
            f = helper
        super(ValueTasklet, self).bind(f, args, kwargs)

    def get(self, block=True, timeout=None):
        """
        Get the result of the tasklet or raise exception
        """
        with atomic():
            if not self._done and block:
                self.join(timeout)
            if not self._done:
                raise TimeoutError
            if self._error:
                raise self._error[0], self._error[1], self._error[2]
            return self._value

    def result(self, timeout=None):
        """
        A function conforming to the futures interface
        """
        return self.get(timeout=timeout)

class WaitChannelMixin(WaitSite, stacklesslib.util.ChannelSequenceMixin):
    """
    A mixin for channels to make them waitable.  A wait will succeed whenever
    the channel changes state from being blocked to unblocked, in whichever
    direction.
    """
    def __init__(self):
        super(WaitChannelMixin, self).__init__()
        # We must temporarily lie about the balance value while making
        # wait callback calls.
        self._balance_adjust = 0

    @property
    def balance(self):
        return super(WaitChannelMixin, self).balance + self._balance_adjust

    def _notify_state_change(self, adjust):
        """
        Called when a channel is about to be receivable.  Should be called
        while the current tasklet is in the atomic mode.
        """
        if super(WaitChannelMixin, self).balance == 0:
            # The channel is becoming receivable or sendable. Let any interested
            # parties know
            self._balance_adjust += adjust
            try:
                self.waitsite_signal()
            finally:
                self._balance_adjust -= adjust

    def send(self, value):
        with atomic():
            self._notify_state_change(1)
            super(WaitChannelMixin, self).send(value)

    def send_exception(self, exc, *args):
        with atomic():
            self._notify_state_change(1)
            super(WaitChannelMixin, self).send_exception(exc, *args)

    def send_throw(self, exc, value=None, tb=None):
        with atomic():
            self._notify_state_change(1)
            super(WaitChannelMixin, self).send_throw(exc, value, tb)

    def receive(self):
        with atomic():
            self._notify_state_change(-1)
            return super(WaitChannelMixin, self).receive()

class Receivable(Observer):
    def filter(self):
        return self.observed.balance > 0

class Sendable(Observer):
    def filter(self):
        return self.observed.balance < 0

class WaitChannel(WaitChannelMixin, stackless.channel):
    """
    A channel that provides wait semantics for receive
    """

def iwait(objects, timeout=None, raise_timeout=False):
    """
    A generator that returns objects as they become ready.  The total waiting
    time will not exceed "timeout" if provided.  Raises TimeoutError if a timeout
    occurs.
    """
    channel = stacklesslib.util.QueueChannel()
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
                    if count:
                        raise TimeoutError()
            else:
                timeouts = stacklesslib.util.Timeouts(timeout)
                for i in xrange(count):
                    with timeouts.timeout():
                        yield channel.receive()
        else:
            for i in xrange(count):
                yield channel.receive()
    finally:
        for obj, cb in callbacks.items():
            obj.remove_done_callback(cb)

def iwait_no_raise(objects, timeout=None):
    """
    Like 'iwait' but doesn't raise a TimeoutError, only stops returning results
    when a timeout occurs.
    """
    try:
        for i in iwait(objects, timeout):
            yield i
    except TimeoutError:
        pass

def wait(objects, timeout=None, count=None):
    """
    Wait for objects, for at most "timeout" seconds or until "count" objects
    are ready.  Returns a list containing the ready objects in the order they became ready.
    """
    if count is None:
        return list(iwait_no_raise(objects, timeout))
    return list(itertools.islice(iwait_no_raise(objects, timeout), count))

def swait(waitable, timeout=None):
    """
    A simple wait function to wait for a single waitable.  Returns the waitable
    or raises TimeoutError.
    """
    channel = stacklesslib.util.QueueChannel()
    with atomic():
        waitable.add_done_callback(channel.send)
        try:
            with stacklesslib.util.timeout(timeout):
                return channel.receive()
        finally:
            waitable.remove_done_callback(channel.send)

def any(iterable):
    """
    Returns a waitable object which is done when any of the waitables
    in iterable is ready.  its "result" method will return the waitable
    that was ready
    """
    def any_func():
        for i in iwait(iterable):
            return i
    t = ValueTasklet(any_func)()
    t.run()
    return t

def all(iterable):
    """
    Returns a waitable object which is done when all of the waitables
    in iterable are ready.  its "result" method returns the waitables
    in the order in which they became ready.
    """
    def all_func():
        return list(iwait(iterable))
    t = ValueTasklet(all_func)()
    t.run()
    return t
