#stacklesslib.locks.py
"""
This module provides locking primitives to be used with stackless.
The primitives have the same semantics as those defined in the threading module
for threads.
The timeout feature of the locks works only if someone is pumping the
stacklesslib.main.event_queue
"""

from __future__ import with_statement
from __future__ import absolute_import

import stackless
import contextlib

from . import main
from .main import set_channel_pref, elapsed_time
from .util import atomic, channel_wait, WaitTimeoutError
from .basiclock import LockMixin


@contextlib.contextmanager
def released(lock):
    """A context manager for temporarily releasing and reacquiring a lock
       using the provide lock's release() and acquire() methods.
    """
    lock.release()
    try:
        yield
    finally:
        lock.acquire()

def lock_channel_wait(chan, timeout):
    """
    Timeouts should be swallowed and we should just exit.
    """
    try:
        channel_wait(chan, timeout)
        return True
    except WaitTimeoutError:
        return False


class Semaphore(LockMixin):
    def __init__(self, value=1):
        if value < 0:
            raise ValueError
        self._value = value
        self._chan = stackless.channel()
        set_channel_pref(self._chan)

    def acquire(self, blocking=True, timeout=None):
        with atomic():
            # Low contention logic: There is no explicit handoff to a target,
            # rather, each tasklet gets its own chance at acquiring the semaphore.
            got_it = self._try_acquire()
            if got_it or not blocking:
                return got_it

            wait_until = None
            while True:
                if timeout is not None:
                    # Adjust time.  We may have multiple wakeups since we are a
                    # low-contention lock.
                    if wait_until is None:
                        wait_until = elapsed_time() + timeout
                    else:
                        timeout = wait_until - elapsed_time()
                        if timeout < 0:
                            return False
                try:
                    lock_channel_wait(self._chan, timeout)
                except:
                    self._safe_pump()
                    raise
                if self._try_acquire():
                    return True

    def _try_acquire(self):
        if self._value > 0:
            self._value -= 1
            return True
        return False

    def release(self, count=1):
        with atomic():
            self._value += count
            self._pump()

    def _pump(self):
        for i in xrange(min(self._value, -self._chan.balance)):
            if self._chan.balance:
                self._chan.send(None)

    def _safe_pump(self):
        # Need a special function for this, since we want to call it from
        # an exception handler and not trample the current exception in case
        # we get one ourselves.
        try:
            self._pump()
        except Exception:
            pass


class BoundedSemaphore(Semaphore):
    def __init__(self, value=1):
        Semaphore.__init__(self, value)
        self._max_value = value

    def release(self, count=1):
        with atomic():
            if self._value + count > self._max_value:
                raise ValueError
            super(BoundedSemaphore, self).release(count)


class Lock(Semaphore):
    def __init__(self):
        super(Lock, self).__init__() #force a count of 1


class RLock(Lock):
    def __init__(self):
        Lock.__init__(self)
        self._owning = None
        self._locked = 0

    def _try_acquire(self):
        if not (super(RLock, self)._try_acquire() or self._owning == stackless.getcurrent()):
            return False
        self._owning = stackless.getcurrent()
        self._locked += 1
        return True

    def release(self):
        if self._owning is not stackless.getcurrent():
            raise RuntimeError("cannot release un-aquired lock")
        with atomic():
            self._locked -= 1
            if not self._locked:
                self._owning = None
                super(RLock, self).release();

    # These three functions form an internal interface for the Condition.
    # It allows the Condition instances to release the lock from any
    # recursion level and reacquire it to the same level.
    def _is_owned(self):
        return self._owning is stackless.getcurrent()

    def _release_save(self):
        r = self._locked
        self._locked = 1
        self.release()
        return r

    def _acquire_restore(self, r):
        self.acquire()
        self._locked = r


def wait_for_condition(cond, predicate, timeout=None):
    """
    Wait on a Condition variable until a predicate becomes true,
    or until an optional timeout elapses. Returns the last value of the predicate.
    """
    result = predicate()
    if result:
        return result
    endtime = None
    while not result:
        if timeout is not None:
            if endtime is None:
                endtime = elapsed_time() + timeout
            else:
                timeout = endtime - elapsed_time()
                if timeout < 0:
                    return result # A timeout occurred
        cond.wait(timeout)
        result = predicate()
    return result


class Condition(LockMixin):
    def __init__(self, lock=None):
        if not lock:
            lock = RLock()
        self.lock = lock

        # We implement the condition using the Semaphore, because the Semaphore
        # embodies the non-blocking send, required to resolve the race condition
        # which would otherwise exist WRT timeouts and the nWaiting bookkeeping.
        self.sem = Semaphore(0)

        # We need bookkeeping to avoid the "missing wakeup" bug.
        self.nWaiting = 0

        # Export the lock's acquire() and release() methods
        self.acquire = lock.acquire
        self.release = lock.release

        # If the lock defines _release_save() and/or _acquire_restore(),
        # these override the default implementations (which just call
        # release() and acquire() on the lock).  Ditto for _is_owned().
        try:
            self._release_save = lock._release_save
            self._acquire_restore = lock._acquire_restore
            self._is_owned = lock._is_owned
        except AttributeError:
            pass

    def _release_save(self):
        self.lock.release()           # No state to save

    def _acquire_restore(self, x):
        self.lock.acquire()           # Ignore saved state

    def _is_owned(self):              # for Lock. RLock has its own.
        if self.lock.acquire(False):
            self.lock.release()
            return False
        else:
            return True # Crude, it could be owned by another tasklet

    def wait(self, timeout=None):
        if not self._is_owned():
            raise RuntimeError("cannot wait on un-aquired lock")
        # To avoid a "missed wakeup" we need this bookkeeping before calling
        # _release_save()
        self.nWaiting += 1
        saved = self._release_save()
        try:
            got_it = self.sem.acquire(timeout=timeout)
            if not got_it:
                self.nWaiting -= 1
        finally:
            self._acquire_restore(saved)
        return got_it

    def wait_for(self, predicate, timeout=None):
        """
        Wait until a predicate becomes true, or until an optional timeout
        elapses. Returns the last value of the predicate.
        """
        return wait_for_condition(self, predicate, timeout)

    def notify(self, n=1):
        if not self._is_owned():
            raise RuntimeError("cannot notify on un-acquired lock")
        n = min(n, self.nWaiting)
        if n > 0:
            self.nWaiting -= n
            self.sem.release(n)

    def notify_all(self):
        self.notify(self.nWaiting)
    notifyAll = notify_all


class NLCondition(LockMixin):
    """
    A special version of the Condition, useful in stackless programs.
    It does not have a lock associated with it (NL=No Lock) because tasklets
    in stackless programs often are not pre-emptable.
    """
    def __init__(self):

        self._chan = stackless.channel()
        set_channel_pref(self._chan)

    def wait(self, timeout=None):
        return lock_channel_wait(self._chan, timeout)

    def wait_for(self, predicate, timeout=None):
        """
        Wait until a predicate becomes true, or until an optional timeout
        elapses. Returns the last value of the predicate.
        """
        return wait_for_condition(self, predicate, timeout)

    def notify(self):
        with atomic():
            if self._chan.balance:
                self._chan.send(None)

    def notify_all(self):
        with atomic():
            for i in xrange(-self._chan.balance):
                #guard ourselves against premature waking of other tasklets
                if self._chan.balance:
                    self._chan.send(None)

    notifyAll = notify_all

    #no-ops for the acquire and release
    def acquire(self):
        pass
    release = acquire


class Event(object):
    def __init__(self):
        self._is_set = False
        self.chan = stackless.channel()
        set_channel_pref(self.chan)

    def is_set(self):
        return self._is_set;
    isSet = is_set

    def clear(self):
        self._is_set = False

    def wait(self, timeout=None):
        with atomic():
            if self._is_set:
                return True
            lock_channel_wait(self.chan, timeout)
            return self._is_set

    def set(self):
        with atomic():
            self._is_set = True
            for i in range(-self.chan.balance):
                if self.chan.balance:
                    self.chan.send(None)


class ValueEvent(stackless.channel):
    """
    This synchronization object wraps channels in a simpler interface
    and takes care of ensuring that any use of the channel after its
    lifetime has finished results in a custom exception being raised
    to the user, rather than the standard StopIteration they would
    otherwise get.

    set() or abort() can only be called once for each instance of this object.
    """

    def __new__(cls, timeout=None, timeoutException=None, timeoutExceptionValue=None):
        obj = super(ValueEvent, cls).__new__(cls)
        obj.timeout = timeout

        if timeout > 0.0:
            if timeoutException is None:
                timeoutException = WaitTimeoutError
                timeoutExceptionValue = "Event timed out"

            def break_wait():
                if not obj.closed:
                    obj.abort(timeoutException, timeoutExceptionValue)
            main.event_queue.push_after(break_wait, timeout)

        return obj

    def __repr__(self):
        return "<ValueEvent object at 0x%x, balance=%s, queue=%s, timeout=%s>" % (id(self), self.balance, self.queue, self.timeout)

    def set(self, value=None):
        """
        Resume all blocking tasklets by signaling or sending them 'value'.
        This function will raise an exception if the object is already signaled or aborted.
        """
        if self.closed:
            raise RuntimeError("ValueEvent object already signaled or aborted.")

        while self.queue:
            self.send(value)

        self.close()
        self.exception, self.value = RuntimeError, ("Already resumed",)

    def abort(self, exception=None, *value):
        """
        Abort all blocking tasklets by raising an exception in them.
        This function will raise an exception if the object is already signaled or aborted.
        """
        if self.closed:
            raise RuntimeError("ValueEvent object already signaled or aborted.")

        if exception is None:
            exception, value = self.exception, self.value
        else:
            self.exception, self.value = exception, value

        while self.queue:
            self.send_exception(exception, *value)

        self.close()

    def wait(self):
        """Wait for the data. If time-out occurs, an exception is raised"""
        if self.closed:
            raise self.exception(*self.value)

        return self.receive()
