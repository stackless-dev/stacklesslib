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
from .base import atomic
from .util import channel_wait
from .errors import TimeoutError
from . import app


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
    except TimeoutError:
        return False


class LockMixin(object):
    def __enter__(self):
        self.acquire()
    def __exit__(self, exc, val, tb):
        self.release()


class Semaphore(LockMixin):
    def __init__(self, value=1):
        if value < 0:
            raise ValueError
        self._value = value
        self._chan = None # Created on demand if contented.

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
        if not self._chan:
            # Lazy creation of the channel
            self._chan = stackless.channel()
            set_channel_pref(self._chan)
        return False

    def release(self, count=1):
        with atomic():
            self._value += count
            self._pump()

    def _pump(self):
        if not self._chan:
            return
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
                super(Lock, self).release();

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


#Direct copy from 3.2 threading.py
class Barrier(object):
    """
    Barrier.  Useful for synchronizing a fixed number of threads
    at known synchronization points.  Threads block on 'wait()' and are
    simultaneously once they have all made that call.
    """
    def __init__(self, parties, action=None, timeout=None):
        """
        Create a barrier, initialised to 'parties' threads.
        'action' is a callable which, when supplied, will be called
        by one of the threads after they have all entered the
        barrier and just prior to releasing them all.
        If a 'timeout' is provided, it is uses as the default for
        all subsequent 'wait()' calls.
        """
        self._cond = Condition(Lock())
        self._action = action
        self._timeout = timeout
        self._parties = parties
        self._state = 0 #0 filling, 1, draining, -1 resetting, -2 broken
        self._count = 0

    def wait(self, timeout=None):
        """
        Wait for the barrier.  When the specified number of threads have
        started waiting, they are all simultaneously awoken. If an 'action'
        was provided for the barrier, one of the threads will have executed
        that callback prior to returning.
        Returns an individual index number from 0 to 'parties-1'.
        """
        if timeout is None:
            timeout = self._timeout
        with self._cond:
            self._enter() # Block while the barrier drains.
            index = self._count
            self._count += 1
            try:
                if index + 1 == self._parties:
                    # We release the barrier
                    self._release()
                else:
                    # We wait until someone releases us
                    self._wait(timeout)
                return index
            finally:
                self._count -= 1
                # Wake up any threads waiting for barrier to drain.
                self._exit()

    # Block until the barrier is ready for us, or raise an exception
    # if it is broken.
    def _enter(self):
        while self._state in (-1, 1):
            # It is draining or resetting, wait until done
            self._cond.wait()
        #see if the barrier is in a broken state
        if self._state < 0:
            raise BrokenBarrierError
        assert self._state == 0

    # Optionally run the 'action' and release the threads waiting
    # in the barrier.
    def _release(self):
        try:
            if self._action:
                self._action()
            # enter draining state
            self._state = 1
            self._cond.notify_all()
        except:
            #an exception during the _action handler.  Break and reraise
            self._break()
            raise

    # Wait in the barrier until we are relased.  Raise an exception
    # if the barrier is reset or broken.
    def _wait(self, timeout):
        if not self._cond.wait_for(lambda : self._state != 0, timeout):
            #timed out.  Break the barrier
            self._break()
            raise BrokenBarrierError
        if self._state < 0:
            raise BrokenBarrierError
        assert self._state == 1

    # If we are the last thread to exit the barrier, signal any threads
    # waiting for the barrier to drain.
    def _exit(self):
        if self._count == 0:
            if self._state in (-1, 1):
                #resetting or draining
                self._state = 0
                self._cond.notify_all()

    def reset(self):
        """
        Reset the barrier to the initial state.
        Any threads currently waiting will get the BrokenBarrier exception
        raised.
        """
        with self._cond:
            if self._count > 0:
                if self._state == 0:
                    #reset the barrier, waking up threads
                    self._state = -1
                elif self._state == -2:
                    #was broken, set it to reset state
                    #which clears when the last thread exits
                    self._state = -1
            else:
                self._state = 0
            self._cond.notify_all()

    def abort(self):
        """
        Place the barrier into a 'broken' state.
        Useful in case of error.  Any currently waiting threads and
        threads attempting to 'wait()' will have BrokenBarrierError
        raised.
        """
        with self._cond:
            self._break()

    def _break(self):
        # An internal error was detected.  The barrier is set to
        # a broken state all parties awakened.
        self._state = -2
        self._cond.notify_all()

    @property
    def parties(self):
        """
        Return the number of threads required to trip the barrier.
        """
        return self._parties

    @property
    def n_waiting(self):
        """
        Return the number of threads that are currently waiting at the barrier.
        """
        # We don't need synchronization here since this is an ephemeral result
        # anyway.  It returns the correct value in the steady state.
        if self._state == 0:
            return self._count
        return 0

    @property
    def broken(self):
        """
        Return True if the barrier is in a broken state
        """
        return self._state == -2

#exception raised by the Barrier class
class BrokenBarrierError(RuntimeError): pass

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

    def notify(self, n=1):
        with atomic():
            n = min(n, -self._chan.balance)
            for i in range(n):
                if self._chan.balance:
                    self._chan.send(None)

    def notify_all(self):
        with atomic():
            for i in xrange(-self._chan.balance):
                #guard ourselves against premature waking of other tasklets
                if self._chan.balance:
                    self._chan.send(None)

    notifyAll = notify_all

    # no-ops for the acquire and release.  They prentend to successfully lock
    def acquire(self, blocking=True, timeout=None):
        return True
    def release(self):
        pass


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
                timeoutException = TimeoutError
                timeoutExceptionValue = "Event timed out"

            def break_wait():
                if not obj.closed:
                    obj.abort(timeoutException, timeoutExceptionValue)
            app.event_queue.call_later(timeout, break_wait)

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
