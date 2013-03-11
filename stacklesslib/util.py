#util.py
import sys
import stackless
import contextlib
import weakref
import collections
from . import events
from . import basiclock


atomic = basiclock.atomic

@contextlib.contextmanager
def block_trap(trap=True):
    """
    A context manager to temporarily set the block trap state of the
    current tasklet.  Defaults to setting it to True
    """
    c = stackless.getcurrent()
    old = c.block_trap
    c.block_trap = trap
    try:
        yield
    finally:
        c.block_trap = old

@contextlib.contextmanager
def ignore_nesting(flag=True):
    """
    A context manager which allows the current tasklet to engage the
    ignoring of nesting levels.  By default pre-emptive switching can
    only happen at the top nesting level, setting this allows it to
    happen at all nesting levels.  Defaults to setting it to True.
    """
    c = stackless.getcurrent()
    old = c.set_ignore_nesting(flag)
    try:
        yield
    finally:
        c.set_ignore_nesting(old)

class local(object):
    """Tasklet local storage.  Similar to threading.local"""
    def __init__(self):
        object.__getattribute__(self, "__dict__")["_tasklets"] = weakref.WeakKeyDictionary()

    def get_dict(self):
        d = object.__getattribute__(self, "__dict__")["_tasklets"]
        try:
            a = d[stackless.getcurrent()]
        except KeyError:
            a = {}
            d[stackless.getcurrent()] = a
        return a

    def __getattribute__(self, name):
        a = object.__getattribute__(self, "get_dict")()
        if name == "__dict__":
            return a
        elif name in a:
            return a[name]
        else:
            return object.__getattribute__(self, name)


    def __setattr__(self, name, value):
        a = object.__getattribute__(self, "get_dict")()
        a[name] = value

    def __delattr__(self, name):
        a = object.__getattribute__(self, "get_dict")()
        try:
            del a[name]
        except KeyError:
            raise AttributeError(name)

class _TimeoutError(RuntimeError):
    pass

class TimeoutError(RuntimeError):
    pass

class timeout(object):
    """A timeout context manager"""
    def __init__(self, timeout=None):
        self.timeout=timeout

    def __enter__(self):
        if self.timeout is None or timeout < 0:
            return
        self.tasklet = stackless.getcurrent()
        # By necessity, this is also an "atomic" context manager.  I don't want
        # to try to solve all the race conditions possible if execution can
        # be interrupted anywhere, even within the __enter__ / __exit__ thingies.
        self.atomic = self.tasklet.set_atomic(True)
        try:
            main.event_queue.push_after(self.break_wait, self.timeout)
        except:
            self.tasklet.set_atomic(self.atomic)
            raise

    def __exit__(self, exc, val, tb):
        t = self.tasklet
        self.tasklet = None
        try:
            # Special magic.  For nested timeout calls, we don't want to invoke
            # exception handlers on inner instances.  Therefore we throw a
            # hidden exception type and only convert it to a proper timeout
            # error when we reach "us"
            if exc is _TimeoutError and val.args[0] is self:
                # our exception! Convert it to a proper TimeoutError
                raise TimeoutError, None, tb
        finally:
            t.set_atomic(self.atomic)

    def break_wait(self):
        with atomic():
            if self.tasklet and self.tasklet.blocked:
                self.tasklet.raise_exception(_TimeoutError(self))

def blocking_op_timeout(function, args, timeout):
    """Perform a blocking operation with a timeout"""
    if timeout is None or timeout < 0:
        return function(*args)

    with timeout(timeout):
        return function(args)

# Three helper functions for channels, or other objects that support
# send and receive operations, adding a timeout to these operations.

def send(chan, data, timeout=None):
    """Perform a "send" operation with a timeout"""
    return blocking_op_timeout(chan.send, (data,), timeout)

def receive(chan, timeout=None):
    """Perform a "receive" operation with a timeout"""
    return blocking_op_timeout(chan.receive, (), timeout)

def send_throw(channel, exc, val=None, tb=None, timeout=None):
    """send exceptions over a channel.  Has the same semantics
       for exc, val and tb as the raise statement has.  Use this
       for backwards compatibility with versions of stackless that
       don't have the "send_throw" method on channels.
    """
    if hasattr(channel, "send_throw"):
        return blocking_op_timeout(channel.send_throw, (exc, val, tb), timeout)

    #currently, channel.send_exception allows only (type, arg1, ...)
    #and can"t cope with tb
    if exc is None:
        if val is None:
            val = sys.exc_info()[1]
        exc = val.__class__
    elif val is None:
        if isinstance(type, exc):
            exc, val = exc, ()
        else:
            exc, val = exc.__class__, exc
    if not isinstance(val, tuple):
        val = val.args
    blocking_op_timeout(channel.send_exception, (exc, *val), timeout)

class qchannel(stackless.channel):
    """
    A qchannel is like a channel except that it contains a queue, so that the
    sender never blocks.  If there isn't a blocked tasklet waiting for the data,
    the data is queued up internally.  The sender always continues.
    """
    def __init__(self):
        self.data_queue = collections.deque()
        self.preference = 1 #sender never blocks

    @property
    def balance(self):
        if self.data_queue:
            return len(self.data_queue)
        return super(qchannel, self).balance

    def send(self, data):
        sup = super(qchannel, self)
        with atomic():
            if sup.balance >= 0 and not sup.closing:
                self.data_queue.append((True, data))
            else:
                sup.send(data)

    def send_exception(self, exc, *args):
        self.send_throw(exc, args)

    def send_throw(self, exc, value=None, tb=None):
        """call with similar arguments as raise keyword"""
        sup = super(qchannel, self)
        with atomic():
            if sup.balance >= 0 and not sup.closing:
                self.data_queue.append((False, (exc, value, tb)))
            else:
                #deal with channel.send_exception signature
                send_throw(sup, exc, value, tb)

    def receive(self):
        with atomic():
            if not self.data_queue:
                return super(qchannel, self).receive()
            ok, data = self.data_queue.popleft()
            if ok:
                return data
            exc, value, tb = data
            try:
                raise exc, value, tb
            finally:
                tb = None

    #iterator protocol
    def send_sequence(self, sequence):
        for i in sequence:
            self.send(i)

    def __next__(self):
        return self.receive()

class result(qchannel):
    """a result is a single-use qchannel.  It is useful to send a single
       result from one tasklet or the other.
    """
    def receive(self):
        with atomic():
            try:
                return super(result, self).receive()
            finally:
                self.close()

    def send(self, value):
        try:
            super(result, self).send(value)
            self.close()
        except StopIteration:
            #the listener has stopped listening.
            pass

    def send_throw(self, exc, value=None, tb=None):
        try:
            super(result, self).send_throw(exc, value, tb)
            self.close()
        except StopIteration:
            #the listener has stopped listening.
            pass

def call_async(dispatcher, function, args=(), kwargs={}, timeout=None, timeout_exception=WaitTimeoutError):
    """Run the given function on a different tasklet and return the result.
       'dispatcher' must be a callable which, when called with with
       (func, args, kwargs), causes asynchronous execution of the function to commence.
       If a result isn't received within an optional time limit, a 'timeout_exception' is raised.
    """
    chan = result()
    def helper():
            try:
                result = function(*args, **kwargs)
            except Exception:
                chan.send_throw(*sys.exc_info())
            else:
                chan.send(result)
            main.mainloop.interrupt_wait() # in case we are on a different thread.

    # submit the helper to the dispatcher
    dispatcher(helper)
    # wait for the result
    return channel_wait(chan, timeout)
