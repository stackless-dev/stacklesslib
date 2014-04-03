#monkeypatch.py
#

import sys
import threading as real_threading
import contextlib
from . import main
from .replacements import thread, threading, popen
from .app import sleep as app_sleep
from .threadpool import call_on_thread

# Use stacklessio if available
try:
    import stacklessio
except ImportError:
    stacklessio = False


# First of all, a set of helper functions to do monkey patching
@contextlib.contextmanager
def stored_modules(modules):
    """
    A context manager that stores the appropriate values of sys.modules
    and restores them afterwards
    """
    stored = []
    sentinel = object()
    for m in modules:
        mod = sys.modules.get(m, sentinel)
        stored.append((m, mod))
    try:
        yield
    finally:
        for m, mod in stored:
            if mod is not sentinel:
                sys.modules[m] = mod
            else:
                try:
                    del sys.modules[m]
                except KeyError:
                    pass

@contextlib.contextmanager
def cleared_modules(modules):
    """
    A context manager which clears the specified modules, i.e. removes them from sys.modules,
    for the duration, and then restores them to the previous state.  Useful to force the load
    of a particular module.
    """
    with stored_modules(modules):
        for m in modules:
            del sys.modules[m]
        yield

@contextlib.contextmanager
def patched(targetname, value, up=0):
    """
    Patch an attribute of a named object.  The object is found in the locals or globals
    and its attributes descended as appropriate.
    """
    frame = sys._getframe(up+1)
    parts = targetname.rsplit(".", 1)
    if len(parts) == 2:
        # at least one dot.  Copmute head . [mid] . attr
        path, attr = parts
        parts = path.split(".")
        head = parts[0]
        mid = pargs[1:]

        # find the root object
        where, obj = find_name(frame, head)
        # descend to the final part
        for s in mid:
            obj = getattr(obj, s)

        with patched_attribute(obj, attr) as old:
            yield old

    else:
        # only a single object named
        where, obj = find_name(frame, targetname)
        with patched_dict(where, targetname, object) as old:
            yield old

def find_name(frame, name):
    """
    find 'name' in a frame's locals or globals and return the corresponding
    dict along with its object
    """
    if name in frame.f_locals:
        return f_locals, f_locals[name]
    elif name in frame.f_globals:
        return f_globals, f_globals[name]
    raise NameError("name %r is not found" % name)


@contextlib.contextmanager
def patched_dictitem(d, name, object):
    """
    Patch an item in a dict
    """
    try:
        old = d[name]
    except KeyError:
        d[name] = object
        try:
            yield
        finally:
            del d[name]
    else:
        d[name] = object
        try:
            yield old
        finally:
            d[name] = old

@contextlib.contextmanager
def patched_attribute(object, attribute, new_object):
    """
    Patch an named attribute of a given object with a target object for the duration
    """
    try:
        old = getattr(object, attribute)
    except AttributeError:
        setattr(object, attribute, new_object)
        try:
            yield
        finally:
            delattr(object, attribute)
    else:
        setattr(object, attribute, new_object)
        try:
            yield old
        finally:
            setattr(object, attribute, old)

@contextlib.contextmanager
def patched_module(name, module, soft=False):
    """
    Patch a named module with an alternative module.  If the target
    module already exists, its dict is updated.  This is to sneak
    the new module into places where import has already been performed.
    """
    old = sys.modules.get(name, None)
    if not old or soft:
        sys.modules[name] = module
        try:
            yield old
        finally:
            if old:
                sys.modules[name] = old
            else:
                del sys.modules[name]
    else:
        # hard monkeypatching, trampling over old instance
        olddict = dict(old.__dict__)
        old.__dict__.clear()
        old.__dict__.update(module.__dict__)
        try:
            yield old
        finally:
            old.__dict__.clear()
            old.__dict__.update(olddict)


# helper functions to disentangle a context manager, e.g. for
# unittests
def cm_start(contextmanager):
    """
    Enter a context manager.  The result of this function should be passed
    to ``cm_stop()`` to exit the context manager.
    """
    contextmanager.__enter__()
    return contextmanager

def cm_stop(ctxt):
    """
    Leave a context manager.  Call with the return value of ``cm_start()``
    """
    ctxt.__exit__(None, None, None)


def patch_all():

    patch_misc()

    patch_thread()
    patch_threading()

    patch_select()
    patch_socket()
    patch_ssl()


def patch_misc():
    # Fudge time.sleep.
    import time
    sleep_orig = time.sleep
    time.sleep = main.sleep
    if not hasattr(time, "real_sleep"):
        time.real_sleep = sleep_orig

    # Fudge popen4 (if it exists).
    import os
    if hasattr(os, "popen4"):
        real_popen4 = os.popen4
        os.popen4 = popen.popen4
        if not hasattr(os, "real_popen4"):
            os.real_popen4 = real_popen4

def patch_thread():
    import thread as real_thread
    if not hasattr(thread, "real_thread"):
        thread.real_thread = real_thread
    sys.modules["thread"] = thread

def patch_threading():
    if not hasattr(threading, "real_threading"):
        threading.real_threading = real_threading
    sys.modules["threading"] = threading

def patch_select():
    """ Selectively choose to monkey-patch the 'select' module. """
    if stacklessio:
        from stacklessio import select
    else:
        from stacklesslib.replacements import select
    import select as real_select
    if not hasattr(select, "real_select"):
        select.real_select = real_select
    sys.modules["select"] = select

def patch_socket(will_be_pumped=True):
    """
    Selectively choose to monkey-patch the 'socket' module.

    If 'will_be_pumped' is set to False, the patched socket module will take
    care of polling networking events in a scheduled tasklet.  Otherwise, the
    controlling application is responsible for pumping these events.
    """

    if stacklessio:
        from stacklessio import _socket
        sys.modules["_socket"] = _socket
    else:
        # Fallback on the generic 'stacklesssocket' module.
        from stacklesslib.replacements import socket
        socket._sleep_func = app_sleep
        socket._schedule_func = lambda: app_sleep(0)
        if will_be_pumped:
            #We will pump it somehow.  Tell the mainloop to pump it too.
            socket.stacklesssocket_manager(None)
            socket.StopManager()
            def pump():
                with Unpatched():
                    socket.pump()
            main.mainloop.add_pump(pump)
        socket.install()

def patch_ssl():
    """
    Patch using a modified _ssl module which allows wrapping any
    Python object, not just sockets.
    """
    try:
        import _ssl
        import socket
        import errno
        from cStringIO import StringIO
    except ImportError:
        return
    from . import util

    class SocketBio(object):
        """This PyBio for the builtin SSL module implements receive buffering
           for performance"""

        default_bufsize = 8192 #read buffer size
        def __init__(self, sock, rbufsize=-1):
            self.sock = sock
            self.bufsize = self.default_bufsize if rbufsize < 0 else rbufsize
            if self.bufsize:
                self.buf = StringIO()

        def write(self, data):
            return self.wrap_errors("write", self.sock.send, (data,))

        def read(self, want):
            if self.bufsize:
                data = self.buf.read(want)
                if not data:
                    buf = self.wrap_errors("read", self.sock.recv, (self.bufsize,))
                    self.buf = StringIO(buf)
                    data = self.buf.read(want)
            else:
                data = self.wrap_errors("read", self.sock.recv, (want,))
            return data

        def wrap_errors(self, name, call, args):
            try:
                return call(*args)
            except socket.timeout:
                if self.sock.gettimeout() == 0.0:
                    return None if name=="read" else 0 #signal EWOULDBLOCK
                #create the exact same error as the _ssl module would
                raise _ssl.SSLError("The %s operation timed out" % (name,))
            except socket.error as e:
                #signal EWOULDBLOCK
                if e.errno == errno.EWOULDBLOCK:
                    return None if name=="read" else 0
                raise

        #pass on stuff to the internal sock object, so that
        #unwrapping works
        def __getattr__(self, attr):
            return getattr(self.sock, attr)

    realwrap = _ssl.sslwrap
    def wrapbio(sock, *args, **kwds):
        bio = SocketBio(sock)
        return call_on_thread(realwrap, (bio,)+args, kwds)
    _ssl.sslwrap = wrapbio

@contextlib.contextmanager
def Unpatched():
    """ A context manager that temporarily un-monkeypatches sleep and select """
    import time, select
    old = time.sleep, select.select
    new = getattr(time, "real_sleep", time.sleep), getattr(select, "real_select", select).select
    time.sleep, select.select = new
    try:
        yield
    finally:
        time.sleep, select.select = old
