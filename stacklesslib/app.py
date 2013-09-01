#app.py
"""
This module contains a collection of classes and functions that are generally
swapped out to a different implementation if an application is running under
the stackless-framework.  It is intended for things such as sleep(), whose
Stackless-implementation won't work unless the framework is being ticket.
Contrast this with stacklesslib.locks.Lock() which also works as a normal
thread locking primitive.
The replacement is indirected, so that client code can bind directly
to the functions here, e.g. use "from stacklesslib.app import sleep"
"""

import time
import threading
from . import events
from .base import get_channel, atomic


class _SleepHandler(object):
    """
    A class to support sleep functionality
    """
    def __init__(self):
        self.chan = get_channel()

    def sleep(self, delay):
        if delay <= 0:
            c = self.chan
        else:
            c = get_channel()
        def wakeup():
            with atomic():
                if c.balance:
                    c.send(None)
        if delay <= 0:
            _event_queue.call_soon(wakeup)
        else:
            _event_queue.call_later(delay, wakeup)
        c.receive()


class _ObjProxy(object):
    def __init__(self, name):
        self._name = name
    def __getattr__(self, attr):
        return getattr(globals()[self._name], attr)

def sleep(delay):
    _sleep(delay)

def Event():
    return _Event()

def Lock():
    return _Lock()

def RLock():
    return _Rlock()

def Condition():
    return _Condition()

def Semaphore():
    return _Semaphore()

event_queue = _ObjProxy("_event_queue")


def install_vanilla():
    """
    Set up the globals to use default thread-blocking features
    """
    g = globals()
    g["_sleep"] = time.sleep
    g["_Event"] = threading.Event
    g["_Lock"] = threading.Lock
    g["_Rlock"] = threading.RLock
    g["_Condition"] = threading.Condition
    g["_Semaphore"] = threading.Semaphore
    g["_event_queue"] = events.DummyEventQueue() # Use the dummy instance which raises an error


def install_stackless():
    """
    Set up the globals for a functioning event event loop
    """
    # import those here, to avoid circular dependencies at import time.
    from . import locks
    from . import main
    g = globals()
    g["_sleep"] = _SleepHandler().sleep
    g["_Event"] = locks.Event
    g["_Lock"] = locks.Lock
    g["_Rlock"] = locks.RLock
    g["_Condition"] = locks.Condition
    g["_Semaphore"] = locks.Semaphore
    g["_event_queue"] = main.event_queue # use the instance from the main

# Run in non-stackless mode until told differently
install_vanilla()
