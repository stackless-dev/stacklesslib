#slthread
# A replacement for the thread module for those uninformed souls that use
# "thread" instead of "threading.  Also a base unit used by
# stacklesslib.replacements.threading.py

from __future__ import absolute_import
#we want the "real" thread and threading modules to work too, so we must
#import them here before hiding them away
import traceback

import stackless

import stacklesslib.locks


class error(RuntimeError): pass


def _count():
    return Thread.thread_count


class Thread(stackless.tasklet):
    # Some tests need this
    __slots__ = ["__dict__"]
    thread_count = 0

    def __init__(self, function, args, kwargs):
        super(Thread, self).__init__(self.thread_main)
        self(function, args, kwargs)
        self.__class__.thread_count += 1

    @classmethod
    def thread_main(cls, func, args, kwargs):
        try:
            try:
                func(*args, **kwargs)
            except SystemExit:
                # Unittests raise system exit sometimes.  Evil.
                raise TaskletExit
        except Exception:
            traceback.print_exc()
        finally:
            cls.thread_count -= 1


def start_new_thread(function, args, kwargs={}):
    t = Thread(function, args, kwargs)
    return id(t)


def interrupt_main():
    # Don't know what to do here, just ignore it
    pass


def exit():
    stackless.getcurrent().kill()


def get_ident():
    return id(stackless.getcurrent())


# Provide this as a no-op.
_stack_size = 0
def stack_size(size=None):
    global _stack_size
    old = _stack_size
    if size is not None:
        _stack_size = size
    return old


def allocate_lock(self=None):
    # Need the self because this function is sometimes placed in classes
    # and then invoked as a method, by the test suite.
    return LockType()


class LockType(stacklesslib.locks.Lock):
    def locked(self):
        return self.owning != None