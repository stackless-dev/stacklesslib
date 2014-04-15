#base.py
"""
The most rudimentary helpers for stackless, that don't depend on
other stacklesslib functionality.  Mostly context managers
"""

import sys
import time
from contextlib import contextmanager
import stackless

__all__ = ["time", "atomic", "block_trap", "ignore_nesting", "switch_trap"]

# Get the best wallclock time to use.
if hasattr(time, "monotonic"):
    time = time.monotonic
else:
    if sys.platform == "win32":
        time = time.clock
    else:
        # Time.clock reports CPU time on unix, not good.
        time = time.time

try:
    # New versions of stackless have a fast binary implementation of this
    from stackless import atomic
except ImportError:
    @contextmanager
    def atomic():
        """a context manager to make the tasklet atomic for the duration"""
        c = stackless.getcurrent()
        old = c.set_atomic(True)
        try:
            yield
        finally:
            c.set_atomic(old)

@contextmanager
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

@contextmanager
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


def switch_trap():
    """
    A context manager to temporarily increase the switch-trap level of
    the current thread.
    """
    stackless.switch_trap(1)
    try:
        yield
    finally:
        stackless.switch_trap(-1)

class SignalChannel(stackless.channel):
    """
    A subclass of channel that is used to implement more complex entities.
    It has sender preference, so that a ``signal()`` call won't block.
    """
    __slots__ = []

    def __init__(self):
        self.preference = 1

    def signal(self, value=None):
        """send(value) if someone is waiting"""
        with atomic():
            if self.balance < 0:
                self.send(value)

    def signal_all(self, value=None):
        """send(value) for every waiting tasklet"""
        with atomic():
            for i in xrange(-self.balance):
                assert self.balance < 0
                self.send(value)

    def asignal(self):
        """send(None) if someone is waiting. Should be called while in an atomic state."""
        if self.balance < 0:
            self.send(None)

    def asignal_all(self):
        """send(None) for every waiting tasklet. Should be called while in an atomic state."""
        for i in xrange(-self.balance):
            assert self.balance < 0
            self.send(None)

