#basiclock.py
import contextlib
import stackless

"""
This is a basic lock that provides no frills.
It is provided here to resolve dependency issues, since it does not
provide any timeout
"""

@contextlib.contextmanager
def atomic():
    """a context manager to make the tasklet atomic for the duration"""
    c = stackless.getcurrent()
    old = c.set_atomic(True)
    try:
        yield
    finally:
        c.set_atomic(old)

class LockMixin(object):
    def __enter__(self):
        self.acquire()
    def __exit__(self, exc, val, tb):
        self.release()

class BasicLock(LockMixin):
    def __init__(self):
        self._value = 1
        self._chan = stackless.channel()
        self._chan.preference = 0
        
    def acquire(self,):
        with atomic():
            while True:
            	if self._value == 1:
            		self._value = 0
            		return True
                try:
                    self._chan.receive()
                except:
                    self._pump()
                    raise

    def release(self, count=1):
        with atomic():
            self._value = 1
            self._pump()

    def _pump(self):
        for i in xrange(min(self._value, -self._chan.balance)):
            if self._chan.balance:
                self._chan.send(None)
