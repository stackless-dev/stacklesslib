# A greenlet emulation module using tealets
import weakref
import sys

import stackless
from stacklesslib.base import atomic

__version__ = "0.3.2"

class Scheduler(object):
    """A scheduler that switches between tasklets like greenlets"""
    def __init__(self):
        self.prev = self.value = None

    def create(self, function):
        """Create a new tasklet, bound to a function that will return a tuple
           on exit: (t, v)
           t = the target tasklet to switch to, and v the value to provide
        """
        def top(args, kwargs):
            self._start(function, args, kwargs)
        return stackless.tasklet(top)

    def start(self, t, args=(), kwargs={}):
        """Start a context previously created"""
        prev = stackless.getcurrent()
        # NOTE: Disable this test to enable a weird crash in stackless when running the greenlet unittests
        if t.thread_id != prev.thread_id:
            raise error("can't switch to a different thread")
        # print "start %s %s" %(id(prev), id(t))
        with atomic():
            t(args, kwargs)
            self.prev = prev
            t.run()
            return self._return()

    def _return(self):
        if self.prev is not None:
            self.prev.remove()
            self.prev = None
        value, self.value = self.value, None
        return value

    def _start(self, function, args, kwargs):
        self.prev.remove()
        self.prev = None
        r, v = function(*args, **kwargs)
        #r is the tasklet to switch back to, v is the value passed to it
        # print "end %s %s" % ( id(stackless.getcurrent()), id(r) )
        with atomic():
            self.value = v
            self.previous = None # the target does not remove us
            r.run()

    def switch(self, target, value=None):
        prev = stackless.getcurrent()
        # print "sw %s %s" % (id(prev), id(target))
        if prev == target:
            return value
        if target.thread_id != prev.thread_id:
            raise error("can't switch to a different thread")
        with atomic():
            self.prev = prev
            self.value = value
            target.run()
            assert self.prev != prev
            return self._return()

class NewScheduler(Scheduler):
    """A version of the above for use when tasklets support the .switch method"""
    def start(self, t, args=(), kwargs={}):
        """Start a context previously created"""
        prev = stackless.getcurrent()
        # NOTE: Disable this test to enable a weird crash in stackless when running the greenlet unittests
        if t.thread_id != prev.thread_id:
            raise error("can't switch to a different thread")
        # print "start %s %s" %(id(prev), id(t))
        with atomic():
            t(args, kwargs)
            t.switch()
            return self._return()

    def _return(self):
        value, self.value = self.value, None
        return value

    def _start(self, function, args, kwargs):
        r, v = function(*args, **kwargs)
        #r is the tasklet to switch back to, v is the value passed to it
        # print "end %s %s" % ( id(stackless.getcurrent()), id(r) )
        with atomic():
            self.value = v
            r.run() #no switch, let this tasklet continue to end

    def switch(self, target, value=None):
        prev = stackless.getcurrent()
        # print "sw %s %s" % (id(prev), id(target))
        if prev == target:
            return value
        if target.thread_id != prev.thread_id:
            raise error("can't switch to a different thread")
        with atomic():
            self.value = value
            target.switch()
            return self._return()

if hasattr(stackless.tasklet, "switch"):
    Scheduler = NewScheduler


class error(Exception):
    pass

class GreenletExit(BaseException):
    pass

class ErrorWrapper(object):
    def __enter__(self):
        pass
    def __exit__(self, tp, val, tb):
        if tp:
            if isinstance(val, TaskletExit):
                raise GreenletExit(*val.args), None, tb
            if type(val) is RuntimeError: #the error stackless raises
                raise error, val, tb
ErrorWrapper = ErrorWrapper() # stateless singleton

taskletmap = weakref.WeakValueDictionary()
scheduler = Scheduler()

def _getmain():
    return _lookup(stackless.main())

def getcurrent():
    return _lookup(stackless.getcurrent())

def _lookup(s):
    try:
        return taskletmap[s]
    except KeyError:
        return greenlet(parent=s)

class greenlet(object):
    def __init__(self, run=None, parent=None):
        # must create it on this thread, not dynamically when run
        # this will bind it to the right thread
        self.dead = False
        if run is not None:
            self.run = run
        if isinstance(parent, stackless.tasklet):
            # we were called by getcurrent.  We are dynamically generated, e.g. the
            # main tealet for the thread.
            self._started = True
            self._tasklet = parent
            if parent.is_main:
                # print "creating main for ", id(parent)
                # the main tasklet's greenlet
                self.parent = self # main greenlets are their own parents and don't go away
                self._main = self
                self._garbage = []
            else:
                # print "creating stooge for", id(parent)
                self.parent = self._main =_getmain()
        else:
            # regular greenlet started to run a function
            self._tasklet = scheduler.create(self._greenlet_main)
            if parent is None:
                parent = getcurrent()
            self.parent = parent
            self._main = parent._main
            self._started = False
            # perform housekeeping
            del self._main._garbage[:]
        taskletmap[self._tasklet] = self

    def __del__(self):
        if self:
            if stackless.getcurrent() == self._tasklet:
                # Can't kill ourselves from here
                return
            taskletmap[self._tasklet] = self # re-insert
            old = self.parent
            self.parent = getcurrent()
            try:
                self.throw()
            except error:
                # This must be a foreign tealet.  Insert it to
                # it's main tealet's garbage heap
                self._main._garbage.append(self)
            finally:
                self.parent = old

    @property
    def gr_frame(self):
        if self._tasklet is stackless.getcurrent():
            return self._tasklet.frame
        # tealet is paused.  Emulated greenlet by returning
        # the frame which called "switch" or "throw"
        f = self._tasklet.frame
        try:
            return f.f_back.f_back.f_back
        except AttributeError:
            return None

    def __nonzero__(self):
        return  self._started and not self.dead

    def switch(self, *args, **kwds):
        return self._Result(self._switch((False, args, kwds)))

    def throw(self, t=None, v=None, tb=None):
        if not t:
            t = GreenletExit
        return self._Result(self._switch((t, v, tb)))

    def _switch(self, arg):
        with ErrorWrapper:
            if not self._started:
                run = self.run
                try:
                    del self.run
                except AttributeError:
                    pass # it was probably a subclass with a run method
                self._started = True
                return scheduler.start(self._tasklet, ((run, arg),))
            else:
                if not self:
                    # target is dead, switch to its next alive parent
                    return scheduler.switch(self._parent()._tasklet, arg)
                return scheduler.switch(self._tasklet, arg)
        return self._Result(arg)

    @staticmethod
    def _Result(arg):
        """Convert the switch args into a single return value or raise exception"""
        # The return value is stored in the current greenlet.
        err, args, kwds = arg
        # most common case first
        if not err:
            if not kwds:
                if len(args) == 1:
                    return args[0]
                elif not args:
                    return None
                return args
            if args:
                return (args, kwds)
            return kwds
        else:
            raise err, args, kwds

    @staticmethod
    def _greenlet_main(arg):
        run, (err, args, kwds) = arg
        try:
            if not err:
                #result = _tealet.hide_frame(run, args, kwds)
                result = run(*args, **kwds)
                arg = (False, (result,), None)
            else:
                raise err, args, kwds
        except GreenletExit as e:
            arg = (False, (e,), None)
        except:
            arg = sys.exc_info()
        c = getcurrent()
        c.dead = True
        p = c._parent()
        #whom do we switch to?
        return p._tasklet, arg

    def _parent(self):
        # Find the closest parent alive
        p = self.parent
        while not p:
            p = p.parent
        return p

    getcurrent = staticmethod(getcurrent)
    error = error
    GreenletExit = GreenletExit
