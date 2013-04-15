#stacklesslib.main.py

import sys
import time
import traceback

from .base import atomic
from .base import time as elapsed_time

import stackless
try:
    import stacklessio
except ImportError:
    stacklessio = None

stacklessio = None # Disabled
asyncore = None
if not stacklessio:
    try:
        import asyncore
    except ImportError:
        pass

_sleep = getattr(time, "real_sleep", time.sleep)

# Tools for adjusting the scheduling mode.

SCHEDULING_ROUNDROBIN = 0
SCHEDULING_IMMEDIATE = 1
scheduling_mode = SCHEDULING_ROUNDROBIN


def set_scheduling_mode(mode):
    global scheduling_mode
    old = scheduling_mode
    if mode is not None:
        scheduling_mode = mode
    return old


def set_channel_pref(c):
    if scheduling_mode == SCHEDULING_ROUNDROBIN:
        c.preference = 0
    else:
        c.preference = -1


from .events import EventQueue

class LoopScheduler(object):
    """ A tasklet scheduler to be used by the loop.  Support tasklet sleeping and sleep_next operations """
    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.chan = stackless.channel()
        set_channel_pref(self.chan)

    def sleep(self, delay):
        if delay <= 0:
            c = self.chan
        else:
            c = stackless.channel()
            set_channel_pref(c)
        def wakeup():
            with atomic():
                if c.balance:
                    c.send(None)
        if delay <= 0:
            self.event_queue.call_soon(wakeup)
        else:
            self.event_queue.call_later(delay, wakeup)
        c.receive()

# A mainloop class.
# It can be subclassed to provide a better interruptable wait, for example on windows
# using the WaitForSingleObject api, to time out waiting for an event.
# If no-one wakes up the loop when IO is ready, then the max_wait_time should be made
# small accordingly.
# Applications that implement their own loops may find it sufficent to simply
# call main.pump()
class MainLoop(object):
    def __init__(self):
        self.max_wait_time = 0.01
        self.running = True
        self.break_wait = False
        self.pumps = []

        #take the app global ones.
        self.event_queue = event_queue
        self.scheduler = scheduler

    def add_pump(self, pump):
        if pump not in self.pumps:
            self.pumps.append(pump)

    def remove_pump(self, pump):
        try:
            self.pumps.remove(pump)
        except ValueError:
            pass

    def pump_pumps(self):
        for pump in self.pumps:
            pump()

    def get_wait_time(self, time):
        """ Get the waitSeconds until the next tasklet is due (0 <= waitSeconds <= delay)  """
        if stackless.runcount > 1:
            return 0.0 # If a tasklet is runnable we do not wait at all.
        next_delay = self.event_queue.due_delay()
        if next_delay:
            delay = min(self.max_wait_time, next_delay)
            delay = max(delay, 0.0)
        else:
            delay = self.max_wait_time
        return delay

    def adjust_wait_times(self, deltaSeconds):
        """ Delay the reawakening of all pending tasklets.

        This is usually done in the case that the Python runtime has not been
        able to be ticked for a period of time, and things that are waiting for
        other things to happen will be reawakened with those things having not
        happened.  Note that this is a hack, no one should _depend_ on things having happened
        after a sleep, since a sleep can end early.
        """
        self.event_queue.reschedule(deltaSeconds)

    def interruptable_wait(self, delay):
        """Wait until the next event is due.  Override this to break when IO is ready """
        try:
            if delay:
                # Sleep with 10ms granularity to allow another thread to wake us up.
                t1 = elapsed_time() + delay
                while True:
                    if self.break_wait:
                        # Ignore wakeup if there is nothing to do.
                        if not event_queue.is_due and stackless.runcount == 1:
                            self.break_wait = False
                        else:
                            break
                    now = elapsed_time()
                    remaining = t1-now
                    if remaining <= 0.0:
                        break
                    self.raw_sleep(min(remaining, 0.01))
        finally:
            self.break_wait = False

    def interrupt_wait(self):
        # If another thread wants to interrupt the mainloop, e.g. if it
        # has added IO to it.
        self.break_wait = True

    def raw_sleep(self, delay):
        _sleep(delay)

    def pump(self, run_for=0):
        """Cause tasklets to wake up.  This includes pumping registered pumps,
           the event queue and the scheduled
        """
        self.pump_pumps()
        self.event_queue.pump()

    def run_tasklets(self, run_for=0):
        """ Run runnable tasklets for as long as necessary """
        try:
            return stackless.run(run_for)
        except Exception:
            self.handle_error(sys.exc_info())

    def handle_error(self, ei):
        traceback.print_exception(*ei)

    def wait(self):
        """ Wait for the next scheduled event, or IO (if IO can notify us) """
        t = elapsed_time()
        wait_time = self.get_wait_time(t)
        if wait_time:
            self.interruptable_wait(wait_time)

    def loop(self):
        self.wait()
        self.pump()
        self.run_tasklets()

    def run(self):
        """Run until stop() gets called"""
        while self.running:
            self.loop()

    def stop(self):
        """Stop the run"""
        self.running = False

    #these two really should be part of the "App" class.
    def sleep(self, delay):
        self.scheduler.sleep(delay)

    def sleep_next(self):
        self.scheduler.sleep(0)


class SLIOMainLoop(MainLoop):
    def wait(self, delay):
        stacklessio.wait(delay)
        stacklessio.dispatch()

    def interrupt_wait(self):
        stacklessio.break_wait()

class AsyncoreMainLoop(MainLoop):
    """If we use asyncore, we use its wait function, rather than sleep"""
    def raw_sleep(self, delay):
        if not asyncore.socket_map:
            # If there are no sockets, then poll returns. Must manually sleep
            _sleep(delay)
        # Undo monkeypatching for sleep and select
        from .monkeypatch import Unpatched
        with Unpatched():
            asyncore.poll(delay)


# Convenience functions to sleep in the global scheduler.
def sleep(delay):
    mainloop.sleep(delay)
def sleep_next():
    mainloop.sleep_next()


# Use the correct main loop type.
if stacklessio:
    mainloopClass = SLIOMainLoop
elif asyncore:
    mainloopClass = AsyncoreMainLoop
else:
    mainloopClass = MainLoop
#mainloopClass = MainLoop

event_queue = EventQueue()
scheduler = LoopScheduler(event_queue)
mainloop = mainloopClass()
