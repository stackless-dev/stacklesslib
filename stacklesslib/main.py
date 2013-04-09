#stacklesslib.main.py

import heapq
import sys
import time
import traceback
import threading

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

# Get the best wallclock time to use.
if sys.platform == "win32":
    elapsed_time = time.clock
else:
    # Time.clock reports CPU time on unix, not good.
    elapsed_time = time.time

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


# A event queue class.
class EventQueue(object):
    """
    This class manages future events.  Its scheduling functions have an interface that
    match PEP 3156: http://www.python.org/dev/peps/pep-3156/#event-loop-interface
    e.g. call_soon, call_later, etc.
    """
    def __init__(self):
        self.queue = [] # A heapq for events
        self.time_offset = 0 # time offset for scheduling
        self.sequence = 0 # unique index
        self.lock = threading.Lock()

    def __len__(self):
        return len(self.queue)

    @staticmethod
    def time():
        return elapsed_time()

    def __len__(self):
        return len(self.queue)

    def reschedule(self, delta_t):
        """
        Apply a delta-t to all existing timed events
        """
        self.time_offset -= delta_t


    def call_soon(self, callback, *args):
        """
        Cause the given callback to be performed as soon as possible
        """
        # -1 is a special time value
        return self._call_at(-1, -1, callback, args)

    def call_later(self, delay, callback, *args):
        """
        Cause the given callback to be scheduled for call after 'delay' seconds
        """
        time = self.time() + self.time_offset + delay
        return self._call_at(time, -1, callback, args)

    def call_later_threadsafe(self, delay, callback, *args):
        """
        Cause the given callback to be scheduled for call after 'delay' seconds
        """
        result = self.call_later(delay, callback, *args)
        self.cancel_sleep()
        return result

    def call_repeatedly(self, interval, callback, *args):
        """
        Cause the given callback to be called every 'interval' seconds.
        """
        time = self.time() + self.offset + interval
        return self._call_at(time, interval, callback, args)

    def _call_at(self, when, interval, callback, args):
        #print self.time(), (when, interval, callback, args)
        with self.lock:
            sequence = self.sequence
            self.sequence += 1
            # s is a disambiguator for equal deadlines.
            entry = (when, sequence, interval, callback, args)
            heapq.heappush(self.queue, entry)
        return Handle(self, sequence, callback, args)

    def _cancel(self, sequence):
        """
        Cancel an event that has been submitted.  Raise ValueError if it isn't there.
        """
        # Note, there is no way currently to ensure that either the event was
        # removed or successfully executed, i.e. no synchronization.
        # Caveat Emptor.
        with self.lock:
            for i, e in enumerate(self.queue):
                if e[1] == sequence:
                    del self.queue[i]
                    heapq.heapify(self.queue) #heapq has no "remove" method
                    return
        raise ValueError("event not in queue")

    def pump(self):
        """
        The worker function for the main loop to process events in the queue
        """
        # produce a batch of events to perform this time.  This makes sure
        # that new events created don't add to our job, thus making the loop
        # infinite.
        now = self.time() + self.time_offset
        batch = []
        with self.lock:
            while self.queue:
                t = self.queue[0][0]
                if t < 0.0 or t <= now:
                    batch.append(heapq.heappop(self.queue))
                else:
                    break

        # Run the events
        for event in batch:
            if event[2] >= 0.0:
                # reschedule a repeated event with the same sequence id
                with self.lock:
                    entry = (now + event[2], ) + event[1:]
                    heapq.heappush(self.queue, entry)
            try:
                event[3](*event[4]) # callback(*args)
            except Exception:
                self.handle_exception(sys.exc_info())
        return len(batch)

    @property
    def is_due(self):
        """Returns true if the queue needs pumping now."""
        return self.due_delay <= 0.0

    def due_delay(self):
        """delay in seconds until the next event, or None"""
        with self.lock:
            if self.queue:
                t = self.queue[0][0]
                if t < 0:
                    return 0.0
                now = self.time() + self.time_offset
                return max(0.0, t-now)
        return None

    def handle_exception(self, exc_info):
        traceback.print_exception(*exc_info)

class Handle(object):
    """
    This object represents a cancelable event from the EventQueue.
    See http://www.python.org/dev/peps/pep-3156
    """
    def __init__(self, queue, sequence, callback, args):
        self._queue = queue
        self._sequence = sequence
        # public attributes
        self.canceled = False
        self.callback = callback
        self.args = args

    def cancel(self):
        """
        exact semantics of this call are not yet defined, see
        http://www.python.org/dev/peps/pep-3156
        """
        if not self.canceled:
            self._queue._cancel(self._sequence)
            self.canceled = True


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

event_queue = EventQueue()
scheduler = LoopScheduler(event_queue)
mainloop = mainloopClass()
