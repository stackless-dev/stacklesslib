#events.py

import sys
import threading
import traceback
import heapq
from .base import time as elapsed_time


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

    def cancel_sleep(self):
        """
        Attempt to wake up any thread that is sleeping until the next event
        is due.  The default implementation does nothing.
        """
        pass

    def call_repeatedly(self, interval, callback, *args):
        """
        Cause the given callback to be called every 'interval' seconds.
        """
        time = self.time() + self.time_offset + interval
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
        return self.due_delay() <= 0.0

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
        self.cancelled = None
        self.callback = callback
        self.args = args

    def cancel(self):
        """
        exact semantics of this call are not yet defined, see
        http://www.python.org/dev/peps/pep-3156
        Currently returns True if it was successfully cancelled, False if it had already run
        """
        if self.cancelled is not None:
            try:
                self._queue._cancel(self._sequence)
            except ValueError:
                self.cancelled = False # it already ran
            else:
                self.cancelled = True
        return self.cancelled

class DummyEventQueue(object):
    """
    Instances of this class raise errors.  Use this in an application where
    there is no pumping of the event queue to detect errors
    """
    def bork(self, *args, **kwds):
        raise NotImplementedError("events are not being pumped")
    call_later = bork
    call_soon = bork
    call_repeatedly = bork
    pump = bork
